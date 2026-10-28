import { isSafeCount } from "./number-utils.ts";
import { open, readdir } from "node:fs/promises";
import path from "node:path";
import { containsLogicalPath } from "./path-utils.ts";
import { type DependencyRole, type ProvenanceTaint, type ResourceTransitionKind } from "./provenance-certificate.ts";
import { FILESYSTEM_OBSERVATION_FIELDS, type FilesystemObservationField, filesystemObservationDigest } from "./filesystem-evidence.ts";
import { type Sha256Digest } from "./stable-value-hash.ts";

const CONFINEMENT_SENSITIVE_SYSCALLS = new Set([
	"seccomp", "capget", "capset", "mount", "umount2", "pivot_root", "swapon", "swapoff", "reboot",
	"sethostname", "setdomainname", "kexec_load", "init_module", "finit_module", "delete_module", "unshare", "setns",
	"perf_event_open", "bpf", "userfaultfd", "keyctl", "add_key", "request_key", "ptrace", "process_vm_readv",
	"process_vm_writev", "open_by_handle_at", "name_to_handle_at", "quotactl", "acct", "lookup_dcookie",
	"io_setup", "io_submit", "io_uring_setup", "io_uring_enter", "io_uring_register", "personality",
]);
const SYSCALL_FILTER = `trace=%file,%process,%network,%ipc,%desc,getpid,getppid,getsid,getpgid,clock_gettime,gettimeofday,time,getrandom,sysinfo,times,getrusage,getrlimit,setrlimit,prlimit64,prctl,mmap,${[...CONFINEMENT_SENSITIVE_SYSCALLS].join(",")}`;

/** One production trace shape shared by execution and dependency-ablation paths. */
export function straceCommand(
	strace: string,
	tracePrefix: string,
	command: readonly string[],
	streams = false,
	continuation = false,
): readonly [string, ...string[]] {
	// Its sandbox launcher's threads would pay a ptrace stop per call they serve; a file's bytes are captured from the filesystem, a stream's only here.
	return [strace, "--kill-on-exit", "--trace-children-only", "--elide-path-data", streams ? "-f" : "-ff", "-q", "-ttt", "-yy", "-v", "-s", "65535", "-e", continuation ? "trace=all" : SYSCALL_FILTER, "-o", streams ? `${tracePrefix}.stream` : tracePrefix, ...command];
}

export type ObservedProcessPath =
	/** `listed` when its entries reached the run: it read them, or a removal or rename found some. */
	| { readonly path: string; readonly role: DependencyRole; readonly listed?: true;
		/** Uniform non-writing lookup error; the dependency walker must independently prove the failing component. */
		readonly lookupFailure?: "ENOENT" | "ENOTDIR" }
	| ({ readonly path: string; readonly role: "metadata"; readonly followSymlinks: boolean; readonly afterWrite?: true } & StatObservation);

export interface StraceObservation {
	readonly complete: boolean;
	readonly paths: readonly ObservedProcessPath[];
	readonly taints: readonly ProvenanceTaint[];
	readonly tracedProcesses: number;
	/** Every process its trace holds, a brokered run's launcher among them. */
	readonly pids?: readonly number[];
	readonly incompleteReasons: readonly string[];
	/** Conflicting observations require an enclosing transaction to prove the private object vanished. */
	readonly metadataChanges?: readonly string[];
	/** Bounded diagnostic names only; never dependency evidence or a completeness condition. */
	readonly unsupportedSyscalls?: readonly string[];
	readonly unsupportedSyscallsTruncated?: true;
	readonly resourceJournal?: readonly { readonly inode: string; readonly description?: number; readonly kind: ResourceTransitionKind; readonly data: Buffer; readonly requested?: number }[];
	readonly retainedDescriptions?: readonly number[];
	readonly finalHandles?: readonly { readonly fd: number; readonly description?: number; readonly cloexec: boolean }[];
	/** Workspace paths, relative to it, the traced processes created, removed, renamed or opened to write. */
	readonly written?: readonly string[];
	/** Paths outside the workspace it wrote, which a private branch took. */
	readonly external?: readonly string[];
	/** Locks it took without waiting, or found free: each holds while no one else holds one. */
	readonly locks?: readonly { readonly path: string; readonly exclusive: boolean }[];
}

export interface StraceObservationOptions {
	/** Exact root launch alias; the prototype separately binds the installed executable bytes. */
	readonly launchPath?: string;
	/** Writes owned by a brokered launcher and its descendants: relative workspace names or absolute external paths. */
	readonly brokeredWrites?: (pid: number) => readonly string[] | undefined;
	/** The session's private branch, where a descriptor on a copy shows the copy: it stands for the host path it shadows. */
	readonly privateUpper?: string;
	/** Bounded prefix lookup for running work; its evidence is always incomplete. */
	readonly previewBytes?: number;
	/** The owned tracer sealed this complete prefix before the pending syscall's entry, while capturing the stopped target. */
	readonly frozen?: { readonly pid: number; readonly bytes: number };
	/**
	 * Workspace roots whose driver-specific unsupported errors must invalidate adoption. This keeps
	 * a COW substrate from changing a command result when the Actor filesystem supports the syscall.
	 */
	readonly guardFilesystemSemanticsWithin?: readonly string[];
	/** Private input images whose inherited OFD flags are reproduced and sealed by the caller. */
	readonly inheritedFileImages?: readonly string[];
	/** Exact kernel entry/cookie images served by the existing syscall broker. */
	readonly inheritedDirectoryImages?: readonly string[];
	readonly inheritedStreams?: readonly string[];
	/** The capture sockets serving the traced command's output (`socket:[inode]`): naming them reveals no outside peer. */
	readonly outputEndpoints?: readonly string[];
	/** Private relay pipes whose retained writer OFDs proved unchanged status after all descendants exited. */
	readonly unchangedOutputPipes?: readonly string[];
	readonly inheritedHandles?: readonly { readonly fd: number; readonly installed?: false; readonly description?: number; readonly inode: string; readonly flags: number; readonly outside: number; readonly queuedBytes?: number; readonly queueData?: Buffer; readonly packet?: boolean; readonly messages?: readonly import("./linux-held-exec.ts").QueueMessage[] }[];
}

interface TraceFile { readonly pid: number; readonly lines: readonly TraceLine[]; }

type TraceRoot = TraceFile & { readonly start: number };
interface TraceProcess extends TraceRoot { readonly cwd: string | undefined; readonly fs: { shared: boolean; changed: boolean }; }
interface TraceLine {
	readonly order?: number;
	readonly name: string;
	readonly args: readonly string[];
	readonly result: string;
	readonly failure?: string;
}
const TRACE_DELIMITERS: Readonly<Record<string, string>> = { "(": ")", "[": "]", "{": "}", "<": ">" }, QUOTED_RUN = /(?:[^"\\]|\\.)+/ys, ANNOTATION_RUN = /(?:[^<>\\]|\\.)+/ys, PLAIN_RUN = /[^"\\()[\]{}<>,/]+/y;

/** Delimit once: quoted paths and descriptor annotations are data, never syscall syntax; what means nothing where it stands is passed whole. */
function parseTraceLine(line: string): TraceLine {
	if (/^\+\+\+ (?:exited with \d+|killed by SIG[A-Z0-9]+(?: \(core dumped\))?) \+\+\+\s*$/.test(line)) return { name: "terminated", args: [], result: "0" };
	const head = /^\s*([a-zA-Z0-9_]+)\(/.exec(line);
	const empty = { name: head?.[1] ?? "", args: [], result: "" };
	if (!head && (!line.trim() || /^(?:\+\+\+|---)/.test(line))) return empty;
	const failure = { ...empty, failure: `syscall_unparsed:${empty.name}` };
	if (!head) return failure;
	const args: string[] = [], stack: string[] = [];
	let start = head[0].length, quoted = false;
	for (let index = start; index < line.length; index++) {
		const character = line[index]!;
		const context = stack.at(-1);
		if (character === "\\" && (quoted || context?.endsWith(">"))) { index++; continue; }
		const run = quoted ? QUOTED_RUN : context?.endsWith(">") ? ANNOTATION_RUN : PLAIN_RUN;
		run.lastIndex = index;
		if (run.test(line)) index = run.lastIndex - 1;
		if (quoted) { if (character === '"') quoted = false; continue; }
		// -yy sockets use -> for peers; filesystem paths escape literal angle brackets.
		if (context?.endsWith(">")) {
			if (character === "<") stack.push(">");
			if (character === ">" && (context === "/>" || line[index - 1] !== "-")) stack.pop();
			continue;
		}
		if (character === '"') { quoted = true; continue; }
		if (character === "/" && line[index + 1] === "*") {
			const end = line.indexOf("*/", index + 2);
			if (end < 0) return failure;
			index = end + 1;
			continue;
		}
		const closing = TRACE_DELIMITERS[character];
		if (closing) { stack.push(character === "<" && line[index + 1] === "/" ? "/>" : closing); continue; }
		if (character === ")" && !stack.length) {
			const result = /^\s+=\s+(.*)$/.exec(line.slice(index + 1))?.[1];
			if (!result) return failure;
			if (index > start || args.length) args.push(line.slice(start, index).trim());
			return { name: head[1]!, args, result };
		}
		if (")]}".includes(character)) {
			if (stack.pop() !== character) return failure;
		} else if (character === "," && !stack.length) { args.push(line.slice(start, index).trim()); start = index + 1; }
	}
	return failure;
}

type TraceRecord = TraceLine & { readonly pid: number; readonly at: number; readonly text: string };

function appendGrouped<Key, Value>(groups: Map<Key, Value[]>, key: Key, value: Value): void {
	(groups.get(key) ?? groups.set(key, []).get(key)!).push(value);
}

/** One decoder for complete transcripts and growing tails; unfinished calls retain their entry time across reads. */
function traceDecoder() {
	const pending = new Map<string, { text: string; pid: number; at: number; order?: number }[]>(), times = new Map<number, number>();
	let sequence = 0;
	return (contents: string, filePID?: number, growing = false) => {
		const rows = contents.split(/\r?\n/), complete = new Map<number, TraceRecord[]>();
		if (filePID !== undefined) complete.set(filePID, []);
		for (const [index, raw] of rows.entries()) {
			const order = filePID === undefined ? sequence++ : undefined;
			if (!raw.trim()) continue;
			const process = filePID === undefined ? /^(\d+)\s+(.*)$/.exec(raw) : [raw, String(filePID), raw];
			if (!process) throw new Error("invalid ordered trace record");
			const stamp = /^(\d+\.\d+) (.*)$/.exec(process[2]!), text = stamp?.[2] ?? process[2]!;
			const current = { pid: Number(process[1]), at: stamp ? Number(stamp[1]) * 1000 : NaN, order };
			const fragment = growing && index === rows.length - 1;
			current.at = Math.max(times.get(current.pid) ?? 0, current.at); times.set(current.pid, current.at);
			const unfinished = /^\s*([a-zA-Z0-9_]+)\(.*\s<unfinished \.\.\.>\s*$/.exec(text), resumed = /^\s*<\.\.\.\s*([a-zA-Z0-9_]+) resumed>(.*)$/.exec(text);
			const name = unfinished?.[1] ?? resumed?.[1], key = `${current.pid}:${name}`;
			if (unfinished && !fragment) {
				appendGrouped(pending, key, { ...current, text: text.replace(/\s*<unfinished \.\.\.>\s*$/, "") });
				continue;
			}
			const prefix = !fragment && resumed && pending.get(key)?.shift();
			if (pending.get(key)?.length === 0) pending.delete(key);
			const line = resumed ? prefix ? parseTraceLine(`${prefix.text}${resumed[2]}`)
				: { name: name!, args: [], result: "", failure: `resumed_without_unfinished:${key}` } : parseTraceLine(text);
			appendGrouped(complete, current.pid, { ...current, ...line, ...(fragment ? { failure: "partial" } : {}), text, ...(prefix ? { at: prefix.at,
				order: /^(?:write|writev|sendto|sendmsg|fork|vfork|clone|clone3)$/.test(name!) ? prefix.order : order } : {}) });
		}
		for (const [key, calls] of pending) appendGrouped(complete, calls[0]!.pid, { ...calls[0]!, name: key.slice(key.indexOf(":") + 1), args: [], result: "", failure: `unfinished:${key}` });
		return complete;
	};
}

/** Reduce handle lifetimes to object releases. Aliases and CLONE_FILES share references;
 * fork copies them. This is the same journal used for queue effects, not another replay path. */
function resourceTransitions(processes: ReadonlyMap<number, TraceProcess>, root: number, options: StraceObservationOptions) {
	const handles = options.inheritedHandles ?? [];
	type Handle = { object: Resource; description?: number; access: number; cloexec: boolean; packet?: boolean; epoll?: Map<string, { handle: Handle; mask: number; data: string; armed: boolean; generation?: number }> };
	type Resource = { inode: string; outside?: number; peer?: Resource; generation: number; read: number; end?: number; content?: Buffer };
	const resources = new Map<string, Resource>();
	const descriptions = new Map<number, Handle>();
	const resource = (inode: string): Resource => {
		let state = resources.get(inode);
		if (!state) resources.set(inode, state = { inode, generation: 0, read: 0 });
		return state;
	};
	for (const handle of handles) {
		const state = resource(handle.inode);
		state.outside = handle.outside; state.end = handle.queuedBytes ?? 0;
		if (handle.queueData) state.content = handle.queueData;
		descriptions.set(handle.fd, { object: state, description: handle.description ?? handle.fd, access: (handle.flags & 3) + 1, cloexec: false, packet: handle.packet });
	}
	const tables = new Map<number, Map<number, Handle>>([[root, new Map(handles.filter(handle => handle.installed !== false)
		.map(handle => [handle.fd, descriptions.get(handle.fd)!]))]]);
	const journal: Array<NonNullable<StraceObservation["resourceJournal"]>[number] & { order: number }> = [];
	const handled = new Set<TraceLine>();
	const messages: Array<{ object: Resource | undefined; start: number; end: number; rights: Handle[] }> = handles.flatMap(handle => (handle.messages ?? []).map(message => ({
		object: resource(handle.inode), start: message.start, end: message.end, rights: message.rights.map(fd => {
			const reference = descriptions.get(fd);
			if (!reference) throw new Error("unproven queued descriptor");
			return reference;
		}),
	})));
	const wake = (object: Resource | undefined) => { if (object) object.generation++; };
	const append = (object: Resource | undefined, data: Buffer) => {
		if (!object) return;
		if (object.content) object.content = Buffer.concat([object.content, data]);
		if (object.end !== undefined) object.end += data.length;
		wake(object);
	};
	const references = () => {
		const masks = new Map<Resource, number>(), descriptions = new Map<number | undefined, Handle>();
		for (const handle of [...new Set(tables.values())].flatMap(table => [...table.values()]).concat(messages.flatMap(message => message.rights))) {
			masks.set(handle.object, (masks.get(handle.object) ?? 0) | handle.access);
			descriptions.set(handle.description, handle);
		}
		return { masks, descriptions };
	};
	const records = [...processes].flatMap(([pid, process]) => process.lines.slice(process.start).map(line => ({ pid, line })))
		.sort((a, b) => (a.line.order ?? Infinity) - (b.line.order ?? Infinity));
	for (const { pid, line } of records) {
		let table = tables.get(pid);
		if (!table || line.order === undefined) continue;
		const lifetime = /^(?:close|close_range|dup|dup2|dup3|fcntl|fcntl64|execve|execveat|terminated)$/.test(line.name);
		let before = lifetime || messages.length && /^(?:read|readv|recvfrom|recvmsg)$/.test(line.name) ? references() : undefined;
		const endpoint = (argument: string | undefined): Handle | undefined => {
			const file = table!.get(Number.parseInt(argument ?? "", 10));
			if (file?.object.inode.startsWith("file:") && /^\d+<\//.test(argument ?? "")) return file;
			const counter = /^(\d+)<anon_inode:\[eventfd\]>$/.exec(argument ?? "");
			if (counter) {
				const handle = table!.get(Number(counter[1]));
				if (handle?.object.inode.startsWith("eventfd:")) { handle.object.peer = handle.object; return handle; }
				return;
			}
			const match = /^(\d+)<(?:pipe|UNIX(?:-[A-Z]+)?):\[(\d+)(?:->(\d+))?\]>$/.exec(argument ?? "");
			if (!match || !options.inheritedStreams?.includes(match[2]!)) return;
			const object = resource(match[2]!);
			if (match[3]) { object.peer = resource(match[3]); object.peer.peer = object; }
			else if (argument!.includes("<pipe:")) object.peer = object;
			const origin = table!.get(Number(match[1]));
			return origin?.object === object ? origin : { object, access: 3, cloexec: false };
		};
		const emit = (handle: Handle, kind: ResourceTransitionKind, data: Buffer, requested?: number) => {
			const state = handle.object, peer = state.peer;
			journal.push({ order: line.order!, inode: state.inode, description: handle.description, kind, data, ...(requested !== undefined ? { requested } : {}) }); handled.add(line);
			const length = kind.endsWith("_message") ? data.length - 4 - 4 * data.readUInt32LE() : data.length;
			if (kind === "consume" || kind === "receive_message") {
				const packet = handle.packet ? messages.find(message => message.object === state) : undefined;
				const consumed = packet ? packet.end - packet.start : length;
				if (state.content) state.content = state.content.subarray(consumed);
				if (state.end !== undefined) { state.read += consumed; for (let index = messages.length - 1; index >= 0; index--)
					if (packet ? messages[index] === packet : messages[index]!.object === state && messages[index]!.start < state.read) messages.splice(index, 1); }
			}
			if (kind === "produce" || kind === "send_message") {
				if (kind === "produce" && handle.packet && peer?.end !== undefined) messages.push({ object: peer, start: peer.end, end: peer.end + length, rights: [] });
				append(peer, data.subarray(data.length - length));
			}
			if (kind === "shutdown") { wake(peer); wake(state); }
		};
		const readiness = (handle: Handle, requested: number, returned: number, projection = 0) => {
			const data = Buffer.alloc(12); data.writeUInt32LE(requested); data.writeUInt32LE(returned, 4); data.writeUInt32LE(projection, 8); emit(handle, "ready", data);
		};
		const stream = endpoint(line.args[0]), syscall = line.name;
		if (stream?.object.inode.startsWith("file:")) { const operation = fileLockTransition(line); if (operation) emit(stream, "lock", operation); }
		if (stream && !stream.object.inode.startsWith("file:")) {
			const object = stream.object, counter = object.inode.startsWith("eventfd:");
			const flags = line.args[3] ?? "";
			const produced = syscall === "write" || syscall === "writev" || syscall === "sendto" && /^(?:0|MSG_(?:NOSIGNAL|DONTWAIT))(?:\|MSG_(?:NOSIGNAL|DONTWAIT))*$/.test(flags) && line.args[4] === "NULL" && line.args[5] === "0";
			const consumed = syscall === "read" || syscall === "readv" || syscall === "recvfrom" && /^(?:0|MSG_(?:PEEK|DONTWAIT|WAITALL|TRUNC))(?:\|MSG_(?:PEEK|DONTWAIT|WAITALL|TRUNC))*$/.test(flags) && line.args[4] === "NULL" && /^(?:NULL|0x0)$/.test(line.args[5] ?? "");
			const bytes = /^"((?:\\.|[^"\\])*)"$/.exec(line.args[1] ?? "");
			const vector = syscall === "readv" || syscall === "writev" ? queueVectors(line.args[1], Number(line.args[2])) : undefined;
			const data = bytes ? decodeCBytes(bytes[1]!) : vector?.data, requested = vector?.length ?? Number(line.args[2]);
			// A kernel restart transferred no bytes; the eventual completion is its own ordered record.
			if ((produced || consumed) && /^\? ERESTARTSYS\b/.test(line.result)) handled.add(line);
			else if ((produced || consumed) && data && /^\d+$/.test(line.result)) {
				const returned = Number(line.result), truncated = /MSG_TRUNC/.test(flags);
				const packet = truncated && stream.packet ? messages.find(message => message.object === object) : undefined;
				const length = truncated ? Math.min(returned, requested) : returned;
				if ((!truncated || packet && returned === packet.end - packet.start) &&
					(produced ? data.length === requested && length <= requested && length <= 4096 : data.length === length)) {
					if (requested || stream.packet && (produced || !/^readv?$/.test(syscall))) emit(stream, produced ? "produce" : /MSG_PEEK/.test(flags) ? "peek" : "consume", data.subarray(0, length), produced || counter || stream.packet ? requested : undefined);
					else handled.add(line);
				}
			} else if ((produced || consumed) && /^-1 (EAGAIN|EWOULDBLOCK|EPIPE|EBADF|EINVAL)\b/.test(line.result)) {
				const error = /^-1 (\w+)/.exec(line.result)![1]!, failure = Buffer.alloc(counter && produced && data && data.length >= 8 ? 12 : 4);
				failure[0] = Number(produced); failure[1] = Number(/MSG_DONTWAIT/.test(flags));
				failure.writeUInt16LE(error === "EINVAL" ? 22 : error === "EBADF" ? 9 : error === "EPIPE" ? 32 : 11, 2);
				if (failure.length === 12) data!.copy(failure, 4, 0, 8); emit(stream, "failure", failure, requested);
			} else if (syscall === "shutdown" && line.result === "0" && /^(SHUT_RD|SHUT_WR|SHUT_RDWR)$/.test(line.args[1] ?? ""))
				emit(stream, "shutdown", Buffer.from([line.args[1] === "SHUT_RD" ? 1 : line.args[1] === "SHUT_WR" ? 2 : 3]));
			else if (/^fcntl(?:64)?$/.test(syscall) && line.args[1] === "F_SETFL" && line.result === "0" && stream.description !== undefined) {
				const flags = symbolicMask(line.args[2], FILE_FLAGS);
				if (flags !== undefined) { const data = Buffer.alloc(4); data.writeUInt32LE(flags); emit(stream, "flags", data); }
			}
			if ((syscall === "sendmsg" || syscall === "recvmsg") && /^\d+$/.test(line.result)) {
				const sent = syscall === "sendmsg", message = streamMessage(line.args[1]), returned = Number(line.result), flags = line.args[2] ?? "";
				const truncated = /MSG_TRUNC/.test(flags), length = truncated && message ? Math.min(returned, message.length) : returned;
				const packet = truncated && stream.packet ? messages.find(message => message.object === object) : undefined;
				if (message && (!truncated || packet && returned === packet.end - packet.start) && (stream.packet || !message.truncated) && /^(?:0|MSG_(?:NOSIGNAL|DONTWAIT|PEEK|WAITALL|CMSG_CLOEXEC|TRUNC))(?:\|MSG_(?:NOSIGNAL|DONTWAIT|PEEK|WAITALL|CMSG_CLOEXEC|TRUNC))*$/.test(flags) &&
					(sent ? !/PEEK|WAITALL|CMSG_CLOEXEC|TRUNC/.test(flags) && message.data.length === message.length && length <= message.length && length <= 4096 :
						!/NOSIGNAL/.test(flags) && message.data.length === length)) {
					const { peer } = object;
					const pending = messages.find(message => message.object === object && (stream.packet || message.start < object.read + length));
					const rights = sent ? message.rights.map(fd => table!.get(fd)) : pending?.rights ?? [];
					if (rights.length === message.rights.length && rights.every(handle => handle?.description !== undefined) &&
						(sent ? !rights.length || !!peer?.inode && (length > 0 || stream.packet) : message.rights.every(fd => !table!.has(fd)))) {
						const handles = rights as Handle[], data = Buffer.alloc(4 + 4 * handles.length + length);
						data.writeUInt32LE(handles.length); handles.forEach((handle, index) => data.writeUInt32LE(handle.description!, 4 + 4 * index));
						message.data.copy(data, 4 + 4 * handles.length, 0, length);
						if (sent && (handles.length || stream.packet)) { const start = peer?.end ?? 0; messages.push({ object: peer, start, end: start + length, rights: handles }); }
						if (!sent) handles.forEach((handle, index) => table!.set(message.rights[index]!, { ...handle, cloexec: /MSG_CMSG_CLOEXEC/.test(flags) }));
						emit(stream, sent ? "send_message" : /MSG_PEEK/.test(flags) ? "peek_message" : "receive_message", data, message.length);
					}
				}
			}
		}
		if ((syscall === "splice" || syscall === "tee") && stream?.description !== undefined && /^\d+$/.test(line.result)) {
			const moved = syscall === "splice", target = endpoint(line.args[moved ? 2 : 1]), length = Number(line.result), requested = Number(line.args[moved ? 4 : 2]);
			const state = stream.object, bytes = state.content, flags = line.args[moved ? 5 : 3];
			if (target && state !== target.object && bytes && length <= bytes.length && length <= requested && length <= 4096 &&
				/^(?:0|SPLICE_F_(?:MOVE|MORE|NONBLOCK))(?:\|SPLICE_F_(?:MOVE|MORE|NONBLOCK))*$/.test(flags ?? "") &&
				(!moved || line.args[1] === "NULL" && line.args[3] === "NULL")) {
				const data = Buffer.alloc(4 + length); data.writeUInt32LE(stream.description); bytes.copy(data, 4, 0, length);
				emit(target, moved ? "splice" : "tee", data, requested);
				append(target.object, bytes.subarray(0, length));
				if (moved) { state.content = bytes.subarray(length); if (state.end !== undefined) state.read += length; }
			}
		}
		if (syscall === "poll" || syscall === "ppoll") {
			const requested = pollEntries(line.args[0], "events"), ready = pollEntries(/^\d+ \((\[.*\])\)$/.exec(line.result)?.[1] ?? (line.result === "0 (Timeout)" ? "[]" : undefined), "revents");
			if (requested && ready && requested.length === Number(line.args[1]) && ready.length === Number.parseInt(line.result, 10)) {
				const number = (entry: { fd: string }) => Number.parseInt(entry.fd, 10);
				const rows = requested.map(entry => ({ ...entry, handle: endpoint(entry.fd), ready: ready.find(value => number(value) === number(entry))?.mask ?? 0 }));
				if (new Set(requested.map(number)).size === requested.length && rows.every(row => row.handle) && ready.every(row => requested.some(entry => number(entry) === number(row)))) {
					for (const row of rows) readiness(row.handle!, row.mask, row.ready);
					handled.add(line);
				}
			}
		}
		if (syscall === "select" || syscall === "pselect6") {
			const input = line.args.slice(1, 4).map(descriptorSet), output = ["in", "out", "except"].map(label =>
				descriptorSet(new RegExp(`(?:\\(|, )${label} (\\[[0-9 ]*\\])`).exec(line.result)?.[1] ?? "NULL"));
			const masks = [1, 4, 2], rows = new Map<number, { handle: Handle; requested: number; returned: number }>();
			let valid = /^\d+ (?:\(|$)/.test(line.result) && input.every(set => set) && output.every(set => set) &&
				output.reduce((sum, set) => sum + (set?.length ?? 0), 0) === Number.parseInt(line.result, 10);
			for (const [index, set] of input.entries()) for (const fd of set ?? []) {
				const number = Number.parseInt(fd, 10), handle = endpoint(fd), returned = output[index]?.some(value => Number(value) === number);
				if (!handle || number >= Number(line.args[0])) { valid = false; continue; }
				const row = rows.get(number) ?? { handle, requested: 0, returned: 0 };
				row.requested |= masks[index]!; if (returned) row.returned |= masks[index]!; rows.set(number, row);
			}
			for (const [index, set] of output.entries()) if (set?.some(fd => !input[index]?.some(value => Number.parseInt(value, 10) === Number(fd)))) valid = false;
			if (valid) { for (const row of rows.values()) readiness(row.handle, row.requested, row.returned, 1); handled.add(line); }
		}
		const poller = table.get(Number.parseInt(line.args[0] ?? "", 10))?.epoll;
		if (/^epoll_create(?:1)?$/.test(syscall) && syscallSucceeded(line)) {
			table.set(Number.parseInt(line.result, 10), { object: resource(`epoll:${line.order}`), access: 3, cloexec: /EPOLL_CLOEXEC/.test(line.args[0] ?? ""), epoll: new Map() }); handled.add(line);
		} else if (syscall === "epoll_ctl" && poller && syscallSucceeded(line)) {
			const handle = endpoint(line.args[2]), event = epollEvent(line.args[3]);
			if (handle && handle.description !== undefined) {
				const key = `${Number.parseInt(line.args[2]!, 10)}:${handle.description}`, operation = line.args[1];
				if (operation === "EPOLL_CTL_DEL" && poller.delete(key)) handled.add(line);
				else if (event && (!(event.mask & 0x80000000) || !(event.mask & (4 | 256 | 512))) &&
					(operation === "EPOLL_CTL_ADD" && !poller.has(key) || operation === "EPOLL_CTL_MOD" && poller.has(key))) {
					poller.set(key, { handle, ...event, armed: true }); handled.add(line);
				}
			}
		} else if (/^epoll_(?:wait|pwait|pwait2)$/.test(syscall) && poller && /^\d+$/.test(line.result)) {
			const returned = Number(line.result), events = epollEvents(line.args[1]);
			const registrations = [...poller.values()].filter(value => value.armed &&
				(!(value.mask & 0x80000000) || value.generation !== value.handle.object.generation));
			if (events && events.length === returned && returned <= Number(line.args[2]) && new Set(registrations.map(entry => entry.data)).size === registrations.length &&
				events.every(event => registrations.some(entry => entry.data === event.data))) {
				for (const entry of registrations) {
					const event = events.find(event => event.data === entry.data);
					if (event || returned < Number(line.args[2])) readiness(entry.handle, entry.mask & ~0xc0000000, event?.mask ?? 0);
					if (event && (entry.mask & 0x80000000)) entry.generation = entry.handle.object.generation;
					if (event && (entry.mask & 0x40000000)) entry.armed = false;
				}
				handled.add(line);
			}
		}
		const child = spawnedPID(line);
		if (child) { tables.set(child, /\bCLONE_FILES\b/.test(line.args.join(" ")) ? table : new Map(table)); continue; }
		if (lifetime && (line.name === "terminated" || syscallSucceeded(line))) {
		const fd = Number.parseInt(line.args[0] ?? "", 10), source = table.get(fd);
		if (line.name === "terminated") tables.delete(pid);
		else if (line.name === "close") table.delete(fd);
		else if (successfulExec(line)) {
			// exec unshares the FD table before applying FD_CLOEXEC.
			tables.set(pid, table = new Map(table));
			for (const [fd, handle] of table) if (handle.cloexec) table.delete(fd);
		} else if (line.name === "close_range") {
			if (/CLOSE_RANGE_UNSHARE/.test(line.args[2] ?? "")) tables.set(pid, table = new Map(table));
			const last = (line.args[1] ?? "").startsWith("~") ? 0xffffffff : Number(line.args[1]);
			for (const [number, handle] of table) if (number >= fd && number <= last)
				/CLOSE_RANGE_CLOEXEC/.test(line.args[2] ?? "") ? table.set(number, { ...handle, cloexec: true }) : table.delete(number);
		} else if (/^dup/.test(line.name) || /^F_DUPFD(?:_CLOEXEC)?$/.test(line.args[1] ?? "")) {
			const target = Number.parseInt(line.result, 10);
			if (target !== fd) {
				if (source) table.set(target, { ...source, cloexec: /(?:O|F_DUPFD)_CLOEXEC/.test(line.args.join(" ")) });
				else table.delete(target);
			}
		} else if (source && line.args[1] === "F_SETFD") table.set(fd, { ...source, cloexec: line.args[2] !== "0" });
		}
		if (!before) continue;
		let after = references();
		for (;;) {
			for (const [description, handle] of before.descriptions) if (description !== undefined && !after.descriptions.has(description) && handle.object.inode.startsWith("file:"))
				journal.push({ order: line.order, inode: handle.object.inode, description, kind: "release", data: Buffer.from([3]) });
			for (const [state, mask] of before.masks) {
				const released = mask & ~(after.masks.get(state) ?? 0) & ~(state.outside ?? 3);
				if (released && options.inheritedStreams?.includes(state.inode)) { journal.push({ order: line.order, inode: state.inode, kind: "release", data: Buffer.from([released]) }); wake(state.peer); }
			}
			// Destroyed receivers drop their rights before the newly unowned objects.
			// An external or cyclic reference still keeps its queue alive.
			const count = messages.length;
			for (let index = count - 1; index >= 0; index--) if (messages[index]!.object?.outside === 0 && !after.masks.has(messages[index]!.object!)) messages.splice(index, 1);
			if (messages.length === count) break;
			before = after; after = references();
		}
		for (const table of new Set(tables.values())) for (const handle of table.values()) if (handle.epoll)
			for (const [key, entry] of handle.epoll) if (!after.descriptions.has(entry.handle.description)) handle.epoll.delete(key);
	}
	return { journal, handled, retained: [...references().descriptions.keys()].filter((id): id is number => id !== undefined),
		finalHandles: [...(tables.get(root) ?? [])].map(([fd, handle]) => ({ fd, description: handle.description, cloexec: handle.cloexec })) };
}

function fileLockTransition(line: TraceLine): Buffer | undefined {
	const result = line.result === "0" ? 0 : /^-1 (?:EAGAIN|EWOULDBLOCK)\b/.test(line.result) ? 11 : /^-1 EBADF\b/.test(line.result) ? 9 : /^-1 EINVAL\b/.test(line.result) ? 22 : undefined;
	if (result === undefined) return;
	if (line.name === "flock") {
		const mode = /^LOCK_(SH|EX|UN)(?:\|LOCK_NB)?$/.exec(line.args[1] ?? "");
		if (mode) return Buffer.from(`0 ${mode[1] === "SH" ? 0 : mode[1] === "EX" ? 1 : 2} 0 0 ${result} 0 0 0`);
	}
	if (!/^fcntl(?:64)?$/.test(line.name) || !/^F_OFD_(?:SETLK|SETLKW|GETLK)$/.test(line.args[1] ?? "")) return;
	const query = line.args[1] === "F_OFD_GETLK";
	const range = /^\{l_type=F_(RDLCK|WRLCK|UNLCK), l_whence=SEEK_SET, l_start=(-?\d+), l_len=(-?\d+)(?:, l_pid=(-?\d+))?\}$/.exec(line.args[2] ?? "");
	if (!range || query && (result || !["-1", "0"].includes(range[4] ?? ""))) return;
	let start = BigInt(range[2]!), length = BigInt(range[3]!);
	if (length < 0n) { start += length; length = -length; }
	if (start < 0n || length < 0n || start + length > 0x7fffffffffffffffn) return;
	const type = range[1] === "RDLCK" ? 0 : range[1] === "WRLCK" ? 1 : 2;
	// strace prints GETLK's output. A stronger write query is checked against the
	// reconstructed graph; ambiguous owners are refused by the native interpreter.
	return Buffer.from(`${query ? 2 : 1} ${query ? 1 : type} ${start} ${length} ${result} ${query ? type : 0} ${query ? start : 0} ${query ? length : 0}`);
}

/** The vector and control envelope must be decoded completely; truncated/unknown
 * control messages retain their ordinary unsupported-syscall outcome. */
function streamMessage(text: string | undefined) {
	const match = /^\{msg_name=NULL, msg_namelen=0, msg_iov=(\[.*\]), msg_iovlen=(\d+), (?:msg_control=(.*), )?msg_controllen=(\d+), msg_flags=((?:0|MSG_CMSG_CLOEXEC|MSG_TRUNC)(?:\|MSG_CMSG_CLOEXEC|\|MSG_TRUNC)*)\}$/.exec(text ?? "");
	if (!match) return;
	const captured = queueVectors(match[1], Number(match[2])); if (!captured) return;
	const vector = { ...captured, truncated: /MSG_TRUNC/.test(match[5]!) };
	if ((match[3] === undefined || match[3] === "NULL") && match[4] === "0") return { ...vector, rights: [] as number[] };
	const control = /^\[\{cmsg_len=(\d+), cmsg_level=SOL_SOCKET, cmsg_type=SCM_RIGHTS, cmsg_data=\[(.*)\]\}\]$/.exec(match[3]!);
	if (!control) return;
	const fds = control[2]!.split(", ");
	if (fds.length > 64 || !fds.every(fd => /^\d+<.*>$/.test(fd)) || Number(control[1]) !== 16 + 4 * fds.length ||
		Number(match[4]) < Number(control[1]) || Number(match[4]) > 16 + 8 * Math.ceil(fds.length / 2)) return;
	return { ...vector, rights: fds.map(fd => Number.parseInt(fd, 10)) };
}

const FILE_FLAGS: Readonly<Record<string, number>> = { "0": 0, O_RDONLY: 0, O_WRONLY: 1, O_RDWR: 2, O_APPEND: 0x400, O_NONBLOCK: 0x800, O_NDELAY: 0x800, O_LARGEFILE: 0x8000 };
const POLL_FLAGS: Readonly<Record<string, number>> = { "0": 0, POLLIN: 1, POLLPRI: 2, POLLOUT: 4, POLLERR: 8, POLLHUP: 16, POLLNVAL: 32,
	POLLRDNORM: 64, POLLRDBAND: 128, POLLWRNORM: 256, POLLWRBAND: 512, POLLRDHUP: 8192 };
function symbolicMask(text: string | undefined, flags: Readonly<Record<string, number>>): number | undefined {
	return text?.split("|").reduce<number | undefined>((mask, flag) => mask === undefined || flags[flag] === undefined ? undefined : mask | flags[flag], 0);
}
function pollEntries(text: string | undefined, field: string): Array<{ fd: string; mask: number }> | undefined {
	if (!text?.startsWith("[") || !text.endsWith("]")) return;
	const entries = [...text.matchAll(new RegExp(`\\{fd=([^,]+), ${field}=([^}]+)\\}`, "g"))];
	if ("[" + entries.map(match => match[0]).join(", ") + "]" !== text) return;
	const values = entries.map(match => ({ fd: match[1]!, mask: symbolicMask(match[2], POLL_FLAGS) }));
	return values.every(value => value.mask !== undefined) ? values as Array<{ fd: string; mask: number }> : undefined;
}
function descriptorSet(text: string | undefined): string[] | undefined {
	if (text === "NULL") return [];
	if (!text || !/^\[(?:\d+(?:<(?:pipe|UNIX(?:-[A-Z]+)?):\[\d+(?:->\d+)?\]>)?(?: )?)*\]$/.test(text)) return;
	return text.slice(1, -1).split(" ").filter(Boolean);
}
const EPOLL_FLAGS = { ...Object.fromEntries(Object.entries(POLL_FLAGS).map(([name, mask]) => [name.replace(/^POLL/, "EPOLL"), mask])), EPOLLONESHOT: 0x40000000, EPOLLET: 0x80000000 };
function epollEvent(text: string | undefined): { mask: number; data: string } | undefined {
	const match = /^\{events=([^,]+), data=\{u32=\d+, u64=(\d+)\}\}$/.exec(text ?? ""), mask = symbolicMask(match?.[1], EPOLL_FLAGS);
	return match && mask !== undefined ? { mask, data: match[2]! } : undefined;
}
function epollEvents(text: string | undefined): Array<{ mask: number; data: string }> | undefined {
	const matches = [...(text ?? "").matchAll(/\{events=[^,]+, data=\{u32=\d+, u64=\d+\}\}/g)], events = matches.map(match => epollEvent(match[0]));
	return "[" + matches.map(match => match[0]).join(", ") + "]" === text && events.every(event => event) ? events as Array<{ mask: number; data: string }> : undefined;
}

function selectTraceRoot(files: readonly TraceFile[], target: string, initialCwd: string): TraceRoot | { readonly reason: string } {
	const parent = new Map<number, number>(), children = new Map<number, number[]>();
	for (const file of files) {
		for (const line of file.lines) {
			const child = spawnedPID(line);
			if (!child) continue;
			const existing = parent.get(child);
			if (existing !== undefined && existing !== file.pid) return { reason: `process_parent_ambiguous:${child}` };
			parent.set(child, file.pid);
			appendGrouped(children, file.pid, child);
		}
	}
	const roots = files.filter((file) => !parent.has(file.pid));
	if (roots.length !== 1) return { reason: `trace_root_ambiguous:${roots.map(({ pid }) => pid).sort().join(",")}` };

	const depth = new Map<number, number>([[roots[0]!.pid, 0]]);
	for (const [pid, level] of depth) for (const child of children.get(pid) ?? []) if (!depth.has(child)) depth.set(child, level + 1);
	const candidates: Array<TraceRoot & { readonly depth: number }> = [];
	for (const file of files) {
		const processDepth = depth.get(file.pid);
		if (processDepth === undefined) continue;
		const start = file.lines.findIndex(line => successfulExec(line) && syscallPaths(line, initialCwd)?.some(name => path.posix.resolve(name) === target));
		if (start >= 0) candidates.push({ ...file, start, depth: processDepth });
	}
	if (!candidates.length) return { reason: "target_exec_not_found" };
	const shallowest = Math.min(...candidates.map((candidate) => candidate.depth));
	const matches = candidates.filter((candidate) => candidate.depth === shallowest);
	if (matches.length !== 1) return { reason: `target_exec_ambiguous:${matches.map(file => file.pid).sort().join(",")}` };
	const match = matches[0]!;
	// exec does not unshare CLONE_FS. An excluded task must not retain authority over the target cwd.
	if (files.some((file) => file.lines.some((line, index) => spawnedPID(line) && sharesFilesystem(line) !== false &&
		(spawnedPID(line) === match.pid || (file.pid === match.pid && index < match.start))))) {
		return { reason: "target_filesystem_context_unproven" };
	}
	return match;
}

/**
 * Decode a strace -ff transcript. The parser deliberately fails closed: only the target
 * exec and its recursively identified descendants contribute a replayable certificate.
 */
export async function observeStrace(
	tracePrefix: string,
	executablePath: string,
	initialCwd: string,
	options: StraceObservationOptions = {},
): Promise<StraceObservation> {
	const files: TraceFile[] = [];
	let remaining = options.frozen?.bytes ?? options.previewBytes, frontier = false;
	if (remaining !== undefined && (!isSafeCount(remaining))) throw new Error("invalid trace preview budget");
	for await (const chunk of traceChunks(tracePrefix, undefined, remaining)) {
		if (options.frozen && chunk.pid !== undefined) throw new Error("continuation requires one ordered trace");
		remaining = chunk.remaining;
		let contents = chunk.bytes.toString("utf8");
		if (options.privateUpper) contents = contents.replaceAll(`<${options.privateUpper}/`, "</");
		for (const [pid, lines] of traceDecoder()(contents, chunk.pid)) {
			if (options.frozen?.pid === pid) {
				frontier = contents.endsWith("\n") && !lines.some(line => /^--- /.test(line.text));
			}
			files.push({ pid, lines });
		}
	}
	const target = path.posix.resolve(initialCwd, executablePath);
	const root = selectTraceRoot(files, options.launchPath ?? target, initialCwd);
	if ("reason" in root) return { complete: false, paths: [], taints: ["trace_incomplete"], tracedProcesses: 0, incompleteReasons: [root.reason] };

	const byPID = new Map(files.map((file) => [file.pid, file]));
	const selected = new Map<number, TraceProcess>([[root.pid, { ...root, cwd: path.posix.resolve(initialCwd), fs: { shared: false, changed: false } }]]);
	const incomplete = new Set<string>(options.previewBytes === undefined ? [] : ["preview_only"]);
	if (options.frozen && (!frontier || root.pid !== options.frozen.pid || remaining !== 0)) incomplete.add("continuation_frontier_unproven");
	for (const [pid, process] of selected) {
		if (process.lines.at(-1)?.name !== "terminated" && options.frozen?.pid !== pid) incomplete.add(`process_exit_unproven:${pid}`);
		let cwd = process.cwd;
		for (const line of process.lines.slice(process.start)) {
			if (process.fs.shared && (line.name === "chdir" || line.name === "fchdir") && syscallSucceeded(line)) process.fs.changed = true;
			cwd = tracedCwd(line, cwd);
			const child = spawnedPID(line);
			if (!child || selected.has(child)) continue;
			const childFile = byPID.get(child);
			if (!childFile) { incomplete.add(`child_trace_missing:${child}`); continue; }
			const shared = sharesFilesystem(line);
			if (shared === undefined) incomplete.add(`clone_flags_unparsed:${pid}`);
			if (shared !== false) process.fs.shared = true;
			selected.set(child, { ...childFile, start: 0, cwd, fs: shared === false ? { shared: false, changed: false } : process.fs });
		}
	}
	if (options.frozen && selected.size !== 1) incomplete.add("continuation_process_tree");

	const paths = new Map<string, DependencyRole>(), listedPaths = new Set<string>(), metadata = new Map<string, Extract<ObservedProcessPath, { role: "metadata" }>>();
	const lookupFailures = new Map<string, "ENOENT" | "ENOTDIR" | undefined>();
	const observePath = (target: string, role: DependencyRole, failure?: "ENOENT" | "ENOTDIR") => {
		if (paths.get(target) !== "executable") paths.set(target, role);
		lookupFailures.set(target, lookupFailures.has(target) && lookupFailures.get(target) !== failure ? undefined : failure);
	};
	// A transcript cannot prove untraced clock/random inputs repeatable; it permits only a one-shot transfer.
	const taints = new Set<ProvenanceTaint>(["clock", "random"]);
	const unsupportedSyscalls = new Set<string>();
	let unsupportedSyscallsTruncated = false;
	const unsupported = (syscall: string) => {
		taints.add("unsupported_syscall");
		const name = syscall.slice(0, 64);
		if (unsupportedSyscalls.size < 16) unsupportedSyscalls.add(name);
		else if (!unsupportedSyscalls.has(name)) unsupportedSyscallsTruncated = true;
	};
	const opened = new Map<number, Set<number>>(); // Descriptors a process opened itself, whose status flags it chose.
	const ownPipes = new Set<string>(); // Pipes and socket pairs the traced processes created, by inode.
	// The semantic roots are one workspace seen from the sandbox and from its source: name a file by its place in it.
	const locks = new Map<string, boolean>(), changedMetadata = new Set<string>(), written = new Set<string>(), writable = new Set<string>(), external = new Set<string>(), workspaceName = (target: string) =>
		semanticRoots.flatMap((root) => containsLogicalPath(root, target) ? [`//${path.posix.relative(root, target)}`] : [])[0];
	const { journal: resourceJournal, handled: streamCalls, retained, finalHandles } = options.inheritedHandles?.length || options.inheritedStreams?.length
		? resourceTransitions(selected, root.pid, options) : { journal: [], handled: new Set<TraceLine>(), retained: [], finalHandles: [] };
	const semanticRoots = (options.guardFilesystemSemanticsWithin ?? []).map((value) => path.posix.resolve(value));
	const brokered = brokeredProcessSegments(selected, options);
	const observeMetadata = (observedPath: string, followSymlinks: boolean, { digest, fields }: StatObservation) => {
		lookupFailures.set(observedPath, undefined);
		const identity = `metadata:${followSymlinks}:${fields?.join(",") ?? ""}:${observedPath}`;
		const afterWrite = external.has(observedPath) || written.has(workspaceName(observedPath) ?? "");
		if (afterWrite && metadata.has(identity)) return;
		const previous = metadata.get(identity);
		if (previous && previous.digest !== digest) changedMetadata.add(observedPath);
		metadata.set(identity, { path: observedPath, role: "metadata", followSymlinks, digest, ...(fields ? { fields } : {}), ...(afterWrite ? { afterWrite: true } : {}) });
	};
	for (const [pid, { lines, start, cwd: initial, fs }] of selected) {
		// -ff files cannot order another task's chdir against this task's pathname lookup.
		if (fs.changed) { incomplete.add("shared_cwd_mutation"); continue; }
		let cwd = initial;
		if (!cwd) { incomplete.add(`cwd_unknown:${pid}`); cwd = path.posix.resolve(initialCwd); }
		for (let index = start; index < lines.length; index++) {
			if (index >= (brokered.get(pid) ?? Infinity)) break;
			if (pid === root.pid && index === start && options.launchPath) continue;
			const line = lines[index]!;
			const spawned = spawnedPID(line);
			if (spawned) opened.set(spawned, new Set(opened.get(pid)));
			const own = opened.get(pid) ?? opened.set(pid, new Set()).get(pid)!, result = Number.parseInt(line.result ?? "", 10);
			// Files and pipes the traced processes created stay private to them, through duplication and inheritance.
			if (/^(?:open|openat|openat2|creat)$/.test(line.name) && isSafeCount(result)) { own.add(result); if (writesPath(line)) writable.add(absoluteDescriptorPath(line.result) ?? ""); }
			else if (/^pipe2?$/.test(line.name) && result === 0) for (const pipe of (line.args[0] ?? "").matchAll(/<pipe:\[(\d+)\]>/g)) ownPipes.add(pipe[1]!);
			else if (line.name === "socketpair" && result === 0) for (const end of (line.args[3] ?? "").matchAll(/<UNIX(?:-[A-Z]+)?:\[(\d+)/g)) ownPipes.add(end[1]!);
			else if (/^dup[23]?$|^fcntl(?:64)?$/.test(line.name) && (line.name.startsWith("dup") || /^F_DUPFD/.test(line.args[1] ?? "")) && isSafeCount(result)) {
				if (own.has(Number.parseInt(line.args[0] ?? "", 10))) own.add(result); else own.delete(result);
			} else if (line.name === "close") own.delete(Number.parseInt(line.args[0] ?? "", 10));
			else if (line.name === "close_range" && result === 0 && !line.args[2]?.includes("CLOSE_RANGE_CLOEXEC"))
				for (const fd of own) if (fd >= Number(line.args[0]) && fd <= (/^~0/.test(line.args[1] ?? "") ? Infinity : Number(line.args[1]))) own.delete(fd);
			if (line.failure) { incomplete.add(line.failure); continue; }
			const { name: syscall, args } = line, succeeded = syscallSucceeded(line);
			if (!syscall) continue;
			// Descriptor images have their own sealed effects; explicitly naming one still observes its pathname.
			const observedPaths = syscallPaths(line, cwd)?.filter(target => !options.inheritedFileImages?.includes(target) ||
				!(writesPath(line) || syscall === "mmap") || PATH_ARGUMENTS[syscall]?.some(([pathname]) => pathname !== undefined));
			if (options.frozen && !continuationCall(line, index === start)) incomplete.add(`continuation_state:${syscall}`);
			if (syscall === "getpid" || syscall === "getppid" || syscall === "getsid" || syscall === "getpgid") { taints.add("pid_observation"); }
			const endpoint = /^\d+<(?:pipe|UNIX(?:-[A-Z]+)?):\[(\d+)(?:->\d+)?\]>$/.exec(args[0] ?? "")?.[1];
			const stream = endpoint && options.inheritedStreams?.includes(endpoint) || options.inheritedStreams?.some(inode => inode.startsWith("eventfd:")) && /^\d+<anon_inode:\[eventfd\]>$/.test(args[0] ?? "");
			const streamCall = streamCalls.has(line);
			if (stream && /^(?:read|write|readv|writev|sendto|recvfrom|sendfile|vmsplice)$/.test(syscall) && !streamCall) unsupported(syscall);
			if (options.inheritedStreams?.length && /^(?:poll|ppoll|select|pselect6|epoll_.*)$/.test(syscall) && !streamCall && !internalPoll(line, ownPipes)) unsupported(syscall);
			// A local socket reaches another process only once connected: a refused path (an NSS cache that is absent here) is a
			// pathname dependency, validated absent where the Actor runs.
			const refused = syscall === "connect" && /^-1 (?:ENOENT|ECONNREFUSED|EACCES)\b/.test(line.result) ? /\bsun_path="(\/[^"]+)"/.exec(args[1] ?? "")?.[1] : undefined;
			if (refused) { observePath(refused, "input", /^-1 ENOENT\b/.test(line.result) ? "ENOENT" : undefined); continue; }
			const output = !!options.outputEndpoints?.includes(`socket:[${/^\d+<UNIX-STREAM:\[(\d+)/.exec(args[0] ?? "")?.[1]}]`);
			const outputQuery = output && /^(?:getsockname|getpeername|getsockopt)$/.test(syscall);
			// Created pairs stay within the tree only while they neither open another connection nor send to another address.
			const localCall = endpoint && ownPipes.has(endpoint) && (syscall === "sendto" ? args[4] === "NULL" && args[5] === "0"
				: syscall === "sendmsg" ? streamMessage(args[1]) !== undefined : !/^(?:connect|bind|listen|accept|accept4|sendmmsg)$/.test(syscall));
			if (NETWORK_SYSCALLS.has(syscall) && !nonSocketQuery(line) && !streamCall && !outputQuery && !(/^socket(?:pair)?$/.test(syscall) && /^AF_UNIX\b/.test(args[0] ?? "")) &&
				!localCall) taints.add("network");
			if (IPC_SYSCALLS.has(syscall)) taints.add("ipc");
			// Descriptor-local state is internal; reproduced OFD flags are sealed with their final offsets. A lock taken without waiting,
			// or a query that found the range free, depends only on no one else holding one, which a replay probes; a refusal, or a
			// holder found, saw another. One taken by waiting ends the same way natively, however long it waited; a release reveals nothing.
			const record = /^fcntl(?:64)?$/.test(syscall) ? /^F_(?:OFD_)?(GETLK|SETLK|SETLKW)(?:64)?$/.exec(args[1] ?? "")?.[1] : undefined;
			const lock = syscall === "flock" ? /\bLOCK_NB\b/.test(args[1] ?? "") && /\bLOCK_(SH|EX)\b/.exec(args[1] ?? "")?.[1]
				: record && record !== "SETLKW" && /\bl_type=F_(RD|WR|UN)LCK\b/.exec(args[2] ?? "")?.[1], locked = absoluteDescriptorPath(args[0]);
			if (lock && !streamCall && (record === "GETLK" || lock !== "UN")) {
				if (locked && succeeded && (record === "GETLK") === (lock === "UN")) locks.set(locked, locks.get(locked) || lock !== "SH" && lock !== "RD");
				else taints.add("ipc");
			}
			if (/^fcntl(?:64)?$/.test(syscall) && !record) {
				const command = args[1] ?? "";
				if (!/^F_(?:GETFD|SETFD|DUPFD|DUPFD_CLOEXEC)$/.test(command) &&
					// A descriptor this process opened reports the flags it chose; an inherited one's flags need evidence.
					// Standard streams' status flags are part of the pinned execution context.
					!(command === "F_GETFL" && (own.has(Number.parseInt(args[0] ?? "", 10)) || /^[012]</.test(args[0] ?? ""))) && !((command === "F_GETFL" || command === "F_SETFL" && succeeded &&
						(args[2] ?? "").split("|").every(flag => /^(?:O_(?:RDONLY|WRONLY|RDWR|APPEND|NONBLOCK|NDELAY|LARGEFILE|DIRECTORY|DSYNC|SYNC|NOFOLLOW)|0)$/.test(flag))) &&
						(options.inheritedFileImages?.includes(absoluteDescriptorPath(args[0]) ?? /^\d+<(pipe:\[\d+\])>$/.exec(args[0] ?? "")?.[1] ?? "") || stream || output ||
							options.unchangedOutputPipes?.includes(/^\d+<(pipe:\[\d+\])>$/.exec(args[0] ?? "")?.[1] ?? "") ||
							own.has(Number.parseInt(args[0] ?? "", 10)) || ownPipes.has(endpoint ?? ""))))
					unsupported(syscall);
			}
			// An unprivileged process holds no capabilities, sandboxed or not (the observer runs as the Actor's user).
			const noCapabilities = syscall === "capget" && process.getuid?.() !== 0 && /\{effective=0, permitted=0, inheritable=0\}\) = 0$/.test(`${args.join(", ")}) = ${line.result}`);
			const limit = syscall === "prlimit64" ? args[1] : /^(?:getrlimit|setrlimit)$/.test(syscall) ? args[0] : undefined;
			// A refused io_uring leaves libuv on epoll and its thread pool: the same results, every file call traced.
			if (syscall === "io_uring_setup" && /^-1 EPERM\b/.test(line.result)) continue;
			if (!noCapabilities && (CONFINEMENT_SENSITIVE_SYSCALLS.has(syscall) || /^-1 (?:EACCES|EPERM)\b/.test(line.result) ||
				syscall === "prctl" && !/^PR_(?:GET_NAME|SET_NAME|SET_VMA)$/.test(args[0] ?? "") ||
				["clone", "clone3", "fork", "vfork"].includes(syscall) && /^-1 EAGAIN\b/.test(line.result) ||
				limit === "RLIMIT_CORE" && syscall !== "setrlimit" && succeeded)) {
				taints.add("confinement_observation");
			}
			// A time set on a file the tree opened for writing reaches its effects, which carry each file's modification time.
			if (/^(?:utime|utimes|utimensat|futimesat)$/.test(syscall) && !(observedPaths ?? [""]).every(target => writable.has(target))) unsupported(syscall);
			if (
				// Even a failed setter can mutate a limit; sandbox caps and foreign limits are unmodeled.
				(syscall === "setrlimit" || syscall === "prlimit64" && args[2] !== "NULL") &&
					(syscall === "prlimit64" && args[0] !== "0" || limit === "RLIMIT_NPROC" || limit === "RLIMIT_CORE") ||
				UNMODELED_FILE_SEMANTICS_SYSCALLS.has(syscall) && !streamCall ||
				(syscall === "pipe2" && /O_DIRECT|O_EXCL/.test(args[1] ?? "")) ||
				(syscall === "ioctl" && unmodeledFileIoctl(line))
			) {
				unsupported(syscall);
			}
			// A filesystem's statistics vary over time like the clock, whose taint every trace carries; the sandbox reports the one a
			// native run sees, the workspace's own included.
			if (/^f?statfs$/.test(syscall)) continue;
			const listed = /^getdents(?:64)?$/.test(syscall) ? absoluteDescriptorPath(args[0]) : undefined;
			const directoryImage = !!listed && !!options.inheritedDirectoryImages?.includes(listed);
			// A listed directory's entry set is its dependency; readdir order and d_ino are volatile identity, as a descriptor's is.
			const listing = !!listed && !directoryImage && succeeded;
			if (listing) { taints.add("descriptor_observation"); listedPaths.add(listed!); if (paths.get(listed!) !== "executable") paths.set(listed!, "input"); }
			if (UNMODELED_METADATA_SYSCALLS.has(syscall) && !listing && (succeeded ? !directoryImage : directoryImage)) {
				unsupported(syscall);
				incomplete.add(`unmodeled_metadata:${syscall}:${pid}`);
			}
			if (semanticRoots.length && workspaceDriverSemanticGap(line, observedPaths, semanticRoots)) {
				unsupported(syscall);
				incomplete.add(`filesystem_semantics:${syscall}:${pid}`);
			}
			if (succeeded && writesPath(line)) for (const target of observedPaths ?? []) { const name = workspaceName(target); if (name) written.add(name); else external.add(target); }
			const [structure, flags] = MODELED_METADATA_SYSCALLS.get(syscall) ?? [];
			if (structure && succeeded) {
				// A recreated null device has the same I/O semantics, but may have a different device-node inode.
				if (/<char 1:3>>$/.test(args[0] ?? "")) { taints.add("descriptor_observation"); continue; }
				// The returned ABI fields are the observation. Program names and argv cannot prove a field unused.
				const observed = statObservationDigest(args[structure] ?? "");
				if (!observedPaths?.length || !observed) {
					// fstat, or an empty *at name, of a pipe or socket
					if (descriptorTarget(line) && !quotedArgument(args[1])) taints.add("descriptor_observation");
					else { unsupported(syscall); incomplete.add(`unparsed_metadata:${syscall}:${pid}`); }
				}
				if (observed && observed.fields?.length !== 0) {
					const followSymlinks = syscall !== "lstat" && !(flags && /\bAT_SYMLINK_NOFOLLOW\b/.test(args[flags] ?? ""));
					for (const metadataPath of observedPaths ?? []) observeMetadata(metadataPath, followSymlinks, observed);
				}
				continue;
			}
			if (!PATH_ARGUMENTS[syscall]) continue;
			const role: DependencyRole = syscall === "execve" || syscall === "execveat" ? "executable" : "input";
			if (!observedPaths) incomplete.add(`unresolved_pathname:${syscall}:${pid}`);
			const nonEmpty = /^(?:rmdir|unlinkat|rename(?:at2?)?)$/.test(syscall) && /^-1 (?:ENOTEMPTY|EEXIST)\b/.test(line.result);
			// Creating a name can fail on its parent; a multi-path syscall does not identify the failed operand.
			const failure = observedPaths?.length === 1 && !writesPath(line) ? /^-1 (ENOENT|ENOTDIR)\b/.exec(line.result)?.[1] as "ENOENT" | "ENOTDIR" | undefined : undefined;
			for (const observed of observedPaths ?? []) {
				observePath(observed, role, failure);
				if (nonEmpty) listedPaths.add(observed);
			}
			if ((syscall === "chdir" || syscall === "fchdir") && succeeded) {
				const changed = tracedCwd(line, cwd);
				if (changed) cwd = changed;
				else incomplete.add(`${syscall}_unparsed:${pid}`);
			}
		}
	}
	// A workspace file the traced processes wrote changes under their own hands; its later state is their effect, not an input.
	// A run brokered from its tree, whose launcher the trace holds, wrote on its behalf.
	for (const pid of selected.keys()) for (const name of options.brokeredWrites?.(pid) ?? []) {
		if (path.posix.isAbsolute(name)) external.add(name); else written.add(`//${name}`);
	}
	// What it wrote outside the workspace, and the directories holding those names, change by its own hand.
	const touched = new Set([...external, ...[...written].flatMap(name => semanticRoots.map(root => path.posix.join(root, name.slice(2))))]
		.flatMap(target => [target, path.posix.dirname(target)]));
	const metadataChanges = [...changedMetadata].filter(observed => !touched.has(observed)).sort();
	if (incomplete.size) taints.add("trace_incomplete");
	return {
		complete: !incomplete.size,
		...(options.inheritedHandles || options.inheritedStreams ? { resourceJournal: resourceJournal.sort((a, b) => a.order - b.order).map(({ order, ...event }) => event), retainedDescriptions: retained,
			...(options.frozen ? { finalHandles } : {}) } : {}),
		paths: Object.freeze([...[...paths].map(([observedPath, role]) => ({ path: observedPath, role: sharedObjectRole(observedPath, role),
			...(lookupFailures.get(observedPath) ? { lookupFailure: lookupFailures.get(observedPath)! } : {}),
			...(listedPaths.has(observedPath) ? { listed: true as const } : {}) })), ...metadata.values()]
			.sort((left, right) => pathOrder(left).localeCompare(pathOrder(right)))),
		taints: Object.freeze([...taints].sort()),
		pids: [...selected.keys()],
		tracedProcesses: [...selected].filter(([pid, { lines, start }]) => start < Math.min(lines.length, brokered.get(pid) ?? Infinity)).length,
		incompleteReasons: Object.freeze([...incomplete].sort()),
		...(metadataChanges.length ? { metadataChanges: Object.freeze(metadataChanges) } : {}),
		...(unsupportedSyscalls.size ? { unsupportedSyscalls: Object.freeze([...unsupportedSyscalls].sort()) } : {}),
		...(unsupportedSyscallsTruncated ? { unsupportedSyscallsTruncated: true as const } : {}),
		written: [...written].map(name => name.slice(2)).sort(), external: [...external].sort(),
		locks: [...locks].map(([target, exclusive]) => ({ path: target, exclusive })),
	};
}

function pathOrder(item: ObservedProcessPath): string {
	return `${item.role}:${item.role === "metadata" ? item.followSymlinks : ""}:${item.path}`;
}

/** State retained across the frontier must be in the image or the common resource
 * journal. The final table is bound back to its original OFDs before handoff. */
function continuationCall(line: TraceLine, initial: boolean): boolean {
	const { name: call, args } = line;
	const fd = Number.parseInt(args[/^dup[23]$/.test(call) ? 1 : 0] ?? "", 10);
	if (/^(?:execve|execveat)$/.test(call)) return initial;
	if (/^(?:close|close_range|dup[23])$/.test(call)) return fd >= 3;
	if (call === "fcntl" || call === "fcntl64") return fd >= 3 || args[1] !== "F_SETFD";
	if (call === "prlimit64" && args[2] === "NULL") return true;
	if (call === "setrlimit" || call === "prlimit64" && args[0] === "0")
		return /^RLIMIT_(?:FSIZE|DATA|STACK|RSS|NOFILE|MEMLOCK|AS|LOCKS|SIGPENDING|MSGQUEUE|NICE|RTPRIO)$/.test(args[call === "prlimit64" ? 1 : 0] ?? "");
	if (call === "mmap") return (args[3] ?? "").split("|").every(flag => /^(?:MAP_PRIVATE|MAP_ANONYMOUS|MAP_FIXED|MAP_DENYWRITE|MAP_STACK)$/.test(flag));
	if (call === "madvise") return args[2] === "MADV_DONTNEED" || args[2] === "MADV_NOHUGEPAGE";
	if (call === "arch_prctl") return /^ARCH_(?:SET|GET)_(?:FS|GS)$/.test(args[0] ?? "");
	if (call === "prctl") return args[0] === "PR_GET_NAME";
	return /^(?:read|pread64|readv|write|writev|pwrite64|lseek|open|openat|access|faccessat|newfstatat|fstat|stat|lstat|statx|readlink|readlinkat|brk|mprotect|munmap|set_tid_address|set_robust_list|rseq|getrlimit|getrandom|rt_sigaction|rt_sigprocmask|sigaltstack|dup|poll|ppoll|select|pselect6|sendto|recvfrom|sendmsg|recvmsg|shutdown|flock|rename|renameat|renameat2|unlink|unlinkat|link|linkat|mkdir|mkdirat|rmdir|chmod|fchmod|fchmodat|truncate|ftruncate)$/.test(call);
}

/** Kernel argument positions own pathname identity; each *at operand has its own directory binding.
 * An empty or NULL name of an operand that accepts AT_EMPTY_PATH names its descriptor. */
const PATH_ARGUMENTS: Readonly<Record<string, readonly (readonly [pathname: number | undefined, dirfd?: number, descriptorPath?: true])[]>> = Object.fromEntries(([
	["access chdir chmod chown creat execve getxattr lgetxattr listxattr llistxattr mkdir mknod open readlink removexattr lremovexattr rmdir setxattr lsetxattr truncate unlink utime utimes stat lstat statfs", [[0]]],
	["rename link mount", [[0], [1]]],
	["symlink", [[1]]],
	["faccessat fchmodat mkdirat mknodat openat openat2 unlinkat", [[1, 0]]],
	["execveat faccessat2 fchownat newfstatat readlinkat statx utimensat", [[1, 0, true]]],
	["renameat renameat2", [[1, 0], [3, 2]]],
	["linkat", [[1, 0, true], [3, 2]]],
	["symlinkat", [[2, 1]]],
	["fchdir fstat fstatfs", [[undefined, 0]]],
	["write writev pwrite64 pwritev pwritev2 ftruncate fchmod fchown fallocate sendfile", [[undefined, 0]]],
	["copy_file_range splice", [[undefined, 2]]], ["tee", [[undefined, 1]]], ["mmap", [[undefined, 4]]],
] as const).flatMap(([names, positions]) => names.split(" ").map((name) => [name, positions])));

const MODELED_METADATA_SYSCALLS = new Map<string, readonly [structure: number, flags?: number]>([["stat", [1]], ["lstat", [1]], ["fstat", [1]], ["newfstatat", [2, 3]], ["statx", [4, 2]]]);
const UNMODELED_METADATA_SYSCALLS = new Set(["getdents", "getdents64"]);

/** Persistent metadata not represented by the typed workspace transaction must never be replayed. */
const UNMODELED_FILE_SEMANTICS_SYSCALLS = new Set(["fallocate", "splice", "tee", "fgetxattr", "flistxattr", "fremovexattr", "fsetxattr",
	"getxattr", "lgetxattr", "listxattr", "llistxattr", "lremovexattr", "lsetxattr", "removexattr", "setxattr"]);

const UNMODELED_MUTATING_IOCTL = /\b(?:FICLONE|FICLONERANGE|FIDEDUPERANGE|FS_IOC_SETFLAGS|FS_IOC_SETVERSION|FS_IOC_FSSETXATTR)\b/;
const DRIVER_SEMANTIC_GAP_RESULT = /^-1\s+(?:EXDEV|EOPNOTSUPP|ENOTSUP|ENOSYS)\b/;

function unmodeledFileIoctl(line: TraceLine): boolean {
	// Close-on-exec is the descriptor's own flag, like F_SETFD (Python sets it on every file it opens).
	if (!syscallSucceeded(line) || /^FIO(?:N?CLEX)$/.test(line.args[1] ?? "")) return false;
	return UNMODELED_MUTATING_IOCTL.test(line.args[1] ?? "") || absoluteDescriptorPath(line.args[0]) !== undefined;
}

function workspaceDriverSemanticGap(line: TraceLine, paths: readonly string[] | undefined, roots: readonly string[]): boolean {
	if (!DRIVER_SEMANTIC_GAP_RESULT.test(line.result)) return false;
	const referenced = new Set(paths);
	for (const argument of line.args) { const target = absoluteDescriptorPath(argument); if (target) referenced.add(target); }
	return [...referenced].some((candidate) => roots.some((root) => containsLogicalPath(root, candidate)));
}

const NETWORK_SYSCALLS = new Set(["getsockname", "getpeername", "getsockopt", "setsockopt", "listen", "shutdown", "accept", "accept4", "bind",
	"connect", "recvfrom", "recvmmsg", "recvmsg", "sendmmsg", "sendmsg", "sendto", "socket", "socketpair"]);
const IPC_SYSCALLS = new Set(["mq_open", "msgget", "semget", "shmat", "shmget"]);

/** A failed query of a proven file/pipe reveals no socket state; unknown descriptor types stay tainted. */
function nonSocketQuery(line: TraceLine): boolean {
	return /^(?:getsockname|getpeername|getsockopt)$/.test(line.name) && /^-1 ENOTSOCK\b/.test(line.result) &&
		Boolean(absoluteDescriptorPath(line.args[0]) || /^\d+<pipe:\[\d+\]>$/.test(line.args[0] ?? ""));
}


/** A poll that only checks descriptors are open, or waits on pipes the traced processes made, observes nothing outside them. */
function internalPoll(line: TraceLine, ownPipes: ReadonlySet<string>): boolean {
	const entries = /^p?poll$/.test(line.name) ? [...(line.args[0] ?? "").matchAll(/\{fd=\d+(<.*?>)?, events=([^}]+)\}/g)] : [];
	return entries.length > 0 && entries.every(([, target, events]) => events === "0" || ownPipes.has(/^<pipe:\[(\d+)\]>$/.exec(target ?? "")?.[1] ?? ""));
}

/** A traced execution's workspace writes so far (it may still run), stamped in wall-clock ms; an unresolvable name is undefined. */
export interface TracedWrite { readonly paths?: readonly string[]; readonly at: number; readonly opened: boolean; readonly mapped?: true }

/** Read complete or growing transcripts through one bounded inventory; only newline-terminated records advance a tail. */
async function* traceChunks(tracePrefix: string, offsets = new Map<string, number>(), remaining = Infinity) {
	const directory = path.dirname(tracePrefix), prefix = `${path.basename(tracePrefix)}.`;
	for (const name of await readdir(directory)) {
		if (!remaining) break;
		if (!name.startsWith(prefix)) continue;
		const pid = name === `${prefix}stream` ? undefined : Number(name.slice(prefix.length));
		if (pid !== undefined && !isSafeCount(pid, 1)) throw new Error("invalid trace filename");
		await using handle = await open(path.join(directory, name), "r");
		const info = await handle.stat(), offset = offsets.get(name) ?? 0;
		if (!info.isFile() || info.size < offset) throw new Error("trace shrank or is not a regular file");
		const buffer = Buffer.allocUnsafe(Math.min(remaining, info.size - offset));
		const bytes = buffer.subarray(0, (await handle.read(buffer, 0, buffer.length, offset)).bytesRead), complete = bytes.lastIndexOf(10) + 1;
		if (bytes.length !== buffer.length) throw new Error("trace changed while reading");
		offsets.set(name, offset + complete); remaining -= bytes.length;
		yield { pid, bytes, remaining };
	}
}

/** A trace read as it grows, each record parsed once however often it is asked: its writes so far, and when it last named each absolute path (by name, through a
 * descriptor, or against the cwd its process followed from `cwd`, unknown until its fork is on record; a failed lookup names one too), in wall-clock milliseconds. */
export interface TraceTail { readonly writes: readonly TracedWrite[]; readonly seen: ReadonlyMap<string, number>; readonly unsafe: boolean; read(): Promise<TraceTail> }

export function traceTail(tracePrefix: string, cwd: string): TraceTail {
	const writes: TracedWrite[] = [], seen = new Map<string, number>(), offsets = new Map<string, number>(),
		cwds = new Map<number, { cwd?: string }>(), decode = traceDecoder(); // One process's cwd, or several's a fork let share it.
	let reading: Promise<unknown> = Promise.resolve(), root: number | undefined, unsafe = false;
	const advance = async () => {
		const records: TraceRecord[] = [];
		for await (const { pid, bytes } of traceChunks(tracePrefix, offsets)) {
			for (const lines of decode(bytes.toString("utf8"), pid, true).values()) for (const line of lines) records.push(line);
		}
		records.sort((a, b) => a.at - b.at); root ??= records[0]?.pid; // A fork precedes its child's records, a chdir the forks after it; the first is the root's.
		for (const line of records) {
			const { pid, at } = line;
			// Path mutations are journaled below. Waiting or installing an image cannot itself mutate file data or names.
			unsafe ||= !Number.isFinite(at) || !!line.failure && !/^(?:wait(?:4|id|pid)|execve(?:at)?)$/.test(line.name) || syscallSucceeded(line) &&
				(/^(?:mount|umount2?|move_mount|pivot_root|chroot|setns|unshare|io_uring_setup|io_submit|[fl]?setxattr|[fl]?removexattr)$/.test(line.name) || line.name === "ioctl" && unmodeledFileIoctl(line));
			if (line.failure) continue;
			const process = cwds.get(pid) ?? (pid === root ? cwds.set(pid, { cwd }).get(pid) : undefined), paths = syscallPaths(line, process?.cwd ?? ""), child = spawnedPID(line);
			const mapped = line.name === "mmap" && /\bMAP_SHARED(?:_VALIDATE)?\b/.test(line.args[3] ?? "");
			if (syscallSucceeded(line) && (writesPath(line) || mapped)) writes.push({ ...(paths?.every(target => target.startsWith("/")) ? { paths } : {}), at,
				opened: /^open(?:at2?)?$/.test(line.name) || line.name === "mmap", ...(line.name === "mmap" ? { mapped: true } : {}) });
			for (const target of [...paths ?? [], ...line.args.flatMap(arg => absoluteDescriptorPath(arg) ?? [])]) if (target.startsWith("/")) seen.set(target, Math.max(seen.get(target) ?? 0, at));
			if (process) process.cwd = tracedCwd(line, process.cwd);
			if (child && !cwds.has(child)) { const shared = sharesFilesystem(line); cwds.set(child, shared && process ? process : { cwd: shared === false ? process?.cwd : undefined }); }
		}
	};
	return { writes, seen, get unsafe() { return unsafe; }, read() { const next = reading.then(advance); reading = next.catch(() => { unsafe = true; }); return next.then(() => this); } };
}

/** Whether writes reached `targets` within [since, until], or held one open to write by `until`. An unresolvable write counts. */
export function writesWithin(writes: readonly TracedWrite[], targets: ReadonlySet<string>, since: number, until: number): boolean {
	return writes.some(({ paths, at, opened }) => at <= until && (opened || at >= since) && (!paths || paths.some(target => targets.has(target))));
}

function writesPath(line: TraceLine): boolean {
	return /^(?:creat|truncate|rename|renameat2?|link|linkat|symlink|symlinkat|mknod|mknodat|unlink|unlinkat|mkdir|mkdirat|rmdir|utimes?|utimensat|futimesat|chmod|fchmodat|chown|fchownat|writev?|pwrite(?:64|v2?)|ftruncate|fchmod|fchown|fallocate|sendfile|copy_file_range|splice|tee)$/.test(line.name) ||
		line.name === "mmap" && /\bPROT_WRITE\b/.test(line.args[2] ?? "") && /\bMAP_SHARED(?:_VALIDATE)?\b/.test(line.args[3] ?? "") ||
		/^open(?:at2?)?$/.test(line.name) && /\bO_(?:WRONLY|RDWR|CREAT|TRUNC)\b/.test(line.args.join(" "));
}

/** Only the owned tracer can write this marker, after successful adoption and before resuming. */
function brokeredProcessSegments(selected: ReadonlyMap<number, TraceProcess>, options: StraceObservationOptions) {
	const brokered = new Map<number, number>();
	for (const [pid, { lines, start }] of selected) for (let index = start; index < lines.length; index++) {
		const line = lines[index]!;
		if (line.name !== "pi_brokered_exec") continue;
		if (!syscallSucceeded(line) || index <= start || !successfulExec(lines[index - 1]!) || brokered.has(pid) || options.brokeredWrites?.(pid) === undefined ||
			lines.slice(index + 1).some(next => next.failure || next.name && !/^(?:exit|exit_group|terminated)$/.test(next.name))) throw new Error(`unproven brokered exec: ${pid}`);
		brokered.set(pid, index - 1);
	}
	return brokered;
}

function tracedCwd(line: TraceLine, cwd: string | undefined): string | undefined {
	return syscallSucceeded(line) && /^(?:chdir|fchdir)$/.test(line.name) ? syscallPaths(line, cwd ?? "")?.[0] : cwd;
}

function successfulExec(line: TraceLine): boolean { return (line.name === "execve" || line.name === "execveat") && syscallSucceeded(line); }

function spawnedPID(line: TraceLine): number | undefined {
	if (!["clone", "clone3", "fork", "vfork"].includes(line.name)) return undefined;
	const pid = Number(line.result);
	return isSafeCount(pid, 1) ? pid : undefined;
}

function sharesFilesystem(line: TraceLine): boolean | undefined {
	if (line.name === "fork" || line.name === "vfork") return false;
	const flags = line.name === "clone3" ? /^\{flags=([^,}]+)(?:,|})/.exec(line.args[0] ?? "")?.[1]
		: line.args.find((argument) => argument.startsWith("flags="))?.slice(6);
	if (!flags) return undefined;
	let shared = false;
	for (const flag of flags.split("|")) {
		const number = parseInteger(flag);
		if (number !== undefined) shared ||= (number & 0x200n) !== 0n;
		else if (flag === "CLONE_FS") shared = true;
		else if (!/^(?:CLONE_[A-Z0-9_]+|SIG[A-Z0-9]+)$/.test(flag)) return undefined;
	}
	return shared;
}

function syscallSucceeded(line: TraceLine): boolean { return /^(?:0x[0-9a-f]+|[0-9]+)(?:\b|<)/i.test(line.result); }

function syscallPaths(line: TraceLine, cwd: string): readonly string[] | undefined {
	const paths = (PATH_ARGUMENTS[line.name] ?? []).map(([pathname, dirfd, descriptorPath]) => {
		// strace names AT_FDCWD by the kernel's cwd, which a sandbox serving chdir itself never moves: the traced cwd is the process's.
		const descriptor = dirfd === undefined ? undefined : line.args[dirfd], current = dirfd === undefined || /^(?:AT_FDCWD|-100)(?:<|$)/.test(descriptor ?? "");
		if (pathname === undefined || descriptorPath && /^(?:""|NULL)$/.test(line.args[pathname] ?? "")) {
			const target = current ? cwd : absoluteDescriptorPath(descriptor); return target ? [target] : current || /^\d+$/.test(descriptor ?? "") ? undefined : [];
		}
		const value = quotedArgument(line.args[pathname]);
		if (value?.startsWith("/")) return [walkedPath(value)]; // Absolute names ignore dirfd, even an invalid one.
		const base = current ? cwd : absoluteDescriptorPath(descriptor);
		// Without a name or a directory, a failed call looked nothing up and a successful one is unresolved.
		return value && base ? [walkedPath(`${base}/${value}`)] : syscallSucceeded(line) ? undefined : [];
	});
	return paths.includes(undefined) ? undefined : paths.flat() as string[];
}

/** Keep `..`: only a walk over the recorded tree knows whether it leaves a symlinked directory. */
function walkedPath(value: string): string {
	return `/${value.split("/").filter((segment) => segment && segment !== ".").join("/")}`;
}

function absoluteDescriptorPath(descriptor: string | undefined): string | undefined {
	const target = /^\d+<(.+)>$/.exec(descriptor?.trim() ?? "")?.[1]?.replace(/<[^<>]*>$/, "");
	return target?.startsWith("/") && !target.endsWith(" (deleted)") ? path.posix.normalize(decodeCString(target)) : undefined;
}

/** Non-path descriptors, and memfds such as Sandlock's virtual /proc/mounts (its content comes from the opened path), are
 * already typed in the process key, but their kernel identity is volatile. */
function descriptorTarget(line: TraceLine): boolean {
	const target = /^\d+<(.+)>(?:\(deleted\))?$/.exec(line.args[0] ?? "")?.[1]; // strace marks an unlinked memfd after its bracket
	return Boolean(target && (!target.startsWith("/") || target.startsWith("/memfd:")));
}

const STAT_MODE_BITS: Readonly<Record<string, bigint>> = { S_IFSOCK: 0o140000n, S_IFLNK: 0o120000n, S_IFREG: 0o100000n, S_IFBLK: 0o060000n,
	S_IFDIR: 0o040000n, S_IFCHR: 0o020000n, S_IFIFO: 0o010000n, S_ISUID: 0o004000n, S_ISGID: 0o002000n, S_ISVTX: 0o001000n };

/** Normalize the successful kernel stat structure printed by strace -v; statx prints the same fields under its own names. */
type StatObservation = { readonly digest: Sha256Digest; readonly fields?: readonly FilesystemObservationField[] };

/** Statx masks select returned fields; unknown bits require the full stat image. */
const STATX_FIELD_MASKS: Readonly<Record<string, readonly FilesystemObservationField[]>> = { STATX_TYPE: ["mode"], STATX_MODE: ["mode"],
	STATX_NLINK: ["nlink"], STATX_UID: ["uid"], STATX_GID: ["gid"], STATX_ATIME: ["atimeNs"], STATX_MTIME: ["mtimeNs"], STATX_CTIME: ["ctimeNs"],
	STATX_INO: ["ino"], STATX_SIZE: ["size"], STATX_BLOCKS: ["blocks"], STATX_BASIC_STATS: FILESYSTEM_OBSERVATION_FIELDS, STATX_ALL: FILESYSTEM_OBSERVATION_FIELDS,
	...Object.fromEntries(["MNT_ID", "MNT_ID_UNIQUE", "BTIME", "DIOALIGN", "DIO_READ_ALIGN", "SUBVOL", "WRITE_ATOMIC"].map((flag) => [`STATX_${flag}`, []])) };

function statObservationDigest(structure: string): StatObservation | undefined {
	const flags = /\bstx_mask=([A-Z_|0-9x]+)/.exec(structure)?.[1]?.split("|");
	const mask = flags?.every((flag) => STATX_FIELD_MASKS[flag]) ? flags.flatMap((flag) => STATX_FIELD_MASKS[flag]!) : undefined;
	const line = structure.replace(/\bstx_(r?dev)_major=(\w+), stx_\1_minor=(\w+)/g, "st_$1=makedev($2, $3)")
		.replace(/\bstx_([amc]time)=\{tv_sec=(-?\d+), tv_nsec=(\d+)\}/g, "st_$1=$2, st_$1_nsec=$3").replace(/\bstx_/g, "st_");
	const field = (name: string): bigint | undefined => parseInteger(new RegExp(`\\b${name}=(-?(?:0x[0-9a-f]+|0[0-7]+|[0-9]+))`, "i").exec(line)?.[1]);
	const device = (name: string): bigint | undefined => {
		const match = new RegExp(`\\b${name}=makedev\\(([^,]+),\\s*([^\\)]+)\\)`).exec(line);
		if (!match) return field(name) ?? (name === "st_rdev" ? 0n : undefined);
		const major = parseInteger(match[1]);
		const minor = parseInteger(match[2]);
		return major === undefined || minor === undefined ? undefined : linuxDevice(major, minor);
	};
	const modeText = /\bst_mode=([^,}]+)/.exec(line)?.[1]?.trim();
	const mode = modeText?.split("|").reduce<bigint | undefined>((combined, token) => {
		const bits = STAT_MODE_BITS[token] ?? parseInteger(token);
		return bits === undefined || combined === undefined ? undefined : combined | bits;
	}, 0n);
	const time = (name: string): bigint | undefined => {
		const seconds = field(name), nanos = field(`${name}_nsec`);
		return seconds === undefined || nanos === undefined ? undefined : seconds * 1_000_000_000n + nanos;
	};
	const fields = mask ? FILESYSTEM_OBSERVATION_FIELDS.filter(field => mask.includes(field)) : FILESYSTEM_OBSERVATION_FIELDS;
	const evidence = Object.fromEntries(fields.map(name => [name,
		name === "mode" ? mode
		: name === "dev" || name === "rdev" ? device(`st_${name}`)
		: name.endsWith("Ns") ? time(`st_${name.slice(0, -2)}`) : field(`st_${name}`),
	]));
	if (fields.some(field => evidence[field] === undefined)) return undefined;
	return { digest: filesystemObservationDigest(evidence, fields), ...(fields.length < FILESYSTEM_OBSERVATION_FIELDS.length ? { fields } : {}) };
}

function parseInteger(value: string | undefined): bigint | undefined {
	if (!value) return undefined;
	const normalized = value.trim();
	try {
		if (/^-?0x[0-9a-f]+$/i.test(normalized)) return BigInt(normalized);
		if (/^-?0[0-7]+$/.test(normalized)) {
			const negative = normalized.startsWith("-");
			const magnitude = BigInt(`0o${normalized.replace(/^-?0/, "") || "0"}`);
			return negative ? -magnitude : magnitude;
		}
		return /^-?[0-9]+$/.test(normalized) ? BigInt(normalized) : undefined;
	} catch {
		return undefined;
	}
}

/** Linux's userspace-compatible new_encode_dev layout. */
function linuxDevice(major: bigint, minor: bigint): bigint {
	return ((major & 0xfffn) << 8n) | (minor & 0xffn) | ((minor & ~0xffn) << 12n) | ((major & ~0xfffn) << 32n);
}

function quotedArgument(argument: string | undefined): string | undefined {
	const match = /^"((?:\\.|[^"\\])*)"$/.exec(argument ?? "");
	return match ? decodeCString(match[1]!) : undefined;
}

function decodeCString(value: string): string {
	// Paths must round-trip through Node's UTF-8 APIs; queue payloads remain bytes.
	return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(decodeCBytes(value));
}

function queueVectors(value: string | undefined, count: number): { data: Buffer; length: number } | undefined {
	if (!value || !/^\[(?:\{iov_base="(?:\\.|[^"\\])*", iov_len=\d+\}(?:, )?)*\]$/.test(value)) return;
	const vectors = [...value.matchAll(/\{iov_base="((?:\\.|[^"\\])*)", iov_len=(\d+)\}/g)];
	if (vectors.length !== count) return;
	return { data: Buffer.concat(vectors.map(vector => decodeCBytes(vector[1]!))), length: vectors.reduce((sum, vector) => sum + Number(vector[2]), 0) };
}

function decodeCBytes(value: string): Buffer {
	const chunks: Buffer[] = [];
	let start = 0;
	for (const match of value.matchAll(/\\(?:x([0-9a-fA-F]{2})|([0-7]{1,3})|(.))/g)) {
		chunks.push(Buffer.from(value.slice(start, match.index), "utf8"));
		const byte = match[1] ? Number.parseInt(match[1], 16) : match[2] ? Number.parseInt(match[2], 8) :
			({ a: 7, b: 8, f: 12, n: 10, r: 13, t: 9, v: 11 } as Record<string, number>)[match[3]!] ?? match[3]!.charCodeAt(0);
		chunks.push(Buffer.from([byte]));
		start = match.index + match[0].length;
	}
	chunks.push(Buffer.from(value.slice(start), "utf8"));
	return Buffer.concat(chunks);
}

function sharedObjectRole(observedPath: string, role: DependencyRole): DependencyRole {
	return role === "input" && /(?:^|\/)lib[^/]*\.so(?:\.|$)/.test(observedPath) ? "shared_object" : role;
}

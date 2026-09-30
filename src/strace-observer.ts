import { open, readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { directoryStatFields, hostStatFields, repeatableExecutions, SHELLS, WITHOUT_IDENTITY, workspaceStatFields, type TracedExecution } from "./deterministic-tools.ts";
import { containsLogicalPath } from "./path-utils.ts";
import { type DependencyRole, FILESYSTEM_OBSERVATION_FIELDS, type FilesystemObservationField, filesystemObservationDigest, ONE_SHOT_TAINTS, type ProvenanceTaint,
	type Sha256Digest, type ResourceTransitionKind } from "./provenance-certificate.ts";

const CONFINEMENT_SENSITIVE_SYSCALLS = new Set([
	"seccomp", "capget", "capset", "mount", "umount2", "pivot_root", "swapon", "swapoff", "reboot",
	"sethostname", "setdomainname", "kexec_load", "init_module", "finit_module", "delete_module", "unshare", "setns",
	"perf_event_open", "bpf", "userfaultfd", "keyctl", "add_key", "request_key", "ptrace", "process_vm_readv",
	"process_vm_writev", "open_by_handle_at", "name_to_handle_at", "quotactl", "acct", "lookup_dcookie",
	"io_setup", "io_submit", "io_uring_setup", "io_uring_enter", "io_uring_register", "personality",
]);
const SYSCALL_FILTER = `trace=%file,%process,%network,%ipc,getpid,getppid,getsid,getpgid,clock_gettime,gettimeofday,time,getrandom,sysinfo,times,getrusage,getrlimit,setrlimit,prlimit64,fchdir,fallocate,pipe2,splice,tee,ioctl,prctl,fstat,fstatfs,getdents,getdents64,fcntl,fcntl64,flock,${[...CONFINEMENT_SENSITIVE_SYSCALLS].join(",")}`;

/** One production trace shape shared by execution and dependency-ablation paths. */
export function straceCommand(
	strace: string,
	tracePrefix: string,
	command: readonly string[],
	streams = false,
	continuation = false,
): readonly string[] {
	// The command runs under a sandbox launcher whose supervisor threads would otherwise pay a ptrace stop per call they serve.
	return [strace, "--kill-on-exit", "--trace-children-only", streams ? "-f" : "-ff", "-q", "-ttt", "-yy", "-v", "-s", "65535", "-e", continuation ? "trace=all" : SYSCALL_FILTER + (streams ? ",read,write,readv,writev,close,close_range,dup,dup2,dup3,eventfd,eventfd2,sendfile,vmsplice,poll,ppoll,select,pselect6,epoll_create,epoll_create1,epoll_ctl,epoll_wait,epoll_pwait,epoll_pwait2" : ""), "-o", streams ? `${tracePrefix}.stream` : tracePrefix, ...command];
}

export type ObservedProcessPath =
	/** `listed` when its entries reached the run: it read them, or a removal or rename found some. */
	| { readonly path: string; readonly role: DependencyRole; readonly listed?: true }
	| { readonly path: string; readonly role: "metadata"; readonly followSymlinks: boolean; readonly digest: Sha256Digest; readonly fields?: readonly FilesystemObservationField[] };

export interface StraceObservation {
	readonly complete: boolean;
	readonly paths: readonly ObservedProcessPath[];
	readonly taints: readonly ProvenanceTaint[];
	readonly tracedProcesses: number;
	/** Every process its trace holds, a brokered run's launcher among them. */
	readonly pids?: readonly number[];
	readonly incompleteReasons: readonly string[];
	readonly resourceJournal?: readonly { readonly inode: string; readonly description?: number; readonly kind: ResourceTransitionKind; readonly data: Buffer; readonly requested?: number }[];
	readonly retainedDescriptions?: readonly number[];
	readonly finalHandles?: readonly { readonly fd: number; readonly description?: number; readonly cloexec: boolean }[];
	/** Processes whose every intercepted exec resumed at its native image in place. */
	readonly resumedInterpositions?: readonly number[];
	/** Workspace paths, relative to it, the traced processes created, removed, renamed or opened to write. */
	readonly written?: readonly string[];
	/** Paths outside the workspace it wrote, which a private branch took. */
	readonly external?: readonly string[];
	/** Locks it took without waiting, or found free: each holds while no one else holds one. */
	readonly locks?: readonly { readonly path: string; readonly exclusive: boolean }[];
}

export interface StraceObservationOptions {
	/** The workspace names a run brokered from a traced launcher pid wrote, its own brokered runs included. */
	readonly brokeredWrites?: (pid: number) => readonly string[] | undefined;
	/** The session's private branch, where a descriptor on a copy shows the copy: it stands for the host path it shadows. */
	readonly privateUpper?: string;
	/** Bounded prefix lookup for running work; its evidence is always incomplete. */
	readonly previewBytes?: number;
	/** An owned tracer flushed this exact byte boundary while the target was stopped in restartable I/O. */
	readonly frozen?: { readonly pid: number; readonly fd: number; readonly syscall: string; readonly bytes: number };
	/** Intercepted path to native target; a later exec of the target in the same process proves descriptor-preserving bypass. */
	readonly interposedExecutables?: readonly (readonly [intercepted: string, original: string])[];
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
	readonly inheritedHandles?: readonly { readonly fd: number; readonly installed?: false; readonly description?: number; readonly inode: string; readonly flags: number; readonly outside: number; readonly queuedBytes?: number; readonly queueData?: Buffer; readonly packet?: boolean; readonly messages?: readonly import("./linux-held-exec.ts").QueueMessage[] }[];
}

interface TraceFile { readonly pid: number; readonly lines: readonly TraceLine[]; readonly terminated: boolean; readonly exitCode?: number; }

type TraceRoot = { readonly file: TraceFile; readonly start: number };
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
/** -ttt stamps each record with its wall-clock start, which orders overlapping workspace writers. */
const untimed = (line: string) => line.replace(/^\d+\.\d+ /, "");

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

function reassembleSyscalls(lines: readonly string[], pid: number, order?: readonly number[]): readonly TraceLine[] {
	const pending = new Map<string, Array<{ text: string; order?: number }>>();
	const complete: TraceLine[] = [];
	for (const [index, line] of lines.entries()) {
		const append = (text: string, sequence = order?.[index]) => complete.push({ ...parseTraceLine(text), ...(sequence !== undefined ? { order: sequence } : {}) });
		const unfinished = line.includes("<unfinished ...>") ? /^\s*([a-zA-Z0-9_]+)\(.*\s<unfinished \.\.\.>\s*$/.exec(line) : null;
		if (unfinished) {
			const name = unfinished[1]!;
			(pending.get(name) ?? pending.set(name, []).get(name)!).push({ text: line.replace(/\s*<unfinished \.\.\.>\s*$/, ""), order: order?.[index] });
			continue;
		}
		const resumed = /^\s*<\.\.\.\s*([a-zA-Z0-9_]+) resumed>(.*)$/.exec(line);
		if (!resumed) { append(line); continue; }
		const name = resumed[1]!;
		const queue = pending.get(name);
		const prefix = queue?.shift();
		if (!prefix) complete.push({ name, args: [], result: "", failure: `resumed_without_unfinished:${pid}:${name}` });
		else append(`${prefix.text}${resumed[2]}`, /^(?:write|writev|sendto|sendmsg|fork|vfork|clone|clone3)$/.test(name) ? prefix.order : order?.[index]);
		if (queue?.length === 0) pending.delete(name);
	}
	for (const name of pending.keys()) complete.push({ name, args: [], result: "", failure: `unfinished:${pid}:${name}` });
	return complete;
}

/** Reduce handle lifetimes to object releases. Aliases and CLONE_FILES share references;
 * fork copies them. This is the same journal used for queue effects, not another replay path. */
function resourceTransitions(processes: ReadonlyMap<number, TraceProcess>, root: number, options: StraceObservationOptions) {
	const handles = options.inheritedHandles ?? [];
	type Handle = { inode: string; description?: number; access: number; cloexec: boolean; packet?: boolean; epoll?: Map<string, { handle: Handle; mask: number; data: string; armed: boolean; generation?: number }> };
	const descriptions = new Map<number, Handle>(handles.map(handle =>
		[handle.fd, { inode: handle.inode, description: handle.description ?? handle.fd, access: (handle.flags & 3) + 1, cloexec: false, packet: handle.packet }]));
	const tables = new Map<number, Map<number, Handle>>([[root, new Map(handles.filter(handle => handle.installed !== false)
		.map(handle => [handle.fd, descriptions.get(handle.fd)!]))]]);
	const outside = new Map(handles.map(handle => [handle.inode, handle.outside]));
	const journal: Array<NonNullable<StraceObservation["resourceJournal"]>[number] & { order: number }> = [];
	const handled = new Set<TraceLine>();
	const peers = new Map<string, string>(), generations = new Map<string, number>();
	const cursors = new Map<string, { read: number; end: number }>(handles.map(handle => [handle.inode, { read: 0, end: handle.queuedBytes ?? 0 }]));
	const contents = new Map(handles.filter(handle => handle.queueData).map(handle => [handle.inode, handle.queueData!]));
	const messages: Array<{ inode: string; start: number; end: number; rights: Handle[] }> = handles.flatMap(handle => (handle.messages ?? []).map(message => ({
		inode: handle.inode, start: message.start, end: message.end, rights: message.rights.map(fd => {
			const reference = descriptions.get(fd);
			if (!reference) throw new Error("unproven queued descriptor");
			return reference;
		}),
	})));
	const wake = (inode: string | undefined) => { if (inode !== undefined) generations.set(inode, (generations.get(inode) ?? 0) + 1); };
	const references = () => {
		const masks = new Map<string, number>();
		for (const table of new Set(tables.values())) for (const handle of table.values())
			masks.set(handle.inode, (masks.get(handle.inode) ?? 0) | handle.access);
		for (const message of messages) for (const handle of message.rights)
			masks.set(handle.inode, (masks.get(handle.inode) ?? 0) | handle.access);
		return masks;
	};
	const liveDescriptions = () => new Set([...new Set(tables.values())].flatMap(table => [...table.values()].map(handle => handle.description))
		.concat(messages.flatMap(message => message.rights.map(handle => handle.description))));
	const records = [...processes].flatMap(([pid, process]) => process.file.lines.slice(process.start).map(line => ({ pid, line })))
		.sort((a, b) => (a.line.order ?? Infinity) - (b.line.order ?? Infinity));
	for (const { pid, line } of records) {
		let table = tables.get(pid);
		if (!table || line.order === undefined) continue;
		const lifetime = /^(?:close|close_range|dup|dup2|dup3|fcntl|fcntl64|execve|execveat|terminated)$/.test(line.name);
		const before = lifetime || messages.length && /^(?:read|readv|recvfrom|recvmsg)$/.test(line.name) ? references() : undefined;
		const previousDescriptions = before ? liveDescriptions() : undefined;
		const endpoint = (argument: string | undefined): Handle | undefined => {
			const file = table!.get(Number.parseInt(argument ?? "", 10));
			if (file?.inode.startsWith("file:") && /^\d+<\//.test(argument ?? "")) return file;
			const counter = /^(\d+)<anon_inode:\[eventfd\]>$/.exec(argument ?? "");
			if (counter) {
				const handle = table!.get(Number(counter[1]));
				if (handle?.inode.startsWith("eventfd:")) { peers.set(handle.inode, handle.inode); return handle; }
				return;
			}
			const match = /^(\d+)<(?:pipe|UNIX(?:-[A-Z]+)?):\[(\d+)(?:->(\d+))?\]>$/.exec(argument ?? "");
			if (!match || !options.inheritedStreams?.includes(match[2]!)) return;
			if (match[3]) { peers.set(match[2]!, match[3]); peers.set(match[3], match[2]!); }
			else if (argument!.includes("<pipe:")) peers.set(match[2]!, match[2]!);
			const origin = table!.get(Number(match[1]));
			return origin?.inode === match[2] ? origin : { inode: match[2]!, access: 3, cloexec: false };
		};
		const emit = (handle: Handle, kind: ResourceTransitionKind, data: Buffer, requested?: number) => {
			journal.push({ order: line.order!, inode: handle.inode, description: handle.description, kind, data, ...(requested !== undefined ? { requested } : {}) }); handled.add(line);
			const length = kind.endsWith("_message") ? data.length - 4 - 4 * data.readUInt32LE() : data.length;
			if (kind === "consume" || kind === "receive_message") {
				const packet = handle.packet ? messages.find(message => message.inode === handle.inode) : undefined;
				const consumed = packet ? packet.end - packet.start : length;
				const bytes = contents.get(handle.inode); if (bytes) contents.set(handle.inode, bytes.subarray(consumed));
				const cursor = cursors.get(handle.inode);
				if (cursor) { cursor.read += consumed; for (let index = messages.length - 1; index >= 0; index--)
					if (packet ? messages[index] === packet : messages[index]!.inode === handle.inode && messages[index]!.start < cursor.read) messages.splice(index, 1); }
			}
			if (kind === "produce" || kind === "send_message") {
				const cursor = cursors.get(peers.get(handle.inode) ?? "");
				if (kind === "produce" && handle.packet && cursor) messages.push({ inode: peers.get(handle.inode)!, start: cursor.end, end: cursor.end + length, rights: [] });
				if (cursor) cursor.end += length;
				const peer = peers.get(handle.inode), bytes = peer && contents.get(peer);
				if (peer && bytes) contents.set(peer, Buffer.concat([bytes, data.subarray(data.length - length)]));
			}
			if (kind === "produce" || kind === "send_message" || kind === "shutdown") wake(peers.get(handle.inode));
			if (kind === "shutdown") wake(handle.inode);
		};
		const readiness = (handle: Handle, requested: number, returned: number, projection = 0) => {
			const data = Buffer.alloc(12); data.writeUInt32LE(requested); data.writeUInt32LE(returned, 4); data.writeUInt32LE(projection, 8); emit(handle, "ready", data);
		};
		const stream = endpoint(line.args[0]), syscall = line.name;
		if (stream?.inode.startsWith("file:")) { const operation = fileLockTransition(line); if (operation) emit(stream, "lock", operation); }
		if (stream && !stream.inode.startsWith("file:")) {
			const counter = stream.inode.startsWith("eventfd:");
			const flags = line.args[3] ?? "";
			const produced = syscall === "write" || syscall === "writev" || syscall === "sendto" && /^(?:0|MSG_(?:NOSIGNAL|DONTWAIT))(?:\|MSG_(?:NOSIGNAL|DONTWAIT))*$/.test(flags) && line.args[4] === "NULL" && line.args[5] === "0";
			const consumed = syscall === "read" || syscall === "readv" || syscall === "recvfrom" && /^(?:0|MSG_(?:PEEK|DONTWAIT|WAITALL|TRUNC))(?:\|MSG_(?:PEEK|DONTWAIT|WAITALL|TRUNC))*$/.test(flags) && line.args[4] === "NULL" && /^(?:NULL|0x0)$/.test(line.args[5] ?? "");
			const bytes = /^"((?:\\.|[^"\\])*)"$/.exec(line.args[1] ?? "");
			const vector = syscall === "readv" || syscall === "writev" ? queueVectors(line.args[1], Number(line.args[2])) : undefined;
			const data = bytes ? decodeCBytes(bytes[1]!) : vector?.data, requested = vector?.length ?? Number(line.args[2]);
			if ((produced || consumed) && data && /^\d+$/.test(line.result)) {
				const returned = Number(line.result), truncated = /MSG_TRUNC/.test(flags);
				const packet = truncated && stream.packet ? messages.find(message => message.inode === stream.inode) : undefined;
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
				const packet = truncated && stream.packet ? messages.find(message => message.inode === stream.inode) : undefined;
				if (message && (!truncated || packet && returned === packet.end - packet.start) && (stream.packet || !message.truncated) && /^(?:0|MSG_(?:NOSIGNAL|DONTWAIT|PEEK|WAITALL|CMSG_CLOEXEC|TRUNC))(?:\|MSG_(?:NOSIGNAL|DONTWAIT|PEEK|WAITALL|CMSG_CLOEXEC|TRUNC))*$/.test(flags) &&
					(sent ? !/PEEK|WAITALL|CMSG_CLOEXEC|TRUNC/.test(flags) && message.data.length === message.length && length <= message.length && length <= 4096 :
						!/NOSIGNAL/.test(flags) && message.data.length === length)) {
					const peer = peers.get(stream.inode), cursor = cursors.get(stream.inode);
					const pending = messages.find(message => message.inode === stream.inode && (stream.packet || message.start < (cursor?.read ?? 0) + length));
					const rights = sent ? message.rights.map(fd => table!.get(fd)) : pending?.rights ?? [];
					if (rights.length === message.rights.length && rights.every(handle => handle?.description !== undefined) &&
						(sent ? !rights.length || !!peer && (length > 0 || stream.packet) : message.rights.every(fd => !table!.has(fd)))) {
						const handles = rights as Handle[], data = Buffer.alloc(4 + 4 * handles.length + length);
						data.writeUInt32LE(handles.length); handles.forEach((handle, index) => data.writeUInt32LE(handle.description!, 4 + 4 * index));
						message.data.copy(data, 4 + 4 * handles.length, 0, length);
						if (sent && (handles.length || stream.packet)) { const start = cursors.get(peer!)?.end ?? 0; messages.push({ inode: peer!, start, end: start + length, rights: handles }); }
						if (!sent) handles.forEach((handle, index) => table!.set(message.rights[index]!, { ...handle, cloexec: /MSG_CMSG_CLOEXEC/.test(flags) }));
						emit(stream, sent ? "send_message" : /MSG_PEEK/.test(flags) ? "peek_message" : "receive_message", data, message.length);
					}
				}
			}
		}
		if ((syscall === "splice" || syscall === "tee") && stream?.description !== undefined && /^\d+$/.test(line.result)) {
			const moved = syscall === "splice", target = endpoint(line.args[moved ? 2 : 1]), length = Number(line.result), requested = Number(line.args[moved ? 4 : 2]);
			const bytes = contents.get(stream.inode), flags = line.args[moved ? 5 : 3];
			if (target && stream.inode !== target.inode && bytes && length <= bytes.length && length <= requested && length <= 4096 &&
				/^(?:0|SPLICE_F_(?:MOVE|MORE|NONBLOCK))(?:\|SPLICE_F_(?:MOVE|MORE|NONBLOCK))*$/.test(flags ?? "") &&
				(!moved || line.args[1] === "NULL" && line.args[3] === "NULL")) {
				const data = Buffer.alloc(4 + length); data.writeUInt32LE(stream.description); bytes.copy(data, 4, 0, length);
				emit(target, moved ? "splice" : "tee", data, requested);
				const destination = contents.get(target.inode); if (destination) contents.set(target.inode, Buffer.concat([destination, bytes.subarray(0, length)]));
				const end = cursors.get(target.inode); if (end) end.end += length;
				if (moved) { contents.set(stream.inode, bytes.subarray(length)); const cursor = cursors.get(stream.inode); if (cursor) cursor.read += length; }
				wake(target.inode);
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
			table.set(Number.parseInt(line.result, 10), { inode: `epoll:${line.order}`, access: 3, cloexec: /EPOLL_CLOEXEC/.test(line.args[0] ?? ""), epoll: new Map() }); handled.add(line);
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
				(!(value.mask & 0x80000000) || value.generation !== (generations.get(value.handle.inode) ?? 0)));
			if (events && events.length === returned && returned <= Number(line.args[2]) && new Set(registrations.map(entry => entry.data)).size === registrations.length &&
				events.every(event => registrations.some(entry => entry.data === event.data))) {
				for (const entry of registrations) {
					const event = events.find(event => event.data === entry.data);
					if (event || returned < Number(line.args[2])) readiness(entry.handle, entry.mask & ~0xc0000000, event?.mask ?? 0);
					if (event && (entry.mask & 0x80000000)) entry.generation = generations.get(entry.handle.inode) ?? 0;
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
		const after = references();
		const descriptions = liveDescriptions();
		for (const description of previousDescriptions!) if (description !== undefined && !descriptions.has(description)) {
			const handle = handles.find(handle => (handle.description ?? handle.fd) === description);
			if (handle?.inode.startsWith("file:")) journal.push({ order: line.order, inode: handle.inode, description, kind: "release", data: Buffer.from([3]) });
		}
		for (const table of new Set(tables.values())) for (const handle of table.values()) if (handle.epoll)
			for (const [key, entry] of handle.epoll) if (!descriptions.has(entry.handle.description)) handle.epoll.delete(key);
		for (const [inode, mask] of before) {
			const released = mask & ~(after.get(inode) ?? 0) & ~(outside.get(inode) ?? 3);
			if (released && options.inheritedStreams?.includes(inode)) { journal.push({ order: line.order, inode, kind: "release", data: Buffer.from([released]) }); wake(peers.get(inode)); }
		}
	}
	return { journal, handled, retained: [...liveDescriptions()].filter((id): id is number => id !== undefined),
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

function selectTraceRoot(files: readonly TraceFile[], target: string): TraceRoot | { readonly reason: string } {
	const parent = new Map<number, number>();
	const children = new Map<number, number[]>();
	for (const file of files) {
		for (const line of file.lines) {
			const child = spawnedPID(line);
			if (!child) continue;
			const existing = parent.get(child);
			if (existing !== undefined && existing !== file.pid) return { reason: `process_parent_ambiguous:${child}` };
			parent.set(child, file.pid);
			(children.get(file.pid) ?? children.set(file.pid, []).get(file.pid)!).push(child);
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
		const start = file.lines.findIndex((line) => successfulExec(line) && quotedStrings(line)[0] !== undefined && path.posix.resolve(quotedStrings(line)[0]!) === target);
		if (start >= 0) candidates.push({ file, start, depth: processDepth });
	}
	if (!candidates.length) return { reason: "target_exec_not_found" };
	const shallowest = Math.min(...candidates.map((candidate) => candidate.depth));
	const matches = candidates.filter((candidate) => candidate.depth === shallowest);
	if (matches.length !== 1) return { reason: `target_exec_ambiguous:${matches.map(({ file }) => file.pid).sort().join(",")}` };
	const match = matches[0]!;
	// exec does not unshare CLONE_FS. An excluded task must not retain authority over the target cwd.
	if (files.some((file) => file.lines.some((line, index) => spawnedPID(line) && sharesFilesystem(line) !== false &&
		(spawnedPID(line) === match.file.pid || (file === match.file && index < match.start))))) {
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
	const directory = path.dirname(tracePrefix);
	const prefix = `${path.basename(tracePrefix)}.`;
	const files: TraceFile[] = [];
	let remaining = options.frozen?.bytes ?? options.previewBytes, frontier = false;
	if (remaining !== undefined && (!Number.isSafeInteger(remaining) || remaining < 0)) throw new Error("invalid trace preview budget");
	for (const name of await readdir(directory)) {
		if (remaining === 0) break;
		if (!name.startsWith(prefix)) continue;
		const pid = Number.parseInt(name.slice(prefix.length), 10);
		const ordered = name === `${prefix}stream`;
		if (options.frozen && !ordered) throw new Error("continuation requires one ordered trace");
		if (!ordered && (!Number.isSafeInteger(pid) || pid <= 0)) continue;
		const target = path.join(directory, name), bytes = remaining === undefined ? await readFile(target) : await readTrace(target, 0, remaining);
		if (remaining !== undefined) remaining -= bytes.length;
		let contents = bytes.toString("utf8");
		if (options.privateUpper) contents = contents.replaceAll(`<${options.privateUpper}/`, "</");
		const groups = new Map<number, { lines: string[]; order?: number[] }>();
		if (!ordered) groups.set(pid, { lines: contents.split(/\r?\n/).map(untimed) });
		else for (const [order, line] of contents.split(/\r?\n/).entries()) {
			if (!line.trim()) continue;
			const match = /^(\d+)\s+(.*)$/.exec(line);
			if (!match) throw new Error("invalid ordered trace record");
			const process = Number(match[1]), group = groups.get(process) ?? { lines: [], order: [] };
			group.lines.push(untimed(match[2]!)); group.order!.push(order); groups.set(process, group);
		}
		for (const [pid, group] of groups) {
			if (options.frozen?.pid === pid) {
				const pending = group.lines.at(-1) ?? "";
				frontier = /^(?:read|readv|write|writev|sendto|recvfrom|sendmsg|recvmsg)$/.test(options.frozen.syscall) &&
					new RegExp(`^${options.frozen.syscall}\\(${options.frozen.fd}(?:<|,)`).test(pending) && !/\)\s+=/.test(pending);
				if (frontier) { group.lines.pop(); group.order?.pop(); }
				if (group.lines.some(line => /^--- /.test(line))) frontier = false;
			}
			const last = group.lines.filter(line => line.trim()).at(-1) ?? "", exited = /^\+\+\+ exited with (\d+) \+\+\+\s*$/.exec(last)?.[1];
			files.push({ pid, lines: reassembleSyscalls(group.lines, pid, group.order), ...(exited === undefined ? {} : { exitCode: Number(exited) }),
				terminated: exited !== undefined || /^\+\+\+ killed by SIG[A-Z0-9]+(?: \(core dumped\))? \+\+\+\s*$/.test(last) });
		}
	}
	const target = path.posix.resolve(executablePath);
	const root = selectTraceRoot(files, target);
	if ("reason" in root) return { complete: false, paths: [], taints: ["trace_incomplete"], tracedProcesses: 0, incompleteReasons: [root.reason] };

	const byPID = new Map(files.map((file) => [file.pid, file]));
	const selected = new Map<number, TraceProcess>([[root.file.pid, { ...root, cwd: path.posix.resolve(initialCwd), fs: { shared: false, changed: false } }]]);
	let complete = options.previewBytes === undefined;
	const incompleteReasons = new Set<string>(complete ? [] : ["preview_only"]);
	if (options.frozen && (!frontier || root.file.pid !== options.frozen.pid || remaining !== 0)) { complete = false; incompleteReasons.add("continuation_frontier_unproven"); }
	for (const [pid, process] of selected) {
		if (!process.file.terminated && options.frozen?.pid !== pid) { complete = false; incompleteReasons.add(`process_exit_unproven:${pid}`); }
		let cwd = process.cwd;
		for (const line of process.file.lines.slice(process.start)) {
			if (process.fs.shared && (line.name === "chdir" || line.name === "fchdir") && syscallSucceeded(line)) process.fs.changed = true;
			cwd = tracedCwd(line, cwd);
			const child = spawnedPID(line);
			if (!child || selected.has(child)) continue;
			const childFile = byPID.get(child);
			if (!childFile) { complete = false; incompleteReasons.add(`child_trace_missing:${child}`); continue; }
			const shared = sharesFilesystem(line);
			if (shared === undefined) { complete = false; incompleteReasons.add(`clone_flags_unparsed:${pid}`); }
			if (shared !== false) process.fs.shared = true;
			selected.set(child, { file: childFile, start: 0, cwd, fs: shared === false ? { shared: false, changed: false } : process.fs });
		}
	}
	if (options.frozen && selected.size !== 1) { complete = false; incompleteReasons.add("continuation_process_tree"); }

	const paths = new Map<string, DependencyRole>(), listedPaths = new Set<string>(), metadata = new Map<string, Extract<ObservedProcessPath, { role: "metadata" }>>();
	// Native instructions and ELF startup state expose clock/random inputs without a syscall.
	// A complete transcript therefore permits only the existing one-shot transfer, never proof
	// that this process can be repeated. Do not infer unused inputs from their absence here.
	const taints = new Set<ProvenanceTaint>(["clock", "random"]), executions: TracedExecution[] = [], listingPIDs = new Set<number>();
	const images = new Map<number, string>(), statFields = new Map<number, readonly FilesystemObservationField[] | undefined>(); // A process's program, inherited across a fork until it execs.
	const opened = new Map<number, Set<number>>(); // Descriptors a process opened itself, whose status flags it chose.
	const refusedIndexLocks = new Set<number>(), ownPipes = new Set<string>(); // Pipes the traced processes created, by inode.
	// The semantic roots are one workspace seen from the sandbox and from its source: name a file by its place in it.
	const locks = new Map<string, boolean>(), changedMetadata = new Set<string>(), written = new Set<string>(), writable = new Set<string>(), external = new Set<string>(), workspaceName = (target: string) =>
		semanticRoots.flatMap((root) => containsLogicalPath(root, target) ? [`//${path.posix.relative(root, target)}`] : [])[0];
	const { journal: resourceJournal, handled: streamCalls, retained, finalHandles } = options.inheritedHandles?.length || options.inheritedStreams?.length
		? resourceTransitions(selected, root.file.pid, options) : { journal: [], handled: new Set<TraceLine>(), retained: [], finalHandles: [] };
	const interposedExecutables = new Map((options.interposedExecutables ?? []).map(([intercepted, original]) => [path.posix.resolve(intercepted), path.posix.resolve(original)]));
	const semanticRoots = (options.guardFilesystemSemanticsWithin ?? []).map((value) => path.posix.resolve(value));
	const { ignored: ignoredSegments, resumed: resumedInterpositions } = ignoredProcessSegments(selected, interposedExecutables);
	const observeMetadata = (observedPath: string, followSymlinks: boolean, { digest, fields }: StatObservation) => {
		const identity = `metadata:${followSymlinks}:${fields?.join(",") ?? ""}:${observedPath}`;
		if (metadata.get(identity)?.digest !== undefined && metadata.get(identity)?.digest !== digest) changedMetadata.add(observedPath);
		metadata.set(identity, { path: observedPath, role: "metadata", followSymlinks, digest, ...(fields ? { fields } : {}) });
	};
	for (const [pid, { file, start, cwd: initial, fs }] of selected) {
		// -ff files cannot order another task's chdir against this task's pathname lookup.
		if (fs.changed) { complete = false; incompleteReasons.add("shared_cwd_mutation"); continue; }
		let cwd = initial;
		if (!cwd) { complete = false; incompleteReasons.add(`cwd_unknown:${pid}`); cwd = path.posix.resolve(initialCwd); }
		for (let index = start; index < file.lines.length; index++) {
			if (ignoredSegments.get(pid)?.some(([from, to]) => index >= from && index < to)) continue;
			const line = file.lines[index]!;
			if (!line) continue;
			const spawned = spawnedPID(line);
			if (spawned && images.has(pid)) { images.set(spawned, images.get(pid)!); statFields.set(spawned, statFields.get(pid)); }
			if (spawned) opened.set(spawned, new Set(opened.get(pid)));
			const own = opened.get(pid) ?? opened.set(pid, new Set()).get(pid)!, result = Number.parseInt(line.result ?? "", 10);
			// Files and pipes the traced processes created stay private to them, through duplication and inheritance.
			if (/^(?:open|openat|openat2|creat)$/.test(line.name) && Number.isSafeInteger(result) && result >= 0) { own.add(result); if (writesPath(line)) writable.add(absoluteDescriptorPath(line.result) ?? ""); }
			else if (/^pipe2?$/.test(line.name) && result === 0) for (const pipe of (line.args[0] ?? "").matchAll(/<pipe:\[(\d+)\]>/g)) ownPipes.add(pipe[1]!);
			else if (line.name === "socketpair" && result === 0) for (const end of (line.args[3] ?? "").matchAll(/<UNIX(?:-[A-Z]+)?:\[(\d+)/g)) ownPipes.add(end[1]!);
			else if (/^dup[23]?$|^fcntl(?:64)?$/.test(line.name) && (line.name.startsWith("dup") || /^F_DUPFD/.test(line.args[1] ?? "")) && Number.isSafeInteger(result) && result >= 0) {
				if (own.has(Number.parseInt(line.args[0] ?? "", 10))) own.add(result); else own.delete(result);
			} else if (line.name === "close") own.delete(Number.parseInt(line.args[0] ?? "", 10));
			if (line.failure) { complete = false; incompleteReasons.add(line.failure); continue; }
			const syscall = line.name;
			if (!syscall) continue;
			if (options.frozen && !continuationCall(line, index === start)) {
				complete = false; incompleteReasons.add(`continuation_state:${syscall}`);
			}
			if (syscall === "getpid" || syscall === "getppid" || syscall === "getsid" || syscall === "getpgid") { taints.add("pid_observation"); }
			const endpoint = /^\d+<(?:pipe|UNIX(?:-[A-Z]+)?):\[(\d+)(?:->\d+)?\]>$/.exec(line.args[0] ?? "")?.[1];
			const stream = endpoint && options.inheritedStreams?.includes(endpoint) || options.inheritedStreams?.some(inode => inode.startsWith("eventfd:")) && /^\d+<anon_inode:\[eventfd\]>$/.test(line.args[0] ?? "");
			const streamCall = streamCalls.has(line);
			if (stream && /^(?:read|write|readv|writev|sendto|recvfrom|sendfile|vmsplice)$/.test(syscall) && !streamCall) taints.add("unsupported_syscall");
			if (options.inheritedStreams?.length && /^(?:poll|ppoll|select|pselect6|epoll_.*)$/.test(syscall) && !streamCall && !internalPoll(line, ownPipes)) taints.add("unsupported_syscall");
			// A local socket reaches another process only once connected: a refused path (an NSS cache that is absent here) is a
			// pathname dependency, validated absent where the Actor runs.
			const refused = syscall === "connect" && /^-1 (?:ENOENT|ECONNREFUSED|EACCES)\b/.test(line.result) ? /\bsun_path="(\/[^"]+)"/.exec(line.args[1] ?? "")?.[1] : undefined;
			if (refused) { if (!paths.has(refused)) paths.set(refused, "input"); continue; }
			const output = !!options.outputEndpoints?.includes(`socket:[${/^\d+<UNIX-STREAM:\[(\d+)/.exec(line.args[0] ?? "")?.[1]}]`);
			const outputQuery = output && /^(?:getsockname|getpeername|getsockopt)$/.test(syscall);
			// A local socket or socketpair the tree made reaches no one else, as its pipes do not.
			if (NETWORK_SYSCALLS.has(syscall) && !nonSocketQuery(line) && !streamCall && !outputQuery && !(/^socket(?:pair)?$/.test(syscall) && /^AF_UNIX\b/.test(line.args[0] ?? "")) &&
				!(endpoint && ownPipes.has(endpoint))) taints.add("network");
			if (IPC_SYSCALLS.has(syscall)) taints.add("ipc");
			// Descriptor-local state is internal; reproduced OFD flags are sealed with their final offsets. A lock taken without waiting,
			// or a query that found the range free, depends only on no one else holding one, which a replay probes; a refusal, or a
			// holder found, saw another. One taken by waiting ends the same way natively, however long it waited; a release reveals nothing.
			const record = /^fcntl(?:64)?$/.test(syscall) ? /^F_(?:OFD_)?(GETLK|SETLK|SETLKW)(?:64)?$/.exec(line.args[1] ?? "")?.[1] : undefined;
			const lock = syscall === "flock" ? /\bLOCK_NB\b/.test(line.args[1] ?? "") && /\bLOCK_(SH|EX)\b/.exec(line.args[1] ?? "")?.[1]
				: record && record !== "SETLKW" && /\bl_type=F_(RD|WR|UN)LCK\b/.exec(line.args[2] ?? "")?.[1], locked = absoluteDescriptorPath(line.args[0]);
			if (lock && !streamCall && (record === "GETLK" || lock !== "UN")) {
				if (locked && syscallSucceeded(line) && (record === "GETLK") === (lock === "UN")) locks.set(locked, locks.get(locked) || lock !== "SH" && lock !== "RD");
				else taints.add("ipc");
			}
			if (/^fcntl(?:64)?$/.test(syscall) && !record) {
				const command = line.args[1] ?? "";
				if (!/^F_(?:GETFD|SETFD|DUPFD|DUPFD_CLOEXEC)$/.test(command) &&
					// A descriptor this process opened reports the flags it chose; an inherited one's flags need evidence.
					// Standard streams' status flags are part of the pinned execution context.
					!(command === "F_GETFL" && (own.has(Number.parseInt(line.args[0] ?? "", 10)) || /^[012]</.test(line.args[0] ?? ""))) && !((command === "F_GETFL" || command === "F_SETFL" && syscallSucceeded(line) &&
						(line.args[2] ?? "").split("|").every(flag => /^(?:O_(?:RDONLY|WRONLY|RDWR|APPEND|NONBLOCK|NDELAY|LARGEFILE|DIRECTORY|DSYNC|SYNC|NOFOLLOW)|0)$/.test(flag))) &&
						(options.inheritedFileImages?.includes(absoluteDescriptorPath(line.args[0]) ?? /^\d+<(pipe:\[\d+\])>$/.exec(line.args[0] ?? "")?.[1] ?? "") || stream || output ||
							own.has(Number.parseInt(line.args[0] ?? "", 10)) || ownPipes.has(/^\d+<pipe:\[(\d+)\]>$/.exec(line.args[0] ?? "")?.[1] ?? ""))))
					taints.add("unsupported_syscall");
			}
			// git refreshes its index's stat cache when it can, and reports the same without it (the repository is read-only
			// here); a git that needed the lock (add, commit, stash) fails instead, and its refusal stays a confinement observation.
			if (images.get(pid) === "git" && /^open(?:at)?$/.test(syscall) && /\/index\.lock"?$/.test(quotedStrings(line).at(-1) ?? "") && confinementDenied(line)) {
				refusedIndexLocks.add(pid); continue;
			}
			// An unprivileged process holds no capabilities, sandboxed or not (the observer runs as the Actor's user).
			const noCapabilities = syscall === "capget" && process.getuid?.() !== 0 && /\{effective=0, permitted=0, inheritable=0\}\) = 0$/.test(`${line.args.join(", ")}) = ${line.result}`);
			// A refused io_uring leaves libuv on epoll and its thread pool: the same results, every file call traced.
			if (syscall === "io_uring_setup" && /^-1 EPERM\b/.test(line.result)) continue;
			if (!noCapabilities && (CONFINEMENT_SENSITIVE_SYSCALLS.has(syscall) || prctlConfinementSensitive(line, syscall) || confinementDenied(line) || processLimitDenied(line, syscall))) {
				taints.add("confinement_observation");
			}
			// A time set on a file the tree opened for writing reaches its effects, which carry each file's modification time.
			if (/^(?:utime|utimes|utimensat|futimesat)$/.test(syscall) && !(syscallPaths(line, syscall, cwd) ?? [""]).every(target => writable.has(target))) taints.add("unsupported_syscall");
			if (
				resourceLimitMutation(line, syscall) || UNMODELED_FILE_SEMANTICS_SYSCALLS.has(syscall) && !streamCall ||
				(syscall === "pipe2" && /O_DIRECT|O_EXCL/.test(line.args[1] ?? "")) ||
				(syscall === "ioctl" && unmodeledFileIoctl(line))
			) {
				taints.add("unsupported_syscall");
			}
			// A filesystem's statistics vary over time like the clock, whose taint every trace carries; the sandbox reports the one a
			// native run sees, the workspace's own included.
			if (/^f?statfs$/.test(syscall)) continue;
			const listed = /^getdents(?:64)?$/.test(syscall) ? absoluteDescriptorPath(line.args[0]) : undefined;
			const directoryImage = !!listed && !!options.inheritedDirectoryImages?.includes(listed);
			// A listed directory's entry set is its dependency; readdir order and d_ino are volatile identity, as a descriptor's is.
			const listing = !!listed && !directoryImage && syscallSucceeded(line);
			if (listing) { taints.add("descriptor_observation"); listingPIDs.add(pid); listedPaths.add(listed!); if (paths.get(listed!) !== "executable") paths.set(listed!, "input"); }
			if (UNMODELED_METADATA_SYSCALLS.has(syscall) && !listing && (syscallSucceeded(line) ? !directoryImage : directoryImage)) {
				taints.add("unsupported_syscall");
				incompleteReasons.add(`unmodeled_metadata:${syscall}:${pid}`);
			}
			if (semanticRoots.length && workspaceDriverSemanticGap(line, syscall, cwd, semanticRoots)) {
				complete = false;
				taints.add("unsupported_syscall");
				incompleteReasons.add(`filesystem_semantics:${syscall}:${pid}`);
			}
			if (syscallSucceeded(line) && writesPath(line)) for (const target of syscallPaths(line, syscall, cwd) ?? []) { const name = workspaceName(target); if (name) written.add(name); else external.add(target); }
			const [structure, flags] = MODELED_METADATA_SYSCALLS.get(syscall) ?? [];
			if (structure && syscallSucceeded(line)) {
				// A recreated null device has the same I/O semantics, but may have a different device-node inode.
				if (/<char 1:3>>$/.test(line.args[0] ?? "")) { taints.add("descriptor_observation"); continue; }
				const metadataPaths = syscallPaths(line, syscall, cwd) ?? [];
				// A directory descriptor's identity serves traversal (ls and fts track loops by it); printed metadata comes from path stats.
				const directory = /\bstx?_mode=S_IFDIR\b/.test(line.args[structure] ?? ""), directoryHandle = directory && (syscall === "fstat" || !quotedArgument(line.args[1]));
				// Only the fields a program reveals of a workspace file are its dependency (see workspaceStatFields).
				const workspace = metadataPaths.length > 0 && metadataPaths.every((target) => semanticRoots.some((root) => containsLogicalPath(root, target)));
				const fields = workspace ? statFields.get(pid) : hostStatFields(images.get(pid) ?? "");
				if (workspace && fields === WITHOUT_IDENTITY && !directory) taints.add("descriptor_observation");
				const observed = statObservationDigest(line.args[structure] ?? "", directoryHandle ? ["mode"] : undefined, directory ? directoryStatFields(images.get(pid) ?? "", fields, workspace) : fields);
				if (!metadataPaths.length || !observed) {
					// fstat, or an empty *at name, of a pipe or socket
					if (descriptorTarget(line) && !quotedArgument(line.args[1])) taints.add("descriptor_observation");
					else { taints.add("unsupported_syscall"); incompleteReasons.add(`unparsed_metadata:${syscall}:${pid}`); }
				}
				// A shell stats directories only to validate $PWD: their sandbox identity never reaches its output.
				if (observed && observed.fields?.length !== 0 && !(SHELLS.has(images.get(pid) ?? "") && directory)) {
					const followSymlinks = syscall !== "lstat" && !(flags && /\bAT_SYMLINK_NOFOLLOW\b/.test(line.args[flags] ?? ""));
					for (const metadataPath of metadataPaths) observeMetadata(metadataPath, followSymlinks, observed);
				}
				continue;
			}
			if (successfulExec(line)) {
				const execution = tracedExecution(pid, line, cwd), image = path.posix.basename(execution.path ?? "");
				executions.push(execution); images.set(pid, image);
				statFields.set(pid, workspaceStatFields(image, execution.argv));
			}
			if (!PATH_ARGUMENTS[syscall]) continue;
			const role: DependencyRole = syscall === "execve" || syscall === "execveat" ? "executable" : "input";
			const observedPaths = syscallPaths(line, syscall, cwd);
			if (!observedPaths) { complete = false; incompleteReasons.add(`unresolved_pathname:${syscall}:${pid}`); }
			const nonEmpty = /^(?:rmdir|unlinkat|rename(?:at2?)?)$/.test(syscall) && /^-1 (?:ENOTEMPTY|EEXIST)\b/.test(line.result);
			for (const observed of observedPaths ?? []) { if (paths.get(observed) !== "executable") paths.set(observed, role); if (nonEmpty) listedPaths.add(observed); }
			if ((syscall === "chdir" || syscall === "fchdir") && syscallSucceeded(line)) {
				const changed = tracedCwd(line, cwd);
				if (changed) cwd = changed;
				else { complete = false; incompleteReasons.add(`${syscall}_unparsed:${pid}`); }
			}
		}
	}
	for (const pid of refusedIndexLocks) if (selected.get(pid)?.file.exitCode !== 0) taints.add("confinement_observation");
	// A workspace file the traced processes wrote changes under their own hands; its later state is their effect, not an input.
	// A run brokered from its tree, whose launcher the trace holds, wrote on its behalf.
	for (const pid of selected.keys()) for (const name of options.brokeredWrites?.(pid) ?? []) written.add(`//${name}`);
	// What it wrote outside the workspace, and the directories holding those names, change by its own hand.
	const touched = new Set([...external].flatMap(target => [target, path.posix.dirname(target)]));
	for (const observed of changedMetadata) if (!written.has(workspaceName(observed) ?? observed) && !touched.has(observed)) {
		taints.add("mutable_input"); incompleteReasons.add(`metadata_changed:${observed}`);
	}
	if (!complete) taints.add("trace_incomplete");
	// Chosen tools cannot pass their one-shot inputs on to output: the transcript may be replayed across turns.
	else if (repeatableExecutions(executions, listingPIDs, semanticRoots)) for (const taint of ONE_SHOT_TAINTS) taints.delete(taint);
	return {
		complete,
		...(options.inheritedHandles || options.inheritedStreams ? { resourceJournal: resourceJournal.sort((a, b) => a.order - b.order).map(({ order, ...event }) => event), retainedDescriptions: retained,
			...(options.frozen ? { finalHandles } : {}) } : {}),
		paths: Object.freeze([...[...paths].map(([observedPath, role]) => ({ path: observedPath, role: sharedObjectRole(observedPath, role),
			...(listedPaths.has(observedPath) ? { listed: true as const } : {}) })), ...metadata.values()]
			.sort((left, right) => pathOrder(left).localeCompare(pathOrder(right)))),
		taints: Object.freeze([...taints].sort()),
		pids: [...selected.keys()],
		tracedProcesses: [...selected].filter(([pid, { file, start }]) => file.lines.slice(start)
			.some((_, offset) => !ignoredSegments.get(pid)?.some(([from, to]) => start + offset >= from && start + offset < to))).length,
		incompleteReasons: Object.freeze([...incompleteReasons].sort()),
		...(resumedInterpositions.length ? { resumedInterpositions } : {}), written: [...written].map(name => name.slice(2)).sort(), external: [...external].sort(),
		locks: [...locks].map(([target, exclusive]) => ({ path: target, exclusive })),
	};
}

function pathOrder(item: ObservedProcessPath): string {
	return `${item.role}:${item.role === "metadata" ? item.followSymlinks : ""}:${item.path}`;
}

/** State retained across the frontier must be in the image or the common resource
 * journal. The final table is bound back to its original OFDs before handoff. */
function continuationCall(line: TraceLine, initial: boolean): boolean {
	const call = line.name;
	const fd = Number.parseInt(line.args[0] ?? "", 10);
	if (/^(?:execve|execveat)$/.test(call)) return initial;
	if (call === "close" || call === "close_range") return fd >= 3;
	if (call === "fcntl" || call === "fcntl64") return fd >= 3 || line.args[1] !== "F_SETFD";
	if (/^dup[23]$/.test(call)) return Number.parseInt(line.args[1] ?? "", 10) >= 3;
	if (call === "mmap") return (line.args[3] ?? "").split("|").every(flag => /^(?:MAP_PRIVATE|MAP_ANONYMOUS|MAP_FIXED|MAP_DENYWRITE|MAP_STACK)$/.test(flag));
	if (call === "madvise") return line.args[2] === "MADV_DONTNEED";
	if (call === "arch_prctl") return /^ARCH_(?:SET|GET)_(?:FS|GS)$/.test(line.args[0] ?? "");
	return /^(?:read|pread64|readv|write|writev|pwrite64|lseek|open|openat|access|faccessat|newfstatat|fstat|stat|lstat|statx|readlink|readlinkat|brk|mprotect|munmap|set_tid_address|set_robust_list|rseq|prlimit64|getrandom|rt_sigaction|rt_sigprocmask|sigaltstack|dup|poll|ppoll|select|pselect6|sendto|recvfrom|sendmsg|recvmsg|shutdown|flock|rename|renameat|renameat2|unlink|unlinkat|link|linkat|mkdir|mkdirat|rmdir|chmod|fchmod|fchmodat|truncate|ftruncate)$/.test(call);
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

function workspaceDriverSemanticGap(line: TraceLine, syscall: string, cwd: string, roots: readonly string[]): boolean {
	if (!DRIVER_SEMANTIC_GAP_RESULT.test(line.result)) return false;
	const referenced = new Set(syscallPaths(line, syscall, cwd));
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


function resourceLimitMutation(line: TraceLine, syscall: string): boolean {
	if (!syscallSucceeded(line) || syscall === "prlimit64" && line.args[2] === "NULL") return false;
	// A process sets its own limits within the hard ones its context pins; only the sandbox's process and core caps differ from native.
	const own = syscall === "setrlimit" ? line.args[0] : syscall === "prlimit64" && line.args[0] === "0" ? line.args[1] : undefined;
	return own === undefined ? syscall === "prlimit64" : own === "RLIMIT_NPROC" || own === "RLIMIT_CORE";
}

/** A poll that only checks descriptors are open, or waits on pipes the traced processes made, observes nothing outside them. */
function internalPoll(line: TraceLine, ownPipes: ReadonlySet<string>): boolean {
	const entries = /^p?poll$/.test(line.name) ? [...(line.args[0] ?? "").matchAll(/\{fd=\d+(<.*?>)?, events=([^}]+)\}/g)] : [];
	return entries.length > 0 && entries.every(([, target, events]) => events === "0" || ownPipes.has(/^<pipe:\[(\d+)\]>$/.exec(target ?? "")?.[1] ?? ""));
}

/** A traced execution's workspace writes so far (it may still run), stamped in wall-clock ms; an unresolvable name is undefined. */
export interface TracedWrite { readonly paths?: readonly string[]; readonly at: number; readonly opened: boolean }

/** A trace file's bytes from `offset`, at most `limit` of them. */
async function readTrace(file: string, offset: number, limit = Number.POSITIVE_INFINITY): Promise<Buffer> {
	const handle = await open(file, "r");
	try {
		const info = await handle.stat();
		if (!info.isFile()) throw new Error("trace is not a regular file");
		const buffer = Buffer.allocUnsafe(Math.max(0, Math.min(limit, info.size - offset)));
		return buffer.subarray(0, (await handle.read(buffer, 0, buffer.length, offset)).bytesRead);
	} finally { await handle.close(); }
}

/** A trace read as it grows, each record parsed once however often it is asked: its writes so far, and when it last named each absolute path
 * (by name or through a descriptor; a failed lookup names one too), in wall-clock milliseconds. */
export interface TraceTail { readonly writes: readonly TracedWrite[]; readonly seen: ReadonlyMap<string, number>; read(): Promise<TraceTail> }

export function traceTail(tracePrefix: string): TraceTail {
	const directory = path.dirname(tracePrefix), prefix = `${path.basename(tracePrefix)}.`, writes: TracedWrite[] = [], seen = new Map<string, number>(), offsets = new Map<string, number>();
	let reading: Promise<unknown> = Promise.resolve();
	const advance = async () => {
		for (const name of (await readdir(directory).catch(() => [] as string[])).filter(name => name.startsWith(prefix))) {
			// A record strace is still writing waits for its newline.
			const offset = offsets.get(name) ?? 0, bytes = await readTrace(path.join(directory, name), offset).catch(() => Buffer.alloc(0)), complete = bytes.lastIndexOf(10) + 1;
			offsets.set(name, offset + complete);
			for (const record of bytes.toString("utf8", 0, complete).split("\n")) {
				const stamped = /^(?:\d+ )?(\d+\.\d+) (.*)$/.exec(record), line = stamped && parseTraceLine(stamped[2]!);
				if (!line) continue;
				const at = Number(stamped[1]) * 1000, paths = syscallPaths(line, line.name, "");
				if (syscallSucceeded(line) && writesPath(line)) writes.push({ ...(paths?.every(target => target.startsWith("/")) ? { paths } : {}), at, opened: /^open(?:at2?)?$/.test(line.name) });
				for (const target of [...paths ?? [], ...line.args.flatMap(arg => absoluteDescriptorPath(arg) ?? [])]) if (target.startsWith("/")) seen.set(target, Math.max(seen.get(target) ?? 0, at));
			}
		}
	};
	return { writes, seen, read() { const next = reading.then(advance); reading = next.catch(() => undefined); return next.then(() => this); } };
}

/** Whether writes reached `targets` within [since, until], or held one open to write by `until`. An unresolvable write counts. */
export function writesWithin(writes: readonly TracedWrite[], targets: ReadonlySet<string>, since: number, until: number): boolean {
	return writes.some(({ paths, at, opened }) => at <= until && (opened || at >= since) && (!paths || paths.some(target => targets.has(target))));
}

function writesPath(line: TraceLine): boolean {
	return /^(?:creat|truncate|rename|renameat2?|link|linkat|symlink|symlinkat|mknod|mknodat|unlink|unlinkat|mkdir|mkdirat|rmdir|utimes?|utimensat|futimesat)$/.test(line.name) ||
		/^open(?:at2?)?$/.test(line.name) && /\bO_(?:WRONLY|RDWR|CREAT|TRUNC)\b/.test(line.args.join(" "));
}

function ignoredProcessSegments(selected: ReadonlyMap<number, TraceProcess>, interposedExecutables: ReadonlyMap<string, string>) {
	const ignored = new Map<number, Array<readonly [number, number]>>(), resumed = new Set<number>();
	if (!interposedExecutables.size) return { ignored, resumed: [] };
	const fullyIgnored = new Map<number, number>();
	for (const [pid, { file, start, cwd: initial }] of selected) {
		let cwd = initial;
		for (let index = start; index < file.lines.length; index++) {
			const line = file.lines[index]!;
			cwd = tracedCwd(line, cwd);
			if (!successfulExec(line)) continue;
			const executable = quotedStrings(line)[0];
			const original = executable && interposedExecutables.get(tracedPath(executable, cwd) ?? "");
			if (!original) continue;
			// An in-place bypass: the launcher at the intercepted path next execs the original image, by its absolute path.
			let resumedAt = index + 1;
			while (resumedAt < file.lines.length && !successfulExec(file.lines[resumedAt]!)) resumedAt++;
			const target = resumedAt < file.lines.length ? quotedStrings(file.lines[resumedAt]!)[0] : undefined;
			if (target && tracedPath(target, cwd) === original) {
				(ignored.get(pid) ?? ignored.set(pid, []).get(pid)!).push([index, resumedAt]);
				resumed.add(pid);
				index = resumedAt - 1;
				continue;
			}
			(ignored.get(pid) ?? ignored.set(pid, []).get(pid)!).push([index, file.lines.length]);
			fullyIgnored.set(pid, index);
			break;
		}
	}
	// Anything the launcher spawned while it was dispatching belongs to its ignored segment.
	for (const [pid, segments] of ignored) if (!fullyIgnored.has(pid)) for (const [from, to] of segments) for (const line of selected.get(pid)!.file.lines.slice(from, to)) {
		const child = spawnedPID(line);
		if (child && !fullyIgnored.has(child)) fullyIgnored.set(child, 0);
	}
	for (const [pid, start] of fullyIgnored) {
		if (start === 0 && !ignored.has(pid)) ignored.set(pid, [[0, selected.get(pid)?.file.lines.length ?? Number.POSITIVE_INFINITY]]);
		const file = selected.get(pid)?.file;
		if (!file) continue;
		for (const line of file.lines.slice(start)) {
			const child = spawnedPID(line);
			if (!child || fullyIgnored.has(child)) continue;
			ignored.set(child, [[0, selected.get(child)?.file.lines.length ?? Number.POSITIVE_INFINITY]]);
			fullyIgnored.set(child, 0);
		}
	}
	return { ignored, resumed: [...resumed].filter((pid) => !fullyIgnored.has(pid)) };
}

function tracedCwd(line: TraceLine, cwd: string | undefined): string | undefined {
	if (!syscallSucceeded(line)) return cwd;
	if (line.name === "fchdir") return absoluteDescriptorPath(line.args[0]);
	if (line.name !== "chdir") return cwd;
	const target = quotedStrings(line)[0];
	return target && (target.startsWith("/") || cwd) ? walkedPath(target.startsWith("/") ? target : `${cwd}/${target}`) : undefined;
}

function tracedPath(target: string, cwd: string | undefined): string | undefined {
	return path.posix.isAbsolute(target) ? path.posix.resolve(target) : cwd ? path.posix.resolve(cwd, target) : undefined;
}


function tracedExecution(pid: number, line: TraceLine, cwd: string): TracedExecution {
	try {
		const image = line.name === "execve" ? quotedArgument(line.args[0]) : undefined, argv = line.args[line.name === "execve" ? 1 : 2] ?? "";
		return { pid, ...(image ? { path: tracedPath(image, cwd) } : {}),
			argv: /^\[.*\]$/su.test(argv) ? [...argv.matchAll(/"((?:\\.|[^"\\])*)"/g)].map((match) => decodeCString(match[1]!)) : [] };
	} catch { return { pid, argv: [] }; } // Undecodable arguments prove nothing.
}

function successfulExec(line: TraceLine): boolean { return (line.name === "execve" || line.name === "execveat") && syscallSucceeded(line); }

function spawnedPID(line: TraceLine): number | undefined {
	if (!["clone", "clone3", "fork", "vfork"].includes(line.name)) return undefined;
	const pid = Number(line.result);
	return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
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

function confinementDenied(line: TraceLine): boolean { return /^-1 (?:EACCES|EPERM)\b/.test(line.result); }

function prctlConfinementSensitive(line: TraceLine, syscall: string): boolean {
	return syscall === "prctl" && !/^PR_SET_(?:NAME|VMA)\b/.test(line.args[0] ?? "");
}

function processLimitDenied(line: TraceLine, syscall: string): boolean {
	return ["clone", "clone3", "fork", "vfork"].includes(syscall) && /^-1 EAGAIN\b/.test(line.result);
}

function syscallPaths(line: TraceLine, syscall: string, cwd: string): readonly string[] | undefined {
	const paths = (PATH_ARGUMENTS[syscall] ?? []).map(([pathname, dirfd, descriptorPath]) => {
		const descriptor = dirfd === undefined ? undefined : line.args[dirfd];
		if (pathname === undefined || descriptorPath && /^(?:""|NULL)$/.test(line.args[pathname] ?? "")) {
			const target = absoluteDescriptorPath(descriptor); return target ? [target] : [];
		}
		const value = quotedArgument(line.args[pathname]);
		if (value?.startsWith("/")) return [walkedPath(value)]; // Absolute names ignore dirfd, even an invalid one.
		const base = dirfd === undefined || descriptor === "AT_FDCWD" || descriptor === "-100" ? cwd : absoluteDescriptorPath(descriptor);
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
	const target = /^(?:\d+|AT_FDCWD)<(.+)>$/.exec(descriptor?.trim() ?? "")?.[1]?.replace(/<[^<>]*>$/, "");
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

/** statx fills what its mask reports and a caller reads only what it asked for; fields outside the stat structure are
 * ignored, and an unknown flag leaves every field required. */
const STATX_FIELD_MASKS: Readonly<Record<string, readonly FilesystemObservationField[]>> = { STATX_TYPE: ["mode"], STATX_MODE: ["mode"],
	STATX_NLINK: ["nlink"], STATX_UID: ["uid"], STATX_GID: ["gid"], STATX_ATIME: [], STATX_MTIME: ["mtimeNs"], STATX_CTIME: ["ctimeNs"],
	STATX_INO: ["ino"], STATX_SIZE: ["size"], STATX_BLOCKS: ["blocks"], STATX_BASIC_STATS: FILESYSTEM_OBSERVATION_FIELDS, STATX_ALL: FILESYSTEM_OBSERVATION_FIELDS,
	...Object.fromEntries(["MNT_ID", "MNT_ID_UNIQUE", "BTIME", "DIOALIGN", "DIO_READ_ALIGN", "SUBVOL", "WRITE_ATOMIC"].map((flag) => [`STATX_${flag}`, []])) };

function statObservationDigest(structure: string, only?: readonly FilesystemObservationField[], revealed?: readonly FilesystemObservationField[]): StatObservation | undefined {
	const flags = /\bstx_mask=([A-Z_|0-9x]+)/.exec(structure)?.[1]?.split("|");
	const requested = only ?? (flags?.every((flag) => STATX_FIELD_MASKS[flag]) ? flags.flatMap((flag) => STATX_FIELD_MASKS[flag]!) : undefined);
	const mask = revealed ? (requested ?? FILESYSTEM_OBSERVATION_FIELDS).filter((field) => revealed.includes(field)) : requested;
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
	const evidence = {
		dev: device("st_dev"),
		ino: field("st_ino"),
		mode,
		nlink: field("st_nlink"),
		uid: field("st_uid"),
		gid: field("st_gid"),
		rdev: device("st_rdev"),
		size: field("st_size"),
		blksize: field("st_blksize"),
		blocks: field("st_blocks"),
		mtimeNs: time("st_mtime"),
		ctimeNs: time("st_ctime"),
	};
	const fields = mask && FILESYSTEM_OBSERVATION_FIELDS.filter((field) => mask.includes(field));
	if (fields && fields.length < FILESYSTEM_OBSERVATION_FIELDS.length) {
		return fields.every((field) => evidence[field] !== undefined) ? { digest: filesystemObservationDigest(evidence, fields), fields } : undefined;
	}
	if (Object.values(evidence).some((value) => value === undefined)) return undefined;
	return { digest: filesystemObservationDigest(evidence) };
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

function quotedStrings(line: TraceLine): string[] {
	return line.args.flatMap((argument) => { const value = quotedArgument(argument); return value === undefined ? [] : [value]; });
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

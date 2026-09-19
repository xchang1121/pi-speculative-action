import { open, readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { containsLogicalPath } from "./path-utils.ts";
import {
	type DependencyRole,
	type FilesystemObservationEvidence,
	filesystemObservationDigest,
	type ProvenanceTaint,
	type Sha256Digest,
	type ResourceTransitionKind,
} from "./provenance-certificate.ts";

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
	return [strace, "--kill-on-exit", streams ? "-f" : "-ff", "-q", "-yy", "-v", "-s", "65535", "-e", continuation ? "trace=all" : SYSCALL_FILTER + (streams ? ",read,write,readv,writev,close,close_range,dup,dup2,dup3,eventfd,eventfd2,sendfile,vmsplice,poll,ppoll,select,pselect6,epoll_create,epoll_create1,epoll_ctl,epoll_wait,epoll_pwait,epoll_pwait2" : ""), "-o", streams ? `${tracePrefix}.stream` : tracePrefix, ...command];
}

export type ObservedProcessPath =
	| { readonly path: string; readonly role: DependencyRole }
	| {
			readonly path: string;
			readonly role: "metadata";
			readonly followSymlinks: boolean;
			readonly digest: Sha256Digest;
	  };

export interface StraceObservation {
	readonly complete: boolean;
	readonly paths: readonly ObservedProcessPath[];
	readonly taints: readonly ProvenanceTaint[];
	readonly tracedProcesses: number;
	readonly incompleteReasons: readonly string[];
	readonly resourceJournal?: readonly { readonly inode: string; readonly description?: number; readonly kind: ResourceTransitionKind; readonly data: Buffer; readonly requested?: number }[];
	readonly retainedDescriptions?: readonly number[];
	readonly finalHandles?: readonly { readonly fd: number; readonly description?: number; readonly cloexec: boolean }[];
}

export interface StraceObservationOptions {
	/** Bounded prefix lookup for running work; its evidence is always incomplete. */
	readonly previewBytes?: number;
	/** An owned tracer flushed this exact byte boundary while the target was stopped in restartable I/O. */
	readonly frozen?: { readonly pid: number; readonly fd: number; readonly syscall: string; readonly bytes: number };
	/** Intercepted path to native target; a direct second exec proves descriptor-preserving bypass. */
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
	readonly inheritedHandles?: readonly { readonly fd: number; readonly installed?: false; readonly description?: number; readonly inode: string; readonly flags: number; readonly outside: number; readonly queuedBytes?: number; readonly queueData?: Buffer; readonly packet?: boolean; readonly messages?: readonly import("./linux-held-exec.ts").QueueMessage[] }[];
}

interface TraceFile {
	readonly pid: number;
	readonly lines: readonly TraceLine[];
	readonly terminated: boolean;
}

type TraceRoot = { readonly file: TraceFile; readonly start: number };
interface TraceProcess extends TraceRoot {
	readonly cwd: string | undefined;
	readonly fs: { shared: boolean; changed: boolean };
}
interface TraceLine {
	readonly order?: number;
	readonly name: string;
	readonly args: readonly string[];
	readonly result: string;
	readonly failure?: string;
}
const TRACE_DELIMITERS: Readonly<Record<string, string>> = { "(": ")", "[": "]", "{": "}", "<": ">" };

/** Delimit once: quoted paths and descriptor annotations are data, never syscall syntax. */
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
		if (quoted) { if (character === '"') quoted = false; continue; }
		// -yy sockets use -> for peers; filesystem paths escape literal angle brackets.
		if (context?.endsWith(">")) {
			if (character === "<") stack.push(">");
			if (character === ">" && (context === "/>" || line[index - 1] !== "-")) stack.pop();
			continue;
		}
		if (character === '"') { quoted = true; continue; }
		if (line.startsWith("/*", index)) {
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
		} else if (character === "," && !stack.length) {
			args.push(line.slice(start, index).trim());
			start = index + 1;
		}
	}
	return failure;
}

function reassembleSyscalls(lines: readonly string[], pid: number, order?: readonly number[]): readonly TraceLine[] {
	const pending = new Map<string, Array<{ text: string; order?: number }>>();
	const complete: TraceLine[] = [];
	for (const [index, line] of lines.entries()) {
		const append = (text: string, sequence = order?.[index]) => complete.push({ ...parseTraceLine(text), ...(sequence !== undefined ? { order: sequence } : {}) });
		const unfinished = /^\s*([a-zA-Z0-9_]+)\(.*\s<unfinished \.\.\.>\s*$/.exec(line);
		if (unfinished) {
			const name = unfinished[1]!;
			(pending.get(name) ?? pending.set(name, []).get(name)!).push({ text: line.replace(/\s*<unfinished \.\.\.>\s*$/, ""), order: order?.[index] });
			continue;
		}
		const resumed = /^\s*<\.\.\.\s*([a-zA-Z0-9_]+) resumed>(.*)$/.exec(line);
		if (!resumed) {
			append(line);
			continue;
		}
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
		if (stream?.inode.startsWith("file:")) {
			const operation = fileLockTransition(line);
			if (operation) emit(stream, "lock", operation);
		}
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
	for (const [pid, level] of depth) {
		for (const child of children.get(pid) ?? []) {
			if (!depth.has(child)) depth.set(child, level + 1);
		}
	}
	const candidates: Array<TraceRoot & { readonly depth: number }> = [];
	for (const file of files) {
		const processDepth = depth.get(file.pid);
		if (processDepth === undefined) continue;
		const start = file.lines.findIndex((line) => {
			if (!successfulExec(line)) return false;
			const executable = quotedStrings(line)[0];
			return executable !== undefined && path.posix.resolve(executable) === target;
		});
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
		const target = path.join(directory, name);
		let contents: string;
		if (remaining === undefined) contents = await readFile(target, "utf8");
		else {
			const handle = await open(target, "r");
			try {
				const info = await handle.stat();
				if (!info.isFile()) throw new Error("trace is not a regular file");
				const buffer = Buffer.allocUnsafe(Math.min(remaining, info.size));
				const { bytesRead } = await handle.read(buffer);
				remaining -= bytesRead;
				contents = buffer.toString("utf8", 0, bytesRead);
			} finally { await handle.close(); }
		}
		const groups = new Map<number, { lines: string[]; order?: number[] }>();
		if (!ordered) groups.set(pid, { lines: contents.split(/\r?\n/) });
		else for (const [order, line] of contents.split(/\r?\n/).entries()) {
			if (!line.trim()) continue;
			const match = /^(\d+)\s+(.*)$/.exec(line);
			if (!match) throw new Error("invalid ordered trace record");
			const process = Number(match[1]), group = groups.get(process) ?? { lines: [], order: [] };
			group.lines.push(match[2]!); group.order!.push(order); groups.set(process, group);
		}
		for (const [pid, group] of groups) {
			if (options.frozen?.pid === pid) {
				const pending = group.lines.at(-1) ?? "";
				frontier = /^(?:read|readv|write|writev|sendto|recvfrom|sendmsg|recvmsg)$/.test(options.frozen.syscall) &&
					new RegExp(`^${options.frozen.syscall}\\(${options.frozen.fd}(?:<|,)`).test(pending) && !/\)\s+=/.test(pending);
				if (frontier) { group.lines.pop(); group.order?.pop(); }
				if (group.lines.some(line => /^--- /.test(line))) frontier = false;
			}
			files.push({ pid, lines: reassembleSyscalls(group.lines, pid, group.order),
				terminated: /^\+\+\+ (?:exited with \d+|killed by SIG[A-Z0-9]+(?: \(core dumped\))?) \+\+\+\s*$/.test(group.lines.filter(line => line.trim()).at(-1) ?? "") });
		}
	}
	const target = path.posix.resolve(executablePath);
	const root = selectTraceRoot(files, target);
	if ("reason" in root) {
		return {
			complete: false,
			paths: [],
			taints: ["trace_incomplete"],
			tracedProcesses: 0,
			incompleteReasons: [root.reason],
		};
	}

	const byPID = new Map(files.map((file) => [file.pid, file]));
	const selected = new Map<number, TraceProcess>([[root.file.pid, {
		...root, cwd: path.posix.resolve(initialCwd), fs: { shared: false, changed: false },
	}]]);
	let complete = options.previewBytes === undefined;
	const incompleteReasons = new Set<string>(complete ? [] : ["preview_only"]);
	if (options.frozen && (!frontier || root.file.pid !== options.frozen.pid || remaining !== 0)) {
		complete = false; incompleteReasons.add("continuation_frontier_unproven");
	}
	for (const [pid, process] of selected) {
		if (!process.file.terminated && options.frozen?.pid !== pid) {
			complete = false;
			incompleteReasons.add(`process_exit_unproven:${pid}`);
		}
		let cwd = process.cwd;
		for (const line of process.file.lines.slice(process.start)) {
			if (process.fs.shared && (line.name === "chdir" || line.name === "fchdir") && syscallSucceeded(line)) process.fs.changed = true;
			cwd = tracedCwd(line, cwd);
			const child = spawnedPID(line);
			if (!child || selected.has(child)) continue;
			const childFile = byPID.get(child);
			if (!childFile) {
				complete = false;
				incompleteReasons.add(`child_trace_missing:${child}`);
				continue;
			}
			const shared = sharesFilesystem(line);
			if (shared === undefined) { complete = false; incompleteReasons.add(`clone_flags_unparsed:${pid}`); }
			if (shared !== false) process.fs.shared = true;
			selected.set(child, { file: childFile, start: 0, cwd, fs: shared === false ? { shared: false, changed: false } : process.fs });
		}
	}
	if (options.frozen && selected.size !== 1) { complete = false; incompleteReasons.add("continuation_process_tree"); }

	const paths = new Map<string, DependencyRole>();
	const metadata = new Map<string, Extract<ObservedProcessPath, { role: "metadata" }>>();
	// Native instructions and ELF startup state expose clock/random inputs without a syscall.
	// A complete transcript therefore permits only the existing one-shot transfer, never proof
	// that this process can be repeated. Do not infer unused inputs from their absence here.
	const taints = new Set<ProvenanceTaint>(["clock", "random"]);
	const { journal: resourceJournal, handled: streamCalls, retained, finalHandles } = options.inheritedHandles?.length || options.inheritedStreams?.length
		? resourceTransitions(selected, root.file.pid, options) : { journal: [], handled: new Set<TraceLine>(), retained: [], finalHandles: [] };
	const interposedExecutables = new Map(
		(options.interposedExecutables ?? []).map(([intercepted, original]) => [
			path.posix.resolve(intercepted), path.posix.resolve(original),
		]),
	);
	const semanticRoots = (options.guardFilesystemSemanticsWithin ?? []).map((value) => path.posix.resolve(value));
	const ignoredSegments = ignoredProcessSegments(selected, interposedExecutables);
	const observeMetadata = (observedPath: string, followSymlinks: boolean, digest: Sha256Digest) => {
		const identity = `metadata:${followSymlinks}:${observedPath}`;
		if (metadata.get(identity)?.digest !== undefined && metadata.get(identity)?.digest !== digest) {
			taints.add("mutable_input");
			incompleteReasons.add(`metadata_changed:${observedPath}`);
		}
		metadata.set(identity, {
			path: observedPath,
			role: "metadata",
			followSymlinks,
			digest,
		});
	};
	for (const [pid, { file, start, cwd: initial, fs }] of selected) {
		// -ff files cannot order another task's chdir against this task's pathname lookup.
		if (fs.changed) { complete = false; incompleteReasons.add("shared_cwd_mutation"); continue; }
		let cwd = initial;
		if (!cwd) {
			complete = false;
			incompleteReasons.add(`cwd_unknown:${pid}`);
			cwd = path.posix.resolve(initialCwd);
		}
		for (let index = start; index < file.lines.length; index++) {
			if (ignoredSegments.get(pid)?.some(([from, to]) => index >= from && index < to)) continue;
			const line = file.lines[index]!;
			if (!line) continue;
			const traceFailure = line.failure;
			if (traceFailure) {
				complete = false;
				incompleteReasons.add(traceFailure);
				continue;
			}
			const syscall = line.name;
			if (!syscall) continue;
			if (options.frozen && !continuationCall(line, index === start)) {
				complete = false; incompleteReasons.add(`continuation_state:${syscall}`);
			}
			if (syscall === "getpid" || syscall === "getppid" || syscall === "getsid" || syscall === "getpgid") {
				taints.add("pid_observation");
			}
			const endpoint = /^\d+<(?:pipe|UNIX(?:-[A-Z]+)?):\[(\d+)(?:->\d+)?\]>$/.exec(line.args[0] ?? "")?.[1];
			const stream = endpoint && options.inheritedStreams?.includes(endpoint) || options.inheritedStreams?.some(inode => inode.startsWith("eventfd:")) && /^\d+<anon_inode:\[eventfd\]>$/.test(line.args[0] ?? "");
			const streamCall = streamCalls.has(line);
			if (stream && /^(?:read|write|readv|writev|sendto|recvfrom|sendfile|vmsplice)$/.test(syscall) && !streamCall) taints.add("unsupported_syscall");
			if (options.inheritedStreams?.length && /^(?:poll|ppoll|select|pselect6|epoll_.*)$/.test(syscall) && !streamCall) taints.add("unsupported_syscall");
			if (NETWORK_SYSCALLS.has(syscall) && !nonSocketQuery(line) && !streamCall) taints.add("network");
			if (IPC_SYSCALLS.has(syscall)) taints.add("ipc");
			// Descriptor-local state is internal; reproduced OFD flags are sealed with their final offsets.
			// Locks, leases, async notifications and owners require additional effect evidence.
			if (syscall === "flock" && !streamCall) taints.add("ipc");
			if (syscall === "fcntl" || syscall === "fcntl64") {
				const command = line.args[1] ?? "";
				if (/^F_(?:OFD_)?(?:GETLK|SETLK|SETLKW)(?:64)?$/.test(command)) { if (!streamCall) taints.add("ipc"); }
				else if (!/^F_(?:GETFD|SETFD|DUPFD|DUPFD_CLOEXEC)$/.test(command) &&
					!((command === "F_GETFL" || command === "F_SETFL" && syscallSucceeded(line) &&
						(line.args[2] ?? "").split("|").every(flag => /^(?:O_(?:RDONLY|WRONLY|RDWR|APPEND|NONBLOCK|NDELAY|LARGEFILE|DIRECTORY|DSYNC|SYNC|NOFOLLOW)|0)$/.test(flag))) &&
						(options.inheritedFileImages?.includes(absoluteDescriptorPath(line.args[0]) ?? /^\d+<(pipe:\[\d+\])>$/.exec(line.args[0] ?? "")?.[1] ?? "") || stream)))
					taints.add("unsupported_syscall");
			}
			if (CONFINEMENT_SENSITIVE_SYSCALLS.has(syscall) || prctlConfinementSensitive(line, syscall) || confinementDenied(line) || processLimitDenied(line, syscall)) {
				taints.add("confinement_observation");
			}
			if (
				resourceLimitMutation(line, syscall) ||
				UNMODELED_FILE_SEMANTICS_SYSCALLS.has(syscall) && !streamCall ||
				(syscall === "pipe2" && /O_DIRECT|O_EXCL/.test(line.args[1] ?? "")) ||
				(syscall === "ioctl" && unmodeledFileIoctl(line))
			) {
				taints.add("unsupported_syscall");
			}
			const directoryImage = /^(getdents|getdents64)$/.test(syscall) && options.inheritedDirectoryImages?.includes(absoluteDescriptorPath(line.args[0]) ?? "");
			if (UNMODELED_METADATA_SYSCALLS.has(syscall) && (syscallSucceeded(line) ? !directoryImage : directoryImage)) {
				taints.add("unsupported_syscall");
				incompleteReasons.add(`unmodeled_metadata:${syscall}:${pid}`);
			}
			if (semanticRoots.length && workspaceDriverSemanticGap(line, syscall, cwd, semanticRoots)) {
				complete = false;
				taints.add("unsupported_syscall");
				incompleteReasons.add(`filesystem_semantics:${syscall}:${pid}`);
			}
			if (MODELED_METADATA_SYSCALLS.has(syscall) && syscallSucceeded(line)) {
				// A recreated null device has the same I/O semantics, but may have a different device-node inode.
				if (/<char 1:3>>$/.test(line.args[0] ?? "")) { taints.add("descriptor_observation"); continue; }
				const metadataPaths = syscallPaths(line, syscall, cwd);
				const digest = statObservationDigest(line.args[syscall === "newfstatat" ? 2 : 1] ?? "");
				if (!metadataPaths.length || !digest) {
					if (syscall === "fstat" && descriptorTarget(line)) taints.add("descriptor_observation");
					else {
						taints.add("unsupported_syscall");
						incompleteReasons.add(`unparsed_metadata:${syscall}:${pid}`);
					}
				}
				if (digest) {
					const followSymlinks = syscall !== "lstat" && !(syscall === "newfstatat" && /\bAT_SYMLINK_NOFOLLOW\b/.test(line.args[3] ?? ""));
					for (const observed of metadataPaths) observeMetadata(observed, followSymlinks, digest);
				}
				continue;
			}
			if (!PATH_ARGUMENTS[syscall]) continue;
			const role: DependencyRole = syscall === "execve" || syscall === "execveat" ? "executable" : "input";
			for (const observed of syscallPaths(line, syscall, cwd)) {
				if (paths.get(observed) !== "executable") paths.set(observed, role);
			}
			if ((syscall === "chdir" || syscall === "fchdir") && syscallSucceeded(line)) {
				const changed = tracedCwd(line, cwd);
				if (changed) cwd = changed;
				else { complete = false; incompleteReasons.add(`${syscall}_unparsed:${pid}`); }
			}
		}
	}
	if (!complete) taints.add("trace_incomplete");
	return {
		complete,
		...(options.inheritedHandles || options.inheritedStreams ? { resourceJournal: resourceJournal.sort((a, b) => a.order - b.order).map(({ order, ...event }) => event), retainedDescriptions: retained,
			...(options.frozen ? { finalHandles } : {}) } : {}),
		paths: Object.freeze(
			[
				...[...paths].map(([observedPath, role]) => ({ path: observedPath, role: sharedObjectRole(observedPath, role) })),
				...metadata.values(),
			]
				.sort((left, right) =>
					`${left.role}:${left.role === "metadata" ? left.followSymlinks : ""}:${left.path}`.localeCompare(
						`${right.role}:${right.role === "metadata" ? right.followSymlinks : ""}:${right.path}`,
					),
				),
		),
		taints: Object.freeze([...taints].sort()),
		tracedProcesses: [...selected].filter(([pid, { file, start }]) =>
			file.lines.slice(start).some((_, offset) =>
				!ignoredSegments.get(pid)?.some(([from, to]) => start + offset >= from && start + offset < to),
			),
		).length,
		incompleteReasons: Object.freeze([...incompleteReasons].sort()),
	};
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
	return /^(?:read|pread64|readv|write|writev|pwrite64|lseek|open|openat|access|faccessat|newfstatat|fstat|stat|lstat|readlink|readlinkat|brk|mprotect|munmap|set_tid_address|set_robust_list|rseq|prlimit64|getrandom|rt_sigaction|rt_sigprocmask|sigaltstack|dup|poll|ppoll|select|pselect6|sendto|recvfrom|sendmsg|recvmsg|shutdown|flock|rename|renameat|renameat2|unlink|unlinkat|link|linkat|mkdir|mkdirat|rmdir|chmod|fchmod|fchmodat|truncate|ftruncate)$/.test(call);
}

/** Kernel argument positions own pathname identity; each *at operand has its own directory binding. */
const PATH_ARGUMENTS: Readonly<Record<string, readonly (readonly [pathname: number | undefined, dirfd?: number, descriptorPath?: "NULL" | '""'])[]>> = Object.fromEntries(([
	["access chdir chmod chown creat execve getxattr lgetxattr listxattr llistxattr mkdir mknod open readlink removexattr lremovexattr rmdir setxattr lsetxattr truncate unlink utime utimes stat lstat statfs", [[0]]],
	["rename link mount", [[0], [1]]],
	["symlink", [[1]]],
	["execveat faccessat faccessat2 fchmodat fchownat mkdirat mknodat openat openat2 readlinkat unlinkat statx", [[1, 0]]],
	["newfstatat", [[1, 0, '""']]],
	["utimensat", [[1, 0, "NULL"]]],
	["renameat renameat2 linkat", [[1, 0], [3, 2]]],
	["symlinkat", [[2, 1]]],
	["fchdir fstat fstatfs", [[undefined, 0]]],
] as const).flatMap(([names, positions]) => names.split(" ").map((name) => [name, positions])));

const MODELED_METADATA_SYSCALLS = new Set(["stat", "lstat", "fstat", "newfstatat"]);
const UNMODELED_METADATA_SYSCALLS = new Set(["statx", "statfs", "fstatfs", "getdents", "getdents64"]);

/** Persistent metadata not represented by the typed workspace transaction must never be replayed. */
const UNMODELED_FILE_SEMANTICS_SYSCALLS = new Set([
	"fallocate", "splice", "tee",
	"fgetxattr",
	"flistxattr",
	"fremovexattr",
	"fsetxattr",
	"futimesat",
	"getxattr",
	"lgetxattr",
	"listxattr",
	"llistxattr",
	"lremovexattr",
	"lsetxattr",
	"removexattr",
	"setxattr",
	"utime",
	"utimensat",
	"utimes",
]);

const UNMODELED_MUTATING_IOCTL = /\b(?:FICLONE|FICLONERANGE|FIDEDUPERANGE|FS_IOC_SETFLAGS|FS_IOC_SETVERSION|FS_IOC_FSSETXATTR)\b/;
const DRIVER_SEMANTIC_GAP_RESULT = /^-1\s+(?:EXDEV|EOPNOTSUPP|ENOTSUP|ENOSYS)\b/;

function unmodeledFileIoctl(line: TraceLine): boolean {
	if (!syscallSucceeded(line)) return false;
	return UNMODELED_MUTATING_IOCTL.test(line.args[1] ?? "") || absoluteDescriptorPath(line.args[0]) !== undefined;
}

function workspaceDriverSemanticGap(
	line: TraceLine,
	syscall: string,
	cwd: string,
	roots: readonly string[],
): boolean {
	if (!DRIVER_SEMANTIC_GAP_RESULT.test(line.result)) return false;
	const referenced = new Set(syscallPaths(line, syscall, cwd));
	for (const argument of line.args) {
		const target = absoluteDescriptorPath(argument);
		if (target) referenced.add(target);
	}
	return [...referenced].some((candidate) => roots.some((root) => containsLogicalPath(root, candidate)));
}

const NETWORK_SYSCALLS = new Set([
	"getsockname", "getpeername", "getsockopt", "setsockopt", "listen", "shutdown",
	"accept",
	"accept4",
	"bind",
	"connect",
	"recvfrom",
	"recvmmsg",
	"recvmsg",
	"sendmmsg",
	"sendmsg",
	"sendto",
	"socket",
	"socketpair",
]);

const IPC_SYSCALLS = new Set([
	"mq_open",
	"msgget",
	"semget",
	"shmat",
	"shmget",
]);

/** A failed query of a proven file/pipe reveals no socket state; unknown descriptor types stay tainted. */
function nonSocketQuery(line: TraceLine): boolean {
	return /^(?:getsockname|getpeername|getsockopt)$/.test(line.name) && /^-1 ENOTSOCK\b/.test(line.result) &&
		Boolean(absoluteDescriptorPath(line.args[0]) || /^\d+<pipe:\[\d+\]>$/.test(line.args[0] ?? ""));
}


function resourceLimitMutation(line: TraceLine, syscall: string): boolean {
	if (!syscallSucceeded(line)) return false;
	return syscall === "setrlimit" || (syscall === "prlimit64" && line.args[2] !== "NULL");
}

function ignoredProcessSegments(
	selected: ReadonlyMap<number, TraceProcess>,
	interposedExecutables: ReadonlyMap<string, string>,
): Map<number, Array<readonly [number, number]>> {
	const ignored = new Map<number, Array<readonly [number, number]>>();
	if (!interposedExecutables.size) return ignored;
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
			let resumed = -1, resumedExecutable: string | undefined, resumedCwd = cwd;
			for (let candidateIndex = index + 1; candidateIndex < file.lines.length; candidateIndex++) {
				const candidate = file.lines[candidateIndex]!;
				resumedCwd = tracedCwd(candidate, resumedCwd);
				if (!successfulExec(candidate)) continue;
				resumed = candidateIndex;
				resumedExecutable = quotedStrings(candidate)[0];
				break;
			}
			if (resumedExecutable && tracedPath(resumedExecutable, resumedCwd) === original) {
				(ignored.get(pid) ?? ignored.set(pid, []).get(pid)!).push([index, resumed]);
				index = resumed - 1;
				continue;
			}
			(ignored.get(pid) ?? ignored.set(pid, []).get(pid)!).push([index, file.lines.length]);
			fullyIgnored.set(pid, index);
			break;
		}
	}
	for (const [pid, start] of fullyIgnored) {
		const file = selected.get(pid)?.file;
		if (!file) continue;
		for (const line of file.lines.slice(start)) {
			const child = spawnedPID(line);
			if (!child || fullyIgnored.has(child)) continue;
			ignored.set(child, [[0, selected.get(child)?.file.lines.length ?? Number.POSITIVE_INFINITY]]);
			fullyIgnored.set(child, 0);
		}
	}
	return ignored;
}

function tracedCwd(line: TraceLine, cwd: string | undefined): string | undefined {
	if (!syscallSucceeded(line)) return cwd;
	if (line.name === "fchdir") return absoluteDescriptorPath(line.args[0]);
	if (line.name !== "chdir") return cwd;
	const target = quotedStrings(line)[0];
	return target ? tracedPath(target, cwd) : undefined;
}

function tracedPath(target: string, cwd: string | undefined): string | undefined {
	return path.posix.isAbsolute(target) ? path.posix.resolve(target) : cwd ? path.posix.resolve(cwd, target) : undefined;
}

function successfulExec(line: TraceLine): boolean {
	return (line.name === "execve" || line.name === "execveat") && syscallSucceeded(line);
}

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

function syscallSucceeded(line: TraceLine): boolean {
	return /^(?:0x[0-9a-f]+|[0-9]+)(?:\b|<)/i.test(line.result);
}

function confinementDenied(line: TraceLine): boolean {
	return /^-1 (?:EACCES|EPERM)\b/.test(line.result);
}

function prctlConfinementSensitive(line: TraceLine, syscall: string): boolean {
	return syscall === "prctl" && !/^PR_SET_(?:NAME|VMA)\b/.test(line.args[0] ?? "");
}

function processLimitDenied(line: TraceLine, syscall: string): boolean {
	return ["clone", "clone3", "fork", "vfork"].includes(syscall) && /^-1 EAGAIN\b/.test(line.result);
}

function syscallPaths(line: TraceLine, syscall: string, cwd: string): readonly string[] {
	return (PATH_ARGUMENTS[syscall] ?? []).flatMap(([pathname, dirfd, descriptorPath]) => {
		const descriptor = dirfd === undefined ? undefined : line.args[dirfd];
		if (pathname === undefined || (descriptorPath !== undefined && line.args[pathname] === descriptorPath)) {
			const target = absoluteDescriptorPath(descriptor); return target ? [target] : [];
		}
		const value = quotedArgument(line.args[pathname]);
		if (value?.startsWith("/")) return [path.posix.normalize(value)]; // Absolute names ignore dirfd, even an invalid one.
		const base = dirfd === undefined || descriptor === "AT_FDCWD" || descriptor === "-100" ? cwd : absoluteDescriptorPath(descriptor);
		if (!value || !base) throw new Error(`unresolved_pathname:${syscall}:${pathname}`);
		return [path.posix.resolve(base, value)];
	});
}

function absoluteDescriptorPath(descriptor: string | undefined): string | undefined {
	const target = /^(?:\d+|AT_FDCWD)<(.+)>$/.exec(descriptor?.trim() ?? "")?.[1]?.replace(/<[^<>]*>$/, "");
	return target?.startsWith("/") && !target.endsWith(" (deleted)") ? path.posix.normalize(decodeCString(target)) : undefined;
}

/** Non-path descriptors are already typed in the process key, but their kernel identity is volatile. */
function descriptorTarget(line: TraceLine): boolean {
	const target = /^\d+<(.+)>$/.exec(line.args[0] ?? "")?.[1];
	return Boolean(target && !target.startsWith("/"));
}

const STAT_MODE_BITS: Readonly<Record<string, bigint>> = {
	S_IFSOCK: 0o140000n,
	S_IFLNK: 0o120000n,
	S_IFREG: 0o100000n,
	S_IFBLK: 0o060000n,
	S_IFDIR: 0o040000n,
	S_IFCHR: 0o020000n,
	S_IFIFO: 0o010000n,
	S_ISUID: 0o004000n,
	S_ISGID: 0o002000n,
	S_ISVTX: 0o001000n,
};

/** Normalize the successful kernel stat structure printed by strace -v. */
function statObservationDigest(line: string): Sha256Digest | undefined {
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
		atimeNs: time("st_atime"),
		mtimeNs: time("st_mtime"),
		ctimeNs: time("st_ctime"),
	};
	if (Object.values(evidence).some((value) => value === undefined)) return undefined;
	return filesystemObservationDigest(evidence as FilesystemObservationEvidence);
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
	return ((major & 0xfffn) << 8n) |
		(minor & 0xffn) |
		((minor & ~0xffn) << 12n) |
		((major & ~0xfffn) << 32n);
}

function quotedStrings(line: TraceLine): string[] {
	return line.args.flatMap((argument) => {
		const value = quotedArgument(argument); return value === undefined ? [] : [value];
	});
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

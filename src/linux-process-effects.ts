import type { BigIntStats } from "node:fs";
import { link, lstat, mkdir, open, readFile, rm, rmdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { speculativeConfig } from "./config-schema.ts";
import {
	ContentChunkPool,
	restoreArtifactContent,
	writeContent,
} from "./content-chunks.ts";
import { EffectCommitFailure, isPoisonedEffectCommit } from "./execution-world.ts";
import {
	assertNoSymlinkPath,
	captureFilesystemEntry,
	filesystemObservationDigest,
	FilesystemReadView,
	settledFilesystemStat,
} from "./filesystem-evidence.ts";
import {
	containsFilesystemPath as pathContains,
	ignoreMissing,
	slash,
	stableEqual,
} from "./primitives.ts";
import {
	captureAbsenceDependency,
	captureDirectoryDependency,
	captureFileDependency,
	captureSymlinkDependency,
} from "./process-reuse.ts";
import type {
	DependencyRole,
	DynamicDependency,
	OrderedEffectEvent,
	ProvenanceTaint,
} from "./provenance-certificate.ts";
import type { ProvenanceCertificateStore, VerifiedArtifactClosure } from "./reuse-store.ts";
import type { SandboxDirectoryState, SandboxWorkspaceChange, WorkspaceSandboxService } from "./workspace-sandbox.ts";
import {
	assertFileAliases,
	captureWorkspaceStructureEntry,
	createParentDirectories,
	directoryEntriesDigest,
	ExecutionPathProjection,
	matchesSandboxBaseline,
	projectFileMutation,
	readRegularState,
} from "./workspace-sandbox.ts";

export interface ExecMount {
	readonly virtualPath: string;
	readonly hostPath: string;
}

export interface SandboxMount extends ExecMount {
	readonly readOnly: boolean;
}

export interface BufferedOutput {
	readonly fd: 1 | 2;
	readonly data: Buffer;
}

export function directoryState({ entriesDigest, mode, uid, gid }: SandboxDirectoryState) {
	return { entriesDigest, mode, uid, gid };
}

/** Output events carry the target's own descriptor. */
export function loadOutputEvents(
	artifacts: VerifiedArtifactClosure,
	journal: readonly OrderedEffectEvent[],
): BufferedOutput[] {
	return journal.flatMap((event) =>
		event.kind === "output" ? [{ fd: event.fd, data: artifacts.read(event.data) }] : [],
	);
}

/** A session's private branch under `storage`: its upper directory, its whiteout log and the host paths that log hides. */
export async function privateBranch(storage: string) {
	const upper = path.join(storage, "writes", "upper");
	const log = path.join(storage, "writes", "deleted.log");
	const deletions = ((await readFile(log, "utf8").catch(ignoreMissing)) ?? "")
		.split("\n")
		.filter(Boolean)
		.map(
			(line) =>
				`/${line.replace(/\\(.)/g, (_, escaped: string) => (escaped === "n" ? "\n" : escaped === "r" ? "\r" : escaped))}`,
		);
	return {
		upper,
		log,
		deleted: (target: string) =>
			deletions.some((entry) => target === entry || target.startsWith(`${entry}/`)),
	};
}

/** The nearest directory above `target` the host holds: where a change outside the workspace stages and commits. */
export async function hostRoot(target: string) {
	for (let directory = path.dirname(target); ; directory = path.dirname(directory)) {
		if (
			(await lstat(directory).catch(() => undefined))?.isDirectory() ||
			directory === path.dirname(directory)
		)
			return directory;
	}
}

export async function replayFilesystemEffects(
	owner: Pick<WorkspaceSandboxService, "commitDelta">,
	store: ProvenanceCertificateStore,
	artifacts: VerifiedArtifactClosure,
	journal: readonly OrderedEffectEvent[],
	projection: ExecutionPathProjection,
	privateStorage?: string,
) {
	await using privateReads = privateStorage ? FilesystemReadView.ambient() : undefined;
	const contents = new ContentChunkPool(store);
	try {
		const changes: SandboxWorkspaceChange[] = [];
		const { workspaceRoot } = projection;
		for (const event of journal) {
			if (event.kind === "output") continue;
			const target = projection.toPhysical(event.path);
			const inside = !!target && pathContains(workspaceRoot, target);
			if (
				!target ||
				target === path.resolve(workspaceRoot) ||
				(!inside && pathContains(projection.sourceRoot, target))
			) {
				throw new Error(`replay effect escapes workspace: ${event.path}`);
			}
			const root = inside ? workspaceRoot : await hostRoot(target);
			const address = {
				root,
				target,
				resource: slash(path.relative(root, target)),
			};
			if (event.before.kind === "directory" || event.after.kind === "directory") {
				if (
					[event.before, event.after].some((state) => state.kind !== "absent" && state.kind !== "directory")
				)
					throw new Error(`unsupported replay type change: ${event.path}`);
				changes.push({
					...address,
					kind: "directory",
					...(event.before.kind === "directory" ? { before: directoryState(event.before) } : {}),
					...(event.after.kind === "directory" ? { after: directoryState(event.after) } : {}),
				});
				continue;
			}
			changes.push({
				...address,
				...projectFileMutation(event, (name) => projection.toPhysical(name)!),
				...(event.before.kind === "file"
					? {
							before: await restoreArtifactContent(contents, event.before.data, artifacts),
							beforeMode: event.before.mode,
						}
					: {}),
				...(event.after.kind === "file"
					? {
							after: await restoreArtifactContent(contents, event.after.data, artifacts),
							afterMode: event.after.mode,
							afterModified: event.after.modified,
						}
					: {}),
			});
		}
		// Inside a speculative session, what lies outside the workspace belongs to its private branch until adoption.
		const { private: external, workspace = [] } = Object.groupBy(changes, (change) =>
			privateStorage && !pathContains(workspaceRoot, change.target) ? "private" : "workspace",
		);
		if (external) await privateReplay(owner, privateStorage!, external, privateReads!, contents, workspace);
		else if (workspace.length)
			await owner.commitDelta(
				{ changes: workspace, output: { result: { content: [], details: {} }, isError: false } },
				privateReads,
			);
	} finally {
		await contents.dispose();
	}
}

/** Apply changes in a session's private branch as its sandboxes would have: over the merged view's state, into its upper
 * directory, a removal of a host object as a whiteout its sandboxes read. */
export async function privateReplay(
	owner: Pick<WorkspaceSandboxService, "commitDelta">,
	storage: string,
	changes: readonly SandboxWorkspaceChange[],
	view: FilesystemReadView,
	contents?: ContentChunkPool,
	workspace: readonly SandboxWorkspaceChange[] = [],
) {
	const { upper, log, deleted } = await privateBranch(storage);
	const physical = (target: string) => path.join(upper, target);
	const projected = [...workspace];
	const seeds = new Map<string, NonNullable<Awaited<ReturnType<typeof readRegularState>>>>();
	const whiteouts: string[] = [];
	await mkdir(upper, { recursive: true });
	for (const change of changes) {
		const target = physical(change.target);
		await assertNoSymlinkPath(upper, target);
		const own = await lstat(target).catch(ignoreMissing);
		const lower = !deleted(change.target);
		const source = own ? target : lower ? change.target : undefined;
		const address = { root: upper, target, resource: path.relative(upper, target) };
		if (change.kind === "directory") {
			const captured =
				source && (await captureFilesystemEntry(source, "directory", view).catch(ignoreMissing));
			const entry = captured ? captureWorkspaceStructureEntry(captured) : undefined;
			if (entry && entry.kind !== "directory")
				throw new Error(`private replay expected directory: ${change.target}`);
			const state = entry && directoryState(entry);
			let visible = state;
			if (own && lower && state && captured) {
				const inherited = (
					await captureFilesystemEntry(change.target, "directory", view).catch(ignoreMissing)
				)?.entries;
				if (inherited?.length) {
					const names = new Map(
						inherited
							.filter((item) => !deleted(path.join(change.target, item.name)))
							.map((item) => [item.name, item]),
					);
					for (const item of captured.entries!) names.set(item.name, item);
					visible = { ...state, entriesDigest: directoryEntriesDigest([...names.values()]) };
				}
			}
			if (!matchesSandboxBaseline(change, visible))
				throw new Error(`resource changed before replay: ${change.target}`);
			const { before, after } = change;
			const modeChange =
				before && after && before.mode !== after.mode && before.entriesDigest === after.entriesDigest;
			if (own || after)
				projected.push({
					...change,
					...address,
					before: own ? state : undefined,
					operation: change.operation === "mkdir" || modeChange ? "copy_up" : change.operation,
				});
		} else {
			const state = source
				? await readRegularState(source, change.before?.byteLength, view, contents)
				: undefined;
			if (!matchesSandboxBaseline(change, state))
				throw new Error(`resource changed before replay: ${change.target}`);
			let aliases = change.aliases;
			if (!own && state) {
				const names = aliases ?? [change.target];
				if (aliases) await assertFileAliases(change.root, change.target, aliases, state.identity, view);
				const visible: string[] = [];
				for (const name of names) {
					const destination = physical(name);
					if (name !== change.target && (deleted(name) || (await lstat(destination).catch(ignoreMissing))))
						continue;
					visible.push(name);
					seeds.set(destination, state);
				}
				if (aliases) aliases = visible;
			}
			projected.push({ ...change, ...address, ...projectFileMutation({ ...change, aliases }, physical) });
		}
		if (!change.after && lower && (await lstat(change.target).catch(ignoreMissing)))
			whiteouts.push(
				change.target
					.slice(1)
					.replace(/[\\\n\r]/g, (char) => ({ "\n": "\\n", "\r": "\\r", "\\": "\\\\" })[char]!),
			);
	}
	const seeded: string[] = [];
	const parents: string[] = [];
	let committed = false;
	try {
		const objects = new Map<string, string>();
		for (const [target, state] of seeds) {
			await assertNoSymlinkPath(upper, target);
			await createParentDirectories(upper, target, view, parents);
			const key = `${state.identity!.dev}:${state.identity!.ino}`;
			const anchor = objects.get(key);
			if (anchor) {
				await link(anchor, target);
				seeded.push(target);
			} else {
				await using handle = await open(target, "wx", state.mode);
				seeded.push(target);
				await writeContent(handle, state.content);
				await handle.chmod(state.mode);
				objects.set(key, target);
			}
		}
		if (projected.length)
			await owner.commitDelta(
				{ changes: projected, output: { result: { content: [], details: {} }, isError: false } },
				view,
			);
		committed = true;
		if (whiteouts.length) await writeFile(log, whiteouts.join("\n") + "\n", { flag: "a" });
	} catch (error) {
		if (committed)
			throw new EffectCommitFailure("poisoned", "private whiteouts could not be published", error);
		if (!isPoisonedEffectCommit(error)) {
			for (const target of seeded.reverse()) await rm(target, { force: true });
			for (const target of parents.reverse()) await rmdir(target);
		}
		throw error;
	}
}

/** A filesystem mount names its access; an exec mount only its image. */
function sandboxMountArgument(mount: SandboxMount | ExecMount) {
	if (
		!path.isAbsolute(mount.virtualPath) ||
		!path.isAbsolute(mount.hostPath) ||
		[mount.virtualPath, mount.hostPath].some((value) => value.includes(":"))
	) {
		throw new Error(`Sandlock mount cannot represent ${mount.virtualPath}:${mount.hostPath}`);
	}
	return `${mount.virtualPath}:${mount.hostPath}${"readOnly" in mount ? (mount.readOnly ? ":ro" : ":rw") : ""}`;
}

export function sandboxPolicyArguments(
	cwd: string,
	deniedPaths: readonly string[],
	writablePaths: readonly string[],
	mounts: readonly SandboxMount[],
	execMounts: readonly ExecMount[],
	privateWrites: string,
) {
	return [
		"run",
		"--chroot",
		"/",
		// A write the host user could make but the policy does not grant lands in a private branch here, as it would succeed natively.
		"--workdir",
		"/",
		"--fs-storage",
		privateWrites,
		"--fs-branch",
		"writes",
		"--on-exit",
		"keep",
		"--on-error",
		"keep",
		...mounts.flatMap((mount) => ["--fs-mount", sandboxMountArgument(mount)]),
		...execMounts.flatMap((mount) => ["--exec-mount", sandboxMountArgument(mount)]),
		"--fs-read",
		"/",
		...writablePaths.flatMap((target) => ["--fs-write", target]),
		...[...speculativeConfig().processReuse.stableDevices].flatMap((target) => ["--fs-write", target]),
		...deniedPaths.flatMap((target) => ["--fs-deny", target]),
		"--no-huge-pages",
		"--no-coredump",
		"--max-processes",
		String(speculativeConfig().processReuse.maxProcesses),
		"--cwd",
		cwd,
	];
}

/** binfmt_script's interpreter (the first word after `#!`) or a little-endian ELF64's PT_INTERP. */
export async function imageInterpreter(file: string): Promise<string | undefined> {
	await using handle = await open(file, "r").catch(() => undefined);
	const head = Buffer.alloc(256);
	const read = handle ? (await handle.read(head, 0, 256, 0)).bytesRead : 0;
	if (head.subarray(0, 2).toString("latin1") === "#!")
		return /^[ \t]*([^ \t\n\0]+)/.exec(head.subarray(2, read).toString("latin1"))?.[1];
	if (read < 64 || head.readUInt32BE(0) !== 0x7f454c46 || head[4] !== 2 || head[5] !== 1) return undefined;
	const offset = Number(head.readBigUInt64LE(32));
	const size = head.readUInt16LE(54);
	const count = Math.min(head.readUInt16LE(56), speculativeConfig().processReuse.interpreterProgramHeaders);
	const table = Buffer.alloc(size * count);
	await handle!.read(table, 0, table.length, offset);
	for (let entry = 0; size >= 56 && entry < count; entry++) {
		if (table.readUInt32LE(entry * size) !== 3) continue;
		const name = Buffer.alloc(Math.min(Number(table.readBigUInt64LE(entry * size + 32)), 4096));
		await handle!.read(name, 0, name.length, Number(table.readBigUInt64LE(entry * size + 8)));
		return name.toString("latin1").split("\0")[0] || undefined;
	}
}

export async function captureHostObjectDependency(
	physical: string,
	logical: string,
	role: DependencyRole,
	listed: boolean,
	info: BigIntStats | undefined,
	view: FilesystemReadView,
	options?: { signal?: AbortSignal; maxFileBytes?: number },
): Promise<DynamicDependency | undefined> {
	if (!info) return captureAbsenceDependency(physical, logical, false, [], undefined, view);
	if (info.isDirectory())
		return captureDirectoryDependency(physical, logical, options !== undefined || !listed, [], listed, undefined, view);
	if (!info.isFile()) return undefined;
	return (await captureFileDependency(physical, logical, role, { ...options, view, includeMetadata: true })).dependency;
}

export async function captureContentAddressedDependency(
	target: string,
	role: DependencyRole | "metadata",
	view: FilesystemReadView,
	settings = speculativeConfig().processReuse,
): Promise<DynamicDependency | undefined> {
	if (role === "metadata" || !settings.contentAddressedPaths.some((root) => pathContains(root, target))) return;
	const info = await view.stat(target, false).catch(ignoreMissing);
	if (!info?.isFile() || !settledFilesystemStat(info, Date.now(), target)) return;
	const fields = ["dev", "ino", "mode", "size", "mtimeNs", "ctimeNs"] as const;
	return {
		kind: "metadata",
		path: slash(target),
		followSymlinks: false,
		fields,
		digest: filesystemObservationDigest(info, fields),
	};
}

export function mergeDynamicDependency(
	existing: DynamicDependency | undefined,
	incoming: DynamicDependency,
): DynamicDependency | "conflict" {
	if (!existing) return incoming;
	if (
		existing.kind === "file" &&
		incoming.kind === "file" &&
		stableEqual({ ...existing, role: incoming.role }, incoming)
	)
		return existing.role !== "executable" && incoming.role === "executable" ? incoming : existing;
	// A listing adds evidence to reaching the same directory; neither observation may replace conflicting evidence.
	if (
		existing.kind === incoming.kind &&
		(incoming.kind === "directory" || incoming.kind === "absence") &&
		Object.entries(incoming).every(
			([key, value]) => !(key in existing) || stableEqual(Reflect.get(existing, key), value),
		)
	)
		return { ...existing, ...incoming };
	return stableEqual(existing, incoming) ? existing : "conflict";
}

/** Also what memory allocators (jemalloc, glibc) read at startup to size their mappings. */
export function classifyObservedSpecialPath(
	observedPath: string,
	settings = speculativeConfig().processReuse,
	includeStableDevices = true,
): "ignored" | ProvenanceTaint | undefined {
	if (
		(includeStableDevices && settings.stableDevices.includes(observedPath)) ||
		settings.kernelConfigurationPaths.some((root) => pathContains(root, observedPath))
	)
		return "ignored";
	// A process reading its own state, or the host's CPU, sysctl, and cgroup limits, observes this one run like the clock or its pid.
	if (observedPath === "/proc/mounts") return "confinement_observation";
	if (/^\/proc\/(?:self|thread-self|\d+)(?:\/|$)/.test(observedPath))
		return /\/(?:limits|mounts|mountinfo|mountstats)$/.test(observedPath)
			? "confinement_observation"
			: "pid_observation";
	// Reopening an inherited descriptor reaches what that descriptor carries, like a jobserver pipe, not a host file.
	if (/^\/dev\/fd\/\d+$/.test(observedPath)) return "descriptor_observation";
	if (/^\/dev\/u?random$/.test(observedPath)) return "random";
	if (
		/^\/proc\/sys(?:\/|$)|^\/sys\/(?:devices\/system\/cpu|fs\/cgroup)(?:\/|$)|^\/proc\/(?:meminfo|version|version_signature|cpuinfo|stat|loadavg|uptime|filesystems)$/.test(
			observedPath,
		)
	)
		return "clock";
	return undefined;
}

export function createSymlinkPrefixCollector(view: FilesystemReadView, deniedPaths: readonly string[]) {
	const prefixes = new Map<string, Promise<DynamicDependency | undefined>>();
	const symlinkAt = (prefix: string) => {
		let pending = prefixes.get(prefix);
		if (!pending)
			prefixes.set(
				prefix,
				(pending = view
					.stat(prefix, false)
					.catch(ignoreMissing)
					.then((info) =>
						info?.isSymbolicLink() ? captureSymlinkDependency(prefix, prefix, view) : info && undefined,
					)),
			);
		return pending;
	};
	return async (observedPathname: string): Promise<DynamicDependency[]> => {
		const links: DynamicDependency[] = [];
		let prefix = path.parse(observedPathname).root;
		for (const segment of observedPathname.slice(prefix.length).split(path.sep).filter(Boolean)) {
			prefix = path.join(prefix, segment);
			const link = await symlinkAt(prefix);
			if (link) {
				const resolved = await view.realpath(prefix).catch(ignoreMissing);
				if (resolved && deniedPaths.some((denied) => pathContains(denied, resolved)))
					throw new Error(`actor_denied_input:${resolved}`);
				links.push(link);
			}
		}
		return links;
	};
}

export function sensitivePaths(storeRoot: string, additional: readonly string[] | undefined) {
	const home = os.homedir();
	const credentials = speculativeConfig().processReuse.sensitivePaths.map((name) =>
		name.startsWith("~/") ? path.join(home, name.slice(2)) : path.resolve(name),
	);
	return Object.freeze([
		...new Set(
			[storeRoot, ...credentials, ...(additional ?? [])]
				.filter(path.isAbsolute)
				.map((value) => path.resolve(value)),
		),
	]);
}

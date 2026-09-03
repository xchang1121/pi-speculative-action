import { createHash } from "node:crypto";
import { type FSWatcher, watch } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import {
	type ActionKey,
	type ActionSemanticsRegistry,
	PI_ACTION_SEMANTICS,
	type ResourceDependencyScope,
} from "./action-semantics.ts";
import {
	captureStableFile,
	containsFilesystemPath,
	filesystemPathKey,
	sameFilesystemIdentity,
} from "./filesystem-evidence.ts";

export type ResourceDependency = {
	readonly path: string;
	readonly scope: ResourceDependencyScope;
};

export type ResourceValidationMetrics = {
	readonly durationMs: number;
	readonly bytesRead: number;
	readonly filesRead: number;
	readonly mode: "watcher" | "exact";
};

export type ResourceVersionValidation = ResourceValidationMetrics & {
	readonly expired: boolean;
	readonly reason?: string;
};

export type ResourceChangeSet = {
	readonly uncertain: boolean;
	readonly paths: ReadonlyArray<string>;
};

export type ResourceVersionToken = {
	readonly root: string;
	readonly dependencies: ReadonlyArray<ResourceDependency>;
	readonly epoch: number;
	readonly versions: ReadonlyArray<number>;
	readonly preciseContent: ReadonlyArray<string>;
	readonly exact: ReadonlyArray<string>;
	readonly captureMs: number;
	readonly captureBytes: number;
	readonly captureFiles: number;
	readonly manager: ResourceVersionManager;
	readonly release: () => void;
};

type ResourceEvent = {
	readonly epoch: number;
	readonly path: string;
	readonly type: "change" | "rename" | "unknown";
	readonly source: "root" | "precise";
};

type ResourceSubscriber = {
	readonly dependencies: ReadonlyArray<ResourceDependency>;
	readonly preciseContent: ReadonlySet<string>;
	readonly callback: (path: string) => void;
};

const MAX_EVENT_HISTORY = 4096;
const FINGERPRINT_CONCURRENCY = 12;
const IGNORED_DIRECTORIES = new Set([".git"]);
const QUERY_CONTROL_FILES = new Set([".gitignore", ".ignore", ".rgignore"]);

export class ResourceVersionManager {
	private epoch = 0;
	private readonly events: ResourceEvent[] = [];
	private readonly subscribers = new Set<ResourceSubscriber>();
	private readonly preciseWatches = new Map<string, { readonly watcher: FSWatcher; references: number }>();
	private readonly treeContentVersions = new Map<string, number>();
	private readonly treeEntryVersions = new Map<string, number>();
	private readonly treeQueryVersions = new Map<string, number>();
	private references = 0;
	private watcher?: FSWatcher;
	private reliable = false;
	private ready: Promise<void> = Promise.resolve();
	readonly root: string;
	private readonly options: { readonly watch?: boolean; readonly onIdle?: () => void };

	constructor(root: string, options: { readonly watch?: boolean; readonly onIdle?: () => void } = {}) {
		this.root = root;
		this.options = options;
		if (options.watch === false) return;
		try {
			this.watcher = watch(root, { recursive: true }, (event, filename) => {
				const changed = filename ? path.resolve(root, filename) : root;
				this.changed(changed, event, "root");
			});
			this.watcher.on("error", () => {
				this.reliable = false;
				this.changed(root, "unknown", "root");
			});
			this.reliable = true;
			this.ready = watcherTurn();
		} catch {
			this.reliable = false;
		}
	}

	async capture(dependencies: ReadonlyArray<ResourceDependency>): Promise<ResourceVersionToken> {
		const started = performance.now();
		const reference = this.acquireReference();
		let normalized: ReadonlyArray<ResourceDependency>;
		try {
			await this.ready;
			normalized = normalizeDependencies(this.root, dependencies);
		} catch (error) {
			reference();
			throw error;
		}
		const precise = this.reliable
			? this.acquirePreciseWatches(normalized)
			: { paths: [] as string[], release: () => {} };
		const release = releaseOnce(() => {
			precise.release();
			reference();
		});
		try {
			const exact = await fingerprintDependencies(normalized, this.root);
			await watcherTurn();
			const epoch = this.epoch;
			const versions = this.reliable ? normalized.map((dependency) => this.dependencyVersion(dependency)) : [];
			return {
				root: this.root,
				dependencies: normalized,
				epoch,
				versions,
				preciseContent: precise.paths,
				exact: exact.fingerprints,
				captureMs: elapsed(started),
				captureBytes: exact.bytesRead,
				captureFiles: exact.filesRead,
				manager: this,
				release,
			};
		} catch (error) {
			release();
			throw error;
		}
	}

	async validate(token: ResourceVersionToken): Promise<ResourceVersionValidation> {
		const started = performance.now();
		if (token.manager !== this || token.root !== this.root) {
			return validation(started, true, "resource_version_owner_changed", "exact");
		}
		await watcherTurn();
		if (this.watcherInvalidated(token)) {
			return validation(started, true, "resource_changed", "watcher");
		}
		try {
			const exact = await fingerprintDependencies(token.dependencies, this.root);
			await watcherTurn();
			if (this.watcherInvalidated(token)) {
				return validation(started, true, "resource_changed", "watcher");
			}
			const expired = !sameStrings(exact.fingerprints, token.exact);
			return {
				expired,
				...validationMetrics(started, exact.bytesRead, exact.filesRead, "exact"),
				...(expired ? { reason: "resource_fingerprint_changed" } : {}),
			};
		} catch {
			return validation(started, true, "resource_validation_failed", "exact");
		}
	}

	changesSince(token: ResourceVersionToken): ResourceChangeSet {
		if (token.manager !== this || token.root !== this.root || !this.reliable) {
			return { uncertain: true, paths: [] };
		}
		const oldest = this.events[0]?.epoch ?? this.epoch;
		if (token.epoch < oldest && this.events.length >= MAX_EVENT_HISTORY) {
			return { uncertain: true, paths: [] };
		}
		const events = this.events.filter((event) => event.epoch > token.epoch);
		return {
			uncertain: events.some((event) => event.type === "unknown"),
			paths: [...new Set(events.map((event) => event.path))],
		};
	}

	subscribe(token: ResourceVersionToken, callback: (path: string) => void) {
		const subscriber: ResourceSubscriber = {
			dependencies: token.dependencies,
			preciseContent: new Set(token.preciseContent.map(filesystemPathKey)),
			callback,
		};
		this.subscribers.add(subscriber);
		let released = false;
		return () => {
			if (released) return;
			released = true;
			this.subscribers.delete(subscriber);
			token.release();
			this.checkIdle();
		};
	}

	close() {
		this.watcher?.close();
		this.watcher = undefined;
		this.reliable = false;
		for (const precise of this.preciseWatches.values()) precise.watcher.close();
		this.preciseWatches.clear();
		this.subscribers.clear();
		this.events.length = 0;
		this.treeContentVersions.clear();
		this.treeEntryVersions.clear();
		this.treeQueryVersions.clear();
	}

	private acquireReference(): () => void {
		this.references++;
		return releaseOnce(() => {
			this.references = Math.max(0, this.references - 1);
			this.checkIdle();
		});
	}

	private contentChangedSince(token: ResourceVersionToken) {
		const dependencies = token.dependencies.filter((dependency) => dependency.scope === "content");
		if (dependencies.length === 0) return false;
		if (this.changesSince(token).uncertain) return true;
		const preciseContent = new Set(token.preciseContent.map(filesystemPathKey));
		return this.events.some(
			(event) =>
				event.epoch > token.epoch && dependencies.some((dependency) => affects(dependency, event, preciseContent)),
		);
	}

	private watcherInvalidated(token: ResourceVersionToken): boolean {
		return (
			this.reliable &&
			token.versions.length === token.dependencies.length &&
			(!sameNumbers(
				token.dependencies.map((dependency) => this.dependencyVersion(dependency)),
				token.versions,
			) ||
				this.contentChangedSince(token))
		);
	}

	private changed(changedPath: string, type: ResourceEvent["type"], source: ResourceEvent["source"]) {
		const absolute = path.resolve(changedPath);
		const event = { epoch: ++this.epoch, path: absolute, type, source };
		this.events.push(event);
		if (this.events.length > MAX_EVENT_HISTORY) this.events.splice(0, this.events.length - MAX_EVENT_HISTORY);
		this.updateTreeVersions(event);
		for (const subscriber of this.subscribers) {
			if (subscriber.dependencies.some((dependency) => affects(dependency, event, subscriber.preciseContent))) {
				subscriber.callback(absolute);
			}
		}
	}

	private dependencyVersion(dependency: ResourceDependency): number {
		const key = filesystemPathKey(dependency.path);
		if (dependency.scope === "tree_content") return this.treeContentVersions.get(key) ?? 0;
		if (dependency.scope === "tree_entries") return this.treeEntryVersions.get(key) ?? 0;
		if (dependency.scope === "tree_query") return this.treeQueryVersions.get(key) ?? 0;
		return 0;
	}

	private updateTreeVersions(event: ResourceEvent): void {
		if (event.type !== "unknown" && path.basename(event.path).startsWith(".pi-speculative-")) return;
		const root = path.resolve(this.root);
		const absolute = path.resolve(event.path);
		if (!containsFilesystemPath(root, absolute)) return;
		const gitBoundary = event.type === "unknown" ? undefined : nestedGitBoundary(root, absolute);
		const entries = event.type === "unknown" || event.type === "rename";
		const query =
			event.type === "unknown" ||
			event.type === "rename" ||
			(event.type === "change" && QUERY_CONTROL_FILES.has(path.basename(event.path).toLowerCase()));
		for (let current = absolute; ; current = path.dirname(current)) {
			const key = filesystemPathKey(current);
			this.treeContentVersions.set(key, event.epoch);
			if (entries) this.treeEntryVersions.set(key, event.epoch);
			if (query) this.treeQueryVersions.set(key, event.epoch);
			if (current === root || current === gitBoundary) break;
		}
	}

	private acquirePreciseWatches(dependencies: ReadonlyArray<ResourceDependency>) {
		const paths: string[] = [];
		for (const target of new Set(
			dependencies.filter((dependency) => dependency.scope === "content").map((dependency) => dependency.path),
		)) {
			const key = filesystemPathKey(target);
			const existing = this.preciseWatches.get(key);
			if (existing) {
				existing.references++;
				paths.push(target);
				continue;
			}
			try {
				const watcher = watch(target, (event) => this.changed(target, event, "precise"));
				watcher.on("error", () => {
					this.reliable = false;
					this.changed(target, "unknown", "precise");
				});
				this.preciseWatches.set(key, { watcher, references: 1 });
				paths.push(target);
			} catch {
				// A missing file is covered conservatively by its nearest root-watcher event.
			}
		}
		let released = false;
		return {
			paths,
			release: () => {
				if (released) return;
				released = true;
				for (const target of paths) {
					const key = filesystemPathKey(target);
					const current = this.preciseWatches.get(key);
					if (!current) continue;
					current.references--;
					if (current.references > 0) continue;
					current.watcher.close();
					this.preciseWatches.delete(key);
				}
			},
		};
	}

	private checkIdle() {
		if (this.references || this.subscribers.size || this.preciseWatches.size) return;
		this.options.onIdle?.();
	}
}

const managers = new Map<string, ResourceVersionManager>();

export function resourceDependencies(
	action: ActionKey,
	root: string,
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
): ReadonlyArray<ResourceDependency> {
	const definition = actionSemantics.definition(action.tool);
	const scope = definition ? definition.resourceScope : "content";
	if (scope === undefined) return [];
	const dependencies = action.resources.map((resource) => ({
		path: path.resolve(root, resource),
		scope,
	}));
	if (scope === "tree_query") {
		for (const resource of action.resources) {
			const base = path.resolve(root, resource);
			const relative = path.relative(root, base);
			if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) continue;
			let current = path.resolve(root);
			for (const segment of relative.split(path.sep).filter(Boolean)) {
				for (const control of QUERY_CONTROL_FILES) {
					dependencies.push({ path: path.join(current, control), scope: "content" });
				}
				current = path.join(current, segment);
			}
		}
		dependencies.push({ path: path.resolve(root, ".git", "info", "exclude"), scope: "content" });
	}
	return dependencies;
}

export function captureResourceVersion(
	action: ActionKey,
	root: string,
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
) {
	return resourceVersionManager(root).capture(resourceDependencies(action, root, actionSemantics));
}

export function validateResourceVersion(token: unknown): Promise<ResourceVersionValidation> {
	if (!isResourceVersionToken(token)) {
		return Promise.resolve({
			expired: true,
			reason: "resource_version_missing",
			durationMs: 0,
			bytesRead: 0,
			filesRead: 0,
			mode: "exact",
		});
	}
	return token.manager.validate(token);
}

export function watchResourceVersion(token: unknown, callback: (path: string) => void) {
	if (!isResourceVersionToken(token)) return () => {};
	return token.manager.subscribe(token, callback);
}

export function releaseResourceVersion(token: unknown): void {
	if (!isResourceVersionToken(token)) return;
	token.release();
}

export function isResourceVersionToken(value: unknown): value is ResourceVersionToken {
	if (!value || typeof value !== "object") return false;
	const token = value as Partial<ResourceVersionToken>;
	return (
		typeof token.root === "string" &&
		typeof token.epoch === "number" &&
		Array.isArray(token.dependencies) &&
		Array.isArray(token.versions) &&
		Array.isArray(token.preciseContent) &&
		Array.isArray(token.exact) &&
		typeof token.release === "function" &&
		token.manager instanceof ResourceVersionManager
	);
}

export function closeResourceVersionManagers() {
	for (const manager of managers.values()) manager.close();
	managers.clear();
}

function resourceVersionManager(root: string) {
	const normalized = path.resolve(root);
	const existing = managers.get(normalized);
	if (existing) return existing;
	let manager: ResourceVersionManager;
	manager = new ResourceVersionManager(normalized, {
		onIdle: () => {
			if (managers.get(normalized) !== manager) return;
			manager.close();
			managers.delete(normalized);
		},
	});
	managers.set(normalized, manager);
	return manager;
}

function normalizeDependencies(root: string, dependencies: ReadonlyArray<ResourceDependency>) {
	const result = new Map<string, ResourceDependency>();
	for (const dependency of dependencies) {
		const absolute = path.resolve(root, dependency.path);
		if (!containsFilesystemPath(root, absolute)) {
			throw new Error(`resource dependency escapes workspace: ${dependency.path}`);
		}
		result.set(`${dependency.scope}:${filesystemPathKey(absolute)}`, { path: absolute, scope: dependency.scope });
	}
	return [...result.values()];
}

function affects(dependency: ResourceDependency, event: ResourceEvent, preciseContent: ReadonlySet<string>) {
	if (event.type === "unknown") return true;
	const dependencyPath = filesystemPathKey(dependency.path);
	const changed = filesystemPathKey(event.path);
	if (dependency.scope === "content") {
		if (dependencyPath === changed) return true;
		// Some recursive watchers report only the containing directory for a file write.
		return !preciseContent.has(dependencyPath) && dependencyPath.startsWith(`${changed}/`);
	}
	const inside = changed === dependencyPath || changed.startsWith(`${dependencyPath}/`);
	if (!inside) return false;
	if (path.basename(event.path).startsWith(".pi-speculative-")) return false;
	const relative = path.relative(dependency.path, event.path);
	if (relative === ".git" || relative.startsWith(`.git${path.sep}`)) return false;
	if (dependency.scope === "tree_entries") return event.type !== "change";
	if (dependency.scope === "tree_query") {
		return event.type !== "change" || QUERY_CONTROL_FILES.has(path.basename(event.path).toLowerCase());
	}
	return true;
}

async function fingerprintDependencies(dependencies: ReadonlyArray<ResourceDependency>, root: string) {
	const realRoot = await fingerprintIO(() => fs.realpath(root));
	const results = await mapLimit(dependencies, FINGERPRINT_CONCURRENCY, (dependency) =>
		fingerprintDependency(dependency, realRoot),
	);
	return {
		fingerprints: results.map((result) => result.fingerprint),
		bytesRead: results.reduce((total, result) => total + result.bytesRead, 0),
		filesRead: results.reduce((total, result) => total + result.filesRead, 0),
	};
}

async function fingerprintDependency(dependency: ResourceDependency, realRoot: string) {
	const result = await fingerprintPath(dependency.path, dependency.scope, realRoot, new Set());
	return {
		fingerprint: createHash("sha256")
			.update(
				JSON.stringify({ path: filesystemPathKey(dependency.path), scope: dependency.scope, value: result.value }),
			)
			.digest("hex"),
		bytesRead: result.bytesRead,
		filesRead: result.filesRead,
	};
}

type FingerprintResult = {
	readonly value: unknown;
	readonly bytesRead: number;
	readonly filesRead: number;
};

async function fingerprintPath(
	target: string,
	scope: ResourceDependencyScope,
	realRoot: string,
	ancestors: ReadonlySet<string>,
): Promise<FingerprintResult> {
	let info: import("node:fs").BigIntStats;
	try {
		info = await fingerprintIO(() => fs.lstat(target, { bigint: true }));
	} catch (error) {
		if (!missingResource(error)) throw error;
		await assertNearestExistingInside(realRoot, target);
		return { value: { exists: false, error: errorCode(error) }, bytesRead: 0, filesRead: 0 };
	}
	const realTarget = await fingerprintIO(() => fs.realpath(target));
	assertInside(realRoot, realTarget);
	const identity = filesystemPathKey(realTarget);
	if (ancestors.has(identity)) throw new Error(`resource_symlink_cycle:${target}`);
	if (info.isSymbolicLink()) {
		const link = await fingerprintIO(() => fs.readlink(target));
		const after = await fingerprintIO(() => fs.lstat(target, { bigint: true }));
		if (!after.isSymbolicLink() || !sameFilesystemIdentity(info, after)) {
			throw new Error(`resource_symlink_changed:${target}`);
		}
		const followed = await fingerprintPath(path.resolve(path.dirname(target), link), scope, realRoot, ancestors);
		return {
			value: {
				type: "symlink",
				link,
				mode: Number(after.mode),
				resolved: identity,
				target: followed.value,
			},
			bytesRead: followed.bytesRead,
			filesRead: followed.filesRead,
		};
	}
	if (info.isFile()) {
		if (scope === "tree_entries") {
			return stableFileEntry(target, info, identity);
		}
		if (scope === "tree_query" && !QUERY_CONTROL_FILES.has(path.basename(target).toLowerCase())) {
			return stableFileEntry(target, info, identity);
		}
		const content = await fingerprintIO(() => captureStableFile(target));
		assertInside(realRoot, content.realPath);
		return {
			value: {
				type: "file",
				mode: Number(content.stat.mode),
				size: content.bytesRead,
				hash: content.hash,
				resolved: filesystemPathKey(content.realPath),
			},
			bytesRead: content.bytesRead,
			filesRead: 1,
		};
	}
	if (!info.isDirectory()) {
		throw new Error(`unsupported_resource_type:${specialFileType(info)}:${target}`);
	}
	const entries = await fingerprintIO(() => fs.readdir(target, { withFileTypes: true }));
	const selected = selectEntries(entries);
	const descendants = new Set(ancestors).add(identity);
	const children = await mapLimit(selected, FINGERPRINT_CONCURRENCY, async (entry) => {
		const child = await fingerprintPath(path.join(target, entry.name), scope, realRoot, descendants);
		return { name: entry.name, ...child };
	});
	const [afterEntries, after] = await Promise.all([
		fingerprintIO(() => fs.readdir(target, { withFileTypes: true })),
		fingerprintIO(() => fs.lstat(target, { bigint: true })),
	]);
	if (
		!after.isDirectory() ||
		!sameFilesystemIdentity(info, after) ||
		!sameStrings(selected.map(entryIdentity), selectEntries(afterEntries).map(entryIdentity))
	) {
		throw new Error(`resource_directory_changed:${target}`);
	}
	return {
		value: {
			type: "directory",
			mode: Number(after.mode),
			resolved: identity,
			children: children.map((child) => ({ name: child.name, value: child.value })),
		},
		bytesRead: children.reduce((total, child) => total + child.bytesRead, 0),
		filesRead: children.reduce((total, child) => total + child.filesRead, 0),
	};
}

async function stableFileEntry(
	target: string,
	before: import("node:fs").BigIntStats,
	resolved: string,
): Promise<FingerprintResult> {
	const after = await fingerprintIO(() => fs.lstat(target, { bigint: true }));
	if (!after.isFile() || !sameFilesystemIdentity(before, after)) {
		throw new Error(`resource_file_changed:${target}`);
	}
	return { value: { type: "file", mode: Number(after.mode), resolved }, bytesRead: 0, filesRead: 0 };
}

function selectEntries(entries: ReadonlyArray<import("node:fs").Dirent>) {
	return entries
		.filter((entry) => !IGNORED_DIRECTORIES.has(entry.name))
		.sort((left, right) => left.name.localeCompare(right.name));
}

function entryIdentity(entry: import("node:fs").Dirent): string {
	return `${specialFileType(entry)}\0${entry.name}`;
}

function specialFileType(value: import("node:fs").Stats | import("node:fs").BigIntStats | import("node:fs").Dirent) {
	return value.isFile()
		? "file"
		: value.isDirectory()
			? "directory"
			: value.isSymbolicLink()
				? "symlink"
				: value.isFIFO()
					? "fifo"
					: value.isSocket()
						? "socket"
						: value.isCharacterDevice()
							? "character_device"
							: value.isBlockDevice()
								? "block_device"
								: "other";
}

async function assertNearestExistingInside(realRoot: string, target: string): Promise<void> {
	let current = path.resolve(target);
	for (;;) {
		try {
			assertInside(realRoot, await fingerprintIO(() => fs.realpath(current)));
			return;
		} catch (error) {
			if (!missingResource(error)) throw error;
		}
		const parent = path.dirname(current);
		if (parent === current) throw new Error(`resource_path_unresolved:${target}`);
		current = parent;
	}
}

function assertInside(realRoot: string, target: string): void {
	if (!containsFilesystemPath(realRoot, target)) throw new Error(`resource_symlink_escapes_workspace:${target}`);
}

async function mapLimit<Input, Output>(
	values: ReadonlyArray<Input>,
	limit: number,
	run: (value: Input, index: number) => Promise<Output>,
) {
	const output: Output[] = [];
	let cursor = 0;
	await Promise.all(
		Array.from({ length: Math.min(Math.max(1, limit), values.length) }, async () => {
			while (cursor < values.length) {
				const index = cursor++;
				output[index] = await run(values[index], index);
			}
		}),
	);
	return output;
}

function validation(
	started: number,
	expired: boolean,
	reason: string,
	mode: ResourceValidationMetrics["mode"],
): ResourceVersionValidation {
	return { expired, reason, ...validationMetrics(started, 0, 0, mode) };
}

function validationMetrics(
	started: number,
	bytesRead: number,
	filesRead: number,
	mode: ResourceValidationMetrics["mode"],
): ResourceValidationMetrics {
	return { durationMs: elapsed(started), bytesRead, filesRead, mode };
}

function elapsed(started: number) {
	return Math.max(0, performance.now() - started);
}

function sameStrings(left: ReadonlyArray<string>, right: ReadonlyArray<string>) {
	return left.length === right.length && left.every((value, index) => value === right[index]);
}

function sameNumbers(left: ReadonlyArray<number>, right: ReadonlyArray<number>) {
	return left.length === right.length && left.every((value, index) => value === right[index]);
}

function nestedGitBoundary(root: string, target: string) {
	const relative = path.relative(root, target);
	if (!relative || path.isAbsolute(relative)) return undefined;
	const parts = relative.split(path.sep);
	const index = parts.indexOf(".git");
	return index < 0 ? undefined : path.resolve(root, ...parts.slice(0, index + 1));
}

function watcherTurn() {
	return new Promise<void>((resolve) => setImmediate(resolve));
}

function errorCode(error: unknown) {
	return error && typeof error === "object" && "code" in error ? String(error.code) : "unknown";
}

function missingResource(error: unknown): boolean {
	return Boolean(
		error &&
			typeof error === "object" &&
			"code" in error &&
			(error.code === "ENOENT" || error.code === "ENOTDIR"),
	);
}

function releaseOnce(release: () => void): () => void {
	let released = false;
	return () => {
		if (released) return;
		released = true;
		release();
	};
}

class AsyncGate {
	private active = 0;
	private readonly waiting: Array<() => void> = [];
	private readonly limit: number;

	constructor(limit: number) {
		this.limit = limit;
	}

	async run<Value>(task: () => Promise<Value>) {
		if (this.active < this.limit) this.active++;
		else await new Promise<void>((resolve) => this.waiting.push(resolve));
		try {
			return await task();
		} finally {
			const next = this.waiting.shift();
			if (next) next();
			else this.active--;
		}
	}
}

const fingerprintGate = new AsyncGate(FINGERPRINT_CONCURRENCY);

function fingerprintIO<Value>(task: () => Promise<Value>) {
	return fingerprintGate.run(task);
}

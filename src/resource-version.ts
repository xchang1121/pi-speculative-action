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
	readonly watching: boolean;
	readonly versions: ReadonlyArray<number>;
	readonly preciseContent: ReadonlyArray<string>;
	readonly exact: ReadonlyArray<string>;
	readonly stamps: ReadonlyArray<string>;
	readonly manager: ResourceVersionManager;
	readonly release: () => void;
};

type ResourceEvent = {
	readonly epoch: number;
	readonly path: string;
	readonly type: "change" | "rename" | "unknown";
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
	private readonly treeVersions = new Map<string, number>();
	private references = 0;
	private watcher?: FSWatcher;
	private reliable = false;
	private ready: Promise<void> = Promise.resolve();
	private open = true;
	readonly root: string;
	private readonly options: { readonly watch?: boolean; readonly onIdle?: () => void };

	constructor(root: string, options: { readonly watch?: boolean; readonly onIdle?: () => void } = {}) {
		this.root = root;
		this.options = options;
		if (options.watch === false) return;
		try {
			this.watcher = watch(root, { recursive: true }, (event, filename) => {
				const changed = filename ? path.resolve(root, filename) : root;
				this.changed(changed, event);
			});
			this.watcher.on("error", () => {
				this.reliable = false;
				this.changed(root, "unknown");
			});
			this.reliable = true;
			this.ready = watcherTurn();
		} catch {
			this.reliable = false;
		}
	}

	async capture(dependencies: ReadonlyArray<ResourceDependency>): Promise<ResourceVersionToken> {
		if (!this.open) throw new Error("resource_version_manager_closed");
		const reference = this.acquireReference();
		let release = reference;
		try {
			await this.ready;
			const normalized = normalizeDependencies(this.root, dependencies);
			const precise = this.reliable
				? this.acquirePreciseWatches(normalized)
				: { paths: [] as string[], release: () => {} };
			release = releaseOnce(() => {
				precise.release();
				reference();
			});
			const exact = await fingerprintDependencies(normalized, this.root);
			await watcherTurn();
			const epoch = this.epoch;
			const versions = this.reliable ? normalized.map((dependency) => this.dependencyVersion(dependency)) : [];
			return {
				root: this.root,
				dependencies: normalized,
				epoch,
				watching: this.reliable,
				versions,
				preciseContent: precise.paths,
				exact: exact.fingerprints,
				stamps: exact.stamps,
				manager: this,
				release,
			};
		} catch (error) {
			release();
			throw error;
		}
	}

	async validate(token: ResourceVersionToken): Promise<ResourceVersionValidation> {
		return this.inspect(token, false);
	}

	async seal(token: ResourceVersionToken): Promise<ResourceVersionValidation> {
		return this.inspect(token, true);
	}

	private async inspect(token: ResourceVersionToken, sealing: boolean): Promise<ResourceVersionValidation> {
		const started = performance.now();
		if (!this.owns(token)) return validation(started, true, "resource_version_owner_changed", "exact");
		await watcherTurn();
		const watcherFailure = this.invalidation(token, sealing);
		if (watcherFailure) return validation(started, true, watcherFailure, "watcher");
		try {
			const current = await fingerprintDependencies(token.dependencies, this.root);
			await watcherTurn();
			const lateFailure = this.invalidation(token, sealing);
			if (lateFailure) return validation(started, true, lateFailure, "watcher");
			const expired =
				!sameValues(current.fingerprints, token.exact) ||
				(sealing && !sameValues(current.stamps, token.stamps));
			const reason = sealing ? "resource_observation_window_changed" : "resource_fingerprint_changed";
			return {
				expired,
				...validationMetrics(started, current.bytesRead, current.filesRead, "exact"),
				...(expired ? { reason } : {}),
			};
		} catch {
			const reason = sealing ? "resource_observation_window_unprovable" : "resource_validation_failed";
			return validation(started, true, reason, "exact");
		}
	}

	private invalidation(token: ResourceVersionToken, sealing: boolean): string | undefined {
		return sealing ? this.windowFailure(token) : this.watcherInvalidated(token) ? "resource_changed" : undefined;
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
		return releaseOnce(() => {
			this.subscribers.delete(subscriber);
			token.release();
			this.checkIdle();
		});
	}

	close() {
		this.open = false;
		this.watcher?.close();
		this.watcher = undefined;
		this.reliable = false;
		for (const precise of this.preciseWatches.values()) precise.watcher.close();
		this.preciseWatches.clear();
		this.subscribers.clear();
		this.events.length = 0;
		this.treeVersions.clear();
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
			(!sameValues(
				token.dependencies.map((dependency) => this.dependencyVersion(dependency)),
				token.versions,
			) ||
				this.contentChangedSince(token))
		);
	}

	private owns(token: ResourceVersionToken): boolean {
		return this.open && token.manager === this && token.root === this.root;
	}

	private windowFailure(token: ResourceVersionToken): string | undefined {
		if (!this.owns(token)) return "resource_version_owner_changed";
		if (!token.watching) return undefined;
		if (!this.reliable || this.changesSince(token).uncertain) return "resource_observation_window_unprovable";
		return this.watcherInvalidated(token) ? "resource_observation_window_changed" : undefined;
	}

	private changed(changedPath: string, type: ResourceEvent["type"]) {
		const absolute = path.resolve(changedPath);
		const event = { epoch: ++this.epoch, path: absolute, type };
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
		return this.treeVersions.get(`${dependency.scope}:${filesystemPathKey(dependency.path)}`) ?? 0;
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
			this.treeVersions.set(`tree_content:${key}`, event.epoch);
			if (entries) this.treeVersions.set(`tree_entries:${key}`, event.epoch);
			if (query) this.treeVersions.set(`tree_query:${key}`, event.epoch);
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
				const watcher = watch(target, (event) => this.changed(target, event));
				watcher.on("error", () => {
					this.reliable = false;
					this.changed(target, "unknown");
				});
				this.preciseWatches.set(key, { watcher, references: 1 });
				paths.push(target);
			} catch {
				// A missing file is covered conservatively by its nearest root-watcher event.
			}
		}
		return {
			paths,
			release: releaseOnce(() => {
				for (const target of paths) {
					const key = filesystemPathKey(target);
					const current = this.preciseWatches.get(key);
					if (!current) continue;
					current.references--;
					if (current.references > 0) continue;
					current.watcher.close();
					this.preciseWatches.delete(key);
				}
			}),
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
	return isResourceVersionToken(token)
		? token.manager.validate(token)
		: Promise.resolve(validation(performance.now(), true, "resource_version_missing", "exact"));
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
		typeof token.watching === "boolean" &&
		[token.dependencies, token.versions, token.preciseContent, token.exact, token.stamps].every(Array.isArray) &&
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
		stamps: results.map((result) => result.stamp),
		bytesRead: results.reduce((total, result) => total + result.bytesRead, 0),
		filesRead: results.reduce((total, result) => total + result.filesRead, 0),
	};
}

async function fingerprintDependency(dependency: ResourceDependency, realRoot: string) {
	const result = await fingerprintPath(dependency.path, dependency.scope, realRoot, new Set());
	return {
		fingerprint: digest({ path: filesystemPathKey(dependency.path), scope: dependency.scope, value: result.value }),
		stamp: result.stamp,
		bytesRead: result.bytesRead,
		filesRead: result.filesRead,
	};
}

type FingerprintResult = {
	readonly value: unknown;
	readonly stamp: string;
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
		return {
			value: { exists: false, error: errorCode(error) },
			stamp: digest([filesystemPathKey(target), await nearestExistingInside(realRoot, target)]),
			bytesRead: 0,
			filesRead: 0,
		};
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
			stamp: digest(["symlink", link, statStamp(after), followed.stamp]),
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
			stamp: digest(["file", statStamp(content.stat), filesystemPathKey(content.realPath)]),
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
		!sameValues(selected.map(entryIdentity), selectEntries(afterEntries).map(entryIdentity))
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
		stamp: digest([statStamp(after), children.map((child) => [child.name, child.stamp])]),
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
	return {
		value: { type: "file", mode: Number(after.mode), resolved },
		stamp: digest(["file", statStamp(after), resolved]),
		bytesRead: 0,
		filesRead: 0,
	};
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
	if (value.isFile()) return "file";
	if (value.isDirectory()) return "directory";
	if (value.isSymbolicLink()) return "symlink";
	if (value.isFIFO()) return "fifo";
	if (value.isSocket()) return "socket";
	if (value.isCharacterDevice()) return "character_device";
	return value.isBlockDevice() ? "block_device" : "other";
}

async function nearestExistingInside(realRoot: string, target: string): Promise<string> {
	let current = path.resolve(target);
	for (;;) {
		try {
			const [real, stat] = await Promise.all([
				fingerprintIO(() => fs.realpath(current)),
				fingerprintIO(() => fs.lstat(current, { bigint: true })),
			]);
			assertInside(realRoot, real);
			return digest([filesystemPathKey(real), statStamp(stat)]);
		} catch (error) {
			if (!missingResource(error)) throw error;
		}
		const parent = path.dirname(current);
		if (parent === current) throw new Error(`resource_path_unresolved:${target}`);
		current = parent;
	}
}

function statStamp(stat: import("node:fs").BigIntStats): string {
	return [stat.dev, stat.ino, stat.mode, stat.nlink, stat.rdev, stat.size, stat.mtimeNs, stat.ctimeNs, stat.birthtimeNs]
		.join(":");
}

function digest(value: unknown): string {
	return createHash("sha256").update(JSON.stringify(value)).digest("hex");
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
	return { durationMs: Math.max(0, performance.now() - started), bytesRead, filesRead, mode };
}

function sameValues<Value>(left: ReadonlyArray<Value>, right: ReadonlyArray<Value>) {
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
	const code = errorCode(error);
	return code === "ENOENT" || code === "ENOTDIR";
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

	async run<Value>(task: () => Promise<Value>) {
		if (this.active < FINGERPRINT_CONCURRENCY) this.active++;
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

const fingerprintGate = new AsyncGate();

function fingerprintIO<Value>(task: () => Promise<Value>) {
	return fingerprintGate.run(task);
}

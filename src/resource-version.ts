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
import { captureStableFile, sameFilesystemIdentity } from "./filesystem-evidence.ts";
import { containsFilesystemPath, filesystemPathKey } from "./path-utils.ts";

export type ResourceDependency = {
	readonly path: string;
	readonly scope: ResourceDependencyScope | "stat" | "names" | "binding";
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
	readonly physicalRoot: string;
	readonly observations: ReadonlyMap<string, ResourceDependency & { readonly fingerprint: string; readonly stamp?: string }>;
	readonly epoch: number;
	readonly watching: boolean;
	readonly preciseContent: ReadonlyArray<string>;
	readonly manager: ResourceVersionManager;
	/** Best-effort retained inputs; absence never weakens the token's exact freshness evidence. */
	readonly view?: ResourceReadView;
	readonly release: () => void;
};

type CapturedResource =
	| { readonly type: "file"; readonly content?: Buffer; readonly size?: number }
	| { readonly type: "directory"; readonly entries?: readonly string[] }
	| { readonly type: "alias"; readonly target: string }
	| { readonly type: "special" }
	| { readonly type: "missing" };

/** Token-owned input data, not a filesystem cache or authority to execute host functions. */
export class ResourceReadView {
	private entries = new Map<string, CapturedResource>();
	private owner?: ResourceReadView;
	private failure?: Error;
	private capturedBytes = 0;
	private sealed = false;
	private pending?: Promise<void>;
	private readonly maxBytes: number;
	private readonly load?: (dependency: ResourceDependency) => Promise<void>;
	constructor(maxBytes: number, load?: (dependency: ResourceDependency) => Promise<void>) {
		if (!Number.isFinite(maxBytes) || maxBytes < 0) throw new Error("resource_snapshot_budget_invalid");
		this.maxBytes = maxBytes;
		this.load = load;
	}
	get bytes(): number { return this.capturedBytes; }
	get retained(): boolean { return this.failure === undefined && this.owner?.retained !== false; }

	reserve(bytes: number): boolean {
		if (this.sealed) throw new Error("resource_snapshot_not_capturing");
		if (this.failure) { if (this.load) throw this.failure; return false; }
		if (!Number.isSafeInteger(bytes) || bytes < 0) throw new Error("resource_snapshot_budget_invalid");
		if (this.bytes + bytes > this.maxBytes) {
			this.failure = new Error("resource_snapshot_budget_exceeded");
			this.entries.clear();
			if (this.load) throw this.failure;
			return false;
		}
		this.capturedBytes += bytes;
		return true;
	}
	capture(target: string, entry: CapturedResource): void {
		if (!this.reserve(Buffer.byteLength(target) + 64 + (entry.type === "directory"
			? entry.entries?.reduce((sum, name) => sum + Buffer.byteLength(name) + 16, 0) ?? 0
			: entry.type === "alias" ? Buffer.byteLength(entry.target) : 0))) return;
		const key = filesystemPathKey(target), previous = this.entries.get(key);
		// Overlapping scopes enrich one input view; a metadata observation cannot erase its payload.
		if ((entry.type === "file" && previous?.type === "file" && entry.content === undefined && (previous.content !== undefined || entry.size === undefined)) ||
			(entry.type === "directory" && previous?.type === "directory" && entry.entries === undefined)) return;
		this.entries.set(key, entry);
	}
	alias(target: string, source: string): void {
		if (!this.retained) return;
		this.entry(source);
		this.capture(target, { type: "alias", target: filesystemPathKey(source) });
	}
	exists = async (target: string): Promise<boolean> => (await this.get(target, "stat")).type !== "missing";
	stat = async (target: string): Promise<{ isDirectory: () => boolean; size?: number }> => {
		const entry = await this.get(target, "stat");
		if (entry.type === "missing") return this.unproven(target);
		return { isDirectory: () => entry.type === "directory", size: entry.type === "file" ? entry.content?.length ?? entry.size : undefined };
	};
	readdir = async (target: string): Promise<string[]> => {
		const entry = await this.get(target, "names");
		return entry.type === "directory" && entry.entries ? [...entry.entries] : this.unproven(target);
	};
	readFile = async (target: string, maxBytes?: number): Promise<Buffer> => {
		const entry = await this.get(target, "content");
		return entry.type === "file" && entry.content !== undefined ? Buffer.from(entry.content.subarray(0, maxBytes)) : this.unproven(target);
	};
	access = async (target: string): Promise<void> => {
		const entry = await this.get(target, "content");
		if (entry.type !== "file" || entry.content === undefined) this.unproven(target);
	};
	/** Each evaluation owns its failures, but borrows the same sealed inputs and lifetime. */
	async evaluate<T>(operation: (view: ResourceReadView) => Promise<T>): Promise<T> {
		this.assertComplete(true);
		const view = new ResourceReadView(0);
		view.entries = this.entries; view.owner = this; view.sealed = true;
		try {
			const output = await operation(view);
			view.assertComplete();
			return output;
		} finally { view.dispose(); }
	}
	assertComplete(sealed = false): void {
		this.owner?.assertComplete(); if (this.failure) throw this.failure;
		if (sealed && !this.sealed) throw new Error("resource_snapshot_not_sealed");
	}
	seal(): void {
		if (this.pending) this.failure ??= new Error("resource_snapshot_capture_pending");
		this.assertComplete(); this.sealed = true;
	}
	dispose(): void { if (!this.owner) this.entries.clear(); this.failure = new Error("resource_snapshot_disposed"); }
	private async get(target: string, scope: ResourceDependency["scope"]) {
		this.assertComplete();
		if (this.load && !this.sealed) {
			const pending = (this.pending ?? Promise.resolve()).then(() => { this.assertComplete(); return this.load!({ path: target, scope }); });
			this.pending = pending;
			try { await pending; }
			catch (error) { throw this.failure ??= error instanceof Error ? error : new Error(String(error)); }
			finally { if (this.pending === pending) this.pending = undefined; }
		}
		return this.entry(target);
	}
	private entry(target: string): Exclude<CapturedResource, { type: "alias" }> {
		this.assertComplete();
		let current = filesystemPathKey(target);
		const visited = new Set<string>();
		while (!visited.has(current) && visited.size <= this.entries.size) {
			visited.add(current);
			const exact = this.entries.get(current);
			if (exact?.type === "alias") { current = exact.target; continue; }
			if (exact) return exact;
			let parent = path.dirname(current);
			while (parent !== path.dirname(parent) && this.entries.get(parent)?.type !== "alias") parent = path.dirname(parent);
			const alias = this.entries.get(parent);
			if (alias?.type !== "alias") break;
			current = filesystemPathKey(path.resolve(alias.target, path.relative(parent, current)));
		}
		return this.unproven(target);
	}
	private unproven(target: string): never {
		throw (this.failure ??= new Error(`resource_access_unproven:${target}`));
	}
}

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

export class ResourceVersionManager {
	private epoch = 0;
	private readonly events: ResourceEvent[] = [];
	private readonly subscribers = new Set<ResourceSubscriber>();
	private readonly preciseWatches = new Map<string, { readonly watcher: FSWatcher; references: number }>();
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

	/** Undefined dependencies grant only bounded on-demand captures; observation of host tools stays eager. */
	async capture(dependencies: ReadonlyArray<ResourceDependency> | undefined, retainBytes?: number): Promise<ResourceVersionToken> {
		if (!this.open) throw new Error("resource_version_manager_closed");
		if (dependencies?.length === 0 || (!dependencies && retainBytes === undefined)) throw new Error("resource_dependencies_unproven");
		const observations = new Map<string, ResourceDependency & { fingerprint: string; stamp?: string }>(), preciseContent: string[] = [];
		const releases = [this.acquireReference()];
		let view: ResourceReadView | undefined;
		const release = releaseOnce(() => { view?.dispose(); observations.clear(); for (const stop of releases.reverse()) stop(); });
		try {
			await this.ready;
			const physicalRoot = await fingerprintIO(() => fs.realpath(this.root));
			const capture = async (requested: ReadonlyArray<ResourceDependency>) => {
				const normalized = normalizeDependencies(this.root, requested).filter((dependency) => !observations.has(dependencyKey(dependency)));
				if (!normalized.length) return;
				// A leaf's identity does not prove its name stayed bound through ancestor A→B→A.
				const ancestors = dependencies ? normalized.map((dependency) => dependency.path) : [];
				while (ancestors.length) {
					const target = ancestors.pop()!, dependency = { path: target, scope: "binding" as const };
					if (observations.has(dependencyKey(dependency))) continue;
					const binding = await fingerprintBinding(dependency);
					observations.set(dependencyKey(dependency), binding);
					if (path.dirname(target) !== target) ancestors.push(path.dirname(target));
					if (binding.link !== undefined) ancestors.push(path.resolve(path.dirname(target), binding.link));
				}
				const precise = this.reliable ? this.acquirePreciseWatches(normalized) : undefined;
				if (precise) { releases.push(precise.release); preciseContent.push(...precise.paths); }
				for (const observation of await fingerprintDependencies(normalized, physicalRoot, view)) observations.set(dependencyKey(observation), observation);
			};
			view = retainBytes === undefined ? undefined : new ResourceReadView(retainBytes, dependencies ? undefined : (dependency) => capture([dependency]));
			if (dependencies) await capture(dependencies);
			await watcherTurn();
			const retained = view?.retained ? view : undefined;
			if (dependencies) retained?.seal();
			return {
				root: this.root, physicalRoot, observations, epoch: this.epoch, watching: this.reliable, preciseContent,
				manager: this, ...(retained ? { view: retained } : {}), release,
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
		if (!this.open || token.manager !== this || token.root !== this.root)
			return validation(started, true, "resource_version_owner_changed", "exact");
		try {
			if (sealing) token.view?.seal(); else token.view?.assertComplete(true);
			// Host windows need eager binding evidence; Windows can restore a junction without changing its stamps.
			if (sealing && (process.platform === "win32" || !token.observations.has(`binding:${filesystemPathKey(this.root)}`))) throw new Error("resource_path_binding_window_unprovable");
			if (!token.observations.size) throw new Error("resource_dependencies_unproven");
			await watcherTurn();
			const watcherFailure = this.invalidation(token, sealing);
			if (watcherFailure) return validation(started, true, watcherFailure, "watcher");
			const current = await fingerprintDependencies([...token.observations.values()].filter((entry) => sealing || entry.scope !== "binding"), token.physicalRoot);
			await watcherTurn();
			const lateFailure = this.invalidation(token, sealing);
			if (lateFailure) return validation(started, true, lateFailure, "watcher");
			const expired = !current.length || current.some((entry) => {
				const captured = token.observations.get(dependencyKey(entry));
				return entry.fingerprint !== captured?.fingerprint || (sealing && (!entry.stamp || !captured?.stamp || entry.stamp !== captured.stamp));
			});
			const reason = sealing ? "resource_observation_window_changed" : "resource_fingerprint_changed";
			return {
				expired,
				...validationMetrics(started, current.reduce((sum, entry) => sum + entry.bytesRead, 0), current.reduce((sum, entry) => sum + entry.filesRead, 0), "exact"),
				...(expired ? { reason } : {}),
			};
		} catch {
			const reason = sealing ? "resource_observation_window_unprovable" : "resource_validation_failed";
			return validation(started, true, reason, "exact");
		}
	}

	private invalidation(token: ResourceVersionToken, sealing: boolean): string | undefined {
		if (!this.open || token.manager !== this || token.root !== this.root) return "resource_version_owner_changed";
		if (!sealing || !token.watching) return undefined; // Future reuse compares semantics; events only disprove a host execution window.
		if (!this.reliable || this.changesSince(token).uncertain) return "resource_observation_window_unprovable";
		const precise = new Set(token.preciseContent.map(filesystemPathKey));
		const changed = this.events.some((event) => event.epoch > token.epoch &&
			[...token.observations.values()].some((dependency) => affects(dependency, event, precise, sealing)));
		return changed ? "resource_observation_window_changed" : undefined;
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
			dependencies: [...token.observations.values()],
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
	}

	private acquireReference(): () => void {
		this.references++;
		return releaseOnce(() => {
			this.references = Math.max(0, this.references - 1);
			this.checkIdle();
		});
	}

	private changed(changedPath: string, type: ResourceEvent["type"]) {
		const absolute = path.resolve(changedPath);
		const event = { epoch: ++this.epoch, path: absolute, type };
		this.events.push(event);
		if (this.events.length > MAX_EVENT_HISTORY) this.events.splice(0, this.events.length - MAX_EVENT_HISTORY);
		for (const subscriber of this.subscribers) {
			if (subscriber.dependencies.some((dependency) => affects(dependency, event, subscriber.preciseContent))) {
				subscriber.callback(absolute);
			}
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
) {
	const definition = actionSemantics.definition(action);
	const scope = definition ? definition.resourceScope : "content";
	if (scope === undefined) return [];
	return action.resources.map((resource) => ({
		path: path.resolve(root, resource),
		scope,
	}));
}

export function captureResourceVersion(
	action: ActionKey | undefined,
	root: string,
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
	retainBytes?: number,
) {
	return resourceVersionManager(root).capture(action ? resourceDependencies(action, root, actionSemantics) : undefined, retainBytes);
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
		typeof token.physicalRoot === "string" &&
		typeof token.epoch === "number" &&
		typeof token.watching === "boolean" &&
		token.observations instanceof Map && Array.isArray(token.preciseContent) &&
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
		const normalized = { path: absolute, scope: dependency.scope };
		result.set(dependencyKey(normalized), normalized);
	}
	return [...result.values()];
}

const dependencyKey = (dependency: ResourceDependency) => `${dependency.scope}:${filesystemPathKey(dependency.path)}`;

function affects(dependency: ResourceDependency, event: ResourceEvent, preciseContent: ReadonlySet<string>, sealing = false) {
	if (dependency.scope === "binding") return sealing && event.type === "rename" && filesystemPathKey(dependency.path) === filesystemPathKey(event.path);
	if (event.type === "unknown") return true;
	const dependencyPath = filesystemPathKey(dependency.path);
	const changed = filesystemPathKey(event.path);
	if (dependency.scope === "content" || dependency.scope === "stat") {
		if (dependencyPath === changed) return true;
		// Some recursive watchers report only the containing directory for a file write.
		return !preciseContent.has(dependencyPath) && containsFilesystemPath(changed, dependencyPath);
	}
	if (!containsFilesystemPath(dependencyPath, changed)) return false;
	if (dependency.scope === "entries" || dependency.scope === "names") {
		return event.type !== "change" && (dependencyPath === changed || dependencyPath === filesystemPathKey(path.dirname(event.path)));
	}
	return true;
}

async function fingerprintDependencies(dependencies: ReadonlyArray<ResourceDependency>, realRoot: string, view?: ResourceReadView) {
	return mapLimit(dependencies, FINGERPRINT_CONCURRENCY, async (dependency) => {
		if (dependency.scope === "binding") return fingerprintBinding(dependency);
		const { value, ...metrics } = await fingerprintPath(dependency.path, dependency.scope, realRoot, new Set(), view);
		return { ...dependency, fingerprint: digest({ path: filesystemPathKey(dependency.path), scope: dependency.scope, value }), ...metrics };
	});
}

async function fingerprintBinding(dependency: ResourceDependency) {
	let stamp: string | undefined, link: string | undefined;
	try {
		const before = await fingerprintIO(() => fs.lstat(dependency.path, { bigint: true }));
		if (before.isSymbolicLink()) link = await fingerprintIO(() => fs.readlink(dependency.path));
		const after = await fingerprintIO(() => fs.lstat(dependency.path, { bigint: true }));
		stamp = sameFilesystemIdentity(before, after) ? digest([statStamp(after), link]) : undefined;
	} catch (error) {
		if (!missingResource(error)) throw error;
		stamp = errorCode(error);
	}
	return { ...dependency, fingerprint: "binding", stamp, link, bytesRead: 0, filesRead: 0 };
}

type FingerprintResult = {
	readonly value: unknown;
	readonly stamp: string;
	readonly bytesRead: number;
	readonly filesRead: number;
};

async function fingerprintPath(
	target: string,
	scope: ResourceDependency["scope"],
	realRoot: string,
	ancestors: ReadonlySet<string>,
	view?: ResourceReadView,
	descend = true,
): Promise<FingerprintResult> {
	let info: import("node:fs").BigIntStats;
	try {
		info = await fingerprintIO(() => fs.lstat(target, { bigint: true }));
	} catch (error) {
		if (!missingResource(error)) throw error;
		view?.capture(target, { type: "missing" });
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
		const source = path.resolve(path.dirname(target), link);
		const followed = await fingerprintPath(source, scope, realRoot, ancestors, view, descend);
		view?.alias(target, source);
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
	if (scope === "stat" || (scope === "entries" && !descend) || (["names", "entries", "tree_entries"].includes(scope) && !info.isDirectory())) {
		view?.capture(target, { type: info.isDirectory() ? "directory" : info.isFile() ? "file" : "special",
			...(scope === "stat" && info.isFile() ? { size: Number(info.size) } : {}) });
		return stableEntry(target, info, identity, scope);
	}
	if (info.isFile()) {
		// Reserve before yielding: concurrent captures cannot each spend the entire token budget.
		const retain = view?.reserve(Number(info.size)) ?? false;
		const content = await fingerprintIO(() => captureStableFile(target, retain ? Number(info.size) : undefined, retain));
		assertInside(realRoot, content.realPath);
		view?.capture(target, { type: "file", content: content.content });
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
	if (!info.isDirectory() || scope === "content") {
		throw new Error(`unsupported_resource_type:${specialFileType(info)}:${target}`);
	}
	const entries = await fingerprintIO(() => fs.readdir(target, { withFileTypes: true }));
	const selected = [...entries].sort((left, right) => left.name.localeCompare(right.name));
	const descendants = new Set(ancestors).add(identity);
	const children = scope === "names" ? [] : await mapLimit(selected, FINGERPRINT_CONCURRENCY, async (entry) => {
		const child = await fingerprintPath(path.join(target, entry.name), scope, realRoot, descendants, view, scope !== "entries");
		return { name: entry.name, ...child };
	});
	const [afterEntries, after] = await Promise.all([
		fingerprintIO(() => fs.readdir(target, { withFileTypes: true })),
		fingerprintIO(() => fs.lstat(target, { bigint: true })),
	]);
	if (
		!after.isDirectory() ||
		!sameFilesystemIdentity(info, after) ||
		!sameValues(entries.map(entryIdentity), afterEntries.map(entryIdentity))
	) {
		throw new Error(`resource_directory_changed:${target}`);
	}
	view?.capture(target, { type: "directory", entries: entries.map((entry) => entry.name) });
	return {
		value: {
			type: "directory",
			mode: Number(after.mode),
			resolved: identity,
			order: entries.map((entry) => entry.name),
			children: children.map((child) => ({ name: child.name, value: child.value })),
		},
		stamp: digest([statStamp(after), children.map((child) => [child.name, child.stamp])]),
		bytesRead: children.reduce((total, child) => total + child.bytesRead, 0),
		filesRead: children.reduce((total, child) => total + child.filesRead, 0),
	};
}

async function stableEntry(
	target: string,
	before: import("node:fs").BigIntStats,
	resolved: string,
	scope: ResourceDependency["scope"],
): Promise<FingerprintResult> {
	const after = await fingerprintIO(() => fs.lstat(target, { bigint: true }));
	if (!sameFilesystemIdentity(before, after)) {
		throw new Error(`resource_file_changed:${target}`);
	}
	return {
		value: { type: specialFileType(after), mode: Number(after.mode), resolved, size: scope === "stat" && after.isFile() ? Number(after.size) : undefined },
		stamp: digest([statStamp(after), resolved]),
		bytesRead: 0,
		filesRead: 0,
	};
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

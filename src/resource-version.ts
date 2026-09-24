import { hash } from "node:crypto";
import { type BigIntStats, type Stats, type FSWatcher, watch } from "node:fs";
import fs, { type FileHandle } from "node:fs/promises";
import path from "node:path";
import { setImmediate as watcherTurn } from "node:timers/promises";
import {
	type ActionKey,
	type ActionSemanticsRegistry,
	PI_ACTION_SEMANTICS,
	type ResourceDependencyScope,
} from "./action-semantics.ts";
import { type StableFilesystemCapture, captureFilesystemEntry, captureStableFile, FILESYSTEM_CONCURRENCY, mapFilesystem, sameFilesystemIdentity, walkFilesystemPath } from "./filesystem-evidence.ts";
import { containsFilesystemPath, filesystemPathKey } from "./path-utils.ts";
import type { ToolFilesystemOperations, ToolFilesystemStat } from "./tool-settlement.ts";
import { RuntimeLifecycleLane } from "./runtime-lifecycle.ts";

export type ResourceDependency = {
	readonly path: string;
	readonly scope: ResourceDependencyScope | "stat" | "type" | "entry" | "names" | "binding" | "resolution";
};

export type ResourceObservation = ResourceDependency & { readonly fingerprint: string; readonly stamp?: string;
	/** Named edges captured in this tree; links exceeding paths indicate an open namespace. */
	readonly aliases?: readonly { readonly paths: readonly string[]; readonly links: number }[] };

/** Declared poststates remain proposals until exact adoption validation. */
export type ResourceInput = Uint8Array | { readonly names?: readonly string[] } | null;

export type ResourceValidationMetrics = {
	readonly durationMs: number;
	readonly bytesRead: number;
	readonly filesRead: number;
	readonly mode: "watcher" | "exact";
};

export type ResourceVersionValidation = ResourceValidationMetrics & {
	readonly expired: boolean;
	readonly reason?: string;
	readonly changed?: readonly string[];
};

export type ResourceChangeSet = {
	readonly uncertain: boolean;
	readonly paths: ReadonlyArray<string>;
};

export type ResourceVersionToken = {
	readonly root: string;
	readonly physicalRoot: string;
	readonly observations: ReadonlyMap<string, ResourceObservation>;
	readonly epoch: number;
	readonly watching: boolean;
	readonly preciseContent: ReadonlyArray<string>;
	readonly manager: ResourceVersionManager;
	/** Best-effort retained inputs; absence never weakens the token's exact freshness evidence. */
	readonly view?: ResourceReadView;
	/** Non-owning revocation link carried by derived proofs; never grants input access. */
	readonly inputView?: WeakRef<ResourceReadView>;
	/** Revoke access immediately; completion includes admitted reads and ownership release. */
	readonly release: () => void | Promise<void>;
};

type CapturedResource = (
	| { readonly type: "file"; readonly content?: Buffer; readonly size?: number }
	| { readonly type: "directory"; readonly entries?: readonly string[] }
	| { readonly type: "alias"; readonly target?: string; readonly link: string }
	| { readonly type: "special" }
	| { readonly type: "missing" }) & { readonly realPath?: string; readonly dependency?: string; readonly metadataDependency?: string;
		readonly object?: StableFilesystemCapture["object"] | null; readonly objectBytes?: number };

type ResourceInputSource = {
	readonly view: ResourceReadView;
	readonly observed: (dependencies: ReadonlySet<string> | undefined) => readonly ResourceVersionToken[] | void;
};
type ResourceInputLookup = (target: string) => Iterable<ResourceInputSource>;
type PreparedResource = {
	value?: unknown; readonly dispose: () => void | Promise<void>;
	readonly resource?: string;
	dependencies?: ReadonlySet<string>; readonly boundary?: { readonly root: string; readonly physicalRoot: string };
	readonly origin: ResourceReadView; destination: ResourceReadView; ready?: Promise<void>; readonly declined: Promise<void>; readonly composed?: Promise<void>;
	borrowers: number; revoked?: boolean; retained?: boolean; shareable?: boolean; proofs?: ResourceVersionToken[];
};

/** Token-owned input data, not a filesystem cache or authority to execute host functions. */
export class ResourceReadView {
	private entries = new Map<string, CapturedResource & { readonly bytes: number }>();
	private owner?: ResourceReadView;
	private failure?: Error;
	private capturedBytes = 0;
	private objectCount = 0;
	private inputEpoch = 0;
	private sealed = false;
	private pending?: Promise<void>;
	private disposal?: Promise<void>;
	private dependencies?: Set<string>;
	private lookup?: ResourceInputLookup;
	private missing?: () => Promise<ResourceInputSource>;
	private foreignInputs = false;
	private onForeignInputs?: (transferable: boolean) => void;
	private collectProofs?: Map<ResourceVersionToken["observations"], ResourceVersionToken>;
	private acceptProofs?: (proofs: readonly ResourceVersionToken[]) => readonly ResourceVersionToken[];
	private prepared?: { readonly lifetime: RuntimeLifecycleLane; readonly bindings: Map<object, Map<string, PreparedResource>> };
	private boundary?: { readonly root: string; readonly physicalRoot: string; readonly dependency?: string };
	private readonly maxBytes: number;
	private readonly load?: (dependency: ResourceDependency) => Promise<void>;
	private readonly observeObject?: (target: string, capture: StableFilesystemCapture) => void;
	constructor(maxBytes: number, load?: (dependency: ResourceDependency) => Promise<void>, boundary?: ResourceReadView["boundary"],
		observeObject?: ResourceReadView["observeObject"]) {
		if (!Number.isFinite(maxBytes) || maxBytes < 0) throw new Error("resource_snapshot_budget_invalid");
		this.maxBytes = maxBytes;
		this.load = load;
		this.boundary = boundary;
		this.observeObject = observeObject;
	}
	get bytes(): number { return this.capturedBytes; }
	get remainingBytes(): number { return Math.max(0, this.maxBytes - this.capturedBytes); }
	get hasPreparedInputs(): boolean { return [...this.prepared?.bindings.values() ?? []].some(entries => [...entries.values()].some(entry => entry.retained && !entry.revoked && entry.destination.retained)); }
	get canRetainObject(): boolean { return process.platform === "linux" && this.objectCount < 64; }
	get retained(): boolean { return this.failure === undefined && this.owner?.retained !== false; }
	get resources() {
		this.assertComplete(true);
		const resources = [...this.entries].map(([path, entry]) => ({ path, descendants: entry.type === "alias" && entry.target !== undefined }));
		for (const entries of this.prepared?.bindings.values() ?? []) for (const cached of entries.values()) {
			if (cached.retained && cached.resource && !this.entries.has(cached.resource)) resources.push({ path: cached.resource, descendants: false });
		}
		return resources;
	}
	/** Keep proven path metadata; payloads and preparations remain with their original owner. */
	retainMetadata(observations: ReadonlyMap<string, ResourceObservation>, maxBytes: number): ResourceReadView | undefined {
		this.assertComplete(true);
		if (maxBytes <= 0) return undefined;
		let retained: ResourceReadView | undefined;
		for (const observation of observations.values()) for (let target: string | undefined = filesystemPathKey(observation.path); target;) {
			const entry = this.entries.get(target);
			if (!entry || (entry.type !== "directory" && entry.type !== "alias" && entry.type !== "file") || retained?.entries.has(target)) break;
			const dependency = entry.metadataDependency && observations.has(entry.metadataDependency) ? entry.metadataDependency : entry.dependency;
			if (!dependency || !observations.has(dependency)) break;
			retained ??= new ResourceReadView(maxBytes, undefined,
				this.boundary?.dependency && observations.has(this.boundary.dependency) ? this.boundary : undefined);
			retained.capture(target, entry.type === "alias" ? entry : { type: entry.type, realPath: entry.realPath, dependency,
				...(entry.type === "file" ? { size: entry.size ?? entry.content?.length } : {}) });
			if (!retained.retained) { void retained.dispose(); return undefined; }
			target = entry.type === "alias" ? entry.target : undefined;
		}
		retained?.seal(); return retained;
	}
	/** Revoke data only; old outputs keep their immutable observations for exact validation. */
	invalidate(dependencies: ReadonlySet<string>): readonly string[] {
		const removed: string[] = [];
		if (!dependencies.size) return removed;
		const preparedNames = new Set<string>();
		this.inputEpoch++;
		if (this.boundary?.dependency && dependencies.has(this.boundary.dependency)) this.boundary = undefined;
		for (const [target, entry] of this.entries) if (!entry.dependency || dependencies.has(entry.dependency) ||
			entry.metadataDependency && dependencies.has(entry.metadataDependency)) {
			this.releaseObject(entry);
			if (entry.object !== undefined) this.objectCount--;
			const dependency = entry.metadataDependency;
			if (dependency && !dependencies.has(dependency) && entry.type !== "alias") {
				const bytes = Buffer.byteLength(target) + Buffer.byteLength(entry.realPath ?? "") + Buffer.byteLength(dependency) * 2 + 64;
				this.entries.set(target, { type: entry.type, realPath: entry.realPath, dependency, metadataDependency: dependency, bytes });
				this.capturedBytes -= entry.bytes - bytes;
			} else { this.entries.delete(target); this.capturedBytes -= entry.bytes; removed.push(target); }
		}
		for (const entries of this.prepared?.bindings.values() ?? []) for (const [key, cached] of entries) {
			if (!cached.dependencies || [...cached.dependencies].some(key => dependencies.has(key)) ||
				cached.proofs?.some(proof => [...proof.observations.keys()].some(key => dependencies.has(key)))) {
				cached.revoked = true; entries.delete(key);
				if (cached.resource) removed.push(cached.resource);
				if (!cached.borrowers) void this.prepared!.lifetime.release(cached);
			} else if (cached.retained && cached.resource) preparedNames.add(cached.resource);
		}
		return [...new Set(removed)].filter(target => !this.entries.has(target) && !preparedNames.has(target));
	}

	reserve(bytes: number): boolean {
		if (this.sealed) throw new Error("resource_snapshot_not_capturing");
		if (this.failure) { if (this.load) throw this.failure; return false; }
		if (!Number.isSafeInteger(bytes) || bytes < 0) throw new Error("resource_snapshot_budget_invalid");
		if (this.bytes + bytes > this.maxBytes) {
			this.failure = new Error("resource_snapshot_budget_exceeded");
			for (const entry of this.entries.values()) this.releaseObject(entry);
			this.entries.clear(); this.objectCount = 0;
			if (this.load) throw this.failure;
			return false;
		}
		this.capturedBytes += bytes;
		return true;
	}
	capture(target: string, entry: CapturedResource, reservedBytes = 0): void {
		const key = filesystemPathKey(target), previous = this.entries.get(key);
		const sameMetadata = previous?.type === entry.type && previous?.realPath === entry.realPath && entry.type !== "alias";
		const metadataDependency = /^(entry|type|stat):/.test(entry.dependency ?? "") ? entry.dependency
			: sameMetadata ? previous?.metadataDependency : undefined;
		// Overlapping scopes enrich one input view; a metadata observation cannot erase its payload.
		const redundant = (entry.type === "file" && previous?.type === "file" && entry.content === undefined && (previous.content !== undefined || entry.size === undefined)) ||
			(entry.type === "directory" && previous?.type === "directory" && entry.entries === undefined);
		let retained = redundant && !sameMetadata ? previous! : { ...(redundant ? previous! : entry), metadataDependency };
		const objectBytes = retained.object !== undefined ? 256 + (retained.objectBytes ?? 0) : 0;
		let bytes = Buffer.byteLength(key) + Buffer.byteLength(retained.realPath ?? "") + Buffer.byteLength(retained.dependency ?? "") +
			Buffer.byteLength(retained.metadataDependency ?? "") + 64 + objectBytes + (retained.type === "file" ? retained.content?.length ?? 0 : retained.type === "directory"
				? retained.entries?.reduce((sum, name) => sum + Buffer.byteLength(name) + 16, 0) ?? 0
				: retained.type === "alias" ? Buffer.byteLength(retained.target ?? "") + Buffer.byteLength(retained.link) : 0);
		if (retained.object !== undefined && (this.bytes + bytes - (previous?.bytes ?? 0) - reservedBytes > this.maxBytes ||
			this.objectCount - Number(previous?.object !== undefined) >= 64)) {
			this.releaseObject(retained); retained = { ...retained, object: undefined, objectBytes: undefined }; bytes -= objectBytes;
		}
		const additional = bytes - (previous?.bytes ?? 0) - reservedBytes;
		try { if (!this.reserve(Math.max(0, additional))) { this.releaseObject(entry); return; } }
		catch (error) { this.releaseObject(entry); throw error; }
		this.capturedBytes += Math.min(0, additional);
		if (previous?.object && retained.object !== previous.object) this.releaseObject(previous);
		this.objectCount += Number(retained.object !== undefined) - Number(previous?.object !== undefined);
		this.entries.set(key, { ...retained, bytes });
	}
	exists = async (target: string): Promise<boolean> => (await this.get(target, "type")).type !== "missing";
	stat = async (target: string, fields?: "type" | "entry"): Promise<ToolFilesystemStat> => {
		const entry = await this.get(target, fields ?? "stat");
		if (entry.type === "missing" || (!fields && entry.type === "file" && entry.content === undefined && entry.size === undefined)) return this.unproven(target);
		return { isDirectory: () => entry.type === "directory", realPath: entry.realPath, size: !fields && entry.type === "file" ? entry.content?.length ?? entry.size : undefined,
			...(fields === "entry" ? { type: entry.type === "alias" ? "symlink" : entry.type, ...(entry.type === "alias" ? { link: entry.link } : {}) } : {}) };
	};
	readdir = async (target: string): Promise<string[]> => {
		const entry = await this.get(target, "names");
		return entry.type === "directory" && entry.entries ? [...entry.entries] : this.unproven(target);
	};
	readFile = async (target: string, maxBytes?: number): Promise<Buffer> => {
		const entry = await this.get(target, "content");
		return entry.type === "file" && entry.content !== undefined ? Buffer.from(entry.content.subarray(0, maxBytes)) : this.unproven(target);
	};
	/** A process borrows the same captured kernel object without inheriting the tool's OFD offset. */
	async borrowObject<T>(target: string, consume: (capture: StableFilesystemCapture, handle: FileHandle) => Promise<T>): Promise<T | undefined> {
		this.assertComplete();
		const entry = this.sealed ? await this.get(target, this.entry(target).entry?.type === "directory" ? "names" : "content") : this.entries.get(filesystemPathKey(target));
		return entry?.object?.borrow(consume);
	}
	/** Fill a pre-budgeted pin slot only with independently read, byte-identical input.
	 * Supplied write/edit bytes alone never create kernel-object evidence. */
	retainObject(target: string, capture: StableFilesystemCapture): boolean {
		if (!this.retained || this.owner || !capture.object || !capture.content) return false;
		if (!this.sealed && this.observeObject) {
			try { this.observeObject(target, capture); } catch { return false; }
			const entry = this.entries.get(filesystemPathKey(target));
			return entry?.object === capture.object;
		}
		const key = filesystemPathKey(target), entry = this.entries.get(key);
		if (entry?.object !== null || (entry.type === "file" ? !capture.stat.isFile() || !entry.content?.equals(capture.content)
			: entry.type !== "directory" || !capture.stat.isDirectory() || capture.bytesRead > (entry.objectBytes ?? 0) ||
				!entry.entries || !capture.entries || !sameValues(entry.entries, capture.entries))) return false;
		this.entries.set(key, { ...entry, ...(entry.type === "file" ? { content: capture.content } : { entries: capture.entries }), object: capture.object });
		return true;
	}
	access = async (target: string): Promise<void> => {
		const entry = await this.get(target, "content");
		if (entry.type !== "file" || entry.content === undefined) this.unproven(target);
	};
	/** Each evaluation owns its failures, but borrows the same sealed inputs and lifetime. */
	async evaluate<T>(operation: (view: ResourceReadView) => Promise<T>, observed?: (dependencies: ReadonlySet<string> | undefined) => void,
		root?: string, lookup?: ResourceInputLookup, missing?: () => Promise<ResourceInputSource>, acceptProofs?: ResourceReadView["acceptProofs"]): Promise<T> {
		this.assertComplete(true);
		return this.borrow(operation, observed, root, lookup, missing, acceptProofs);
	}
	/** Retain preparations only while capturing, so the sealed branch accounts for every owned byte. */
	prepare: NonNullable<ToolFilesystemOperations["prepare"]> = (binding, key, build, consume, target) => {
		this.assertComplete();
		let owner: ResourceReadView = this;
		while (owner.owner) owner = owner.owner;
		const prepared = owner.prepared ??= { lifetime: new RuntimeLifecycleLane(), bindings: new Map() };
		return prepared.lifetime.admit(async () => {
			this.assertComplete();
			const inputEpoch = owner.inputEpoch;
			const cached = prepared.bindings.get(binding)?.get(key);
			const compatible = (cached: PreparedResource | undefined) => cached && !cached.revoked && cached.destination.retained &&
				cached.boundary?.root === this.boundary?.root && cached.boundary?.physicalRoot === this.boundary?.physicalRoot;
			const inherit = (dependencies: ReadonlySet<string> | undefined) => {
				if (!dependencies) this.dependencies = undefined;
				else if (this.dependencies) for (const dependency of dependencies) this.dependencies.add(dependency);
			};
			const release = (cached: PreparedResource) => {
				if (--cached.borrowers || cached.retained && !cached.revoked) return;
				if (prepared.bindings.get(binding)?.get(key) === cached) prepared.bindings.get(binding)!.delete(key);
				return (cached.destination.prepared?.lifetime ?? prepared.lifetime).release(cached);
			};
			const consumePrepared = async (cached: PreparedResource) => {
				inherit(cached.origin === owner ? cached.dependencies : undefined);
				const transferred = cached.proofs?.length && (!cached.retained || cached.destination !== owner);
				if (transferred) {
					for (const proof of this.acceptProofs!(cached.proofs!)) this.collectProofs?.set(proof.observations, proof);
				}
				const run = async () => {
					this.assertComplete(); cached.destination.assertComplete();
					const result = await consume(cached.value as Parameters<typeof consume>[0]);
					this.assertComplete(); if (!transferred && !(this.acceptProofs && cached.origin === owner && cached.dependencies)) cached.destination.assertComplete(); return result;
				};
				return cached.destination === owner ? run() : cached.destination.prepared!.lifetime.admit(run);
			};
			if (cached && compatible(cached)) {
				cached.borrowers++;
				try {
					// A composed waiter must take its own proof references before the producer can retire.
					if (cached.retained && cached.destination === owner ||
						await Promise.race([cached.ready!.then(() => cached.shareable), cached.declined.then(() => cached.shareable),
							...(!this.acceptProofs && cached.composed ? [cached.composed.then(() => cached.shareable && !cached.proofs?.length)] : [])]) &&
						(!cached.proofs?.length || this.acceptProofs)) return await consumePrepared(cached);
				} finally { await release(cached); }
			}
			if (target) for (const source of this.lookup?.(target) ?? []) {
				if (source.view.entries === this.entries || !source.view.retained || !source.view.sealed ||
					!source.view.prepared?.bindings.get(binding)?.get(key)?.retained ||
					!compatible(source.view.prepared.bindings.get(binding)?.get(key))) continue;
				const result = await source.view.borrow(view => view.prepare(binding, key, build, consume), this.observeSource(source), this.boundary,
					undefined, undefined, this.acceptProofs);
				this.assertComplete(); return result;
			}
			let resource: Awaited<ReturnType<typeof build>> | undefined;
			let bytes = 0;
			let decline!: () => void;
			let composed: (() => void) | undefined;
			const name = target && filesystemPathKey(target);
			const pending: PreparedResource = { resource: name, origin: owner, destination: owner, borrowers: 1, boundary: this.boundary,
				declined: new Promise<void>(resolve => { decline = resolve; }),
				composed: this.acceptProofs && new Promise<void>(resolve => { composed = resolve; }),
				dispose: async () => { try { await resource?.dispose(); } finally {
					if (prepared.bindings.get(binding)?.get(key) === pending) prepared.bindings.get(binding)!.delete(key);
					await Promise.allSettled(pending.proofs?.map(proof => proof.release()) ?? []);
					if (pending.retained) pending.destination.capturedBytes -= bytes;
				} } };
			let entries = prepared.bindings.get(binding);
			if (!entries) prepared.bindings.set(binding, entries = new Map());
			if (!entries.has(key)) entries.set(key, pending);
			pending.ready = (async () => {
				await this.borrow(async view => {
					const foreign = view.onForeignInputs; view.onForeignInputs = transferable => { if (transferable) composed?.(); else decline(); foreign?.(transferable); };
					view.collectProofs = this.acceptProofs ? new Map() : undefined;
					resource = await build(view); pending.shareable = !view.foreignInputs;
					if (pending.shareable && view.collectProofs?.size) {
						pending.proofs = [];
						for (const proof of view.collectProofs.values()) pending.proofs.push(proof.manager.retain({ ...proof, view: undefined }));
					}
				}, observed => { pending.dependencies = observed; });
				if (!resource) throw new Error("resource_preparation_missing");
				pending.value = resource.value;
				bytes = resource.bytes + (key.length + (name?.length ?? 0)) * 2 + 192 + [...pending.dependencies ?? []].reduce((sum, name) => sum + name.length * 2 + 64, 0);
				for (const proof of pending.proofs ?? []) for (const [key, entry] of proof.observations)
					bytes += (key.length + entry.path.length + entry.fingerprint.length + (entry.stamp?.length ?? 0)) * 2 + 128;
				if (!Number.isSafeInteger(resource.bytes) || resource.bytes < 0) throw new Error("resource_snapshot_budget_invalid");
				const destination = pending.destination = owner.sealed && this.missing ? (await this.missing()).view : owner;
				this.assertComplete(); destination.assertComplete();
				const retention = destination.prepared ??= { lifetime: new RuntimeLifecycleLane(), bindings: new Map() };
				if (inputEpoch === owner.inputEpoch && !destination.sealed && destination.bytes + bytes <= destination.maxBytes) {
					let entries = retention.bindings.get(binding);
					if (!entries) retention.bindings.set(binding, entries = new Map());
					if (!entries.has(key) || entries.get(key) === pending) {
						pending.retained = true; entries.set(key, pending); destination.capturedBytes += bytes;
					}
				}
				// The origin keeps a borrowed lookup; the destination owns its bytes and disposal.
			})();
			const settled = () => { decline(); composed?.(); };
			void pending.ready.then(settled, settled);
			try { await pending.ready; return await consumePrepared(pending); }
			finally { await release(pending); }
		});
	};
	private async borrow<T>(operation: (view: ResourceReadView) => Promise<T>, observed?: (dependencies: ReadonlySet<string> | undefined) => void,
		root?: string | { readonly root: string; readonly physicalRoot: string }, lookup = this.lookup, missing = this.missing,
		acceptProofs = this.acceptProofs): Promise<T> {
		this.assertComplete();
		const view = new ResourceReadView(0);
		view.entries = this.entries; view.owner = this; view.sealed = true; view.dependencies = new Set(); view.lookup = lookup; view.missing = missing;
		view.onForeignInputs = this.onForeignInputs;
		view.acceptProofs = acceptProofs; view.collectProofs = this.collectProofs;
		try {
			if (typeof root === "object") view.boundary = { root: root.root, physicalRoot: root.physicalRoot };
			else if (root === undefined || this.boundary && filesystemPathKey(root) === filesystemPathKey(this.boundary.root)) {
				view.boundary = this.boundary;
				if (this.boundary?.dependency) view.dependencies.add(this.boundary.dependency);
			} else if (filesystemPathKey(root) === filesystemPathKey(path.parse(root).root)) {
				view.boundary = { root, physicalRoot: root };
			} else {
				const entry = await view.stat(root, "type");
				if (!entry.isDirectory() || !entry.realPath) view.unproven(root);
				view.boundary = { root, physicalRoot: entry.realPath! };
			}
			const output = await operation(view);
			view.assertComplete();
			this.foreignInputs ||= view.foreignInputs;
			observed?.(view.dependencies);
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
	dispose(): void | Promise<void> {
		if (!this.owner) { for (const entry of this.entries.values()) this.releaseObject(entry); this.entries.clear(); this.objectCount = 0; }
		this.failure = new Error("resource_snapshot_disposed");
		return this.disposal ??= this.prepared ? this.prepared.lifetime.close(async () => {
			await this.prepared!.lifetime.drain();
			await Promise.allSettled([this.pending, ...[...this.prepared!.bindings.values()].flatMap(entries => [...entries.values()].filter(resource => resource.destination === this).map(resource =>
				this.prepared!.lifetime.release(resource)))]);
			this.prepared!.bindings.clear();
		}) : this.pending?.then(() => {}, () => {});
	}
	private releaseObject(entry: CapturedResource): void {
		if (!entry.object) return;
		const prepared = this.prepared ??= { lifetime: new RuntimeLifecycleLane(), bindings: new Map() };
		void prepared.lifetime.release(entry.object);
	}
	private async get(target: string, scope: ResourceDependency["scope"]): Promise<CapturedResource> {
		this.assertComplete();
		if (this.owner && this.boundary && !containsFilesystemPath(this.boundary.root, target)) this.unproven(target);
		if (this.owner && !this.owner.sealed) await this.owner.get(target, scope);
		if (this.load && !this.sealed) {
			const pending = (this.pending ?? Promise.resolve()).then(() => {
				const { entry } = this.entry(target, scope !== "entry");
				// Existing input evidence also owns the metadata derivable from those bytes or names.
				if (resourceCovers(entry, scope)) return;
				return this.load!({ path: target, scope });
			});
			this.pending = pending;
			try { await pending; }
			catch (error) { throw this.failure ??= error instanceof Error ? error : new Error(String(error)); }
			finally { if (this.pending === pending) this.pending = undefined; }
		}
		const { entry, resolved } = this.entry(target, scope !== "entry", scope);
		if (resourceCovers(entry, scope)) return entry!;
		// The local alias proof owns this translation; the source owns the resolved resource.
		for (const query of resolved === filesystemPathKey(target) ? [target] : [resolved, target]) for (const source of this.lookup?.(query) ?? []) {
			if (source.view.entries === this.entries || !source.view.retained) continue;
			try {
				source.view.assertComplete(true);
				// The caller already owns the boundary proof; the source contributes only its resource evidence.
				const boundary = this.boundary && query !== target
					? { root: this.boundary.physicalRoot, physicalRoot: this.boundary.physicalRoot } : this.boundary;
				return await source.view.borrow(view => view.get(query, scope), this.observeSource(source), boundary);
			} catch { /* An indexed name alone grants no coverage; another sealed owner may supply it. */ }
		}
		if (this.missing) {
			try {
				const source = await this.missing();
				return await source.view.borrow(view => view.get(target, scope), this.observeSource(source), this.boundary);
			} catch (error) { throw this.failure ??= error instanceof Error ? error : new Error(String(error)); }
		}
		return this.unproven(target);
	}
	private observeSource(source: ResourceInputSource) {
		return (dependencies: ReadonlySet<string> | undefined) => {
			const proofs = source.observed(dependencies);
			if (this.collectProofs && proofs?.length) {
				for (const proof of proofs) this.collectProofs.set(proof.observations, proof);
				this.onForeignInputs?.(true);
			} else { this.foreignInputs = true; this.onForeignInputs?.(false); }
		};
	}
	private entry(target: string, follow = true, scope: ResourceDependency["scope"] = "content"): { entry?: CapturedResource; resolved: string } {
		this.assertComplete();
		let current = filesystemPathKey(target);
		const visited = new Set<string>();
		while (!visited.has(current) && visited.size <= this.entries.size) {
			visited.add(current);
			const exact = this.observe(this.entries.get(current), scope);
			if (exact?.type === "alias" && follow) { if (!exact.target) break; current = exact.target; continue; }
			if (exact) return { entry: exact, resolved: exact.type !== "alias" && exact.realPath ? filesystemPathKey(exact.realPath) : current };
			let parent = path.dirname(current);
			while (parent !== path.dirname(parent) && this.entries.get(parent)?.type !== "alias") parent = path.dirname(parent);
			const alias = this.entries.get(parent);
			if (alias?.type !== "alias" || !alias.target) break;
			this.observe(alias);
			current = filesystemPathKey(path.resolve(alias.target, path.relative(parent, current)));
		}
		return { resolved: current };
	}
	private observe(entry: CapturedResource | undefined, scope?: ResourceDependency["scope"]): CapturedResource | undefined {
		if (this.owner && this.boundary && entry?.realPath && (entry.type !== "alias" || scope === "entry") &&
			!containsFilesystemPath(this.boundary.physicalRoot, entry.realPath)) this.unproven(entry.realPath);
		if (entry && this.dependencies) {
			const dependency = entry.type !== "alias" && (scope === "type" || scope === "entry" || (scope === "stat" && entry.type !== "file"))
				? entry.metadataDependency ?? entry.dependency : entry.dependency;
			if (dependency) this.dependencies.add(dependency); else this.dependencies = undefined;
		}
		return entry;
	}
	private unproven(target: string): never {
		throw (this.failure ??= new Error(`resource_access_unproven:${target}`));
	}
}

function resourceCovers(entry: CapturedResource | undefined, scope: ResourceDependency["scope"]): boolean {
	return !!entry && (scope === "entry" || scope === "type" ||
		(scope === "stat" && (entry.type !== "file" || entry.size !== undefined || entry.content !== undefined)) ||
		(scope === "names" && entry.type === "directory" && entry.entries !== undefined) ||
		(scope === "content" && entry.type === "file" && entry.content !== undefined));
}

type ResourceEvent = {
	readonly epoch: number;
	readonly path: string;
	readonly type: "change" | "rename" | "unknown";
};

const MAX_EVENT_HISTORY = 4096;

export class ResourceVersionManager {
	private epoch = 0;
	private readonly events: ResourceEvent[] = [];
	private readonly preciseWatches = new Map<string, { readonly watcher: FSWatcher; references: number }>();
	private references = 0;
	private watcher?: FSWatcher;
	private reliable = false;
	private ready?: Promise<void>;
	private open = true;
	readonly root: string;
	private readonly snapshotExcludes: ReadonlySet<string>;
	private readonly onIdle?: () => void;

	constructor(root: string, options: {
		readonly watch?: boolean; readonly onIdle?: () => void;
		/** Private snapshot entries only; filtered tokens cannot authorize input views or host execution windows. */
		readonly snapshotExcludes?: readonly string[];
	} = {}) {
		this.root = root;
		this.snapshotExcludes = new Set(options.snapshotExcludes);
		this.onIdle = options.onIdle;
		if (options.watch === false) this.ready = Promise.resolve();
	}

	private async startWatching() {
		try {
			this.watcher = watch(this.root, { recursive: true }, (event, filename) => {
				const changed = filename ? path.resolve(this.root, filename) : this.root;
				this.changed(changed, event);
			});
			this.watcher.on("error", () => {
				this.reliable = false;
				this.changed(this.root, "unknown");
			});
			this.reliable = true;
			await watcherTurn();
		} catch {
			this.reliable = false;
		}
	}

	/** Undefined dependencies grant only bounded on-demand captures; observation of host tools stays eager. */
	async capture(dependencies: ReadonlyArray<ResourceDependency> | undefined, retainBytes?: number,
		providedInputs?: ReadonlyMap<string, ResourceInput>): Promise<ResourceVersionToken> {
		if (dependencies?.length === 0 || (!dependencies && retainBytes === undefined)) throw new Error("resource_dependencies_unproven");
		if (providedInputs && retainBytes === undefined) throw new Error("resource_snapshot_budget_invalid");
		return this.captureToken(dependencies, retainBytes, providedInputs && new Map([...providedInputs]
			.map(([target, input]) => [filesystemPathKey(path.resolve(this.root, target)), input && !(input instanceof Uint8Array)
				? { names: input.names && [...input.names] } : input])));
	}

	/** Notification cursor for preparation; empty observations cannot validate or seal any resource. */
	observeChanges(): Promise<ResourceVersionToken> {
		return this.captureToken([]);
	}

	/** Own an evaluated query's evidence without keeping its input buffers or source branch alive. */
	retain(token: ResourceVersionToken, metadataBytes?: number): ResourceVersionToken {
		if (!this.open || token.manager !== this || token.root !== this.root || !token.observations.size)
			throw new Error("resource_version_owner_changed");
		token.view?.assertComplete(true);
		const observations = new Map(token.observations);
		const view = metadataBytes === undefined ? undefined : token.view?.retainMetadata(observations, metadataBytes);
		this.references++;
		return { ...token, observations, view, watching: false, preciseContent: Object.freeze([]), release: releaseOnce(() => {
			const finish = () => { observations.clear(); if (--this.references === 0 && !this.preciseWatches.size) this.onIdle?.(); };
			const pending = view?.dispose(); return pending ? pending.then(finish) : finish();
		}) };
	}

	private async captureToken(dependencies: ReadonlyArray<ResourceDependency> | undefined, retainBytes?: number,
		providedInputs?: ReadonlyMap<string, ResourceInput>): Promise<ResourceVersionToken> {
		if (!this.open) throw new Error("resource_version_manager_closed");
		if (this.snapshotExcludes.size && retainBytes !== undefined) throw new Error("resource_filtered_snapshot_not_readable");
		const observations = new Map<string, ResourceDependency & { fingerprint: string; stamp?: string }>();
		let precise: ReturnType<ResourceVersionManager["acquirePreciseWatches"]> | undefined;
		this.references++;
		let view: ResourceReadView | undefined;
		const observing = dependencies !== undefined && !providedInputs;
		const release = releaseOnce(() => {
			const finish = () => {
				observations.clear(); precise?.release();
				if (--this.references === 0 && !this.preciseWatches.size) this.onIdle?.();
			};
			const pending = view?.dispose();
			return pending ? pending.then(finish) : finish();
		});
		try {
			if (observing) await (this.ready ??= this.startWatching());
			const physicalRoot = await fingerprintIO(() => fs.realpath(this.root));
			const capture = async (requested: ReadonlyArray<ResourceDependency>) => {
				const normalized = normalizeDependencies(this.root, requested).filter((dependency) => !observations.has(dependencyKey(dependency)));
				if (!normalized.length) return;
				if (observing && this.reliable) precise = this.acquirePreciseWatches(normalized);
				for (const observation of await fingerprintDependencies(normalized, physicalRoot, this.snapshotExcludes, view,
					observing && !this.snapshotExcludes.size ? { root: this.root, observations } : undefined, providedInputs)) observations.set(dependencyKey(observation), observation);
			};
			const boundary = { path: this.root, scope: "resolution" as const }, boundaryKey = dependencyKey(boundary);
			view = retainBytes === undefined ? undefined : new ResourceReadView(retainBytes, dependencies ? undefined : (dependency) => capture([dependency]),
				{ root: this.root, physicalRoot, dependency: boundaryKey }, dependencies ? undefined : (target, content) => {
					if (!containsFilesystemPath(this.root, target) || !containsFilesystemPath(physicalRoot, target) ||
						filesystemPathKey(target) !== filesystemPathKey(content.realPath) ||
						!(content.stat.isDirectory() ? content.entries : content.stat.isFile())) return;
					const dependency = { path: target, scope: content.entries ? "names" as const : "content" as const }, key = dependencyKey(dependency);
					view!.capture(target, { ...(content.entries ? { type: "directory" as const, entries: content.entries, objectBytes: content.bytesRead }
						: { type: "file" as const, content: content.content }), object: content.object, realPath: content.realPath, dependency: key });
					const { value, stamp } = content.entries ? fingerprintDirectory(content.stat, content.realPath, content.entries) : fingerprintFile(content);
					observations.set(key, { ...dependency, stamp, fingerprint: digest({ path: filesystemPathKey(target), scope: dependency.scope, value }) });
				});
			if (view) observations.set(boundaryKey, { ...boundary, stamp: filesystemPathKey(physicalRoot),
				fingerprint: digest({ path: filesystemPathKey(this.root), scope: "resolution", value: filesystemPathKey(physicalRoot) }) });
			if (dependencies) await capture(dependencies);
			if (observing) await watcherTurn();
			const retained = view?.retained ? view : undefined;
			if (dependencies) retained?.seal();
			return {
				root: this.root, physicalRoot, observations, epoch: this.epoch,
				watching: Boolean(observing && this.reliable), preciseContent: Object.freeze(precise?.paths ?? []),
				manager: this, ...(retained ? { view: retained, inputView: new WeakRef(retained) } : {}), release,
			};
		} catch (error) {
			await release();
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
			return validation(started, "resource_version_owner_changed");
		try {
			if (sealing) token.view?.seal(); else token.view?.assertComplete(true);
			if (sealing && this.snapshotExcludes.size) throw new Error("resource_filtered_snapshot_not_observable");
			// Host windows need eager binding evidence; Windows can restore a junction without changing its stamps.
			if (sealing && (process.platform === "win32" || !token.observations.has(`binding:${filesystemPathKey(this.root)}`))) throw new Error("resource_path_binding_window_unprovable");
			if (!token.observations.size) throw new Error("resource_dependencies_unproven");
			// Watcher delivery fences host execution windows; future adoption uses the exact fingerprints below.
			if (sealing) await watcherTurn();
			const watcherFailure = this.invalidation(token, sealing);
			if (watcherFailure) return validation(started, watcherFailure, "watcher");
			const current = await fingerprintDependencies([...token.observations.values()].filter((entry) => sealing || entry.scope !== "binding"), token.physicalRoot, this.snapshotExcludes);
			if (sealing) await watcherTurn();
			const lateFailure = this.invalidation(token, sealing);
			if (lateFailure) return validation(started, lateFailure, "watcher");
			const changed = current.filter((entry) => {
				const captured = token.observations.get(dependencyKey(entry));
				return entry.fingerprint !== captured?.fingerprint || (sealing && (!entry.stamp || !captured?.stamp || entry.stamp !== captured.stamp));
			}).map(dependencyKey);
			const expired = !current.length || changed.length > 0;
			const reason = sealing ? "resource_observation_window_changed" : "resource_fingerprint_changed";
			return { ...validation(started, expired ? reason : undefined, "exact", current), ...(!sealing && changed.length ? { changed } : {}) };
		} catch {
			const reason = sealing ? "resource_observation_window_unprovable" : "resource_validation_failed";
			return validation(started, reason);
		}
	}

	private invalidation(token: ResourceVersionToken, sealing: boolean): string | undefined {
		if (!this.open || token.manager !== this || token.root !== this.root) return "resource_version_owner_changed";
		if (!sealing || !token.watching) return undefined; // Future reuse compares semantics; events only disprove a host execution window.
		if (!this.reliable || this.changesSince(token).uncertain) return "resource_observation_window_unprovable";
		const precise = new Set(token.preciseContent.map(filesystemPathKey));
		const changed = this.events.some((event) => event.epoch > token.epoch &&
			[...token.observations.values()].some((dependency) => affects(dependency, event, precise)));
		return changed ? "resource_observation_window_changed" : undefined;
	}

	changesSince(token: ResourceVersionToken): ResourceChangeSet {
		if (token.manager !== this || token.root !== this.root || !token.watching || !this.reliable) {
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

	close() {
		this.open = false;
		this.watcher?.close();
		this.watcher = undefined;
		this.reliable = false;
		for (const precise of this.preciseWatches.values()) precise.watcher.close();
		this.preciseWatches.clear();
		this.events.length = 0;
	}

	private changed(changedPath: string, type: ResourceEvent["type"]) {
		const absolute = path.resolve(changedPath);
		const event = { epoch: ++this.epoch, path: absolute, type };
		this.events.push(event);
		if (this.events.length > MAX_EVENT_HISTORY) this.events.splice(0, this.events.length - MAX_EVENT_HISTORY);
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

}

const managers = new Map<string, ResourceVersionManager>();

export function resourceDependencies(
	action: ActionKey,
	root: string,
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
) {
	const definition = actionSemantics.definition(action);
	const scope = definition ? definition.resourceScope : "content";
	if (scope === undefined || scope === "captured_inputs") return [];
	return action.resources.map((resource) => ({
		path: path.resolve(root, resource),
		scope,
	}));
}

export async function captureResourceVersion(
	action: ActionKey | undefined,
	root: string,
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS,
	retainBytes?: number,
	providedInputs?: ReadonlyMap<string, ResourceInput>,
) {
	const dependencies = providedInputs ? [...providedInputs].map(([path, input]): ResourceDependency => ({ path,
		scope: input && !(input instanceof Uint8Array) ? input.names ? "names" : "type" : "content" }))
		: action ? resourceDependencies(action, root, actionSemantics) : undefined;
	if (dependencies?.length === 0 || (!dependencies && retainBytes === undefined)) throw new Error("resource_dependencies_unproven");
	const normalized = path.resolve(root);
	let manager = managers.get(normalized);
	if (!manager) {
		manager = new ResourceVersionManager(normalized, {
			watch: path.dirname(normalized) !== normalized,
			onIdle: () => { if (managers.get(normalized) === manager) { manager!.close(); managers.delete(normalized); } },
		});
		managers.set(normalized, manager);
	}
	return manager.capture(dependencies, retainBytes, providedInputs);
}

export async function validateResourceVersion(token: unknown): Promise<ResourceVersionValidation> {
	if (!Array.isArray(token)) return isResourceVersionToken(token)
		? token.manager.validate(token) : validation(performance.now(), "resource_version_missing");
	if (token.length === 1) return validateResourceVersion(token[0]);
	const started = performance.now(), checked: ResourceVersionValidation[] = [];
	const groups = new Map<ResourceVersionManager, ResourceVersionToken & { observations: Map<string, ResourceObservation> }>();
	try {
		if (!token.length) throw new Error("resource_version_missing");
		for (const source of token) {
			if (!isResourceVersionToken(source)) throw new Error("resource_version_missing");
			source.view?.assertComplete(true);
			let group = groups.get(source.manager);
			if (!group) groups.set(source.manager, group = { ...source, observations: new Map() });
			if (group.root !== source.root || group.physicalRoot !== source.physicalRoot) throw new Error("resource_version_conflict");
			for (const [key, entry] of source.observations) {
				if (group.observations.has(key) && group.observations.get(key)!.fingerprint !== entry.fingerprint) throw new Error("resource_version_conflict");
				group.observations.set(key, entry);
			}
		}
		for (const group of groups.values()) {
			const result = await group.manager.validate(group); checked.push(result);
		}
		const expired = checked.find(result => result.expired);
		if (expired) return { ...validation(started, expired.reason, "exact", checked), changed: [...new Set(checked.flatMap(result => result.changed ?? []))] };
		for (const source of token as ResourceVersionToken[]) source.view?.assertComplete(true);
		return validation(started, undefined, "exact", checked);
	} catch (error) { return validation(started, error instanceof Error ? error.message : "resource_validation_failed", "exact", checked); }
}


export function invalidateResourceInputs(tokens: readonly ResourceVersionToken[], paths: readonly string[]): readonly string[] {
	const dependencies = new Set<string>(), precise = new Set<string>();
	for (const token of tokens) for (const [key, dependency] of token.observations) {
		if (paths.some(changed => dependency.scope === "resolution" ? containsFilesystemPath(changed, dependency.path)
			: affects(dependency, { path: changed, type: "rename", epoch: 0 }, precise))) dependencies.add(key);
	}
	return [...new Set(tokens.flatMap(token => token.view?.invalidate(dependencies) ?? []))];
}

export function releaseResourceVersion(token: unknown): void | Promise<void> {
	if (!isResourceVersionToken(token)) return;
	return token.release();
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

function affects(dependency: ResourceDependency, event: ResourceEvent, preciseContent: ReadonlySet<string>) {
	if (dependency.scope === "binding") return event.type === "rename" && filesystemPathKey(dependency.path) === filesystemPathKey(event.path);
	if (event.type === "unknown") return true;
	const dependencyPath = filesystemPathKey(dependency.path);
	const changed = filesystemPathKey(event.path);
	if (["content", "stat", "type", "entry"].includes(dependency.scope)) {
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

type FingerprintResult = {
	readonly value: unknown;
	readonly stamp: string;
	readonly bytesRead: number;
	readonly filesRead: number;
};

function fingerprintFile(content: StableFilesystemCapture) {
	return { value: { type: "file", mode: Number(content.stat.mode), size: content.bytesRead, hash: content.hash,
		resolved: filesystemPathKey(content.realPath) }, stamp: digest(["file", statStamp(content.stat), filesystemPathKey(content.realPath)]) };
}

function fingerprintDirectory(stat: BigIntStats, realPath: string, names: readonly string[], children: readonly { name: string; value: unknown; stamp: string }[] = []) {
	return { value: { type: "directory", mode: Number(stat.mode), resolved: filesystemPathKey(realPath), order: names,
		children: children.map(({ name, value }) => ({ name, value })) }, stamp: digest([statStamp(stat), children.map(({ name, stamp }) => [name, stamp])]) };
}

/** A sealed names view reserves its first verified kernel image, just as write/edit reserve file pins. */
function directoryObjectSlot(names: readonly string[], view?: ResourceReadView) {
	return view?.canRetainObject ? { object: null, objectBytes: names.reduce((sum, name) => sum + Math.ceil((20 + Buffer.byteLength(name)) / 8) * 8, 48) } : {};
}

async function fingerprintDependencies(
	dependencies: ReadonlyArray<ResourceDependency>, realRoot: string, excludes: ReadonlySet<string>, view?: ResourceReadView,
	bindings?: { readonly root: string; readonly observations: Map<string, ResourceDependency & { fingerprint: string; stamp?: string }> },
	providedInputs?: ReadonlyMap<string, ResourceInput>,
) {
	const files = new Map<string, Promise<FingerprintResult>>(), nearestExisting = missingResourceResolver(realRoot);
	const aliases = new Map<string, Map<string, { paths: Set<string>; links: number }>>();
	const entries = new Map<string, ReturnType<typeof captureFilesystemEntry>>();
	const capture = (target: string) => {
		const key = filesystemPathKey(target), previous = entries.get(key);
		if (previous) return previous;
		const pending = fingerprintIO(() => captureFilesystemEntry(target)).then((entry) => {
			// Eager captures own namespace evidence; ordinary validation joins only pending reads.
			if (bindings && (entry.info.isDirectory() || entry.link !== undefined)) {
				const dependency = { path: target, scope: "binding" as const };
				bindings.observations.set(dependencyKey(dependency), { ...dependency, fingerprint: "binding", stamp: digest([statStamp(entry.info), entry.link]) });
			}
			return entry;
		}).finally(() => { if (!bindings) entries.delete(key); });
		entries.set(key, pending); return pending;
	};
	if (bindings) await capture(bindings.root);
	const captureEntry = async (target: string, scope: ResourceDependency["scope"]) => {
		if (bindings) {
			for await (const entry of walkFilesystemPath(target, { capture, followFinal: scope !== "entry" })) {
				if (entry.info && !entry.info.isDirectory() && entry.link === undefined && !entry.terminal) break;
			}
		}
		return capture(target);
	};
	return mapFilesystem(dependencies, async (dependency) => {
		if (dependency.scope === "binding") return fingerprintBinding(dependency);
		const key = dependencyKey(dependency);
		if (dependency.scope === "tree_content" || dependency.scope === "tree_entries") aliases.set(key, new Map());
		const { value, ...metrics } = dependency.scope === "resolution"
			? await fingerprintIO(async () => { const resolved = filesystemPathKey(await fs.realpath(dependency.path)); return { value: resolved, stamp: resolved, bytesRead: 0, filesRead: 0 }; })
			: await fingerprintPath(dependency.path, dependency.scope, key);
		const groups = [...(aliases.get(key)?.values() ?? [])].map(group => Object.freeze({ links: group.links, paths: Object.freeze([...group.paths].sort()) }))
			.sort((left, right) => left.paths[0]! < right.paths[0]! ? -1 : 1);
		const topology = groups.length ? { aliases: Object.freeze(groups) } : {};
		return { ...dependency, ...topology, fingerprint: digest({ path: filesystemPathKey(dependency.path), scope: dependency.scope, value, ...topology }), ...metrics };
	});

	async function fingerprintPath(
		target: string,
		scope: ResourceDependency["scope"],
		dependency: string,
		ancestors: ReadonlySet<string> = new Set(),
		descend = true,
	): Promise<FingerprintResult> {
		let captured: Awaited<ReturnType<typeof captureFilesystemEntry>>;
		try {
			captured = await captureEntry(target, scope);
		} catch (error) {
			if (!missingResource(error)) throw error;
			if (providedInputs && providedInputs.get(filesystemPathKey(target)) !== null) throw error;
			const nearest = await nearestExisting(target);
			view?.capture(target, { type: "missing", realPath: nearest.realPath, dependency });
			return {
				value: { exists: false, error: errorCode(error), resolved: nearest.realPath },
				stamp: digest([filesystemPathKey(target), nearest.fingerprint]),
				bytesRead: 0,
				filesRead: 0,
			};
		}
		const { info, link } = captured;
		const supplied = providedInputs?.get(filesystemPathKey(target));
		if (providedInputs && !(supplied instanceof Uint8Array ? info.isFile() : supplied && info.isDirectory()))
			throw new Error(supplied instanceof Uint8Array ? "resource_input_not_regular" : "resource_input_type_changed");
		const realTarget = info.isSymbolicLink()
			? path.join(await fingerprintIO(() => fs.realpath(path.dirname(target))), path.basename(target))
			: await fingerprintIO(() => fs.realpath(target));
		assertInside(realRoot, realTarget);
		const identity = filesystemPathKey(realTarget);
		const grouping = aliases.get(dependency);
		if (grouping && info.isFile() && info.nlink > 1n) {
			const key = `${info.dev}:${info.ino}`, links = Number(info.nlink), previous = grouping.get(key);
			if (!Number.isSafeInteger(links) || previous && previous.links !== links) throw new Error(`resource_file_changed:${target}`);
			const group = previous ?? { paths: new Set<string>(), links };
			group.paths.add(identity); grouping.set(key, group);
		}
		if (ancestors.has(identity)) throw new Error(`resource_symlink_cycle:${target}`);
		if (info.isSymbolicLink()) {
			let source: string | undefined;
			const links: unknown[] = [];
			if (scope !== "entry") await fingerprintIO(async () => {
				for await (const entry of walkFilesystemPath(target)) {
					source = entry.path;
					if (entry.link !== undefined) links.push([filesystemPathKey(source), entry.link, statStamp(entry.info!)]);
				}
			});
			const followed = source === undefined ? undefined : await fingerprintPath(source, scope, dependency, new Set(ancestors).add(identity), descend);
			view?.capture(target, { type: "alias", target: source && filesystemPathKey(source), link: link!, realPath: realTarget, dependency });
			return {
				value: {
					type: "symlink",
					link,
					mode: Number(info.mode),
					resolved: identity,
					target: followed?.value,
				},
				stamp: digest(["symlink", link, statStamp(info), links, followed?.stamp]),
				bytesRead: followed?.bytesRead ?? 0,
				filesRead: followed?.filesRead ?? 0,
			};
		}
		if (["stat", "type", "entry"].includes(scope) || (scope === "entries" && !descend) || (["names", "entries", "tree_entries"].includes(scope) && !info.isDirectory())) {
			view?.capture(target, { type: info.isDirectory() ? "directory" : info.isFile() ? "file" : "special", realPath: realTarget, dependency,
				...(scope === "stat" && info.isFile() ? { size: Number(info.size) } : {}) });
			return stableEntry(target, info, identity, scope);
		}
		if (info.isFile()) {
			const key = JSON.stringify([filesystemPathKey(target), identity, statStamp(info), String(info.uid), String(info.gid)]), existing = files.get(key);
			if (existing) return { ...await existing, bytesRead: 0, filesRead: 0 };
			const pending = (async () => {
				// Join only concurrent reads of this identity; settled captures never authorize a later read.
				const provided = supplied instanceof Uint8Array ? supplied : undefined;
				const retain = view?.reserve(provided?.byteLength ?? Number(info.size)) ?? false;
				if (provided && !retain) throw new Error("resource_snapshot_budget_exceeded");
				const bytes = provided && Buffer.from(provided);
				// Supplied bytes own data, never a host observation window; adoption validates them exactly.
				const content: StableFilesystemCapture = bytes ? { content: bytes, bytesRead: bytes.length, hash: hash("sha256", bytes), stat: info, realPath: realTarget }
					: await fingerprintIO(() => captureStableFile(target, retain ? Number(info.size) : undefined, retain, {
						stat: info, realPath: realTarget, retainObject: retain && view!.canRetainObject }));
				assertInside(realRoot, content.realPath);
				view?.capture(target, { type: "file", content: content.content,
					object: content.object ?? (bytes && retain && view!.canRetainObject ? null : undefined),
					realPath: content.realPath, dependency }, retain ? provided?.byteLength ?? Number(info.size) : 0);
				return {
					...fingerprintFile(content),
					bytesRead: bytes || content.shared ? 0 : content.bytesRead,
					filesRead: bytes || content.shared ? 0 : 1,
				};
			})().finally(() => files.delete(key));
			files.set(key, pending);
			return pending;
		}
		if (!info.isDirectory() || scope === "content") {
			throw new Error(`unsupported_resource_type:${specialFileType(info)}:${target}`);
		}
		if (supplied && !(supplied instanceof Uint8Array) && supplied.names) {
			view?.capture(target, { type: "directory", entries: supplied.names, realPath: realTarget, dependency, ...directoryObjectSlot(supplied.names, view) });
			return { ...fingerprintDirectory(info, realTarget, supplied.names), bytesRead: 0, filesRead: 0 };
		}
		const entries = await fingerprintIO(() => fs.readdir(target, { withFileTypes: true }));
		const selected = excludes.size && (scope === "tree_content" || scope === "tree_entries") ? entries.filter((entry) => !excludes.has(entry.name)) : entries;
		const descendants = new Set(ancestors).add(identity);
		const children = scope === "names" ? [] : await mapFilesystem([...selected].sort((left, right) => left.name.localeCompare(right.name)), async (entry) => {
			const child = await fingerprintPath(path.join(target, entry.name), scope, dependency, descendants, scope !== "entries");
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
		const names = selected.map(entry => entry.name);
		view?.capture(target, { type: "directory", entries: names, realPath: realTarget, dependency, ...directoryObjectSlot(names, view) });
		return {
			...fingerprintDirectory(after, realTarget, names, children),
			bytesRead: children.reduce((total, child) => total + child.bytesRead, 0),
			filesRead: children.reduce((total, child) => total + child.filesRead, 0),
		};
	}
}

async function fingerprintBinding(dependency: ResourceDependency) {
	let stamp: string;
	try {
		const { info, link } = await fingerprintIO(() => captureFilesystemEntry(dependency.path, "identity"));
		stamp = digest([statStamp(info), link]);
	} catch (error) {
		if (!missingResource(error)) throw error;
		stamp = errorCode(error);
	}
	return { ...dependency, fingerprint: "binding", stamp, bytesRead: 0, filesRead: 0 };
}

async function stableEntry(
	target: string,
	before: BigIntStats,
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

function specialFileType(value: Stats | BigIntStats | import("node:fs").Dirent) {
	if (value.isFile()) return "file";
	if (value.isDirectory()) return "directory";
	if (value.isSymbolicLink()) return "symlink";
	if (value.isFIFO()) return "fifo";
	if (value.isSocket()) return "socket";
	if (value.isCharacterDevice()) return "character_device";
	return value.isBlockDevice() ? "block_device" : "other";
}

function missingResourceResolver(realRoot: string) {
	// Negative queries commonly share ancestors. Join only concurrent reads in this proof batch;
	// settled observations are never cached for another query or Actor validation.
	type Resolution = { readonly realPath: string; readonly fingerprint: string };
	const pending = new Map<string, Promise<Resolution>>();
	const resolve = (target: string): Promise<Resolution> => {
		const current = path.resolve(target), existing = pending.get(current);
		if (existing) return existing;
		const task = (async () => {
			try {
				const [real, stat] = await Promise.all([
					fingerprintIO(() => fs.realpath(current)),
					fingerprintIO(() => fs.lstat(current, { bigint: true })),
				]);
				assertInside(realRoot, real);
				return { realPath: filesystemPathKey(real), fingerprint: digest([filesystemPathKey(real), statStamp(stat)]) };
			} catch (error) {
				if (!missingResource(error)) throw error;
			}
			const parent = path.dirname(current);
			if (parent === current) throw new Error(`resource_path_unresolved:${target}`);
			const ancestor = await resolve(parent);
			return { realPath: filesystemPathKey(path.join(ancestor.realPath, path.basename(current))), fingerprint: ancestor.fingerprint };
		})().finally(() => pending.delete(current));
		pending.set(current, task);
		return task;
	};
	return resolve;
}

function statStamp(stat: BigIntStats): string {
	return [stat.dev, stat.ino, stat.mode, stat.nlink, stat.rdev, stat.size, stat.mtimeNs, stat.ctimeNs, stat.birthtimeNs]
		.join(":");
}

function digest(value: unknown): string {
	return hash("sha256", JSON.stringify(value));
}

function assertInside(realRoot: string, target: string): void {
	if (!containsFilesystemPath(realRoot, target)) throw new Error(`resource_symlink_escapes_workspace:${target}`);
}

function validation(
	started: number,
	reason?: string,
	mode: ResourceValidationMetrics["mode"] = "exact",
	observed: ReadonlyArray<Pick<ResourceValidationMetrics, "bytesRead" | "filesRead">> = [],
): ResourceVersionValidation {
	return {
		expired: reason !== undefined, ...(reason === undefined ? {} : { reason }),
		durationMs: Math.max(0, performance.now() - started), mode,
		bytesRead: observed.reduce((total, entry) => total + entry.bytesRead, 0),
		filesRead: observed.reduce((total, entry) => total + entry.filesRead, 0),
	};
}

function sameValues<Value>(left: ReadonlyArray<Value>, right: ReadonlyArray<Value>) {
	return left.length === right.length && left.every((value, index) => value === right[index]);
}


function errorCode(error: unknown) {
	return error && typeof error === "object" && "code" in error ? String(error.code) : "unknown";
}

function missingResource(error: unknown): boolean {
	const code = errorCode(error);
	return code === "ENOENT" || code === "ENOTDIR";
}

function releaseOnce<Result>(release: () => Result): () => Result | undefined {
	let released = false;
	let result: Result | undefined;
	return () => {
		if (!released) { released = true; result = release(); }
		return result;
	};
}

const fingerprintIO = (() => {
	let active = 0;
	const waiting: Array<() => void> = [];
	return async <Value>(task: () => Promise<Value>): Promise<Value> => {
		if (active < FILESYSTEM_CONCURRENCY) active++;
		else await new Promise<void>((resolve) => waiting.push(resolve));
		try { return await task(); }
		finally {
			const next = waiting.shift();
			if (next) next();
			else active--;
		}
	};
})();

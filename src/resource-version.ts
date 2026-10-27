import { isSafeCount } from "./number-utils.ts";
import { AsyncLocalStorage } from "node:async_hooks";
import { hash } from "node:crypto";
import { types } from "node:util";
import { type BigIntStats, type FSWatcher, watch, closeSync, constants, fstatSync, readlinkSync } from "node:fs";
import { ensureNativeFilesystem, nativeFilesystem } from "./process-startup.mjs";
import path from "node:path";
import { setImmediate as watcherTurn } from "node:timers/promises";
import { type ActionKey, type ActionSemanticsRegistry, PI_ACTION_SEMANTICS, type ResourceDependencyScope } from "./action-semantics.ts";
import {
	type StableFilesystemCapture, type FilesystemObjectHandle, type FilesystemReadView, openFilesystemReadView, withFilesystemReadView, cachedCapture, captureFilesystemEntry, captureStableFile,
	FILESYSTEM_CONCURRENCY, mapFilesystem, sameFilesystemIdentity, walkFilesystemPath, filesystemEntryType, type FilesystemTypeEvidence, captureFilesystemDirectory, readFilesystemDirectory, resolveFilesystemDestination,
} from "./filesystem-evidence.ts";
import { containsFilesystemPath, filesystemPathKey, relativeFilesystemPath } from "./path-utils.ts";
import { RuntimeLifecycleLane, SharedAbortScope } from "./runtime-lifecycle.ts";
import { TimelineInterval } from "./task-timing.ts";
import type { ValidationMetrics } from "./settlement.ts";
import { ChunkedContent, ContentChunkPool, parseContentManifest, contentDigest, contentMemory, readContent, type ContentVersion, type ContentChunkLimits, type ChunkedContentManifest } from "./content-chunks.ts";
import type { ProvenanceCertificateStore } from "./reuse-store.ts";
import { validateArtifact, type ArtifactReference } from "./artifact-reference.ts";
import { isSha256Digest, sha256Digest, type Sha256Digest } from "./stable-value-hash.ts";
import { stableStringify } from "./stable-json.ts";

export type ToolFilesystemStat = { isDirectory: () => boolean; size?: number; type?: "file" | "directory" | "symlink" | "special"; link?: string; realPath?: string };

/** Filesystem capabilities supplied by an execution world, never ambient host defaults. */
export interface ToolFilesystemOperations {
	readonly readFile: (target: string, maxBytes?: number) => Promise<Buffer>;
	readonly readFileRange?: (target: string, offset: number, length: number) => Promise<Buffer>;
	readonly access: (target: string, writable?: boolean) => Promise<void>;
	readonly exists?: (target: string) => boolean | Promise<boolean>;
	/** `entry` does not follow the final link; includeSize=false returns only type/path fields and their evidence. */
	readonly stat?: (target: string, fields?: "type" | "entry", includeSize?: boolean) => ToolFilesystemStat | Promise<ToolFilesystemStat>;
	readonly readdir?: (target: string) => string[] | Promise<string[]>;
	readonly writeFile?: (target: string, content: string) => Promise<void>;
	readonly mkdir?: (target: string) => Promise<void>;
	/** Build is pure and reads only view inputs; binding/key cover other inputs and values stay immutable. Resource is a hint; signal owns this consumer. */
	readonly prepare?: <Value, Result>(binding: object, key: string,
		build: (view: ToolFilesystemOperations, signal: AbortSignal) => Promise<{ readonly value: Value; readonly bytes: number; readonly dispose: () => void | Promise<void> }>,
		consume: (value: Value) => Promise<Result>, resource?: string, signal?: AbortSignal) => Promise<Result>;
}

export type ResourceDependency = {
	readonly path: string;
	readonly scope: ResourceDependencyScope | "stat" | "type" | "entry" | "exists" | "names" | "binding" | "resolution";
};

export type ResourceObservation = ResourceDependency & { readonly fingerprint: string; readonly stamp?: string;
	/** Named edges captured in this tree; links exceeding paths indicate an open namespace. */
	readonly aliases?: readonly { readonly paths: readonly string[]; readonly links: number }[] };

/** Supplied bytes and poststates remain proposals until exact adoption validation. */
export type ResourceInput = ContentVersion | { readonly content: ContentVersion; readonly computation: TimelineInterval } |
	{ readonly names?: readonly string[]; readonly computation?: TimelineInterval } |
	{ readonly type: "file" | "directory"; readonly computation?: TimelineInterval } | null;

const isInputContent = (input: ResourceInput | undefined): input is ContentVersion => input instanceof Uint8Array || input instanceof ChunkedContent;
const inputContent = (input: ResourceInput | undefined) => isInputContent(input) ? input : input && "content" in input ? input.content : undefined;

export type ResourceValidationMetrics = ValidationMetrics;

export type ResourceVersionValidation = ResourceValidationMetrics & {
	readonly expired: boolean;
	readonly reason?: string;
	readonly changed?: readonly string[];
};

export type ResourceChangeSet = { readonly uncertain: boolean; readonly paths: ReadonlyArray<string>; };
export interface ResourceInputPersistence { readonly store: ProvenanceCertificateStore; readonly limits?: Partial<ContentChunkLimits>; }

/** A captured tree entry as Git stages it: a regular file's object id, or a symbolic link's target. */
export type TreeObject = { readonly mode: number; readonly blob: string; readonly link?: never } | { readonly mode: number; readonly link: string; readonly blob?: never };

export type ResourceVersionToken = {
	readonly root: string;
	readonly physicalRoot: string;
	readonly treeObjects?: ReadonlyMap<string, TreeObject>;
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
	| { readonly type: "file"; readonly content?: Buffer | ChunkedContent; readonly version?: { readonly hash: string; readonly size: number } }
	| { readonly type: "directory"; readonly entries?: readonly string[] }
	| { readonly type: "alias"; readonly target?: string; readonly link: string }
	| { readonly type: "special" | "missing" }) & { readonly realPath?: string; readonly dependency?: string;
		readonly object?: StableFilesystemCapture["object"] | null; readonly objectBytes?: number;
		readonly computation?: TimelineInterval; readonly metadata?: { readonly dependency?: string; readonly computation?: TimelineInterval; readonly size?: number } };

type ResourceInputSource = {
	readonly view: ResourceReadView;
	/** Selection is side-effect free when commit is false; only accepted inputs become dependencies. */
	readonly observed: (dependencies: ReadonlySet<string> | undefined, commit?: boolean) => readonly ResourceVersionToken[] | void;
};
type PersistentResourceEntry = readonly [string, Omit<CapturedResource, "computation" | "object" | "objectBytes" | "metadata" | "content"> & {
	readonly content?: ChunkedContentManifest; readonly metadata?: { readonly dependency?: string; readonly size?: number }; readonly entries?: readonly string[]; readonly target?: string; readonly link?: string }];
type PersistentPreparation = { readonly binding: Sha256Digest; readonly key: string; readonly resource?: string;
	readonly dependencies: readonly string[]; readonly value: ArtifactReference };
type PersistentResourcePayload = { readonly root: string; readonly physicalRoot: string; readonly observations: readonly ResourceObservation[];
	readonly entries: readonly PersistentResourceEntry[]; readonly preparations: readonly PersistentPreparation[] };

const MAX_PREPARATION_DATA_BYTES = 1024 * 1024;
const preparationBindings = new WeakMap<object, { readonly id: Sha256Digest; readonly accepts: (value: unknown) => boolean }>();
/** Internal adapters must audit their complete pure closure; registering an arbitrary callback does not prove it pure. */
export function createSerializablePreparationBinding(implementation: unknown, accepts: (value: unknown) => boolean): object {
	const binding = Object.freeze({});
	preparationBindings.set(binding, { accepts, id: sha256Digest(stableStringify({ implementation, codec: "prepared-json-v1",
		loaded: [snapshotPreparation, accepts].map(fn => Function.prototype.toString.call(fn)) })) });
	return binding;
}

/** Own a bounded, lossless JSON tree without invoking getters or flattening opaque resources. */
function snapshotPreparation(value: unknown): { readonly value: unknown; readonly data: Buffer } | undefined {
	let remaining = MAX_PREPARATION_DATA_BYTES, nodes = 0;
	const seen = new WeakSet<object>(), budget = Symbol(), invalid = () => { throw new Error("resource_preparation_data_invalid"); };
	const charge = (value: string) => { if (value.length > remaining || (remaining -= Buffer.byteLength(value)) < 0) throw budget; };
	const copy = (item: unknown, depth: number): unknown => {
		if (++nodes > 65536 || depth > 64) throw budget;
		if (item === null || typeof item === "boolean" || typeof item === "number" && Number.isFinite(item) && !Object.is(item, -0)) { charge(JSON.stringify(item)); return item; }
		if (typeof item === "string") { if (item.length > remaining) throw budget; charge(JSON.stringify(item)); return item; }
		if (!item || typeof item !== "object" || types.isProxy(item) || seen.has(item)) return invalid();
		const array = Array.isArray(item), keys = Reflect.ownKeys(item);
		if (Object.getPrototypeOf(item) !== (array ? Array.prototype : Object.prototype) ||
			array && (keys.length !== item.length + 1 || keys.some((key, index) => key !== (index === item.length ? "length" : String(index))))) return invalid();
		seen.add(item); charge(array ? "[]" : "{}");
		const owned = array ? [] : {};
		for (const key of keys) {
			if (array && key === "length") continue;
			const property = Object.getOwnPropertyDescriptor(item, key)!;
			if (typeof key !== "string" || !property.enumerable || !("value" in property)) return invalid();
			if (key.length > remaining) throw budget;
			charge(array ? "," : JSON.stringify(key) + ":,");
			Object.defineProperty(owned, key, { value: copy(property.value, depth + 1), enumerable: true });
		}
		return Object.freeze(owned);
	};
	try { const owned = copy(value, 0); return { value: owned, data: Buffer.from(stableStringify(owned)) }; }
	catch (error) { if (error !== budget) throw error; return undefined; }
}

// Includes the decoded tree, encoded bytes and transient decoding copies, not just the CAS payload.
const preparationDataBytes = (size: number) => size * 64 + 512;
const preparationMetadataBytes = (key: string, resource: string | undefined, dependencies: Iterable<string>) =>
	(key.length + (resource?.length ?? 0)) * 2 + 192 + [...dependencies].reduce((sum, name) => sum + name.length * 2 + 64, 0);

function parsePersistentResourcePayload(value: unknown, root: string): PersistentResourcePayload {
	const payload = value as PersistentResourcePayload, invalid = () => { throw new Error("invalid persistent input closure"); };
	const absolute = (value: unknown): value is string => typeof value === "string" && !value.includes("\0") && path.isAbsolute(value) && filesystemPathKey(value) === value;
	if (!payload || payload.root !== root || !absolute(payload.physicalRoot) || !Array.isArray(payload.observations) || !payload.observations.length ||
		payload.observations.length > 65536 || !Array.isArray(payload.entries) || payload.entries.length > 65536) return invalid();
	const scopes = new Set(["content", "tree_content", "entries", "tree_entries", "stat", "type", "entry", "exists", "names", "resolution"]), keys = new Set<string>();
	for (const observation of payload.observations) {
		if (!observation || !absolute(observation.path) || !containsFilesystemPath(root, observation.path) || !scopes.has(observation.scope) ||
			typeof observation.fingerprint !== "string" || !/^[0-9a-f]{64}$/.test(observation.fingerprint) ||
			observation.stamp !== undefined && typeof observation.stamp !== "string") return invalid();
		if (observation.aliases && (!Array.isArray(observation.aliases) || observation.aliases.some((group: { links: number; paths: unknown[] }) => !group || !isSafeCount(group.links, 1) ||
			!Array.isArray(group.paths) || group.paths.some((target: unknown) => !absolute(target) || !containsFilesystemPath(payload.physicalRoot, target))))) return invalid();
		const key = dependencyKey(observation); if (keys.has(key)) return invalid(); keys.add(key);
	}
	if (!keys.has(dependencyKey({ path: root, scope: "resolution" }))) return invalid();
	const targets = new Set<string>();
	const entries = payload.entries.map((item): PersistentResourceEntry => {
		if (!Array.isArray(item) || item.length !== 2) return invalid();
		const [target, entry] = item;
		if (!absolute(target) || !containsFilesystemPath(root, target) || targets.has(target) || !entry || !["file", "directory", "alias", "special", "missing"].includes(entry.type) ||
			!entry.dependency || !keys.has(entry.dependency) || entry.realPath !== undefined && (!absolute(entry.realPath) || !containsFilesystemPath(payload.physicalRoot, entry.realPath)) ||
			entry.metadata && (entry.metadata.dependency !== undefined && !keys.has(entry.metadata.dependency) || entry.metadata.size !== undefined && !isSafeCount(entry.metadata.size))) return invalid();
		targets.add(target);
		if (entry.content && entry.type !== "file" || entry.entries && entry.type !== "directory" || entry.type === "alias" &&
			(typeof entry.link !== "string" || entry.link.includes("\0") || entry.target !== undefined && (!absolute(entry.target) || !containsFilesystemPath(root, entry.target)))) return invalid();
		if (entry.entries && (!Array.isArray(entry.entries) || new Set(entry.entries).size !== entry.entries.length || entry.entries.some((name: unknown) =>
			typeof name !== "string" || !name || name === "." || name === ".." || /[\0/]/.test(name)))) return invalid();
		return [target, { type: entry.type, realPath: entry.realPath, dependency: entry.dependency,
			metadata: entry.metadata && { dependency: entry.metadata.dependency, size: entry.metadata.size },
			...(entry.content ? { content: parseContentManifest(entry.content) } : {}),
			...(entry.type === "directory" ? { entries: entry.entries && [...entry.entries] } : entry.type === "alias" ? { link: entry.link, target: entry.target } : {}) }];
	});
	const preparations = payload.preparations ?? [], identities = new Set<string>();
	if (!Array.isArray(preparations) || preparations.length > 65536) return invalid();
	for (const preparation of preparations) {
		if (!preparation || !isSha256Digest(preparation.binding) || typeof preparation.key !== "string" || preparation.key.length > 65536 ||
			preparation.resource !== undefined && (!absolute(preparation.resource) || !containsFilesystemPath(root, preparation.resource)) ||
			!Array.isArray(preparation.dependencies) || !preparation.dependencies.length || preparation.dependencies.length > keys.size ||
			new Set(preparation.dependencies).size !== preparation.dependencies.length || preparation.dependencies.some((key: unknown) => typeof key !== "string" || !keys.has(key)) ||
			!preparation.dependencies.includes(dependencyKey({ path: root, scope: "resolution" }))) return invalid();
		validateArtifact(preparation.value);
		if (preparation.value.size > MAX_PREPARATION_DATA_BYTES) return invalid();
		const identity = JSON.stringify([preparation.binding, preparation.key]); if (identities.has(identity)) return invalid(); identities.add(identity);
	}
	return { root, physicalRoot: payload.physicalRoot, observations: payload.observations, entries, preparations };
}
type ResourceInputLookup = (target?: string) => Iterable<ResourceInputSource>;
type PreparedResource = {
	readonly cancellation: SharedAbortScope;
	readonly waiting: Set<PreparedResource>;
	computation?: TimelineInterval;
	value?: unknown; data?: Buffer; readonly dispose: () => void | Promise<void>;
	readonly resource?: string;
	dependencies?: ReadonlySet<string>; readonly boundary?: { readonly root: string; readonly physicalRoot: string };
	readonly origin: ResourceReadView; destination: ResourceReadView; ready?: Promise<void>; readonly declined: Promise<void>; readonly composed?: Promise<void>;
	borrowers: number; revoked?: boolean; retained?: boolean; shareable?: boolean; proofs?: ResourceVersionToken[];
};

function preparationWaitsFor(resource: PreparedResource, target: PreparedResource): boolean {
	const pending = new Set([resource]);
	for (const current of pending) {
		if (current === target) return true;
		for (const next of current.waiting) pending.add(next);
	}
	return false;
}

/** Token-owned input data, not a filesystem cache or authority to execute host functions. */
export class ResourceReadView {
	private entries = new Map<string, CapturedResource & { readonly bytes: number }>();
	private owner?: ResourceReadView;
	private loadOwner = false;
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
	private observed?: ResourceInputSource["observed"];
	private validateInputs?: (proofs: readonly ResourceVersionToken[]) => Promise<void>;
	private acceptProofs?: (proofs: readonly ResourceVersionToken[]) => readonly ResourceVersionToken[];
	private preparing?: PreparedResource;
	private prepared?: { readonly lifetime: RuntimeLifecycleLane; readonly bindings: Map<object | Sha256Digest, Map<string, PreparedResource>> };
	private boundary?: { readonly root: string; readonly physicalRoot: string; readonly dependency?: string };
	private readonly maxBytes: number;
	private readonly load?: (dependency: ResourceDependency) => Promise<void>;
	private readonly observeObject?: (target: string, capture: StableFilesystemCapture) => void;
	readonly contents?: ContentChunkPool;
	private readonly contentOwners = new Map<ContentChunkPool, { readonly dispose: () => Promise<void> }>();
	readonly verifyingInputs: boolean;
	constructor(maxBytes: number, load?: (dependency: ResourceDependency) => Promise<void>, boundary?: ResourceReadView["boundary"],
		observeObject?: ResourceReadView["observeObject"], storage?: ContentChunkPool | "verify") {
		if (!Number.isFinite(maxBytes) || maxBytes < 0) throw new Error("resource_snapshot_budget_invalid");
		this.maxBytes = maxBytes;
		this.load = load;
		this.boundary = boundary;
		this.observeObject = observeObject;
		this.contents = storage instanceof ContentChunkPool ? storage : undefined;
		this.verifyingInputs = storage === "verify";
		if (this.contents) this.retainContentPool(this.contents);
	}
	get bytes(): number { return this.capturedBytes; }
	get remainingBytes(): number { return Math.max(0, this.maxBytes - this.capturedBytes); }
	get hasPreparedInputs(): boolean { return [...this.prepared?.bindings.values() ?? []].some(entries => [...entries.values()].some(entry => entry.retained && !entry.revoked && entry.destination.retained)); }
	get canRetainObject(): boolean { return !this.verifyingInputs && process.platform === "linux" && this.objectCount < 64; }
	get retained(): boolean { return this.failure === undefined && this.owner?.retained !== false; }
	get resources() {
		this.assertComplete(true);
		const resources = [...this.entries].map(([path, entry]) => ({ path, descendants: entry.type === "alias" && entry.target !== undefined }));
		for (const entries of this.prepared?.bindings.values() ?? []) for (const cached of entries.values()) {
			if (cached.retained && cached.resource && !this.entries.has(cached.resource)) resources.push({ path: cached.resource, descendants: false });
		}
		return resources;
	}
	/** Read completed entries without allowing a borrower to load or seal its active owner. */
	readOnly(load = false): ResourceReadView {
		this.assertComplete();
		return Object.assign(new ResourceReadView(0), { entries: this.entries, owner: this, sealed: true, boundary: this.boundary, prepared: this.prepared, loadOwner: load });
	}
	/** Keep proven path metadata; payloads and preparations remain with their original owner. */
	retainMetadata(observations: ReadonlyMap<string, ResourceObservation>, maxBytes: number): ResourceReadView | undefined {
		this.assertComplete();
		if (maxBytes <= 0) return undefined;
		// The root proof still anchors queries whose retained metadata belongs to another token.
		let retained = this.boundary?.dependency && observations.has(this.boundary.dependency)
			? new ResourceReadView(maxBytes, undefined, this.boundary) : undefined;
		for (const observation of observations.values()) for (let target: string | undefined = filesystemPathKey(observation.path); target;) {
			const entry = this.entries.get(target);
			if (!entry || retained?.entries.has(target)) break;
			const dependency = entry.metadata?.dependency && observations.has(entry.metadata.dependency) ? entry.metadata.dependency : entry.dependency;
			if (!dependency || !observations.has(dependency)) break;
			retained ??= new ResourceReadView(maxBytes);
			retained.capture(target, resourceMetadata(entry, dependency));
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
			entry.metadata?.dependency && dependencies.has(entry.metadata.dependency)) {
			this.releaseObject(entry);
			if (entry.object !== undefined) this.objectCount--;
			const dependency = entry.metadata?.dependency;
			if (dependency && !dependencies.has(dependency) && entry.type !== "alias") {
				const bytes = Buffer.byteLength(target) + Buffer.byteLength(entry.realPath ?? "") + Buffer.byteLength(dependency) * 2 + 64 + (entry.metadata?.computation ? 32 : 0);
				this.entries.set(target, { ...resourceMetadata(entry, dependency), bytes });
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
		if (!isSafeCount(bytes)) throw new Error("resource_snapshot_budget_invalid");
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
	captureObject(target: string, input: StableFilesystemCapture, dependency?: string): void {
		this.capture(target, { ...(input.entries ? { type: "directory", entries: input.entries, objectBytes: input.bytesRead } : { type: "file", content: input.content }),
			object: input.object, computation: input.computation, realPath: input.realPath, dependency });
	}
	capture(target: string, entry: CapturedResource, reservedBytes = 0): void {
		const key = filesystemPathKey(target), previous = this.entries.get(key);
		const sameMetadata = previous?.type === entry.type && previous?.realPath === entry.realPath && entry.type !== "alias";
		const metadata = /^(entry|type|stat|exists):/.test(entry.dependency ?? "") ? entry.metadata ?? { dependency: entry.dependency, computation: entry.computation }
			: sameMetadata && previous?.metadata || entry.metadata;
		// Overlapping scopes enrich one input view; a metadata observation cannot erase its payload.
		const redundant = (entry.type === "file" && previous?.type === "file" && entry.content === undefined && entry.version === undefined && (previous.content !== undefined || previous.version !== undefined || resourceSize(entry) === undefined)) ||
			(entry.type === "directory" && previous?.type === "directory" && entry.entries === undefined);
		let retained = redundant && !sameMetadata ? previous! : { ...(redundant ? previous! : entry), metadata };
		const objectBytes = retained.object !== undefined ? 256 + (retained.objectBytes ?? 0) : 0;
		let bytes = Buffer.byteLength(key) + Buffer.byteLength(retained.realPath ?? "") + Buffer.byteLength(retained.dependency ?? "") +
			Buffer.byteLength(retained.metadata?.dependency ?? "") + 64 + objectBytes + 32 * (Number(!!retained.computation) + Number(!!retained.metadata?.computation && retained.metadata.computation !== retained.computation)) + (retained.type === "file" ? retained.content ? contentMemory(retained.content) : retained.version ? 96 : 0 : retained.type === "directory"
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
		if (retained.type === "file" && retained.content instanceof ChunkedContent) this.retainContentPool(retained.content.pool);
		this.entries.set(key, { ...retained, bytes });
	}
	// Windows F_OK observes the final reparse point, including a dangling junction.
	exists = async (target: string): Promise<boolean> => (await this.get(target, process.platform === "win32" ? "exists" : "type")).type !== "missing";
	stat = async (target: string, fields?: "type" | "entry", includeSize = true): Promise<ToolFilesystemStat> => {
		const entry = await this.get(target, fields === "entry" ? includeSize ? "entry" : "exists" : includeSize ? "stat" : "type");
		if (entry.type === "missing") return this.unproven(target);
		return { isDirectory: () => entry.type === "directory", size: includeSize ? resourceSize(entry) : undefined,
			type: entry.type === "alias" ? "symlink" : entry.type, ...(entry.type === "alias" ? { link: entry.link } : { realPath: entry.realPath }) };
	};
	readdir = async (target: string): Promise<string[]> => {
		const entry = await this.get(target, "names");
		return entry.type === "directory" && entry.entries ? [...entry.entries] : this.unproven(target);
	};
	readFile = (target: string, maxBytes?: number): Promise<Buffer> => this.readFileRange(target, 0, maxBytes ?? Number.MAX_SAFE_INTEGER);
	readFileRange = async (target: string, offset: number, length: number): Promise<Buffer> => {
		if (!isSafeCount(offset) || !isSafeCount(length)) throw new Error("invalid content read budget");
		const entry = await this.get(target, "content");
		if (entry.type !== "file" || entry.content === undefined) return this.unproven(target);
		return readContent(entry.content, offset, length);
	};
	/** A process borrows the same captured kernel object without inheriting the tool's OFD offset. */
	async borrowObject<T>(target: string, consume: (capture: StableFilesystemCapture, handle: FilesystemObjectHandle) => Promise<T>): Promise<T | undefined> {
		this.assertComplete();
		return collectResourceComputation(async () => {
			const entry = this.sealed ? await this.get(target, this.entry(target).entry?.type === "directory" ? "names" : "content") : this.entries.get(filesystemPathKey(target));
			const result = await entry?.object?.borrow(consume);
			if (result !== undefined) TimelineInterval.use(entry?.computation);
			return result;
		}, result => result !== undefined);
	}
	/** Loan only owned pins; immutable bytes alone cannot authorize native input reuse. */
	*lendObjects(accept: (object: NonNullable<StableFilesystemCapture["object"]>) => boolean = () => true) {
		if (!this.retained) return;
		for (const [target, entry] of this.entries) if (entry.object && accept(entry.object)) {
			const loan = entry.object.lend(entry.computation);
			if (loan) yield { ...loan, transfer: { ...loan.transfer, realPath: entry.realPath ?? target } };
		}
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
		if (entry?.object !== null || (entry.type === "file" ? !capture.stat.isFile() || !Buffer.isBuffer(entry.content) || !entry.content.equals(capture.content)
			: entry.type !== "directory" || !capture.stat.isDirectory() || capture.bytesRead > (entry.objectBytes ?? 0) ||
				!entry.entries || !capture.entries || !sameValues(entry.entries, capture.entries))) return false;
		this.entries.set(key, { ...entry, ...(entry.type === "file" ? { content: capture.content } : { entries: capture.entries }), object: capture.object, computation: capture.computation });
		return true;
	}
	access = async (target: string): Promise<void> => { await this.readFile(target, 0); };
	/** Retain preparations only while capturing, so the sealed branch accounts for every owned byte. */
	prepare: NonNullable<ToolFilesystemOperations["prepare"]> = (binding, key, build, consume, target, signal) => TimelineInterval.outside(() => {
		signal?.throwIfAborted();
		this.assertComplete();
		let owner: ResourceReadView = this;
		while (owner.owner) owner = owner.owner;
		const prepared = owner.preparations();
		const registration = preparationBindings.get(binding), identity = registration?.id ?? binding;
		return prepared.lifetime.admit(async () => {
			this.assertComplete();
			const cached = prepared.bindings.get(identity)?.get(key);
			const compatible = (cached: PreparedResource | undefined) => cached && !cached.revoked && cached.destination.retained &&
				(cached.retained || !cached.cancellation.signal.aborted) &&
				cached.boundary?.root === this.boundary?.root && cached.boundary?.physicalRoot === this.boundary?.physicalRoot;
			const use = async (cached: PreparedResource, building = false, source?: ResourceInputSource) => {
				if (this.preparing && preparationWaitsFor(cached, this.preparing)) return undefined;
				this.preparing?.waiting.add(cached);
				cached.borrowers++;
				const withdraw = cached.cancellation.acquire(signal);
				let accepted = building;
				try {
					// A composed waiter must take its own proof references before the producer can retire.
					if (!building && !(cached.retained && cached.destination === owner ||
						await Promise.race([cached.ready!.then(() => cached.shareable), cached.declined.then(() => cached.shareable),
							...(!this.acceptProofs && cached.composed ? [cached.composed.then(() => cached.shareable && !cached.proofs?.length)] : [])]) &&
						(!cached.proofs?.length || this.acceptProofs))) return undefined;
					if (building) await cached.ready;
					signal?.throwIfAborted();
					if (registration && !registration.accepts(cached.value)) throw new Error("resource_preparation_data_invalid");
					const dependencies = cached.origin === owner && cached.dependencies && this.dependencies ? new Set([...this.dependencies, ...cached.dependencies]) : undefined;
					const sourceDependencies = source && cached.origin.entries === source.view.entries ? cached.dependencies : undefined;
					const proofs = [...source?.observed(sourceDependencies, false) ?? [], ...cached.proofs ?? []];
					return await cached.destination.prepared!.lifetime.admit(async () => {
						this.assertComplete(); cached.destination.assertComplete();
						await this.validateInputs?.([...(this.observed?.(dependencies, false) ?? []), ...proofs]);
						signal?.throwIfAborted();
						accepted = true; this.dependencies = dependencies;
						// Retain the validated proof before effects; its source may already have retired.
						const transferred = this.acceptProofs && proofs.length && (source || !cached.retained || cached.destination !== owner);
						if (transferred) { for (const proof of this.acceptProofs!(proofs)) this.collectProofs?.set(proof.observations, proof); this.onForeignInputs?.(true); }
						else if (source) this.observeSource(source)(sourceDependencies);
						const evaluation = await TimelineInterval.measure(() => consume(cached.value as Parameters<typeof consume>[0]));
						TimelineInterval.own(evaluation.computation);
						return TimelineInterval.outside(() => {
							TimelineInterval.use(cached.computation);
							this.assertComplete(); if (!transferred && !(this.acceptProofs && cached.origin === owner && cached.dependencies)) cached.destination.assertComplete();
							return { value: evaluation.output };
						});
					});
				} catch (error) {
					if (accepted) throw error;
					this.assertComplete(); signal?.throwIfAborted();
					return undefined;
				} finally {
					this.preparing?.waiting.delete(cached);
					withdraw();
					if (!--cached.borrowers && !(cached.retained && !cached.revoked))
						await TimelineInterval.outside(() => (cached.destination.prepared?.lifetime ?? prepared.lifetime).release(cached));
				}
			};
			const reused = cached && compatible(cached) && await use(cached);
			if (reused) return reused.value;
			for (const source of this.lookup?.() ?? []) {
				const cached = source.view.prepared?.bindings.get(identity)?.get(key);
				if (source.view.entries === this.entries || !source.view.retained || !source.view.sealed ||
					!cached || !compatible(cached)) continue;
				const result = await use(cached, false, source);
				if (result) return result.value;
			}
			const inputEpoch = owner.inputEpoch; // Rejected old sources precede this new computation.
			let resource: Awaited<ReturnType<typeof build>> | undefined;
			let bytes = 0, computationBytes = 32;
			const declined = Promise.withResolvers<void>(), composed = this.acceptProofs && Promise.withResolvers<void>();
			const name = target && filesystemPathKey(target);
			const pending: PreparedResource = { resource: name, origin: owner, destination: owner, borrowers: 0, boundary: this.boundary, cancellation: new SharedAbortScope(), waiting: new Set(),
				declined: declined.promise, composed: composed?.promise,
				dispose: async () => { try { await resource?.dispose(); } finally {
					if (prepared.bindings.get(identity)?.get(key) === pending) prepared.bindings.get(identity)!.delete(key);
					await Promise.allSettled(pending.proofs?.map(proof => proof.release()) ?? []);
					if (pending.retained) pending.destination.capturedBytes -= bytes;
					pending.value = undefined; pending.data = undefined;
				} } };
			let entries = prepared.bindings.get(identity);
			if (!entries) prepared.bindings.set(identity, entries = new Map());
			if (!entries.has(key) || !entries.get(key)!.retained && entries.get(key)!.cancellation.signal.aborted) entries.set(key, pending);
			pending.ready = this.evaluate(async view => {
				view.preparing = pending;
				const foreign = view.onForeignInputs; view.onForeignInputs = transferable => { if (transferable) composed?.resolve(); else declined.resolve(); foreign?.(transferable); };
				view.collectProofs = this.acceptProofs ? new Map() : undefined;
				const evaluation = await TimelineInterval.measure(() => build(view, pending.cancellation.signal));
				resource = evaluation.output; pending.computation = TimelineInterval.own(evaluation.computation);
				pending.cancellation.signal.throwIfAborted();
				computationBytes += evaluation.dependencies.length * 96;
				pending.shareable = !view.foreignInputs;
				if (pending.shareable && view.collectProofs?.size) for (const proof of view.collectProofs.values())
					(pending.proofs ??= []).push(proof.manager.retain({ ...proof, view: undefined }));
			}, (observed, commit) => {
				if (commit === false) return observed && this.observed?.(observed, false);
				pending.dependencies = observed;
				if (owner.sealed && this.missing && observed) {
					const proofs = this.observed?.(observed, false);
					if (!proofs || [...observed].some(key => !proofs.some(proof => proof.observations.has(key)))) pending.dependencies = undefined;
					else for (const proof of proofs) (pending.proofs ??= []).push(proof.manager.retain({ ...proof, view: undefined }));
				}
			}).then(async () => {
				if (!resource) throw new Error("resource_preparation_missing");
				if (registration && !registration.accepts(resource.value)) throw new Error("resource_preparation_data_invalid");
				const data = registration && snapshotPreparation(resource.value);
				pending.value = data ? data.value : resource.value; pending.data = data?.data;
				bytes = (data ? preparationDataBytes(data.data.length) : resource.bytes) + computationBytes + resourceProofBytes(pending.proofs ?? []) +
					preparationMetadataBytes(key, name, pending.dependencies ?? []);
				if (!isSafeCount(resource.bytes)) throw new Error("resource_snapshot_budget_invalid");
				const destination = pending.destination = owner.sealed && this.missing ? (await this.missing()).view : owner;
				pending.cancellation.signal.throwIfAborted();
				this.assertComplete(); destination.assertComplete();
				const retention = destination.preparations();
				if (inputEpoch === owner.inputEpoch && !destination.sealed && destination.bytes + bytes <= destination.maxBytes) {
					let entries = retention.bindings.get(identity);
					if (!entries) retention.bindings.set(identity, entries = new Map());
					if (!entries.has(key) || entries.get(key) === pending) {
						pending.retained = true; entries.set(key, pending); destination.capturedBytes += bytes;
					}
				}
				// The origin keeps a borrowed lookup; the destination owns its bytes and disposal.
			});
			void pending.ready.finally(() => { declined.resolve(); composed?.resolve(); }).catch(() => {});
			return (await use(pending, true))!.value;
		});
	});
	/** Each evaluation owns its proof and failures while borrowing the input lifetime. */
	async evaluate<T>(operation: (view: ResourceReadView) => Promise<T>, observed?: ResourceInputSource["observed"],
		root?: string | { readonly root: string; readonly physicalRoot: string }, lookup = this.lookup, missing = this.missing,
		acceptProofs = this.acceptProofs, validateInputs = this.validateInputs): Promise<T> {
		return collectResourceComputation(async () => {
			const view = this.readOnly(true);
			view.dependencies = new Set();
			Object.assign(view, { lookup, missing, acceptProofs, observed, validateInputs, onForeignInputs: this.onForeignInputs, collectProofs: this.collectProofs, preparing: this.preparing });
			try {
				if (typeof root === "object") view.boundary = { root: root.root, physicalRoot: root.physicalRoot };
				else if (root === undefined || this.boundary && filesystemPathKey(root) === filesystemPathKey(this.boundary.root)) {
					if (this.boundary?.dependency) view.dependencies.add(this.boundary.dependency);
				} else if (filesystemPathKey(root) === filesystemPathKey(path.parse(root).root)) {
					view.boundary = { root, physicalRoot: root };
				} else {
					const entry = await view.get(root, "type");
					if (entry.type !== "directory" || !entry.realPath) view.unproven(root);
					view.boundary = { root, physicalRoot: entry.realPath! };
				}
				const output = await operation(view);
				await TimelineInterval.outside(() => {
					view.assertComplete();
					this.foreignInputs ||= view.foreignInputs;
					observed?.(view.dependencies);
				});
				return output;
			} finally { view.dispose(); }
		});
	}
	assertComplete(sealed = false): void {
		this.owner?.assertComplete(); if (this.failure) throw this.failure;
		if (sealed && !this.sealed) throw new Error("resource_snapshot_not_sealed");
	}
	/** Compare restored query results to a fresh capture, using hashes without retaining a second byte image. */
	sameInputs(current: ResourceReadView): boolean {
		this.assertComplete(true); current.seal();
		const value = (entry: CapturedResource) => ({ type: entry.type, realPath: entry.realPath, size: resourceSize(entry),
			...(entry.type === "file" ? { hash: entry.content instanceof ChunkedContent ? entry.content.manifest.digest.slice(7)
				: entry.content ? hash("sha256", entry.content) : entry.version?.hash }
				: entry.type === "directory" ? { entries: entry.entries } : entry.type === "alias" ? { target: entry.target, link: entry.link } : {}) });
		return this.entries.size === current.entries.size && [...this.entries].every(([target, entry]) => {
			const actual = current.entries.get(target); return actual !== undefined && digest(value(entry)) === digest(value(actual));
		});
	}
	/** Persist owned data only; object pins, live resources, callbacks and timing receipts stay with this owner. */
	async persistentSnapshot(observations: ReadonlyMap<string, ResourceObservation>, store: ProvenanceCertificateStore) {
		this.assertComplete(true);
		if (!this.contents) throw new Error("persistent input storage is not configured");
		const entries: PersistentResourceEntry[] = [];
		for (const [target, entry] of this.entries) {
			const { type, realPath, dependency } = entry;
			const metadata = entry.metadata && { dependency: entry.metadata.dependency, size: entry.metadata.size };
			let content: ChunkedContentManifest | undefined;
			if (type === "file" && entry.content) {
				const bytes = entry.content;
				const version = bytes instanceof ChunkedContent ? bytes : (await this.contents.capture(async sink => {
					await sink(bytes); return { bytesRead: bytes.length, hash: hash("sha256", bytes) };
				})).content;
				await version.publish(); content = version.manifest;
			}
			entries.push([target, { type, realPath, dependency, metadata, ...(content ? { content } : {}),
				...(type === "directory" ? { entries: entry.entries } : type === "alias" ? { target: entry.target, link: entry.link } : {}) }]);
		}
		const preparations: PersistentPreparation[] = [];
		for (const [binding, values] of this.prepared?.bindings ?? []) for (const [key, prepared] of values) {
			if (!this.boundary || typeof binding !== "string" || !prepared.retained || prepared.revoked || !prepared.data || prepared.destination !== this ||
				!prepared.shareable || !prepared.dependencies || key.length > 65536 ||
				prepared.resource && !containsFilesystemPath(this.boundary.root, prepared.resource) ||
				prepared.boundary?.root !== this.boundary?.root || prepared.boundary?.physicalRoot !== this.boundary?.physicalRoot ||
				!this.boundary?.dependency || !prepared.dependencies.has(this.boundary.dependency) || [...prepared.dependencies].some(key => !observations.has(key))) continue;
			// A query may build through one view and retain in another. Its complete proof, not that temporary owner identity, closes the record.
			const dependencies = new Set(prepared.dependencies);
			if (prepared.proofs?.some(proof => proof.root !== this.boundary!.root || proof.physicalRoot !== this.boundary!.physicalRoot ||
				[...proof.observations].some(([key, observation]) => observations.get(key)?.fingerprint !== observation.fingerprint))) continue;
			for (const proof of prepared.proofs ?? []) for (const key of proof.observations.keys()) dependencies.add(key);
			preparations.push({ binding, key, resource: prepared.resource, dependencies: [...dependencies], value: await store.artifacts.put(prepared.data) });
		}
		this.assertComplete(true); return { entries, preparations };
	}
	/** Restore into the same preparation table with fresh cancellation and lifetime state, never old execution authority. */
	async restorePreparations(preparations: readonly PersistentPreparation[], store: ProvenanceCertificateStore): Promise<void> {
		for (const preparation of preparations) {
			this.assertComplete();
			const { binding, key, resource, dependencies, value } = preparation;
			const bytes = preparationDataBytes(value.size) + preparationMetadataBytes(key, resource, dependencies);
			if (!this.reserve(bytes)) this.assertComplete();
			const data = await store.artifacts.get(value);
			if (!data) throw new Error("persistent preparation artifact missing");
			const snapshot = snapshotPreparation(JSON.parse(data.toString("utf8")));
			if (!snapshot || !snapshot.data.equals(data)) throw new Error("resource_preparation_data_invalid");
			if (!this.reserve(0)) this.assertComplete();
			const prepared = this.preparations();
			let entries = prepared.bindings.get(binding); if (!entries) prepared.bindings.set(binding, entries = new Map());
			const restored: PreparedResource = { resource, dependencies: new Set(dependencies), boundary: this.boundary,
				origin: this, destination: this, borrowers: 0, retained: true, shareable: true, cancellation: new SharedAbortScope(), waiting: new Set(),
				ready: Promise.resolve(), declined: Promise.resolve(), value: snapshot.value, data: snapshot.data,
				dispose: () => { if (entries!.get(key) === restored) entries!.delete(key); this.capturedBytes -= bytes; restored.value = undefined; restored.data = undefined; } };
			entries.set(key, restored);
		}
	}
	seal(): void { if (this.pending) this.failure ??= new Error("resource_snapshot_capture_pending"); this.assertComplete(); this.sealed = true; }
	dispose(): void | Promise<void> {
		this.failure = new Error("resource_snapshot_disposed");
		if (this.owner) return;
		for (const owner of this.contentOwners.values()) void this.preparations().lifetime.release(owner);
		this.contentOwners.clear();
		for (const entry of this.entries.values()) this.releaseObject(entry);
		this.entries.clear(); this.objectCount = 0;
		return this.disposal ??= this.prepared ? this.prepared.lifetime.close(async () => {
			await this.prepared!.lifetime.drain();
			await Promise.allSettled([this.pending, ...[...this.prepared!.bindings.values()].flatMap(entries => [...entries.values()].filter(resource => resource.destination === this).map(resource =>
				this.prepared!.lifetime.release(resource)))]);
			this.prepared!.bindings.clear();
		}) : this.pending?.then(() => {}, () => {});
	}
	private retainContentPool(pool: ContentChunkPool): void {
		if (!this.contentOwners.has(pool)) this.contentOwners.set(pool, { dispose: pool.retain() });
	}
	private releaseObject(entry: CapturedResource): void {
		if (entry.object) void this.preparations().lifetime.release(entry.object);
	}
	private preparations() { return this.prepared ??= { lifetime: new RuntimeLifecycleLane(), bindings: new Map() }; }
	private async get(target: string, scope: ResourceDependency["scope"]): Promise<CapturedResource> {
		this.assertComplete();
		if (this.owner && this.boundary && !containsFilesystemPath(this.boundary.root, target)) this.unproven(target);
		if (this.loadOwner && this.owner && !this.owner.sealed) await this.owner.get(target, scope);
		if (this.load && !this.sealed) {
			const pending = (this.pending ?? Promise.resolve()).then(() => {
				const { entry } = this.entry(target, followsResource(scope));
				// Existing input evidence also owns the metadata derivable from those bytes or names.
				if (resourceCovers(entry, scope)) return;
				return this.load!({ path: target, scope });
			});
			this.pending = pending;
			try { await pending; }
			catch (error) { throw this.failure ??= error instanceof Error ? error : new Error(String(error)); }
			finally { if (this.pending === pending) this.pending = undefined; }
		}
		let entry: CapturedResource | undefined, resolved = filesystemPathKey(target);
		try {
			// Probe only this read; rejected aliases leave neither dependencies nor computation receipts.
			({ entry, resolved } = this.validateInputs ? await this.evaluate(async view => {
				const input = view.entry(target, followsResource(scope), scope);
				if (input.entry || input.resolved !== resolved)
					await TimelineInterval.outside(() => this.validateInputs!(this.observed?.(view.dependencies, false) ?? []));
				return input;
			}, observed => {
				if (!observed) this.dependencies = undefined;
				else if (this.dependencies) for (const dependency of observed) this.dependencies.add(dependency);
			}, this.boundary) : this.entry(target, followsResource(scope), scope));
		} catch { this.assertComplete(); }
		if (resourceCovers(entry, scope)) {
			TimelineInterval.use(["stat", "type", "entry", "exists"].includes(scope) ? entry?.metadata?.computation : entry?.computation);
			return entry!;
		}
		// The local alias proof owns this translation; the source owns the resolved resource.
		for (const query of resolved === filesystemPathKey(target) ? [target] : [resolved, target]) for (const source of this.lookup?.(query) ?? []) {
			if (source.view.entries === this.entries || !source.view.retained) continue;
			try {
				source.view.assertComplete(true);
				// The caller already owns the boundary proof; the source contributes only its resource evidence.
				const boundary = this.boundary && query !== target
					? { root: this.boundary.physicalRoot, physicalRoot: this.boundary.physicalRoot } : this.boundary;
				return await source.view.evaluate(async view => {
					const entry = await view.get(query, scope);
					await TimelineInterval.outside(() => this.validateInputs?.(source.observed(view.dependencies, false) ?? []));
					return entry;
				}, this.observeSource(source), boundary);
			} catch { /* An indexed name alone grants no coverage; another sealed owner may supply it. */ }
		}
		if (this.missing) {
			try {
				const source = await this.missing();
				return await source.view.evaluate(view => view.get(target, scope), this.observeSource(source), this.boundary);
			} catch (error) { throw this.failure ??= error instanceof Error ? error : new Error(String(error)); }
		}
		return this.unproven(target);
	}
	private observeSource(source: ResourceInputSource) {
		return (dependencies: ReadonlySet<string> | undefined, commit = true) => {
			const proofs = source.observed(dependencies, commit);
			if (!commit) return proofs;
			if (this.collectProofs && proofs?.length) {
				for (const proof of proofs) this.collectProofs.set(proof.observations, proof);
				this.onForeignInputs?.(true);
			} else { this.foreignInputs = true; this.onForeignInputs?.(false); }
			return proofs;
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
		if (this.owner && this.boundary && entry?.realPath && (entry.type !== "alias" || scope !== undefined && !followsResource(scope)) &&
			!containsFilesystemPath(this.boundary.physicalRoot, entry.realPath)) this.unproven(entry.realPath);
		if (entry?.type === "alias") TimelineInterval.use(entry.computation);
		if (entry && this.dependencies) {
			const dependency = entry.type !== "alias" && (scope === "type" || scope === "exists" || ((scope === "entry" || scope === "stat") && !(entry.type === "file" && entry.content !== undefined)))
				? entry.metadata?.dependency ?? entry.dependency : entry.dependency;
			if (dependency) this.dependencies.add(dependency); else this.dependencies = undefined;
		}
		return entry;
	}
	private unproven(target: string): never {
		throw (this.failure ??= new Error(`resource_access_unproven:${target}`));
	}
}

/** Failed resource attempts keep their actual computation, never receipts for discarded inputs. */
export async function collectResourceComputation<T>(operation: () => T | Promise<T>, accept: (value: T) => boolean = () => true): Promise<T> {
	const evaluation = await TimelineInterval.collect(async () => {
		try { return { ok: true as const, value: await operation() }; }
		catch (error) { return { ok: false as const, error }; }
	});
	const accepted = evaluation.output.ok && accept(evaluation.output.value);
	for (const { computation, owned, overhead, computeUncertain } of evaluation.dependencies) {
		if (overhead) TimelineInterval.exclude(computation, computeUncertain);
		else if (owned) TimelineInterval.own(accepted ? computation : TimelineInterval.attempted(computation));
		else if (accepted) TimelineInterval.use(computation);
	}
	if (!evaluation.output.ok) throw evaluation.output.error;
	return evaluation.output.value;
}

const resourceSize = (entry: CapturedResource) => (entry.type === "file" ? entry.content?.length ?? entry.version?.size : undefined) ?? entry.metadata?.size;

function resourceMetadata(entry: CapturedResource, dependency: string): CapturedResource {
	const metadata = { ...entry.metadata, dependency,
		size: entry.type === "file" && entry.content && entry.dependency === dependency ? entry.content.length
			: entry.metadata?.dependency === dependency ? entry.metadata.size : undefined };
	return entry.type === "alias" ? entry : { type: entry.type, realPath: entry.realPath, dependency, computation: metadata.computation, metadata };
}

const followsResource = (scope: ResourceDependency["scope"]) => scope !== "entry" && scope !== "exists";

function resourceCovers(entry: CapturedResource | undefined, scope: ResourceDependency["scope"]): boolean {
	return !!entry && (scope === "type" || scope === "exists" ||
		((scope === "entry" || scope === "stat") && (entry.type === "missing" || resourceSize(entry) !== undefined)) ||
		(scope === "names" && entry.type === "directory" && entry.entries !== undefined) ||
		(scope === "content" && entry.type === "file" && entry.content !== undefined));
}

type ResourceEvent = { readonly epoch: number; readonly path: string; readonly type: "change" | "rename" | "unknown"; };

const MAX_EVENT_HISTORY = 4096;

type ResourceWatcher = Pick<FSWatcher, "close"> & { readonly settle?: () => Promise<void> };

/** Linux's recursive Node watcher performs ordinary stat/readdir calls. Install
 * nonrecursive inode watches through the read view instead, and rescan renamed
 * subtrees through one fresh view. Symbolic links are covered by their parent. */
async function watchReadonlyFilesystem(root: string, changed: (target: string, type: "change" | "rename") => void,
	failed: (error: Error) => void): Promise<ResourceWatcher> {
	const watched = new Map<string, { readonly watcher: FSWatcher; readonly identity: string }>(), dirty = new Set([root]);
	let closed = false, pending: Promise<void> | undefined, failure: Error | undefined;
	const close = () => { if (!closed) { closed = true; for (const entry of watched.values()) entry.watcher.close(); watched.clear(); } };
	const fail = (error: Error) => { if (!closed) { failure = error; close(); failed(error); } };
	const refresh = (): Promise<void> => {
		if (closed) return failure ? Promise.reject(failure) : Promise.resolve();
		return pending ??= (async () => {
			while (dirty.size && !closed) {
				const roots = [...dirty]; dirty.clear();
				await using filesystem = await openFilesystemReadView();
				const sourceRoot = await filesystem.realpath(root);
				for (const subtree of roots) {
					const seen = new Set<string>(), paths = [subtree];
					for (let cursor = 0; cursor < paths.length && !closed; cursor++) {
						const target = paths[cursor]; let descriptor: number | undefined;
						try {
							descriptor = filesystem.open(target, 0x200000 | (target === root ? 0 : constants.O_NOFOLLOW));
							const stat = fstatSync(descriptor, { bigint: true });
							if (stat.isSymbolicLink()) continue;
							if (!containsFilesystemPath(sourceRoot, readlinkSync(`/proc/self/fd/${descriptor}`))) throw new Error("filesystem watcher directory escaped its root");
							seen.add(target);
							const identity = `${stat.dev}:${stat.ino}`, prior = watched.get(target), directory = stat.isDirectory();
							if (prior?.identity !== identity) {
								prior?.watcher.close();
								const watcher = watch(`/proc/self/fd/${descriptor}`, (event, filename) => {
									if (closed) return;
									changed(directory && filename ? path.join(target, filename.toString()) : target, event);
									if (event === "rename") {
										changed(target, event); dirty.add(directory ? target : path.dirname(target));
										void refresh().catch(fail);
									}
								});
								watcher.on("error", fail); watched.set(target, { watcher, identity });
							}
							if (directory) for (const { name } of await readFilesystemDirectory(target, filesystem)) paths.push(path.join(target, name));
						} catch (error) { if (!missingResource(error)) throw error; }
						finally { if (descriptor !== undefined) closeSync(descriptor); }
					}
					if (closed) break;
					for (const [target, entry] of watched) if (containsFilesystemPath(subtree, target) && !seen.has(target)) {
						entry.watcher.close(); watched.delete(target);
					}
				}
				await filesystem.verify([...roots, sourceRoot]);
			}
			if (failure) throw failure;
		})().finally(() => { pending = undefined; });
	};
	try {
		await refresh();
		return { close, settle: async () => { do { await refresh(); } while (pending || !closed && dirty.size); } };
	} catch (error) { close(); throw error; }
}

export class ResourceVersionManager {
	private epoch = 0;
	private readonly events: ResourceEvent[] = [];
	private readonly preciseWatches = new Map<string, { readonly watcher: FSWatcher; references: number }>();
	private references = 0;
	private watcher?: ResourceWatcher;
	private reliable = false;
	private ready?: Promise<void>;
	private open = true;
	readonly root: string;
	private readonly snapshotExcludes: ReadonlySet<string>;
	private readonly gitObjects: boolean;
	private readonly onIdle?: () => void;
	private readonly persistentInputs?: ResourceInputPersistence;

	constructor(root: string, options: {
		readonly watch?: boolean; readonly onIdle?: () => void;
		/** Private snapshot entries only; filtered tokens cannot authorize input views or host execution windows. */
		readonly snapshotExcludes?: readonly string[];
		/** Tree captures also name each file's Git object, from the same bytes their fingerprint read. */
		readonly gitObjects?: boolean;
		/** Linux input content uses the existing CAS; live kernel objects and prepared callbacks remain ephemeral. */
		readonly persistentInputs?: ResourceInputPersistence;
	} = {}) {
		this.root = options.persistentInputs ? path.resolve(root) : root;
		this.snapshotExcludes = new Set(options.snapshotExcludes);
		this.gitObjects = options.gitObjects === true;
		this.onIdle = options.onIdle;
		this.persistentInputs = options.persistentInputs;
		if (this.persistentInputs && !path.isAbsolute(root)) throw new Error("persistent input root must be absolute");
		if (options.watch === false) this.ready = Promise.resolve();
	}

	private async startWatching() {
		try {
			const failed = () => { this.reliable = false; this.changed(this.root, "unknown"); };
			if (nativeFilesystem) this.watcher = await watchReadonlyFilesystem(this.root, (target, event) => this.changed(target, event), failed);
			else {
				const watcher = watch(this.root, { recursive: true }, (event, filename) => this.changed(filename ? path.resolve(this.root, filename) : this.root, event));
				watcher.on("error", failed); this.watcher = watcher;
			}
			if (!this.open) { this.watcher?.close(); this.watcher = undefined; return; }
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
			.map(([target, input]) => [filesystemPathKey(path.resolve(this.root, target)), input && !isInputContent(input)
				? "names" in input ? { ...input, names: input.names && [...input.names] } : { ...input } : input])));
	}

	/** Notification cursor for preparation; empty observations cannot validate or seal any resource. */
	observeChanges(): Promise<ResourceVersionToken> { return this.captureToken([]); }

	/** Store a sealed input closure. Publication never upgrades its historical observations into current authority. */
	async persist(token: ResourceVersionToken): Promise<Sha256Digest> {
		if (!this.open || token.manager !== this || token.root !== this.root || !token.view || !this.persistentInputs) throw new Error("persistent input owner changed");
		await this.assertPersistentStore();
		return this.persistentInputs.store.withPublication(async () => {
			const observations = [...token.observations.values()].filter(observation => observation.scope !== "binding");
			const { entries, preparations } = await token.view!.persistentSnapshot(new Map(observations.map(observation => [dependencyKey(observation), observation])), this.persistentInputs!.store);
			const payload: PersistentResourcePayload = { root: this.root, physicalRoot: token.physicalRoot, observations, entries, preparations };
			const references = new Map<Sha256Digest, ArtifactReference>();
			for (const [, entry] of entries) for (const reference of entry.content?.chunks ?? []) if (reference) references.set(reference.digest, reference);
			for (const preparation of preparations) references.set(preparation.value.digest, preparation.value);
			token.view!.assertComplete(true);
			if (!this.open || !observations.length) throw new Error("persistent input owner changed");
			return (await this.persistentInputs!.store.putInputVersion(payload, [...references.values()])).id;
		});
	}

	/** Cold recovery grants a new input lease only after content integrity and current query validation. */
	async restore(id: Sha256Digest, retainBytes: number): Promise<ResourceVersionToken | undefined> {
		if (!this.open || !this.persistentInputs || this.snapshotExcludes.size) throw new Error("persistent input owner changed");
		await this.assertPersistentStore();
		const { store, limits } = this.persistentInputs;
		return store.withPublication(async () => {
			const record = await store.getInputVersion(id); if (!record) return;
			const payload = parsePersistentResourcePayload(record.payload, this.root);
			const observations = new Map(payload.observations.map(observation => [dependencyKey(observation), observation]));
			const contents = new ContentChunkPool(store, limits), view = new ResourceReadView(retainBytes, undefined,
				{ root: this.root, physicalRoot: payload.physicalRoot, dependency: dependencyKey({ path: this.root, scope: "resolution" }) }, undefined, contents);
			this.references++;
			const release = releaseOnce(() => this.releaseCapture(observations, view));
			try {
				const references = new Map<Sha256Digest, number>();
				for (const [target, entry] of payload.entries) {
					const content = entry.content && await contents.restore(entry.content);
					for (const reference of content?.manifest.chunks ?? []) if (reference) references.set(reference.digest, reference.size);
					view.capture(target, { ...entry, ...(content ? { content } : {}) } as CapturedResource);
					view.assertComplete();
				}
				for (const preparation of payload.preparations) validateArtifact(preparation.value, references);
				if (record.artifacts.length !== references.size || record.artifacts.some(reference => references.get(reference.digest) !== reference.size)) throw new Error("persistent input artifact closure mismatch");
				await view.restorePreparations(payload.preparations, store);
				view.seal();
				const token: ResourceVersionToken = { root: this.root, physicalRoot: payload.physicalRoot, observations, epoch: this.epoch,
					watching: false, preciseContent: Object.freeze([]), manager: this, view, inputView: new WeakRef(view), release };
				const proof = new ResourceReadView(retainBytes, undefined, undefined, undefined, "verify");
				try { if ((await this.inspect(token, false, proof)).expired) { await release(); return; } }
				finally { await proof.dispose(); }
				return token;
			} catch (error) { await release(); throw error; }
		});
	}

	/** Discover existing input closures without a separate ID index. The caller owns every yielded token. */
	async *recover(retainBytes: number, options: { readonly maxCandidates?: number; readonly signal?: AbortSignal } = {}):
		AsyncGenerator<{ readonly id: Sha256Digest; readonly token: ResourceVersionToken }> {
		if (!this.open || !this.persistentInputs || this.snapshotExcludes.size) throw new Error("persistent input owner changed");
		if (!Number.isFinite(retainBytes) || retainBytes < 0) throw new Error("resource_snapshot_budget_invalid");
		const { store } = this.persistentInputs, { signal } = options;
		signal?.throwIfAborted(); await this.assertPersistentStore();
		for await (const record of store.candidatesInputVersions(options.maxCandidates ?? Math.min(store.limits.maxCertificates, 4096), signal)) {
			signal?.throwIfAborted(); store.signal.throwIfAborted();
			if (!this.open) throw new Error("persistent input owner changed");
			if ((record.payload as Partial<PersistentResourcePayload> | null)?.root !== this.root) continue;
			let token: ResourceVersionToken | undefined;
			try {
				token = await this.restore(record.id, retainBytes);
				signal?.throwIfAborted(); store.signal.throwIfAborted();
				if (!this.open) throw new Error("persistent input owner changed");
			} catch (error) {
				await token?.release(); signal?.throwIfAborted(); store.signal.throwIfAborted();
				if (!this.open) throw error;
				continue; // An unavailable cache record cannot hide another independently valid input closure.
			}
			if (token) yield Object.freeze({ id: record.id, token });
		}
	}

	/** Resolve existing ancestors before creating the cache: persistent capture must not write into its source. */
	private async assertPersistentStore(): Promise<void> {
		if (!this.persistentInputs) return;
		await withFilesystemReadView(undefined, async filesystem => {
			const root = await filesystem.realpath(this.root), { path: resolved } = await resolveFilesystemDestination(this.persistentInputs!.store.root, filesystem);
			if (containsFilesystemPath(root, resolved) || containsFilesystemPath(resolved, root)) throw new Error("persistent input cache must be outside its source tree");
		});
	}

	/** Own an evaluated query's evidence without keeping its input buffers or source branch alive. */
	retain(token: ResourceVersionToken, metadataBytes?: number): ResourceVersionToken {
		if (!this.open || token.manager !== this || token.root !== this.root || !token.observations.size)
			throw new Error("resource_version_owner_changed");
		const observations = new Map(token.observations);
		const view = metadataBytes === undefined || !token.view?.retained ? undefined : token.view.retainMetadata(observations, metadataBytes);
		this.references++;
		return { ...token, observations, view, watching: false, preciseContent: Object.freeze([]), release: releaseOnce(() => this.releaseCapture(observations, view)) };
	}

	private async captureToken(dependencies: ReadonlyArray<ResourceDependency> | undefined, retainBytes?: number,
		providedInputs?: ReadonlyMap<string, ResourceInput>): Promise<ResourceVersionToken> {
		if (!this.open) throw new Error("resource_version_manager_closed");
		if (this.snapshotExcludes.size && retainBytes !== undefined) throw new Error("resource_filtered_snapshot_not_readable");
		const observations = new Map<string, ResourceObservation>();
		const treeObjects = this.gitObjects && dependencies?.some((dependency) => dependency.scope === "tree_content") ? new Map<string, TreeObject>() : undefined;
		let precise: Awaited<ReturnType<ResourceVersionManager["acquirePreciseWatches"]>> | undefined;
		this.references++;
		let view: ResourceReadView | undefined;
		const observing = dependencies !== undefined && !providedInputs;
		const release = releaseOnce(() => this.releaseCapture(observations, view, precise));
		try {
			await ensureNativeFilesystem();
			if (retainBytes !== undefined) await this.assertPersistentStore();
			if (observing) { await (this.ready ??= this.startWatching()); await this.watcher?.settle?.(); }
			const physicalRoot = await fingerprintIO(() => withFilesystemReadView(undefined, filesystem => filesystem.realpath(this.root)));
			const capture = async (requested: ReadonlyArray<ResourceDependency>) => {
				const normalized = [...new Map(requested.map(({ path: target, scope }) => {
					const dependency = { path: path.resolve(this.root, target), scope };
					if (!containsFilesystemPath(this.root, dependency.path)) throw new Error(`resource dependency escapes workspace: ${target}`);
					return [dependencyKey(dependency), dependency];
				})).values()].filter(dependency => !observations.has(dependencyKey(dependency)));
				if (!normalized.length) return;
				if (observing && this.reliable) precise = await this.acquirePreciseWatches(normalized);
				for (const observation of await fingerprintDependencies(normalized, physicalRoot, this.snapshotExcludes, view,
					observing && !this.snapshotExcludes.size ? { root: this.root, observations } : undefined, providedInputs, treeObjects)) observations.set(dependencyKey(observation), observation);
			};
			const boundary = { path: this.root, scope: "resolution" as const }, boundaryKey = dependencyKey(boundary);
			view = retainBytes === undefined ? undefined : new ResourceReadView(retainBytes, dependencies ? undefined : (dependency) => capture([dependency]),
				{ root: this.root, physicalRoot, dependency: boundaryKey }, dependencies ? undefined : (target, content) => {
					if (!containsFilesystemPath(this.root, target) || !containsFilesystemPath(physicalRoot, target) ||
						filesystemPathKey(target) !== filesystemPathKey(content.realPath) ||
						!(content.stat.isDirectory() ? content.entries : content.stat.isFile())) return;
					const dependency = { path: target, scope: content.entries ? "names" as const : "content" as const }, key = dependencyKey(dependency);
					view!.captureObject(target, content, key);
					const { value, stamp } = content.entries ? fingerprintDirectory(content.stat, content.realPath, content.entries) : fingerprintFile(content);
					observations.set(key, { ...dependency, stamp, fingerprint: digest({ path: filesystemPathKey(target), scope: dependency.scope, value }) });
				}, retainBytes !== undefined && this.persistentInputs ? new ContentChunkPool(this.persistentInputs.store, this.persistentInputs.limits) : undefined);
			if (view) observations.set(boundaryKey, { ...boundary, stamp: filesystemPathKey(physicalRoot),
				fingerprint: digest({ path: filesystemPathKey(this.root), scope: "resolution", value: filesystemPathKey(physicalRoot) }) });
			if (dependencies) await capture(dependencies);
			if (observing) { await watcherTurn(); await this.watcher?.settle?.(); }
			if (!this.open) throw new Error("resource_version_manager_closed");
			const retained = view?.retained ? view : undefined;
			if (dependencies) retained?.seal();
			return {
				root: this.root, physicalRoot, observations, epoch: this.epoch,
				watching: Boolean(observing && this.reliable), preciseContent: Object.freeze(precise?.paths ?? []),
				manager: this, ...(retained ? { view: retained, inputView: new WeakRef(retained) } : {}), ...(treeObjects ? { treeObjects } : {}), release,
			};
		} catch (error) { await release(); throw error; }
	}

	private releaseCapture(observations: Map<string, ResourceObservation>, view?: ResourceReadView, precise?: { release: () => void }) {
		const finish = () => { observations.clear(); precise?.release(); if (--this.references === 0 && !this.preciseWatches.size) this.onIdle?.(); };
		const pending = view?.dispose(); return pending ? pending.then(finish) : finish();
	}

	/** Adoption waits on these few checks, so their I/O never queues behind a whole-tree scan's waiters. */
	async validate(token: ResourceVersionToken): Promise<ResourceVersionValidation> { return priorityIO.run(true, () => this.inspect(token, false)); }

	async seal(token: ResourceVersionToken): Promise<ResourceVersionValidation> { return this.inspect(token, true); }

	private async inspect(token: ResourceVersionToken, sealing: boolean, restoredInputs?: ResourceReadView): Promise<ResourceVersionValidation> {
		if (!this.open || token.manager !== this || token.root !== this.root)
			return validation("resource_version_owner_changed");
		try {
			if (sealing) token.view?.seal(); else token.view?.assertComplete(true);
			if (sealing && this.snapshotExcludes.size) throw new Error("resource_filtered_snapshot_not_observable");
			// Host windows need eager binding evidence; Windows can restore a junction without changing its stamps.
			if (sealing && (process.platform === "win32" || !token.observations.has(`binding:${filesystemPathKey(this.root)}`))) throw new Error("resource_path_binding_window_unprovable");
			if (!token.observations.size) throw new Error("resource_dependencies_unproven");
			// Watcher delivery fences host execution windows; future adoption uses the exact fingerprints below.
			if (sealing) await watcherTurn();
			const watcherFailure = this.invalidation(token, sealing);
			if (watcherFailure) return validation(watcherFailure, "watcher");
			const current = await fingerprintDependencies([...token.observations.values()].filter((entry) => sealing || entry.scope !== "binding"), token.physicalRoot, this.snapshotExcludes, restoredInputs);
			if (sealing) await watcherTurn();
			const lateFailure = this.invalidation(token, sealing);
			if (lateFailure) return validation(lateFailure, "watcher");
			const changed = current.filter((entry) => {
				const captured = token.observations.get(dependencyKey(entry));
				return entry.fingerprint !== captured?.fingerprint || (sealing && (!entry.stamp || !captured?.stamp || entry.stamp !== captured.stamp));
			}).map(dependencyKey);
			const expired = !current.length || changed.length > 0 || !!restoredInputs && !token.view!.sameInputs(restoredInputs);
			const reason = sealing ? "resource_observation_window_changed" : "resource_fingerprint_changed";
			return { ...validation(expired ? reason : undefined, "exact", current), ...(!sealing && changed.length ? { changed } : {}) };
		} catch {
			const reason = sealing ? "resource_observation_window_unprovable" : "resource_validation_failed";
			return validation(reason);
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
		if (token.manager !== this || token.root !== this.root || !token.watching || !this.reliable) return { uncertain: true, paths: [] };
		const oldest = this.events[0]?.epoch ?? this.epoch;
		if (token.epoch < oldest && this.events.length >= MAX_EVENT_HISTORY) return { uncertain: true, paths: [] };
		// Excluded segments (a workspace's .git) are never snapshotted: their churn is no change to prepare for.
		const events = this.events.filter((event) => event.epoch > token.epoch &&
			!relativeFilesystemPath(this.root, event.path)?.split(/[\\/]/u).some((segment) => this.snapshotExcludes.has(segment)));
		return { uncertain: events.some((event) => event.type === "unknown"), paths: [...new Set(events.map((event) => event.path))] };
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

	private async acquirePreciseWatches(dependencies: ReadonlyArray<ResourceDependency>) {
		return withFilesystemReadView(undefined, async filesystem => {
		const paths: string[] = [];
		for (const target of new Set(
			dependencies.filter((dependency) => dependency.scope === "content").map((dependency) => dependency.path),
		)) {
			const key = filesystemPathKey(target);
			const existing = this.preciseWatches.get(key);
			if (existing) { existing.references++; paths.push(target); continue; }
			try {
				let descriptor: number | undefined, watcher: FSWatcher;
				try {
					if (nativeFilesystem) descriptor = filesystem.open(target, 0x200000);
					watcher = watch(descriptor === undefined ? target : `/proc/self/fd/${descriptor}`, (event) => this.changed(target, event));
				} finally { if (descriptor !== undefined) closeSync(descriptor); }
				watcher.on("error", () => { this.reliable = false; this.changed(target, "unknown"); });
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
		}, async result => result.release());
	}

}

const managers = new Map<string, ResourceVersionManager>();

export function resourceDependencies(action: ActionKey, root: string, actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS) {
	const definition = actionSemantics.definition(action);
	const scope = definition ? definition.resourceScope : "content";
	if (scope === undefined || scope === "captured_inputs") return [];
	return action.resources.map((resource) => ({ path: path.resolve(root, resource), scope }));
}

export async function captureResourceVersion(action: ActionKey | undefined, root: string,
	actionSemantics: ActionSemanticsRegistry = PI_ACTION_SEMANTICS, retainBytes?: number, providedInputs?: ReadonlyMap<string, ResourceInput>) {
	const dependencies = providedInputs ? [...providedInputs].map(([path, input]): ResourceDependency => ({ path,
		scope: input === null || inputContent(input) ? "content" : "names" in input && input.names ? "names" : "type" }))
		: action ? resourceDependencies(action, root, actionSemantics) : undefined;
	if (dependencies?.length === 0 || (!dependencies && retainBytes === undefined)) throw new Error("resource_dependencies_unproven");
	return resourceManager(root).capture(dependencies, retainBytes, providedInputs);
}

/** Shared notification cursor for preparation only; it cannot validate or seal an action's inputs. */
export function observeResourceChanges(root: string): Promise<ResourceVersionToken> { return resourceManager(root).observeChanges(); }

function resourceManager(root: string): ResourceVersionManager {
	const normalized = path.resolve(root);
	let manager = managers.get(normalized);
	if (!manager) {
		manager = new ResourceVersionManager(normalized, {
			watch: path.dirname(normalized) !== normalized,
			onIdle: () => { if (managers.get(normalized) === manager) { manager!.close(); managers.delete(normalized); } },
		});
		managers.set(normalized, manager);
	}
	return manager;
}

export async function validateResourceVersion(token: unknown): Promise<ResourceVersionValidation> {
	if (!Array.isArray(token)) return isResourceVersionToken(token)
		? token.manager.validate(token) : validation("resource_version_missing");
	if (token.length === 1) return validateResourceVersion(token[0]);
	const checked: ResourceVersionValidation[] = [];
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
		for (const group of groups.values()) { const result = await group.manager.validate(group); checked.push(result); }
		const expired = checked.find(result => result.expired);
		if (expired) return { ...validation(expired.reason, "exact", checked), changed: [...new Set(checked.flatMap(result => result.changed ?? []))] };
		for (const source of token as ResourceVersionToken[]) source.view?.assertComplete(true);
		return validation(undefined, "exact", checked);
	} catch (error) { return validation(error instanceof Error ? error.message : "resource_validation_failed", "exact", checked); }
}


export function invalidateResourceInputs(tokens: readonly ResourceVersionToken[], paths: readonly string[]): readonly string[] {
	const dependencies = new Set<string>(), precise = new Set<string>();
	for (const token of tokens) for (const [key, dependency] of token.observations) {
		if (paths.some(changed => dependency.scope === "resolution" ? containsFilesystemPath(changed, dependency.path)
			: affects(dependency, { path: changed, type: "rename", epoch: 0 }, precise))) dependencies.add(key);
	}
	return [...new Set(tokens.flatMap(token => token.view?.invalidate(dependencies) ?? []))];
}

export function resourceProofBytes(versions: readonly ResourceVersionToken[]): number {
	let bytes = 0;
	for (const token of versions) for (const [key, entry] of token.observations)
		bytes += (key.length + entry.path.length + entry.fingerprint.length + (entry.stamp?.length ?? 0)) * 2 + 128;
	return bytes;
}

export function releaseResourceVersion(token: unknown): void | Promise<void> { if (!isResourceVersionToken(token)) return; return token.release(); }

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

const dependencyKey = (dependency: ResourceDependency) => `${dependency.scope}:${filesystemPathKey(dependency.path)}`;

function affects(dependency: ResourceDependency, event: ResourceEvent, preciseContent: ReadonlySet<string>) {
	if (dependency.scope === "binding") return event.type === "rename" && filesystemPathKey(dependency.path) === filesystemPathKey(event.path);
	if (event.type === "unknown") return true;
	const dependencyPath = filesystemPathKey(dependency.path);
	const changed = filesystemPathKey(event.path);
	if (["content", "stat", "type", "entry", "exists"].includes(dependency.scope)) {
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

type FingerprintResult = { readonly value: unknown; readonly stamp: string; readonly bytesRead: number; readonly filesRead: number; };

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
	bindings?: { readonly root: string; readonly observations: Map<string, ResourceObservation> },
	providedInputs?: ReadonlyMap<string, ResourceInput>,
	treeObjects?: Map<string, TreeObject>, filesystem?: FilesystemReadView,
): Promise<(ResourceObservation & { readonly bytesRead: number; readonly filesRead: number })[]> {
	if (!filesystem) return withFilesystemReadView(undefined, active => fingerprintDependencies(dependencies, realRoot, excludes, view, bindings, providedInputs, treeObjects, active));
	const files = new Map<string, Promise<FingerprintResult & { readonly blob?: string }>>(), nearestExisting = missingResourceResolver(realRoot, filesystem);
	const aliases = new Map<string, Map<string, { paths: Set<string>; links: number }>>();
	const entries = new Map<string, ReturnType<typeof captureFilesystemEntry>>();
	const capture = (target: string) => {
		const key = filesystemPathKey(target), previous = entries.get(key);
		if (previous) return previous;
		const pending = fingerprintIO(() => captureFilesystemEntry(target, undefined, filesystem)).then((entry) => {
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
	return mapFilesystem(dependencies, async (dependency) => {
		if (dependency.scope === "binding") return fingerprintBinding(dependency, filesystem);
		const key = dependencyKey(dependency);
		if (dependency.scope === "tree_content" || dependency.scope === "tree_entries") aliases.set(key, new Map());
		const { value, ...metrics } = dependency.scope === "resolution"
			? await fingerprintIO(async () => { const resolved = filesystemPathKey(await filesystem.realpath(dependency.path)); return { value: resolved, stamp: resolved, bytesRead: 0, filesRead: 0 }; })
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
		/** A walked directory's real path: a child that is no link resolves to its entry there, with no window to re-check. */
		parentReal?: string,
	): Promise<FingerprintResult> {
		const startedAt = performance.now();
		let captured: Awaited<ReturnType<typeof captureFilesystemEntry>>;
		try {
			if (bindings) for await (const entry of walkFilesystemPath(target, { capture, followFinal: followsResource(scope), view: filesystem })) {
				if (entry.info && !entry.info.isDirectory() && entry.link === undefined && !entry.terminal) break;
			}
			captured = await capture(target);
		} catch (error) {
			if (!missingResource(error)) throw error;
			if (providedInputs && providedInputs.get(filesystemPathKey(target)) !== null) throw error;
			const nearest = await nearestExisting(target);
			view?.capture(target, { type: "missing", realPath: nearest.realPath, dependency, computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now())) });
			return {
				value: { exists: false, error: errorCode(error), resolved: nearest.realPath },
				stamp: digest([filesystemPathKey(target), nearest.fingerprint]),
				bytesRead: 0,
				filesRead: 0,
			};
		}
		const { info, link } = captured;
		const supplied = providedInputs?.get(filesystemPathKey(target));
		const suppliedContent = inputContent(supplied);
		if (providedInputs && !(suppliedContent ? info.isFile() : supplied && ("type" in supplied ? specialFileType(info) === supplied.type : info.isDirectory())))
			throw new Error(suppliedContent ? "resource_input_not_regular" : "resource_input_type_changed");
		const realTarget = info.isSymbolicLink()
			? path.join(parentReal ?? await fingerprintIO(() => filesystem!.realpath(path.dirname(target))), path.basename(target))
			: parentReal === undefined ? await fingerprintIO(() => filesystem!.realpath(target)) : path.join(parentReal, path.basename(target));
		// An entry name joined to a walked directory stays where the directory was proven to be.
		if (parentReal === undefined) assertInside(realRoot, realTarget);
		const identity = filesystemPathKey(realTarget);
		const metadata = { dependency, computation: captured.computation };
		const retainEntry = (entry: CapturedResource, reserved = 0) => view?.capture(target,
			{ realPath: realTarget, ...metadata, metadata, ...entry }, reserved);
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
			if (followsResource(scope)) await fingerprintIO(async () => {
				for await (const entry of walkFilesystemPath(target, { view: filesystem })) {
					source = entry.path;
					if (entry.link !== undefined) links.push([filesystemPathKey(source), entry.link, statStamp(entry.info!)]);
				}
			});
			const followed = source === undefined ? undefined : await fingerprintPath(source, scope, dependency, new Set(ancestors).add(identity), descend);
			retainEntry({ type: "alias", target: source && filesystemPathKey(source), link: link!, metadata: { ...metadata, size: Number(info.size) } });
			if (scope === "tree_content") treeObjects?.set(target, { mode: Number(info.mode), link: link! });
			return {
				value: { type: "symlink", link, size: Number(info.size), mode: Number(info.mode), resolved: identity, target: followed?.value },
				stamp: digest(["symlink", link, statStamp(info), links, followed?.stamp]),
				bytesRead: followed?.bytesRead ?? 0,
				filesRead: followed?.filesRead ?? 0,
			};
		}
		if (["stat", "type", "entry", "exists"].includes(scope) || (scope === "entries" && !descend) || (["names", "entries", "tree_entries"].includes(scope) && !info.isDirectory())) {
			const after = parentReal === undefined ? await fingerprintIO(() => filesystem!.stat(target)) : info;
			if (!sameFilesystemIdentity(info, after)) throw new Error(`resource_file_changed:${target}`);
			const size = ["stat", "entry"].includes(scope) ? Number(after.size) : undefined;
			const computation = supplied && "computation" in supplied ? supplied.computation : captured.computation;
			retainEntry({ type: after.isDirectory() ? "directory" : after.isFile() ? "file" : "special", computation, metadata: { dependency, computation, size } });
			return { value: { type: specialFileType(after), mode: Number(after.mode), resolved: identity, size },
				stamp: digest([statStamp(after), identity]), bytesRead: 0, filesRead: 0 };
		}
		if (info.isFile()) {
			const key = JSON.stringify([filesystemPathKey(target), identity, statStamp(info), String(info.uid), String(info.gid)]), existing = files.get(key);
			const staged = async (result: Promise<FingerprintResult & { readonly blob?: string }>) => {
				const { blob, ...fingerprint } = await result;
				if (blob) treeObjects?.set(target, { mode: Number(info.mode), blob });
				return fingerprint;
			};
			if (existing) return { ...await staged(existing), bytesRead: 0, filesRead: 0 };
			const pending = (async () => {
				// Join concurrent reads of this identity; a settled capture stands for a later read only below, by the file's identity.
				const provided = suppliedContent;
				const chunked = !provided && view?.contents;
				const reserved = chunked ? 96 + Math.ceil(Number(info.size) / (1024 * 1024)) * 112 : provided ? contentMemory(provided) : Number(info.size);
				const retain = !view?.verifyingInputs && (view?.reserve(reserved) ?? false);
				if ((provided || chunked) && !retain) throw new Error("resource_snapshot_budget_exceeded");
				const bytes = provided && (provided instanceof ChunkedContent ? provided : Buffer.from(provided));
				// Supplied bytes own data, never a host observation window; adoption validates them exactly.
				// A file unchanged since an earlier read of it, by the racy-git rule, needs no read unless its bytes are retained.
				const gitBlob = scope === "tree_content" && treeObjects !== undefined;
				const streamed = chunked && await chunked.capture(onChunk => fingerprintIO(() => captureStableFile(target, chunked.limits.maxLogicalBytes, false,
					{ stat: info, realPath: realTarget, gitBlob, view: filesystem, onChunk })));
				const content: StableFilesystemCapture = streamed ? streamed.capture : bytes ? { content: bytes instanceof ChunkedContent ? undefined : bytes, bytesRead: bytes.length, hash: contentDigest(bytes).slice(7), stat: info, realPath: realTarget,
					computation: supplied && "computation" in supplied ? supplied.computation : undefined }
					: !retain && cachedCapture(info, realTarget, gitBlob) || await fingerprintIO(() => captureStableFile(target, retain ? Number(info.size) : undefined, retain, {
						stat: info, realPath: realTarget, retainObject: retain && view!.canRetainObject, gitBlob, view: filesystem }));
				assertInside(realRoot, content.realPath);
				retainEntry({ type: "file", content: streamed ? streamed.content : bytes ?? content.content, computation: content.computation,
					version: view?.verifyingInputs ? { hash: content.hash, size: content.bytesRead } : undefined,
					object: content.object ?? (bytes instanceof Buffer && retain && view!.canRetainObject ? null : undefined),
					realPath: content.realPath }, retain ? reserved : 0);
				return {
					...fingerprintFile(content),
					bytesRead: bytes || content.shared ? 0 : content.bytesRead,
					filesRead: bytes || content.shared ? 0 : 1,
					...(content.blob ? { blob: content.blob } : {}),
				};
			})().finally(() => files.delete(key));
			files.set(key, pending);
			return staged(pending);
		}
		if (!info.isDirectory() || scope === "content") throw new Error(`unsupported_resource_type:${specialFileType(info)}:${target}`);
		if (supplied && "names" in supplied && supplied.names) {
			retainEntry({ type: "directory", entries: supplied.names, computation: supplied.computation, metadata: { dependency, computation: supplied.computation },
				...directoryObjectSlot(supplied.names, view) });
			return { ...fingerprintDirectory(info, realTarget, supplied.names), bytesRead: 0, filesRead: 0 };
		}
		const directory = await captureFilesystemDirectory(target, { view: filesystem, stat: info, io: fingerprintIO, visit: async entries => {
			const selected = excludes.size && (scope === "tree_content" || scope === "tree_entries") ? entries.filter((entry) => !excludes.has(entry.name)) : entries;
			const descendants = new Set(ancestors).add(identity);
			const children = scope === "names" ? [] : await mapFilesystem([...selected].sort((left, right) => left.name.localeCompare(right.name)), async (entry) => {
				const child = await fingerprintPath(path.join(target, entry.name), scope, dependency, descendants, scope !== "entries", realTarget);
				return { name: entry.name, ...child };
			});
			return { names: selected.map(entry => entry.name), children };
		} });
		const { names, children } = directory.value!;
		retainEntry({ type: "directory", entries: names, computation: directory.computation, ...directoryObjectSlot(names, view) });
		return {
			...fingerprintDirectory(directory.info, realTarget, names, children),
			bytesRead: children.reduce((total, child) => total + child.bytesRead, 0),
			filesRead: children.reduce((total, child) => total + child.filesRead, 0),
		};
	}
}

async function fingerprintBinding(dependency: ResourceDependency, filesystem: FilesystemReadView) {
	let stamp: string;
	try {
		const { info, link } = await fingerprintIO(() => captureFilesystemEntry(dependency.path, "identity", filesystem));
		stamp = digest([statStamp(info), link]);
	} catch (error) { if (!missingResource(error)) throw error; stamp = errorCode(error); }
	return { ...dependency, fingerprint: "binding", stamp, bytesRead: 0, filesRead: 0 };
}

function specialFileType(value: FilesystemTypeEvidence) {
	const kind = filesystemEntryType(value);
	return kind === "char" ? "character_device" : kind === "block" ? "block_device" : kind;
}

function missingResourceResolver(realRoot: string, filesystem: FilesystemReadView) {
	// Negative queries commonly share ancestors. Join only concurrent reads in this proof batch;
	// settled observations are never cached for another query or Actor validation.
	type Resolution = { readonly realPath: string; readonly fingerprint: string };
	const pending = new Map<string, Promise<Resolution>>();
	const resolve = (target: string): Promise<Resolution> => {
		const current = path.resolve(target), existing = pending.get(current);
		if (existing) return existing;
		const task = (async () => {
			try {
				const real = await fingerprintIO(() => filesystem.realpath(current));
				const stat = await fingerprintIO(() => filesystem.stat(current));
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

function digest(value: unknown): string { return hash("sha256", JSON.stringify(value)); }

function assertInside(realRoot: string, target: string): void {
	if (!containsFilesystemPath(realRoot, target)) throw new Error(`resource_symlink_escapes_workspace:${target}`);
}

function validation(reason?: string, mode: ResourceValidationMetrics["mode"] = "exact",
	observed: ReadonlyArray<Pick<ResourceValidationMetrics, "bytesRead" | "filesRead">> = []): ResourceVersionValidation {
	return {
		expired: reason !== undefined, ...(reason === undefined ? {} : { reason }),
		mode,
		bytesRead: observed.reduce((total, entry) => total + entry.bytesRead, 0),
		filesRead: observed.reduce((total, entry) => total + entry.filesRead, 0),
	};
}

function sameValues<Value>(left: ReadonlyArray<Value>, right: ReadonlyArray<Value>) {
	return left.length === right.length && left.every((value, index) => value === right[index]);
}


function errorCode(error: unknown) { return error && typeof error === "object" && "code" in error ? String(error.code) : "unknown"; }

function missingResource(error: unknown): boolean { const code = errorCode(error); return code === "ENOENT" || code === "ENOTDIR"; }

function releaseOnce<Result>(release: () => Result): () => Result | undefined {
	let released = false;
	let result: Result | undefined;
	return () => { if (!released) { released = true; result = release(); } return result; };
}

const priorityIO = new AsyncLocalStorage<true>();
export const fingerprintIO = (() => {
	let active = 0;
	const waiting: Array<() => void> = [], priority: Array<() => void> = [];
	return async <Value>(task: () => Promise<Value>): Promise<Value> => {
		if (active < FILESYSTEM_CONCURRENCY) active++;
		else await new Promise<void>((resolve) => (priorityIO.getStore() ? priority : waiting).push(resolve));
		try { return await task(); }
		finally { const next = priority.shift() ?? waiting.shift(); if (next) next(); else active--; }
	};
})();

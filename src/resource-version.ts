import { AsyncLocalStorage } from "node:async_hooks";
import { hash } from "node:crypto";
import {
	type BigIntStats,
	closeSync,
	constants,
	fstatSync,
	type FSWatcher,
	readlinkSync,
	watch,
} from "node:fs";
import path from "node:path";
import { setImmediate as watcherTurn } from "node:timers/promises";
import { types } from "node:util";
import { deserialize, serialize } from "node:v8";
import {
	type ActionKey,
	type ActionSemanticsRegistry,
	PI_ACTION_SEMANTICS,
	type ResourceDependencyScope,
} from "./action-semantics.ts";
import { speculativeConfig } from "./config-schema.ts";
import {
	type ArtifactReference,
	ChunkedContent,
	type ChunkedContentManifest,
	type ContentChunkLimits,
	ContentChunkPool,
	contentDigest,
	contentMemory,
	type ContentVersion,
	DEFAULT_CONTENT_LIMITS,
	parseContentManifest,
	readContent,
	validateArtifact,
} from "./content-chunks.ts";
import {
	cachedCapture,
	captureFilesystemDirectory,
	captureFilesystemEntry,
	captureStableFile,
	filesystemEntryType,
	type FilesystemObjectHandle,
	type FilesystemReadView,
	type FilesystemTypeEvidence,
	mapFilesystem,
	openFilesystemReadView,
	readFilesystemDirectory,
	resolveFilesystemDestination,
	sameFilesystemIdentity,
	type StableFilesystemCapture,
	walkFilesystemPath,
	withFilesystemReadView,
} from "./filesystem-evidence.ts";
import {
	containsFilesystemPath,
	filesystemPathKey,
	isSafeCount,
	isSha256Digest,
	relativeFilesystemPath,
	resourceBudgetError,
	digestObject,
	type Sha256Digest,
	releaseOnce,
} from "./primitives.ts";
import { ensureNativeFilesystem, nativeFilesystem } from "./process-startup.mjs";
import type { ProvenanceCertificateStore, StoredInputVersion } from "./reuse-store.ts";
import { WorkspaceStructureCapture } from "./workspace-sandbox.ts";
import { RuntimeLifecycleLane, SharedAbortScope, waitForCompletion } from "./runtime-lifecycle.ts";
import type { ValidationMetrics } from "./settlement.ts";
import {
	normalizeTimelineComputation,
	subcomputeTiming,
	type SerializedTimelineComputation,
	TimelineInterval,
} from "./task-timing.ts";
export type ToolFilesystemStat = {
	isDirectory: () => boolean;
	size?: number;
	type?: "file" | "directory" | "symlink" | "special";
	link?: string;
	realPath?: string;
};
/** Filesystem capabilities supplied by an execution world, never ambient host defaults. */
export interface ToolFilesystemOperations {
	readonly computeStep?: <Value>(
		request: ComputeStepRequest,
		calculate: (view: ToolFilesystemOperations, signal: AbortSignal) => Value | Promise<Value>,
		signal?: AbortSignal,
	) => Promise<Value>;
	readonly readFile: (target: string, maxBytes?: number) => Promise<Buffer>;
	readonly readFileRange?: (target: string, offset: number, length: number) => Promise<Buffer>;
	readonly access: (target: string, writable?: boolean) => Promise<void>;
	readonly exists?: (target: string) => boolean | Promise<boolean>;
	/** `entry` does not follow the final link; includeSize=false returns only type/path fields and their evidence. */
	readonly stat?: (
		target: string,
		fields?: "type" | "entry",
		includeSize?: boolean,
	) => ToolFilesystemStat | Promise<ToolFilesystemStat>;
	readonly readdir?: (target: string) => string[] | Promise<string[]>;
	readonly writeFile?: (target: string, content: string) => Promise<void>;
	readonly mkdir?: (target: string) => Promise<void>;
	/** Build is pure and reads only view inputs; binding/key cover other inputs and values stay immutable. Resource is a hint; signal owns this consumer. */
	readonly prepare?: <Value, Result>(
		binding: object,
		key: string,
		build: (
			view: ToolFilesystemOperations,
			signal: AbortSignal,
		) => Promise<{
			readonly value: Value;
			readonly bytes: number;
			readonly dispose: () => void | Promise<void>;
			readonly cacheable?: boolean;
		}>,
		consume: (value: Value, computation?: TimelineInterval) => Promise<Result>,
		resource?: string,
		signal?: AbortSignal,
	) => Promise<Result>;
}
export interface ComputeStepRequest {
	readonly namespace: string;
	readonly key: unknown;
	readonly inputResources?: readonly ResourceDependency[];
	readonly envKeys?: readonly string[];
	readonly environment?: Readonly<Record<string, string | undefined>>;
	/** Include captured file bytes in protocol misses; local computeStep always reads through its view. */
	readonly includeInputContent?: boolean;
}
/** Observe the declared scope without materializing file contents in the caller. */
export async function observeStepInput(inputs: ToolFilesystemOperations, resource: ResourceDependency): Promise<void> {
	if (resource.scope === "content") await inputs.readFile(resource.path, 0);
	else if (resource.scope === "names" || resource.scope === "entries")
		await inputs.readdir!(resource.path);
	else if (resource.scope === "exists") await inputs.exists!(resource.path);
	else if (resource.scope === "type" || resource.scope === "stat" || resource.scope === "entry")
		await inputs.stat!(
			resource.path,
			resource.scope === "entry" ? "entry" : "type",
			resource.scope !== "type",
		);
	else throw new Error("Unsupported explicit input resource scope: " + resource.scope);
}
/** The caller versions every dependency other than the explicitly captured resources and environment. */
export async function computeStepInView<Value>(
	view: ToolFilesystemOperations,
	request: ComputeStepRequest,
	calculate: (view: ToolFilesystemOperations, signal: AbortSignal) => Value | Promise<Value>,
	signal: AbortSignal = new AbortController().signal,
): Promise<Value> {
	if (!request.namespace.trim()) throw new Error("A compute step requires a namespace");
	signal.throwIfAborted();
	const environment = request.environment ?? process.env;
	const selectedEnvironment = () => (request.envKeys ?? []).map((key) => [key, environment[key]]);
	const input = snapshotPreparation([request.key, request.inputResources ?? [], selectedEnvironment()]);
	if (!input) throw new Error("compute_step_input_budget_exceeded");
	const key = hash("sha256", input.data);
	const binding = createSerializablePreparationBinding({ namespace: request.namespace }, () => true);
	let built = false;
	const build = async (inputs: ToolFilesystemOperations, sharedSignal: AbortSignal) => {
		built = true;
		subcomputeTiming.getStore()?.(request.namespace, false);
		for (const resource of request.inputResources ?? []) await observeStepInput(inputs, resource);
		const result = await calculate(inputs, sharedSignal);
		sharedSignal.throwIfAborted();
		if (!serialize([request.key, request.inputResources ?? [], selectedEnvironment()]).equals(input.data))
			throw new Error("compute_step_contract_changed");
		const owned = snapshotPreparation(result);
		if (!owned) throw new Error("compute_step_result_budget_exceeded");
		return { value: owned.data, bytes: owned.data.length, dispose: () => {} };
	};
	const consume = async (bytes: Buffer, computation?: TimelineInterval): Promise<Value> => {
		const result = deserialize(Buffer.from(bytes)) as Value;
		if (!built) subcomputeTiming.getStore()?.(request.namespace, true, computation);
		return result;
	};
	if (view.prepare)
		return view.prepare(binding, key, build, consume, request.inputResources?.[0]?.path, signal);
	const measured = await TimelineInterval.measure(() => build(view, signal));
	TimelineInterval.own(measured.computation);
	return consume(measured.output.value, measured.computation);
}
export type ResourceDependency = {
	readonly path: string;
	readonly scope:
		ResourceDependencyScope | "stat" | "type" | "entry" | "exists" | "names" | "binding" | "resolution";
};
export type ResourceObservation = ResourceDependency & {
	readonly fingerprint: string;
	readonly stamp?: string;
	/** Named edges captured in this tree; links exceeding paths indicate an open namespace. */
	readonly aliases?: readonly {
		readonly paths: readonly string[];
		readonly links: number;
	}[];
};
/** Supplied bytes and poststates remain proposals until exact adoption validation. */
export type ResourceInput =
	| ContentVersion
	| {
			readonly content: ContentVersion;
			readonly computation: TimelineInterval;
	  }
	| {
			readonly names?: readonly string[];
			readonly computation?: TimelineInterval;
	  }
	| {
			readonly type: "file" | "directory";
			readonly computation?: TimelineInterval;
	  }
	| null;
const isInputContent = (input: ResourceInput | undefined): input is ContentVersion =>
	input instanceof Uint8Array || input instanceof ChunkedContent;
const inputContent = (input: ResourceInput | undefined) =>
	isInputContent(input) ? input : input && "content" in input ? input.content : undefined;
export type ResourceValidationMetrics = ValidationMetrics;
export type ResourceVersionValidation = ResourceValidationMetrics & {
	readonly expired: boolean;
	readonly reason?: string;
	readonly changed?: readonly string[];
};
export type ResourceChangeSet = {
	readonly uncertain: boolean;
	readonly paths: ReadonlyArray<string>;
};
export interface ResourceInputPersistence {
	/** Resolve a local file token using the lstat already taken in the current filesystem view. */
	readonly inputs?: (
		dependency: ResourceDependency,
		stat: BigIntStats,
	) => Promise<ResourceVersionToken | undefined>;
	readonly store: ProvenanceCertificateStore;
	readonly limits?: Partial<ContentChunkLimits>;
	/** Explicit internal state: this cache and its aliases are outside the source-unchanged guarantee. */
	readonly internalCache?: boolean;
	/** Ephemeral shared lease; never serialized or restored as execution authority. */
	readonly contents?: ContentChunkPool;
	readonly memory?: ResourceInputMemory;
}
/** Shared retention accounting permits nested queries without serializing their consumers. */
class ResourceInputMemory {
	private retained = 0;
	readonly limit: number;
	constructor(limit: number) {
		this.limit = limit;
	}
	get bytes(): number {
		return this.retained;
	}
	get remainingBytes(): number {
		return this.limit - this.retained;
	}
	change(bytes: number): void {
		if (bytes > this.remainingBytes) throw resourceBudgetError("resource_snapshot_budget_exceeded");
		if (this.retained + bytes < 0) throw new Error("resource_snapshot_budget_underflow");
		this.retained += bytes;
	}
}
const observationBytes = (key: string, entry: ResourceObservation) => serialize([key, entry]).byteLength;
class ResourceObservations extends Map<string, ResourceObservation> {
	private readonly memory?: ResourceInputMemory;
	private bytes = 0;
	constructor(memory?: ResourceInputMemory, entries?: Iterable<readonly [string, ResourceObservation]>) {
		super();
		this.memory = memory;
		try {
			for (const [key, value] of entries ?? []) this.set(key, value);
		} catch (error) {
			this.clear();
			throw error;
		}
	}
	override set(key: string, value: ResourceObservation): this {
		const prior = this.get(key);
		const bytes = observationBytes(key, value) - (prior ? observationBytes(key, prior) : 0);
		this.memory?.change(bytes);
		this.bytes += bytes;
		return super.set(key, value);
	}
	override delete(key: string): boolean {
		const prior = this.get(key);
		const bytes = prior ? observationBytes(key, prior) : 0;
		this.memory?.change(-bytes);
		this.bytes -= bytes;
		return super.delete(key);
	}
	override clear(): void {
		this.memory?.change(-this.bytes);
		this.bytes = 0;
		super.clear();
	}
}
/** A captured tree entry as Git stages it: a regular file's object id, or a symbolic link's target. */
export type TreeObject =
	| {
			readonly mode: number;
			readonly blob: string;
			readonly link?: never;
	  }
	| {
			readonly mode: number;
			readonly link: string;
			readonly blob?: never;
	  };
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
	| {
			readonly type: "file";
			readonly content?: Buffer | ChunkedContent;
			readonly version?: {
				readonly hash: string;
				readonly size: number;
			};
	  }
	| {
			readonly type: "directory";
			readonly entries?: readonly string[];
	  }
	| {
			readonly type: "alias";
			readonly target?: string;
			readonly link: string;
	  }
	| {
			readonly type: "special" | "missing";
	  }
) & {
	readonly realPath?: string;
	readonly dependency?: string;
	readonly object?: StableFilesystemCapture["object"] | null;
	readonly objectBytes?: number;
	readonly computation?: TimelineInterval;
	readonly metadata?: {
		readonly dependency?: string;
		readonly computation?: TimelineInterval;
		readonly size?: number;
	};
};
type ResourceInputSource = {
	readonly view: ResourceReadView;
	/** Selection is side-effect free when commit is false; only accepted inputs become dependencies. */
	readonly observed: (
		dependencies: ReadonlySet<string> | undefined,
		commit?: boolean,
	) => readonly ResourceVersionToken[] | void;
};
type PersistentResourceEntry = readonly [
	string,
	Omit<CapturedResource, "computation" | "object" | "objectBytes" | "metadata" | "content"> & {
		readonly content?: ChunkedContentManifest;
		readonly computation?: SerializedTimelineComputation;
		readonly metadata?: {
			readonly dependency?: string;
			readonly size?: number;
		};
		readonly entries?: readonly string[];
		readonly target?: string;
		readonly link?: string;
	},
];
type PersistentPreparation = {
	readonly binding: Sha256Digest;
	readonly key: string;
	readonly resource?: string;
	readonly dependencies: readonly string[];
	readonly value: ArtifactReference;
	readonly computation?: SerializedTimelineComputation;
};
type PersistentResourcePayload = {
	readonly root: string;
	readonly physicalRoot: string;
	readonly observations: readonly ResourceObservation[];
	readonly entries: readonly PersistentResourceEntry[];
	readonly preparations: readonly PersistentPreparation[];
};
const preparationBindings = new WeakMap<
	object,
	{
		readonly id: Sha256Digest;
		readonly accepts: (value: unknown) => boolean;
	}
>();
/** Identity is an explicit implementation contract supplied by the adapter. */
export function createSerializablePreparationBinding(
	implementation: unknown,
	accepts: (value: unknown) => boolean,
): object {
	const binding = Object.freeze({});
	preparationBindings.set(binding, {
		accepts,
		id: digestObject(implementation),
	});
	return binding;
}
/** Validate own data properties, then own the exact serialized representation. */
export function snapshotPreparation(value: unknown):
	| {
			readonly value: unknown;
			readonly data: Buffer;
	  }
	| undefined {
	const limits = speculativeConfig().subcomputation;
	let nodes = 0;
	let byteLength = 0;
	const seen = new WeakSet<object>();
	const budget = Symbol();
	const visit = (item: unknown, depth: number): void => {
		if (++nodes > limits.maxStepObjectNodes || depth > limits.maxStepObjectDepth) throw budget;
		if (typeof item === "string") byteLength += Buffer.byteLength(item);
		if (byteLength > limits.maxStepResultBytes) throw budget;
		if (item === null || ["undefined", "boolean", "number", "string", "bigint"].includes(typeof item)) return;
		if (typeof item !== "object" || types.isProxy(item)) throw new Error("resource_preparation_data_invalid");
		if (seen.has(item)) throw new Error("resource_preparation_data_cycle");
		seen.add(item);
		if (item instanceof Uint8Array || item instanceof ArrayBuffer) {
			byteLength += item.byteLength;
			if (byteLength > limits.maxStepResultBytes) throw budget;
			return;
		}
		if (Object.getPrototypeOf(item) !== (Array.isArray(item) ? Array.prototype : Object.prototype))
			throw new Error("resource_preparation_data_invalid");
		for (const key of Reflect.ownKeys(item)) {
			if (Array.isArray(item) && key === "length") continue;
			const property = Object.getOwnPropertyDescriptor(item, key)!;
			if (typeof key !== "string" || !property.enumerable || !("value" in property))
				throw new Error("resource_preparation_data_invalid");
			byteLength += Buffer.byteLength(key);
			visit(property.value, depth + 1);
		}
		seen.delete(item);
	};
	try {
		visit(value, 0);
		const data = serialize(value);
		return data.byteLength <= limits.maxStepResultBytes ? { value: deserialize(data), data } : undefined;
	} catch (error) {
		if (error === budget) return undefined;
		throw error;
	}
}
export const preparationDataBytes = (size: number) => size;
const preparationMetadataBytes = (
	key: string,
	resource: string | undefined,
	dependencies: Iterable<string>,
) => serialize([key, resource, [...dependencies]]).byteLength;
function parsePersistentResourcePayload(value: unknown, root: string): PersistentResourcePayload {
	const payload = value as PersistentResourcePayload;
	const invalid = () => {
		throw new Error("invalid persistent input closure");
	};
	const absolute = (value: unknown): value is string =>
		typeof value === "string" &&
		!value.includes("\0") &&
		path.isAbsolute(value) &&
		(value === path.resolve(value) || value === filesystemPathKey(value));
	if (
		!payload ||
		payload.root !== root ||
		!absolute(payload.physicalRoot) ||
		!Array.isArray(payload.observations) ||
		!payload.observations.length ||
		payload.observations.length > speculativeConfig().subcomputation.maxStepObjectNodes ||
		!Array.isArray(payload.entries) ||
		payload.entries.length > speculativeConfig().subcomputation.maxStepObjectNodes
	)
		return invalid();
	const scopes = new Set([
		"content",
		"tree_content",
		"entries",
		"tree_entries",
		"stat",
		"type",
		"entry",
		"exists",
		"names",
		"resolution",
	]);
	const keys = new Set<string>();
	for (const observation of payload.observations) {
		if (
			!observation ||
			!absolute(observation.path) ||
			!containsFilesystemPath(root, observation.path) ||
			!scopes.has(observation.scope) ||
			typeof observation.fingerprint !== "string" ||
			!/^[0-9a-f]{64}$/.test(observation.fingerprint) ||
			(observation.stamp !== undefined && typeof observation.stamp !== "string")
		)
			return invalid();
		if (
			observation.aliases &&
			(!Array.isArray(observation.aliases) ||
				observation.aliases.some(
					(group: { links: number; paths: unknown[] }) =>
						!group ||
						!isSafeCount(group.links, 1) ||
						!Array.isArray(group.paths) ||
						group.paths.some(
							(target: unknown) => !absolute(target) || !containsFilesystemPath(payload.physicalRoot, target),
						),
				))
		)
			return invalid();
		const key = dependencyKey(observation);
		if (keys.has(key)) return invalid();
		keys.add(key);
	}
	if (!keys.has(dependencyKey({ path: root, scope: "resolution" }))) return invalid();
	const targets = new Set<string>();
	const entries = payload.entries.map((item): PersistentResourceEntry => {
		if (!Array.isArray(item) || item.length !== 2) return invalid();
		const [target, entry] = item;
		if (
			!absolute(target) ||
			!containsFilesystemPath(root, target) ||
			targets.has(target) ||
			!entry ||
			!["file", "directory", "alias", "special", "missing"].includes(entry.type) ||
			!entry.dependency ||
			!keys.has(entry.dependency) ||
			(entry.realPath !== undefined &&
				(!absolute(entry.realPath) || !containsFilesystemPath(payload.physicalRoot, entry.realPath))) ||
			(entry.metadata &&
				((entry.metadata.dependency !== undefined && !keys.has(entry.metadata.dependency)) ||
					(entry.metadata.size !== undefined && !isSafeCount(entry.metadata.size))))
		)
			return invalid();
		targets.add(target);
		if (
			(entry.content && entry.type !== "file") ||
			(entry.computation !== undefined && (!entry.content || !normalizeTimelineComputation(entry.computation))) ||
			(entry.entries && entry.type !== "directory") ||
			(entry.type === "alias" &&
				(typeof entry.link !== "string" ||
					entry.link.includes("\0") ||
					(entry.target !== undefined &&
						(!absolute(entry.target) || !containsFilesystemPath(root, entry.target)))))
		)
			return invalid();
		if (
			entry.entries &&
			(!Array.isArray(entry.entries) ||
				new Set(entry.entries).size !== entry.entries.length ||
				entry.entries.some(
					(name: unknown) =>
						typeof name !== "string" || !name || name === "." || name === ".." || /[\0/]/.test(name),
				))
		)
			return invalid();
		return [
			target,
			{
				type: entry.type,
				realPath: entry.realPath,
				dependency: entry.dependency,
				metadata: entry.metadata && {
					dependency: entry.metadata.dependency,
					size: entry.metadata.size,
				},
				...(entry.content ? { content: parseContentManifest(entry.content), computation: entry.computation } : {}),
				...(entry.type === "directory"
					? { entries: entry.entries && [...entry.entries] }
					: entry.type === "alias"
						? { link: entry.link, target: entry.target }
						: {}),
			},
		];
	});
	const preparations = payload.preparations ?? [];
	const identities = new Set<string>();
	if (
		!Array.isArray(preparations) ||
		preparations.length > speculativeConfig().subcomputation.maxStepObjectNodes
	)
		return invalid();
	for (const preparation of preparations) {
		if (
			!preparation ||
			!isSha256Digest(preparation.binding) ||
			typeof preparation.key !== "string" ||
			preparation.key.length > speculativeConfig().subcomputation.maxStepObjectNodes ||
			(preparation.resource !== undefined &&
				(!absolute(preparation.resource) || !containsFilesystemPath(root, preparation.resource))) ||
			!Array.isArray(preparation.dependencies) ||
			!preparation.dependencies.length ||
			preparation.dependencies.length > keys.size ||
			new Set(preparation.dependencies).size !== preparation.dependencies.length ||
			preparation.dependencies.some((key: unknown) => typeof key !== "string" || !keys.has(key)) ||
			!preparation.dependencies.includes(dependencyKey({ path: root, scope: "resolution" }))
		)
			return invalid();
		validateArtifact(preparation.value);
		if (preparation.computation && !normalizeTimelineComputation(preparation.computation)) return invalid();
		if (preparation.value.size > speculativeConfig().subcomputation.maxStepResultBytes) return invalid();
		const identity = JSON.stringify([preparation.binding, preparation.key]);
		if (identities.has(identity)) return invalid();
		identities.add(identity);
	}
	return {
		root,
		physicalRoot: payload.physicalRoot,
		observations: payload.observations,
		entries,
		preparations,
	};
}
type PreparationLookup = { readonly binding?: string; readonly key: string };
type ResourceInputLookup = (
	target?: string,
	preparation?: PreparationLookup,
) => Iterable<ResourceInputSource> | AsyncIterable<ResourceInputSource>;
type PreparedSharingState =
	| { readonly kind: "building"; requiresProofs: boolean; readonly waiters: Set<() => void> }
	| { readonly kind: "settled"; readonly shareable: boolean };
type PreparedResource = {
	readonly cancellation: SharedAbortScope;
	readonly waiting: Set<PreparedResource>;
	computation?: TimelineInterval;
	value?: unknown;
	data?: Buffer;
	readonly dispose: () => void | Promise<void>;
	readonly resource?: string;
	dependencies?: ReadonlySet<string>;
	readonly boundary?: { readonly root: string; readonly physicalRoot: string };
	readonly origin: ResourceReadView;
	destination: ResourceReadView;
	ready?: Promise<void>;
	sharing: PreparedSharingState;
	borrowers: number;
	phase: "building" | "retained" | "revoked";
	proofs?: ResourceVersionToken[];
};
function updatePreparedSharing(
	resource: PreparedResource,
	next: "proof_acceptors" | { readonly kind: "settled"; readonly shareable: boolean },
): void {
	if (resource.sharing.kind !== "building") return;
	const { waiters } = resource.sharing;
	if (next === "proof_acceptors") resource.sharing.requiresProofs = true;
	else resource.sharing = next;
	for (const wake of [...waiters]) wake();
	if (next !== "proof_acceptors") waiters.clear();
}
async function waitForPreparedSharing(
	resource: PreparedResource,
	building: boolean,
	owner: ResourceReadView,
	acceptsProofs: boolean,
): Promise<boolean> {
	if (building) return resource.ready!.then(() => true);
	if (resource.phase === "retained" && resource.destination === owner) return true;
	const current = () => resource.sharing;
	const initial = current();
	if (initial.kind === "building") {
		const { waiters } = initial;
		await new Promise<void>((resolve) => {
			const wake = () => {
				const state = current();
				if (state.kind === "settled" || (!acceptsProofs && state.requiresProofs)) {
					waiters.delete(wake);
					resolve();
				}
			};
			waiters.add(wake);
			wake();
		});
		const after = current();
		if (after.kind === "building" || !after.shareable) return false;
		await resource.ready;
	}
	const settled = current();
	return settled.kind === "settled" && settled.shareable && (!resource.proofs?.length || acceptsProofs);
}
function preparationWaitsFor(resource: PreparedResource, target: PreparedResource): boolean {
	const pending = new Set([resource]);
	for (const current of pending) {
		if (current === target) return true;
		for (const next of current.waiting) pending.add(next);
	}
	return false;
}
type ResourceViewMode = "capturing" | "sealed" | "read_only" | "read_through";
type ResourceEvaluationContext = {
	dependencies?: Set<string>;
	lookup?: ResourceInputLookup;
	missing?: () => Promise<ResourceInputSource>;
	observed?: ResourceInputSource["observed"];
	validateInputs?: (proofs: readonly ResourceVersionToken[]) => Promise<void>;
	acceptProofs?: (proofs: readonly ResourceVersionToken[]) => readonly ResourceVersionToken[];
	onForeignInputs?: (transferable: boolean) => void;
	collectProofs?: Map<ResourceVersionToken["observations"], ResourceVersionToken>;
	preparing?: PreparedResource;
	foreignInputs: boolean;
};
type ResourceViewOptions = Partial<
	Pick<ResourceReadView, "contents" | "verifyingInputs"> & {
		readonly mode: ResourceViewMode;
		readonly owner: ResourceReadView;
		readonly entries: ResourceReadView["entries"];
		readonly prepared: ResourceReadView["prepared"];
		readonly boundary: ResourceReadView["boundary"];
		readonly load: ResourceReadView["load"];
		readonly observeObject: ResourceReadView["observeObject"];
		readonly memory: ResourceInputMemory;
		readonly evaluation: ResourceEvaluationContext;
	}
>;
/** Token-owned input data, not a filesystem cache or authority to execute host functions. */
export class ResourceReadView {
	computeStep = <Value>(
		request: ComputeStepRequest,
		calculate: (view: ToolFilesystemOperations, signal: AbortSignal) => Value | Promise<Value>,
		signal?: AbortSignal,
	): Promise<Value> => computeStepInView(this, request, calculate, signal);
	private readonly entries: Map<
		string,
		CapturedResource & {
			readonly bytes: number;
		}
	>;
	private readonly owner?: ResourceReadView;
	private failure?: Error;
	private allocatedBytes = 0;
	private readonly memory?: ResourceInputMemory;
	private get capturedBytes(): number {
		return this.allocatedBytes;
	}
	private set capturedBytes(bytes: number) {
		this.memory?.change(bytes - this.allocatedBytes);
		this.allocatedBytes = bytes;
	}
	private objectCount = 0;
	private inputEpoch = 0;
	private mode: ResourceViewMode;
	private get sealed() {
		return this.mode !== "capturing";
	}
	private pending?: Promise<void>;
	private readonly evaluation?: ResourceEvaluationContext;
	private prepared?: {
		readonly lifetime: RuntimeLifecycleLane;
		readonly bindings: Map<object | Sha256Digest, Map<string, PreparedResource>>;
	};
	private boundary?: {
		readonly root: string;
		readonly physicalRoot: string;
		readonly dependency?: string;
	};
	private readonly maxBytes: number;
	private readonly load?: (dependency: ResourceDependency) => Promise<void>;
	private readonly observeObject?: (target: string, capture: StableFilesystemCapture) => void;
	readonly contents?: ContentChunkPool;
	private readonly contentOwners = new Set<ContentChunkPool>();
	readonly verifyingInputs: boolean;
	constructor(maxBytes: number, options: ResourceViewOptions = {}) {
		if (!Number.isFinite(maxBytes) || maxBytes < 0) throw new Error("resource_snapshot_budget_invalid");
		this.maxBytes = maxBytes;
		this.mode = options.mode ?? "capturing";
		this.owner = options.owner;
		this.entries = options.entries ?? new Map();
		this.prepared = options.prepared;
		this.memory = options.memory;
		this.load = options.load;
		this.boundary = options.boundary;
		this.observeObject = options.observeObject;
		this.contents = options.contents;
		this.verifyingInputs = options.verifyingInputs ?? false;
		this.evaluation = options.evaluation;
		if (this.contents) this.retainContentPool(this.contents);
	}
	get bytes(): number {
		return this.capturedBytes;
	}
	get remainingBytes(): number {
		return Math.max(
			0,
			Math.min(this.maxBytes - this.capturedBytes, this.memory?.remainingBytes ?? this.maxBytes),
		);
	}
	get hasPreparedInputs(): boolean {
		return [...(this.prepared?.bindings.values() ?? [])].some((entries) =>
			[...entries.values()].some((entry) => entry.phase === "retained" && entry.destination.retained),
		);
	}
	get canRetainObject(): boolean {
		return (
			!this.verifyingInputs &&
			process.platform === "linux" &&
			this.objectCount < speculativeConfig().filesystem.maxPinnedObjects
		);
	}
	get retained(): boolean {
		return this.failure === undefined && this.owner?.retained !== false;
	}
	get resources() {
		this.assertComplete(true);
		const resources = [...this.entries].map(([path, entry]) => ({
			path,
			descendants: entry.type === "alias" && entry.target !== undefined,
		}));
		for (const entries of this.prepared?.bindings.values() ?? [])
			for (const cached of entries.values()) {
				if (cached.phase === "retained" && cached.resource && !this.entries.has(cached.resource))
					resources.push({ path: cached.resource, descendants: false });
			}
		return resources;
	}
	/** Read completed entries without allowing a borrower to load or seal its active owner. */
	readOnly(load = false, memory = this.memory, evaluation?: ResourceEvaluationContext): ResourceReadView {
		this.assertComplete();
		return new ResourceReadView(0, {
			mode: load ? "read_through" : "read_only",
			owner: this,
			entries: this.entries,
			boundary: this.boundary,
			prepared: this.prepared,
			memory,
			evaluation,
		});
	}
	/** Keep proven path metadata; payloads and preparations remain with their original owner. */
	retainMetadata(
		observations: ReadonlyMap<string, ResourceObservation>,
		maxBytes: number,
		memory = this.memory,
	): ResourceReadView | undefined {
		this.assertComplete();
		if (maxBytes <= 0) return undefined;
		// The root proof still anchors queries whose retained metadata belongs to another token.
		let retained =
			this.boundary?.dependency && observations.has(this.boundary.dependency)
				? new ResourceReadView(maxBytes, { boundary: this.boundary, memory })
				: undefined;
		for (const observation of observations.values())
			for (let target: string | undefined = filesystemPathKey(observation.path); target;) {
				const entry = this.entries.get(target);
				if (!entry || retained?.entries.has(target)) break;
				const dependency =
					entry.metadata?.dependency && observations.has(entry.metadata.dependency)
						? entry.metadata.dependency
						: entry.dependency;
				if (!dependency || !observations.has(dependency)) break;
				retained ??= new ResourceReadView(maxBytes, { memory });
				retained.capture(target, resourceMetadata(entry, dependency));
				if (!retained.retained) {
					void retained.dispose();
					return undefined;
				}
				target = entry.type === "alias" ? entry.target : undefined;
			}
		retained?.seal();
		return retained;
	}
	/** Revoke data only; old outputs keep their immutable observations for exact validation. */
	invalidate(dependencies: ReadonlySet<string>): readonly string[] {
		const removed: string[] = [];
		if (!dependencies.size) return removed;
		const preparedNames = new Set<string>();
		this.inputEpoch++;
		if (this.boundary?.dependency && dependencies.has(this.boundary.dependency)) this.boundary = undefined;
		for (const [target, entry] of this.entries)
			if (
				!entry.dependency ||
				dependencies.has(entry.dependency) ||
				(entry.metadata?.dependency && dependencies.has(entry.metadata.dependency))
			) {
				this.releaseObject(entry);
				if (entry.object !== undefined) this.objectCount--;
				const dependency = entry.metadata?.dependency;
				if (dependency && !dependencies.has(dependency) && entry.type !== "alias") {
					const bytes =
						Buffer.byteLength(target) +
						Buffer.byteLength(entry.realPath ?? "") +
						Buffer.byteLength(dependency) * 2 +
						64 +
						(entry.metadata?.computation ? 32 : 0);
					this.entries.set(target, {
						...resourceMetadata(entry, dependency),
						bytes,
					});
					this.capturedBytes -= entry.bytes - bytes;
				} else {
					this.entries.delete(target);
					this.capturedBytes -= entry.bytes;
					removed.push(target);
				}
			}
		for (const entries of this.prepared?.bindings.values() ?? [])
			for (const [key, cached] of entries) {
				if (
					!cached.dependencies ||
					[...cached.dependencies].some((key) => dependencies.has(key)) ||
					cached.proofs?.some((proof) => [...proof.observations.keys()].some((key) => dependencies.has(key)))
				) {
					cached.phase = "revoked";
					entries.delete(key);
					if (cached.resource) removed.push(cached.resource);
					if (!cached.borrowers) void this.prepared!.lifetime.release(cached);
				} else if (cached.phase === "retained" && cached.resource) preparedNames.add(cached.resource);
			}
		return [...new Set(removed)].filter((target) => !this.entries.has(target) && !preparedNames.has(target));
	}
	reserve(bytes: number): boolean {
		if (this.sealed) throw new Error("resource_snapshot_not_capturing");
		if (this.failure) {
			if (this.load) throw this.failure;
			return false;
		}
		if (!isSafeCount(bytes)) throw new Error("resource_snapshot_budget_invalid");
		if (bytes > this.remainingBytes) {
			this.failure = resourceBudgetError("resource_snapshot_budget_exceeded");
			for (const entry of this.entries.values()) this.releaseObject(entry);
			this.entries.clear();
			this.objectCount = 0;
			if (this.load) throw this.failure;
			return false;
		}
		this.capturedBytes += bytes;
		return true;
	}
	captureObject(target: string, input: StableFilesystemCapture, dependency?: string): void {
		this.capture(target, {
			...(input.entries
				? {
						type: "directory",
						entries: input.entries,
						objectBytes: input.bytesRead,
					}
				: { type: "file", content: input.content }),
			object: input.object,
			computation: input.computation,
			realPath: input.realPath,
			dependency,
		});
	}
	capture(target: string, entry: CapturedResource, reservedBytes = 0): void {
		const key = filesystemPathKey(target);
		const previous = this.entries.get(key);
		const sameMetadata =
			previous?.type === entry.type && previous?.realPath === entry.realPath && entry.type !== "alias";
		const metadata = /^(entry|type|stat|exists):/.test(entry.dependency ?? "")
			? (entry.metadata ?? {
					dependency: entry.dependency,
					computation: entry.computation,
				})
			: (sameMetadata && previous?.metadata) || entry.metadata;
		// Overlapping scopes enrich one input view; a metadata observation cannot erase its payload.
		const redundant =
			(entry.type === "file" &&
				previous?.type === "file" &&
				entry.content === undefined &&
				entry.version === undefined &&
				(previous.content !== undefined ||
					previous.version !== undefined ||
					resourceSize(entry) === undefined)) ||
			(entry.type === "directory" && previous?.type === "directory" && entry.entries === undefined);
		let retained = redundant && !sameMetadata ? previous! : { ...(redundant ? previous! : entry), metadata };

		const objectBytes = retained.object === undefined ? 0 : (retained.objectBytes ?? 0);
		const billingMetadata = {
			type: retained.type,
			realPath: retained.realPath,
			dependency: retained.dependency,
			metadata: retained.metadata?.dependency,
		};
		let payload: unknown;
		let contentBytes = 0;
		if (retained.type === "file") {
			payload = retained.version;
			contentBytes = contentMemory(retained.content);
		} else if (retained.type === "directory") {
			payload = retained.entries;
		} else if (retained.type === "alias") {
			payload = { target: retained.target, link: retained.link };
		}
		let bytes =
			Buffer.byteLength(key) +
			serialize({ ...billingMetadata, payload }).byteLength +
			contentBytes +
			objectBytes;

		if (
			retained.object !== undefined &&
			(bytes - (previous?.bytes ?? 0) - reservedBytes > this.remainingBytes ||
				this.objectCount - Number(previous?.object !== undefined) >=
					speculativeConfig().filesystem.maxPinnedObjects)
		) {
			this.releaseObject(retained);
			retained = { ...retained, object: undefined, objectBytes: undefined };
			bytes -= objectBytes;
		}
		const additional = bytes - (previous?.bytes ?? 0) - reservedBytes;
		try {
			if (!this.reserve(Math.max(0, additional))) {
				this.releaseObject(entry);
				return;
			}
		} catch (error) {
			this.releaseObject(entry);
			throw error;
		}
		this.capturedBytes += Math.min(0, additional);
		if (previous?.object && retained.object !== previous.object) this.releaseObject(previous);
		this.objectCount += Number(retained.object !== undefined) - Number(previous?.object !== undefined);
		if (retained.type === "file" && retained.content instanceof ChunkedContent)
			this.retainContentPool(retained.content.pool);
		if (retained.object) this.preparations().lifetime.own(retained.object);
		this.entries.set(key, { ...retained, bytes });
	}
	// Windows F_OK observes the final reparse point, including a dangling junction.
	exists = async (target: string): Promise<boolean> =>
		(await this.get(target, process.platform === "win32" ? "exists" : "type")).type !== "missing";
	stat = async (
		target: string,
		fields?: "type" | "entry",
		includeSize = true,
	): Promise<ToolFilesystemStat> => {
		const entry = await this.get(
			target,
			fields === "entry" ? (includeSize ? "entry" : "exists") : includeSize ? "stat" : "type",
		);
		if (entry.type === "missing") return this.unproven(target);
		return {
			isDirectory: () => entry.type === "directory",
			size: includeSize ? resourceSize(entry) : undefined,
			type: entry.type === "alias" ? "symlink" : entry.type,
			...(entry.type === "alias" ? { link: entry.link } : { realPath: entry.realPath }),
		};
	};
	readdir = async (target: string): Promise<string[]> => {
		const entry = await this.get(target, "names");
		return entry.type === "directory" && entry.entries ? [...entry.entries] : this.unproven(target);
	};
	readFile = (target: string, maxBytes?: number): Promise<Buffer> =>
		this.readFileRange(target, 0, maxBytes ?? Number.MAX_SAFE_INTEGER);
	readFileRange = async (target: string, offset: number, length: number): Promise<Buffer> => {
		if (!isSafeCount(offset) || !isSafeCount(length)) throw new Error("invalid content read budget");
		const entry = await this.get(target, "content");
		if (entry.type !== "file" || entry.content === undefined) return this.unproven(target);
		return readContent(entry.content, offset, length);
	};
	/** Borrow immutable bytes or directory names from a stat-verified local input token. */
	snapshotInput(target: string, consumer?: ResourceReadView): ResourceInput | undefined {
		const entry = this.entries.get(filesystemPathKey(target));
		if (entry?.type === "directory" && entry.entries)
			return { names: entry.entries, computation: entry.computation };
		if (entry?.type !== "file" || !entry.content) return;
		if (entry.content instanceof ChunkedContent && consumer && !consumer.verifyingInputs)
			consumer.retainContentPool(entry.content.pool);
		return { content: entry.content, computation: entry.computation };
	}
	async validateObserved(): Promise<void> {
		const evaluation = this.evaluation;
		const proofs = [
			...(evaluation?.observed?.(evaluation.dependencies, false) ?? []),
			...(evaluation?.collectProofs?.values() ?? []),
		];
		if ((await validateResourceVersion(proofs, true)).expired) throw new Error("subcompute_inputs_changed");
	}
	/** A process borrows the same captured kernel object without inheriting the tool's OFD offset. */
	async borrowObject<T>(
		target: string,
		consume: (capture: StableFilesystemCapture, handle: FilesystemObjectHandle) => Promise<T>,
	): Promise<T | undefined> {
		this.assertComplete();
		return collectResourceComputation(
			async () => {
				const entry = this.sealed
					? await this.get(target, this.entry(target).entry?.type === "directory" ? "names" : "content")
					: this.entries.get(filesystemPathKey(target));
				const result = await entry?.object?.borrow(consume);
				if (result !== undefined) TimelineInterval.use(entry?.computation);
				return result;
			},
			(result) => result !== undefined,
		);
	}
	/** Fill a pre-budgeted pin slot only with independently read, byte-identical input.
	 * Supplied write/edit bytes alone never create kernel-object evidence. */
	retainObject(target: string, capture: StableFilesystemCapture): boolean {
		if (!this.retained || this.owner || !capture.object || !capture.content) return false;
		if (!this.sealed && this.observeObject) {
			try {
				this.observeObject(target, capture);
			} catch {
				return false;
			}
			const entry = this.entries.get(filesystemPathKey(target));
			return entry?.object === capture.object;
		}
		const key = filesystemPathKey(target);
		const entry = this.entries.get(key);
		if (
			entry?.object !== null ||
			(entry.type === "file"
				? !capture.stat.isFile() || !Buffer.isBuffer(entry.content) || !entry.content.equals(capture.content)
				: entry.type !== "directory" ||
					!capture.stat.isDirectory() ||
					capture.bytesRead > (entry.objectBytes ?? 0) ||
					!entry.entries ||
					!capture.entries ||
					!sameValues(entry.entries, capture.entries))
		)
			return false;
		this.entries.set(key, {
			...entry,
			...(entry.type === "file" ? { content: capture.content } : { entries: capture.entries }),
			object: this.preparations().lifetime.own(capture.object),
			computation: capture.computation,
		});
		return true;
	}
	access = async (target: string): Promise<void> => {
		await this.readFile(target, 0);
	};
	/** Retain preparations only while capturing, so the sealed branch accounts for every owned byte. */
	prepare: NonNullable<ToolFilesystemOperations["prepare"]> = (
		binding,
		key,
		build,
		consume,
		target,
		signal,
	) =>
		TimelineInterval.outside(() => {
			signal?.throwIfAborted();
			this.assertComplete();
			let owner: ResourceReadView = this;
			while (owner.owner) owner = owner.owner;
			const prepared = owner.preparations();
			const registration = preparationBindings.get(binding);
			const identity = registration?.id ?? binding;
			return prepared.lifetime.admit(async () => {
				this.assertComplete();
				const cached = prepared.bindings.get(identity)?.get(key);
				const compatible = (cached: PreparedResource | undefined) =>
					cached &&
					cached.phase !== "revoked" &&
					cached.destination.retained &&
					(cached.phase === "retained" || !cached.cancellation.signal.aborted) &&
					cached.boundary?.root === this.boundary?.root &&
					cached.boundary?.physicalRoot === this.boundary?.physicalRoot;
				const use = async (cached: PreparedResource, building = false, source?: ResourceInputSource) => {
					const context = this.evaluation;
					if (context?.preparing && preparationWaitsFor(cached, context.preparing)) return undefined;
					context?.preparing?.waiting.add(cached);
					cached.borrowers++;
					const withdraw = cached.cancellation.acquire(signal);
					let accepted = building;
					try {
						// A composed waiter must take its own proof references before the producer can retire.
						const ready = waitForPreparedSharing(cached, building, owner, Boolean(context?.acceptProofs));
						const readiness = await waitForCompletion(ready, signal);
						signal?.throwIfAborted();
						if (readiness.status !== "completed" || !readiness.value) return undefined;
						if (registration && !registration.accepts(cached.value))
							throw new Error("resource_preparation_data_invalid");
						const dependencies =
							cached.origin === owner && cached.dependencies && context?.dependencies
								? new Set([...context.dependencies, ...cached.dependencies])
								: undefined;
						const sourceDependencies =
							source && cached.origin.entries === source.view.entries ? cached.dependencies : undefined;
						// Relocated preparations retain their own input proofs; their new owner may also contain unrelated effects.
						const independent =
							source &&
							cached.origin.entries !== source.view.entries &&
							cached.dependencies &&
							[...cached.dependencies].every((key) =>
								cached.proofs?.some((proof) => proof.observations.has(key)),
							);
						const proofs = [
							...(!independent ? (source?.observed(sourceDependencies, false) ?? []) : []),
							...(cached.proofs ?? []),
						];
						return await cached.destination.prepared!.lifetime.admit(async () => {
							this.assertComplete();
							cached.destination.assertComplete();
							await context?.validateInputs?.([...(context.observed?.(dependencies, false) ?? []), ...proofs]);
							signal?.throwIfAborted();
							accepted = true;
							if (context) context.dependencies = dependencies;
							// Retain the validated proof before effects; its source may already have retired.
							const transferred =
								context?.acceptProofs &&
								proofs.length &&
								(source || cached.phase !== "retained" || cached.destination !== owner);
							if (transferred) {
								for (const proof of context.acceptProofs!(proofs))
									context.collectProofs?.set(proof.observations, proof);
								context.onForeignInputs?.(true);
							} else if (source) this.observeSource(source)(sourceDependencies);
							const evaluation = await TimelineInterval.measure(() =>
								consume(cached.value as Parameters<typeof consume>[0], cached.computation),
							);
							TimelineInterval.own(evaluation.computation);
							return TimelineInterval.outside(() => {
								TimelineInterval.use(cached.computation);
								this.assertComplete();
								if (!transferred && !(context?.acceptProofs && cached.origin === owner && cached.dependencies))
									cached.destination.assertComplete();
								return { value: evaluation.output };
							});
						});
					} catch (error) {
						if (accepted) throw error;
						this.assertComplete();
						signal?.throwIfAborted();
						return undefined;
					} finally {
						context?.preparing?.waiting.delete(cached);
						withdraw();
						if (!--cached.borrowers && cached.phase !== "retained") {
							const released = TimelineInterval.outside(() =>
								(cached.destination.prepared?.lifetime ?? prepared.lifetime).release(cached),
							);
							if (!signal?.aborted) await released;
						}
					}
				};
				const reused = cached && compatible(cached) && (await use(cached));
				if (reused) return reused.value;
				for await (const source of this.evaluation?.lookup?.(target, { binding: registration?.id, key }) ?? []) {
					const cached = source.view.prepared?.bindings.get(identity)?.get(key);
					if (
						source.view.entries === this.entries ||
						!source.view.retained ||
						!source.view.sealed ||
						!cached ||
						!compatible(cached)
					)
						continue;
					const result = await use(cached, false, source);
					if (result) return result.value;
				}
				const context = this.evaluation;
				const inputEpoch = owner.inputEpoch; // Rejected old sources precede this new computation.
				let resource: Awaited<ReturnType<typeof build>> | undefined;
				let shareable = false;
				let charged = 0;
				let computationBytes = 0;
				const name = target && filesystemPathKey(target);
				const pending: PreparedResource = {
					phase: "building",
					resource: name,
					origin: owner,
					destination: owner,
					borrowers: 0,
					boundary: this.boundary,
					cancellation: new SharedAbortScope(),
					waiting: new Set(),
					sharing: { kind: "building", requiresProofs: false, waiters: new Set() },
					dispose: async () => {
						try {
							await pending.ready?.catch(() => undefined);
							await resource?.dispose();
						} finally {
							if (prepared.bindings.get(identity)?.get(key) === pending)
								prepared.bindings.get(identity)!.delete(key);
							await Promise.allSettled(pending.proofs?.map((proof) => proof.release()) ?? []);
							pending.destination.capturedBytes -= charged;
							pending.value = undefined;
							pending.data = undefined;
						}
					},
				};
				const retainProofs = (proofs: Iterable<ResourceVersionToken>) => {
					for (const proof of proofs)
						(pending.proofs ??= []).push(proof.manager.retain({ ...proof, view: undefined }, undefined, this.memory));
				};
				const entries = this.bindingEntries(prepared, identity);
				if (
					!entries.has(key) ||
					(entries.get(key)!.phase !== "retained" && entries.get(key)!.cancellation.signal.aborted)
				)
					entries.set(key, pending);
				pending.ready = prepared.lifetime.track(
					this.evaluate(
						async (view) => {
							const frame = view.evaluation!;
							frame.preparing = pending;
							const foreign = frame.onForeignInputs;
							frame.onForeignInputs = (transferable) => {
								if (transferable) updatePreparedSharing(pending, "proof_acceptors");
								else updatePreparedSharing(pending, { kind: "settled", shareable: false });
								foreign?.(transferable);
							};
							frame.collectProofs = context?.acceptProofs ? new Map() : undefined;
							const evaluation = await TimelineInterval.measure(() =>
								build(view, pending.cancellation.signal),
							);
							resource = evaluation.output;
							pending.computation = TimelineInterval.own(evaluation.computation);
							pending.cancellation.signal.throwIfAborted();
							computationBytes = serialize(TimelineInterval.serialize(pending.computation)).byteLength;
							shareable = !frame.foreignInputs && resource.cacheable !== false;
							if (!shareable) updatePreparedSharing(pending, { kind: "settled", shareable: false });
							else if (frame.collectProofs?.size) retainProofs(frame.collectProofs.values());
						},
						(observed, commit) => {
							if (commit === false) return observed && context?.observed?.(observed, false);
							pending.dependencies = observed;
							if (owner.sealed && context?.missing && observed) {
								const proofs = context.observed?.(observed, false);
								if (
									!proofs ||
									[...observed].some((key) => !proofs.some((proof) => proof.observations.has(key)))
								)
									pending.dependencies = undefined;
								else retainProofs(proofs);
							}
						},
					).then(async () => {
						if (!resource) throw new Error("resource_preparation_missing");
						if (registration && !registration.accepts(resource.value))
							throw new Error("resource_preparation_data_invalid");
						const data = registration && snapshotPreparation(resource.value);
						pending.value = data ? data.value : resource.value;
						pending.data = data?.data;
						const bytes =
							(data ? preparationDataBytes(data.data.length) : resource.bytes) +
							computationBytes +
							resourceProofBytes(pending.proofs ?? []) +
							preparationMetadataBytes(key, name, pending.dependencies ?? []);
						if (!isSafeCount(resource.bytes)) throw new Error("resource_snapshot_budget_invalid");
						const destination = (pending.destination =
							owner.sealed && context?.missing ? (await context.missing()).view : owner);
						pending.cancellation.signal.throwIfAborted();
						this.assertComplete();
						destination.assertComplete();
						const retention = destination.preparations();
						if (
							pending.phase === "building" &&
							resource.cacheable !== false &&
							inputEpoch === owner.inputEpoch &&
							!destination.sealed &&
							bytes <= destination.remainingBytes
						) {
							const entries = this.bindingEntries(retention, identity);
							if (!entries.has(key) || entries.get(key) === pending) {
								pending.phase = "retained";
								charged = bytes;
								entries.set(key, retention.lifetime.own(pending));
								destination.capturedBytes += bytes;
							}
						}
						updatePreparedSharing(pending, { kind: "settled", shareable });
						// The origin keeps a borrowed lookup; the destination owns its bytes and disposal.
					}),
				);
				void pending.ready
					.finally(() => {
						updatePreparedSharing(pending, { kind: "settled", shareable: false });
					})
					.catch(() => {});
				return (await use(pending, true))!.value;
			});
		});
	/** Each evaluation owns its proof and failures while borrowing the input lifetime. */
	async evaluate<T>(
		operation: (view: ResourceReadView) => Promise<T>,
		observed?: ResourceInputSource["observed"],
		root?:
			| string
			| {
					readonly root: string;
					readonly physicalRoot: string;
			  },
		lookup = this.evaluation?.lookup,
		missing = this.evaluation?.missing,
		acceptProofs = this.evaluation?.acceptProofs,
		validateInputs = this.evaluation?.validateInputs,
		memory = this.memory,
	): Promise<T> {
		return collectResourceComputation(async () => {
			const frame: ResourceEvaluationContext = {
				dependencies: new Set(),
				lookup,
				missing,
				acceptProofs,
				observed,
				validateInputs,
				onForeignInputs: this.evaluation?.onForeignInputs,
				collectProofs: this.evaluation?.collectProofs,
				preparing: this.evaluation?.preparing,
				foreignInputs: false,
			};
			const view = this.readOnly(true, memory, frame);
			try {
				if (typeof root === "object") view.boundary = { root: root.root, physicalRoot: root.physicalRoot };
				else if (
					root === undefined ||
					(this.boundary && filesystemPathKey(root) === filesystemPathKey(this.boundary.root))
				) {
					if (this.boundary?.dependency) frame.dependencies!.add(this.boundary.dependency);
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
					if (this.evaluation) this.evaluation.foreignInputs ||= frame.foreignInputs;
					observed?.(frame.dependencies);
				});
				return output;
			} finally {
				view.dispose();
			}
		});
	}
	assertComplete(sealed = false): void {
		this.owner?.assertComplete();
		if (this.failure) throw this.failure;
		if (sealed && !this.sealed) throw new Error("resource_snapshot_not_sealed");
	}
	/** Compare restored query results to a fresh capture, using hashes without retaining a second byte image. */
	sameInputs(current: ResourceReadView): boolean {
		this.assertComplete(true);
		current.seal();

		const value = (entry: CapturedResource) => {
			const metadata = {
				type: entry.type,
				realPath: entry.realPath,
				size: resourceSize(entry),
			};
			switch (entry.type) {
				case "file":
					return {
						...metadata,
						hash: entry.content ? contentDigest(entry.content)?.slice(7) : entry.version?.hash,
					};
				case "directory":
					return { ...metadata, entries: entry.entries };
				case "alias":
					return { ...metadata, target: entry.target, link: entry.link };
				default:
					return metadata;
			}
		};

		return (
			this.entries.size === current.entries.size &&
			[...this.entries].every(([target, entry]) => {
				const actual = current.entries.get(target);
				return actual !== undefined && digest(value(entry)) === digest(value(actual));
			})
		);
	}
	/** Persist owned data only; object pins, live resources, callbacks and timing receipts stay with this owner. */
	async persistentSnapshot(
		observations: ReadonlyMap<string, ResourceObservation>,
		store: ProvenanceCertificateStore,
		inputs: readonly ResourceReadView[] = [],
	) {
		this.assertComplete(true);
		if (!this.contents) throw new Error("persistent input storage is not configured");
		const sources = [...new Set([this, ...inputs])];
		const aggregate =
			sources.length > 1
				? new ResourceReadView(this.remainingBytes, {
						boundary: this.boundary,
						contents: this.contents,
						memory: this.memory,
					})
				: this;
		try {
			if (aggregate !== this)
				for (const source of sources) {
					source.assertComplete(true);
					for (const [target, entry] of source.entries) {
						const selected =
							entry.dependency && observations.has(entry.dependency)
								? entry
								: entry.metadata?.dependency && observations.has(entry.metadata.dependency)
									? resourceMetadata(entry, entry.metadata.dependency)
									: undefined;
						if (!selected) continue;
						const {
							object: _object,
							objectBytes: _objectBytes,
							computation: _computation,
							...data
						} = selected;
						aggregate.capture(target, {
							...data,
							metadata: selected.metadata && {
								...selected.metadata,
								computation: undefined,
							},
						});
					}
				}
			aggregate.seal();
			const entries: PersistentResourceEntry[] = [];
			for (const [target, entry] of aggregate.entries) {
				const { type, realPath, dependency } = entry;
				const metadata = entry.metadata && {
					dependency: entry.metadata.dependency,
					size: entry.metadata.size,
				};
				let content: ChunkedContentManifest | undefined;
				if (type === "file" && entry.content) {
					const bytes = entry.content;
					const version =
						bytes instanceof ChunkedContent
							? bytes
							: (
									await this.contents.capture(async (sink) => {
										await sink(bytes);
										return {
											bytesRead: bytes.length,
											hash: hash("sha256", bytes),
										};
									})
								).content;
					if (version.pool.storeRoot !== store.root)
						throw new Error("persistent input content store changed");
					await version.publish();
					content = version.manifest;
				}
				entries.push([
					target,
					{
						type,
						realPath,
						dependency,
						metadata,
						...(content ? { content } : {}),
						...(content && entry.computation && speculativeConfig().subcomputation.persistTimelineIntervals
							? { computation: TimelineInterval.serialize(entry.computation) } : {}),
						...(type === "directory"
							? { entries: entry.entries }
							: type === "alias"
								? { target: entry.target, link: entry.link }
								: {}),
					},
				]);
			}
			const preparations: PersistentPreparation[] = [];
			for (const [binding, values] of this.prepared?.bindings ?? [])
				for (const [key, prepared] of values) {
					if (
						!this.boundary ||
						typeof binding !== "string" ||
						prepared.phase !== "retained" ||
						!prepared.data ||
						prepared.destination !== this ||
						!(prepared.sharing.kind === "settled" && prepared.sharing.shareable) ||
						!prepared.dependencies ||
						key.length > speculativeConfig().subcomputation.maxStepObjectNodes ||
						(prepared.resource && !containsFilesystemPath(this.boundary.root, prepared.resource)) ||
						prepared.boundary?.root !== this.boundary?.root ||
						prepared.boundary?.physicalRoot !== this.boundary?.physicalRoot ||
						!this.boundary?.dependency ||
						!prepared.dependencies.has(this.boundary.dependency) ||
						[...prepared.dependencies].some((key) => !observations.has(key))
					)
						continue;
					// A query may build through one view and retain in another. Its complete proof, not that temporary owner identity, closes the record.
					const dependencies = new Set(prepared.dependencies);
					if (
						prepared.proofs?.some(
							(proof) =>
								proof.root !== this.boundary!.root ||
								proof.physicalRoot !== this.boundary!.physicalRoot ||
								[...proof.observations].some(
									([key, observation]) => observations.get(key)?.fingerprint !== observation.fingerprint,
								),
						)
					)
						continue;
					for (const proof of prepared.proofs ?? [])
						for (const key of proof.observations.keys()) dependencies.add(key);
					preparations.push({
						binding,
						key,
						resource: prepared.resource,
						dependencies: [...dependencies],
						value: await store.artifacts.put(prepared.data),
						...(speculativeConfig().subcomputation.persistTimelineIntervals && prepared.computation
							? {
									computation: TimelineInterval.serialize(prepared.computation),
								}
							: {}),
					});
				}
			for (const source of sources) source.assertComplete(true);
			return { entries, preparations };
		} finally {
			if (aggregate !== this) await aggregate.dispose();
		}
	}
	/** Restore into the same preparation table with fresh cancellation and lifetime state, never old execution authority. */
	async restorePreparations(
		preparations: readonly PersistentPreparation[],
		store: ProvenanceCertificateStore,
		signal?: AbortSignal,
	): Promise<void> {
		for (const preparation of preparations) {
			this.assertComplete();
			const { binding, key, resource, dependencies, value } = preparation;
			const bytes = preparationDataBytes(value.size) + preparationMetadataBytes(key, resource, dependencies);
			if (!this.reserve(bytes)) this.assertComplete();
			const data = await store.artifacts.get(value, signal);
			if (!data) throw new Error("persistent preparation artifact missing");
			let decoded: unknown;
			try {
				decoded = deserialize(data);
			} catch (cause) {
				throw new Error("resource_preparation_data_invalid", { cause });
			}
			const snapshot = snapshotPreparation(decoded);
			if (!snapshot) throw new Error("resource_preparation_data_invalid");
			if (!this.reserve(0)) this.assertComplete();
			this.retainPreparation(binding, key, { value: snapshot.value, data }, bytes, {
				resource,
				dependencies: new Set(dependencies),
				boundary: this.boundary,
				computation: preparation.computation && TimelineInterval.restore(preparation.computation),
			});
		}
	}
	seal(): void {
		if (this.pending) this.failure ??= new Error("resource_snapshot_capture_pending");
		this.assertComplete();
		if (!this.sealed) this.mode = "sealed";
	}
	dispose(): void | Promise<void> {
		this.failure = new Error("resource_snapshot_disposed");
		if (this.owner) return;
		this.contentOwners.clear();
		this.entries.clear();
		this.objectCount = 0;
		if (this.pending) this.preparations().lifetime.track(this.pending);
		if (this.prepared)
			return this.prepared.lifetime.dispose(() => {
				this.prepared!.bindings.clear();
				this.capturedBytes = 0;
			});
		this.capturedBytes = 0;
	}
	private retainContentPool(pool: ContentChunkPool): void {
		if (this.contentOwners.has(pool)) return;
		this.contentOwners.add(pool);
		this.preparations().lifetime.own({ dispose: pool.retain() });
	}
	private releaseObject(entry: CapturedResource): void {
		if (entry.object) void this.preparations().lifetime.release(entry.object);
	}
	private preparations() {
		return (this.prepared ??= {
			lifetime: new RuntimeLifecycleLane(),
			bindings: new Map(),
		});
	}
	private bindingEntries(prepared = this.preparations(), binding: object | Sha256Digest) {
		let entries = prepared.bindings.get(binding);
		if (!entries) prepared.bindings.set(binding, (entries = new Map()));
		return entries;
	}
	private retainPreparation(
		binding: object | Sha256Digest,
		key: string,
		snapshot: NonNullable<ReturnType<typeof snapshotPreparation>>,
		bytes: number,
		metadata: Pick<PreparedResource, "resource" | "dependencies" | "boundary" | "computation">,
	) {
		const prepared = this.preparations();
		const entries = this.bindingEntries(prepared, binding);
		const retained: PreparedResource = {
			...metadata,
			origin: this,
			destination: this,
			borrowers: 0,
			phase: "retained",
			sharing: { kind: "settled", shareable: true },
			cancellation: new SharedAbortScope(),
			waiting: new Set(),
			ready: Promise.resolve(),
			...snapshot,
			dispose: () => {
				if (entries!.get(key) === retained) entries!.delete(key);
				this.capturedBytes -= bytes;
				retained.value = undefined;
				retained.data = undefined;
			},
		};
		entries.set(key, prepared.lifetime.own(retained));
	}
	private async get(target: string, scope: ResourceDependency["scope"]): Promise<CapturedResource> {
		this.assertComplete();
		if (this.owner && this.boundary && !containsFilesystemPath(this.boundary.root, target))
			this.unproven(target);
		if (this.mode === "read_through" && this.owner && !this.owner.sealed) await this.owner.get(target, scope);
		if (this.load && !this.sealed) {
			const pending = (this.pending ?? Promise.resolve()).then(() => {
				const { entry } = this.entry(target, followsResource(scope));
				// Existing input evidence also owns the metadata derivable from those bytes or names.
				if (resourceCovers(entry, scope)) return;
				return this.load!({ path: target, scope });
			});
			this.pending = pending;
			try {
				await pending;
			} catch (error) {
				throw (this.failure ??= error instanceof Error ? error : new Error(String(error)));
			} finally {
				if (this.pending === pending) this.pending = undefined;
			}
		}
		let entry: CapturedResource | undefined;
		let resolved = filesystemPathKey(target);
		const evaluation = this.evaluation;
		try {
			// Probe only this read; rejected aliases leave neither dependencies nor computation receipts.
			({ entry, resolved } = evaluation?.validateInputs
				? await this.evaluate(
						async (view) => {
							const input = view.entry(target, followsResource(scope), scope);
							if (input.entry || input.resolved !== resolved)
								await TimelineInterval.outside(() =>
									evaluation.validateInputs!(evaluation.observed?.(view.evaluation?.dependencies, false) ?? []),
								);
							return input;
						},
						(observed) => {
							if (!observed) evaluation.dependencies = undefined;
							else if (evaluation.dependencies)
								for (const dependency of observed) evaluation.dependencies.add(dependency);
						},
						this.boundary,
					)
				: this.entry(target, followsResource(scope), scope));
		} catch {
			this.assertComplete();
		}
		if (resourceCovers(entry, scope)) {
			TimelineInterval.use(
				["stat", "type", "entry", "exists"].includes(scope)
					? entry?.metadata?.computation
					: entry?.computation,
			);
			return entry!;
		}
		// The local alias proof owns this translation; the source owns the resolved resource.
		for (const query of resolved === filesystemPathKey(target) ? [target] : [resolved, target])
			for await (const source of evaluation?.lookup?.(query) ?? []) {
				if (source.view.entries === this.entries || !source.view.retained) continue;
				try {
					source.view.assertComplete(true);
					// The caller already owns the boundary proof; the source contributes only its resource evidence.
					const boundary =
						this.boundary && query !== target
							? {
									root: this.boundary.physicalRoot,
									physicalRoot: this.boundary.physicalRoot,
								}
							: this.boundary;
					return await source.view.evaluate(
						async (view) => {
							const entry = await view.get(query, scope);
							await TimelineInterval.outside(() =>
								evaluation?.validateInputs?.(source.observed(view.evaluation?.dependencies, false) ?? []),
							);
							return entry;
						},
						this.observeSource(source),
						boundary,
					);
				} catch {
					/* An indexed name alone grants no coverage; another sealed owner may supply it. */
				}
			}
		if (evaluation?.missing) {
			try {
				const source = await evaluation.missing();
				return await source.view.evaluate(
					(view) => view.get(target, scope),
					this.observeSource(source),
					this.boundary,
				);
			} catch (error) {
				throw (this.failure ??= error instanceof Error ? error : new Error(String(error)));
			}
		}
		return this.unproven(target);
	}
	private observeSource(source: ResourceInputSource) {
		return (dependencies: ReadonlySet<string> | undefined, commit = true) => {
			const proofs = source.observed(dependencies, commit);
			if (!commit) return proofs;
			const evaluation = this.evaluation;
			if (evaluation?.collectProofs && proofs?.length) {
				for (const proof of proofs) evaluation.collectProofs.set(proof.observations, proof);
				evaluation.onForeignInputs?.(true);
			} else if (evaluation) {
				evaluation.foreignInputs = true;
				evaluation.onForeignInputs?.(false);
			}
			return proofs;
		};
	}
	private entry(
		target: string,
		follow = true,
		scope: ResourceDependency["scope"] = "content",
	): {
		entry?: CapturedResource;
		resolved: string;
	} {
		this.assertComplete();
		let current = filesystemPathKey(target);
		const visited = new Set<string>();
		while (!visited.has(current) && visited.size <= this.entries.size) {
			visited.add(current);
			const exact = this.observe(this.entries.get(current), scope);
			if (exact?.type === "alias" && follow) {
				if (!exact.target) break;
				current = exact.target;
				continue;
			}
			if (exact)
				return {
					entry: exact,
					resolved: exact.type !== "alias" && exact.realPath ? filesystemPathKey(exact.realPath) : current,
				};
			let parent = path.dirname(current);
			while (parent !== path.dirname(parent) && this.entries.get(parent)?.type !== "alias")
				parent = path.dirname(parent);
			const alias = this.entries.get(parent);
			if (alias?.type !== "alias" || !alias.target) break;
			this.observe(alias);
			current = filesystemPathKey(path.resolve(alias.target, path.relative(parent, current)));
		}
		return { resolved: current };
	}
	private observe(
		entry: CapturedResource | undefined,
		scope?: ResourceDependency["scope"],
	): CapturedResource | undefined {
		if (
			this.owner &&
			this.boundary &&
			entry?.realPath &&
			(entry.type !== "alias" || (scope !== undefined && !followsResource(scope))) &&
			!containsFilesystemPath(this.boundary.physicalRoot, entry.realPath)
		)
			this.unproven(entry.realPath);
		if (entry?.type === "alias") TimelineInterval.use(entry.computation);
		if (entry && this.evaluation?.dependencies) {
			const dependency =
				entry.type !== "alias" &&
				(scope === "type" ||
					scope === "exists" ||
					((scope === "entry" || scope === "stat") &&
						!(entry.type === "file" && entry.content !== undefined)))
					? (entry.metadata?.dependency ?? entry.dependency)
					: entry.dependency;
			if (dependency) this.evaluation.dependencies.add(dependency);
			else this.evaluation.dependencies = undefined;
		}
		return entry;
	}
	private unproven(target: string): never {
		throw (this.failure ??= new Error(`resource_access_unproven:${target}`));
	}
}
/** Failed resource attempts keep their actual computation, never receipts for discarded inputs. */
export async function collectResourceComputation<T>(
	operation: () => T | Promise<T>,
	accept: (value: T) => boolean = () => true,
): Promise<T> {
	const evaluation = await TimelineInterval.collect(async () => {
		try {
			return { ok: true as const, value: await operation() };
		} catch (error) {
			return { ok: false as const, error };
		}
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
const resourceSize = (entry: CapturedResource) =>
	(entry.type === "file" ? (entry.content?.length ?? entry.version?.size) : undefined) ??
	entry.metadata?.size;
function resourceMetadata(entry: CapturedResource, dependency: string): CapturedResource {
	const metadata = {
		...entry.metadata,
		dependency,
		size:
			entry.type === "file" && entry.content && entry.dependency === dependency
				? entry.content.length
				: entry.metadata?.dependency === dependency
					? entry.metadata.size
					: undefined,
	};
	return entry.type === "alias"
		? entry
		: {
				type: entry.type,
				realPath: entry.realPath,
				dependency,
				computation: metadata.computation,
				metadata,
			};
}
const followsResource = (scope: ResourceDependency["scope"]) => scope !== "entry" && scope !== "exists";
function resourceCovers(entry: CapturedResource | undefined, scope: ResourceDependency["scope"]): boolean {
	return (
		!!entry &&
		(scope === "type" ||
			scope === "exists" ||
			((scope === "entry" || scope === "stat") &&
				(entry.type === "missing" || resourceSize(entry) !== undefined)) ||
			(scope === "names" && entry.type === "directory" && entry.entries !== undefined) ||
			(scope === "content" && entry.type === "file" && entry.content !== undefined))
	);
}
type ResourceEvent = {
	readonly epoch: number;
	readonly path: string;
	readonly type: "change" | "rename" | "unknown";
};
type ResourceWatcher = Pick<FSWatcher, "close"> & {
	readonly settle?: () => Promise<void>;
};
/** Linux's recursive Node watcher performs ordinary stat/readdir calls. Install
 * nonrecursive inode watches through the read view instead, and rescan renamed
 * subtrees through one fresh view. Symbolic links are covered by their parent. */
async function watchReadonlyFilesystem(
	root: string,
	changed: (target: string, type: "change" | "rename") => void,
	failed: (error: Error) => void,
): Promise<ResourceWatcher> {
	const watched = new Map<
		string,
		{
			readonly watcher: FSWatcher;
			readonly identity: string;
		}
	>();
	const dirty = new Set([root]);
	let stopped: Error | "closed" | undefined;
	let pending: Promise<void> | undefined;
	const close = (reason: Error | "closed" = "closed") => {
		if (stopped) return;
		stopped = reason;
		for (const entry of watched.values()) entry.watcher.close();
		watched.clear();
	};
	const fail = (error: Error) => {
		if (!stopped) {
			close(error);
			failed(error);
		}
	};
	const refresh = (): Promise<void> => {
		if (stopped) return stopped === "closed" ? Promise.resolve() : Promise.reject(stopped);
		return (pending ??= (async () => {
			while (dirty.size && !stopped) {
				const roots = [...dirty];
				dirty.clear();
				await using filesystem = await openFilesystemReadView();
				const sourceRoot = await filesystem.realpath(root);
				for (const subtree of roots) {
					const seen = new Set<string>();
					const paths = [subtree];
					for (let cursor = 0; cursor < paths.length && !stopped; cursor++) {
						const target = paths[cursor];
						let descriptor: number | undefined;
						try {
							descriptor = filesystem.open(target, 0x200000 | (target === root ? 0 : constants.O_NOFOLLOW));
							const stat = fstatSync(descriptor, { bigint: true });
							if (stat.isSymbolicLink()) continue;
							if (!containsFilesystemPath(sourceRoot, readlinkSync(`/proc/self/fd/${descriptor}`)))
								throw new Error("filesystem watcher directory escaped its root");
							seen.add(target);
							const identity = `${stat.dev}:${stat.ino}`;
							const prior = watched.get(target);
							const directory = stat.isDirectory();
							if (prior?.identity !== identity) {
								prior?.watcher.close();
								const watcher = watch(`/proc/self/fd/${descriptor}`, (event, filename) => {
									if (stopped) return;
									changed(directory && filename ? path.join(target, filename.toString()) : target, event);
									if (event === "rename") {
										changed(target, event);
										dirty.add(directory ? target : path.dirname(target));
										void refresh().catch(fail);
									}
								});
								watcher.on("error", fail);
								watched.set(target, { watcher, identity });
							}
							if (directory)
								for (const { name } of await readFilesystemDirectory(target, filesystem))
									paths.push(path.join(target, name));
						} catch (error) {
							if (!missingResource(error)) throw error;
						} finally {
							if (descriptor !== undefined) closeSync(descriptor);
						}
					}
					if (stopped) break;
					for (const [target, entry] of watched)
						if (containsFilesystemPath(subtree, target) && !seen.has(target)) {
							entry.watcher.close();
							watched.delete(target);
						}
				}
				await filesystem.verify([...roots, sourceRoot]);
			}
			if (stopped && stopped !== "closed") throw stopped;
		})().finally(() => {
			pending = undefined;
		}));
	};
	try {
		await refresh();
		return {
			close,
			settle: async () => {
				do {
					await refresh();
				} while (pending || (!stopped && dirty.size));
			},
		};
	} catch (error) {
		close();
		throw error;
	}
}
export class ResourceVersionManager {
	private epoch = 0;
	private readonly events: ResourceEvent[] = [];
	private readonly preciseWatches = new Map<
		string,
		{
			readonly watcher: FSWatcher;
			references: number;
		}
	>();
	private references = 0;
	private watcher?: ResourceWatcher;
	private status: "open" | "watching" | "degraded" | "closed" = "open";
	private get open(): boolean {
		return this.status !== "closed";
	}
	private ready?: Promise<void>;
	readonly root: string;
	private readonly snapshotExcludes: ReadonlySet<string>;
	private readonly onIdle?: () => void;
	private readonly persistentInputs?: ResourceInputPersistence;
	constructor(
		root: string,
		options: {
			readonly watch?: boolean;
			readonly onIdle?: () => void;
			/** Private snapshot entries only; filtered tokens cannot authorize input views or host execution windows. */
			readonly snapshotExcludes?: readonly string[];
			/** Linux input content uses the existing CAS; live kernel objects and prepared callbacks remain ephemeral. */
			readonly persistentInputs?: ResourceInputPersistence;
		} = {},
	) {
		this.root = options.persistentInputs ? path.resolve(root) : root;
		this.snapshotExcludes = new Set(options.snapshotExcludes);
		this.onIdle = options.onIdle;
		this.persistentInputs = options.persistentInputs;
		if (this.persistentInputs && !path.isAbsolute(root))
			throw new Error("persistent input root must be absolute");
		if (options.watch === false) this.ready = Promise.resolve();
	}
	private async startWatching() {
		try {
			const failed = () => this.changed(this.root, "unknown");
			if (nativeFilesystem)
				this.watcher = await watchReadonlyFilesystem(
					this.root,
					(target, event) => this.changed(target, event),
					failed,
				);
			else {
				const watcher = watch(this.root, { recursive: true }, (event, filename) =>
					this.changed(filename ? path.resolve(this.root, filename) : this.root, event),
				);
				watcher.on("error", failed);
				this.watcher = watcher;
			}
			if (!this.open) {
				this.watcher?.close();
				this.watcher = undefined;
				return;
			}
			if (this.status === "open") this.status = "watching";
			await watcherTurn();
		} catch {
			if (this.open) this.status = "degraded";
		}
	}
	/** Undefined dependencies grant only bounded on-demand captures; observation of host tools stays eager. */
	async capture(
		dependencies: ReadonlyArray<ResourceDependency> | undefined,
		retainBytes?: number,
		providedInputs?: ReadonlyMap<string, ResourceInput>,
	): Promise<ResourceVersionToken> {
		if (dependencies?.length === 0 || (!dependencies && retainBytes === undefined))
			throw new Error("resource_dependencies_unproven");
		if (providedInputs && retainBytes === undefined) throw new Error("resource_snapshot_budget_invalid");
		return this.captureToken(
			dependencies,
			retainBytes,
			providedInputs &&
				new Map(
					[...providedInputs].map(([target, input]) => [
						filesystemPathKey(path.resolve(this.root, target)),
						input && !isInputContent(input)
							? "names" in input
								? { ...input, names: input.names && [...input.names] }
								: { ...input }
							: input,
					]),
				),
		);
	}
	/** Notification cursor for preparation; empty observations cannot validate or seal any resource. */
	observeChanges(): Promise<ResourceVersionToken> {
		return this.captureToken([]);
	}
	/** Keep discovery alive between independently rejected or released input records. */
	acquire(): () => void | Promise<void> {
		if (!this.open) throw new Error("resource_version_manager_closed");
		this.references++;
		return releaseOnce(() => this.releaseCapture(new Map()));
	}
	/** Store a sealed input closure. Publication never upgrades its historical observations into current authority. */
	async persist(
		token: ResourceVersionToken,
		inputs: readonly ResourceVersionToken[] = [],
	): Promise<Sha256Digest> {
		if (
			!this.open ||
			token.manager !== this ||
			token.root !== this.root ||
			!token.view ||
			!this.persistentInputs
		)
			throw new Error("persistent input owner changed");
		await this.assertPersistentStore();
		return this.persistentInputs.store.withPublication(async () => {
			const sources = [...new Set([token, ...inputs.filter((input) => input.view?.retained)])];
			const proof = new ResourceObservations(this.persistentInputs!.memory);
			try {
				for (const source of sources) {
					if (source.root !== token.root || source.physicalRoot !== token.physicalRoot)
						throw new Error("persistent input closure root changed");
					for (const [key, observation] of source.observations)
						if (observation.scope !== "binding") {
							if (proof.has(key) && proof.get(key)!.fingerprint !== observation.fingerprint)
								throw new Error("persistent input closure conflict");
							proof.set(key, observation);
						}
				}
				const observations = [...proof.values()];
				const { entries, preparations } = await token.view!.persistentSnapshot(
					proof,
					this.persistentInputs!.store,
					sources.map((source) => source.view!),
				);
				if (!entries.length && !preparations.length) throw new Error("persistent input closure empty");
				const payload: PersistentResourcePayload = {
					root: this.root,
					physicalRoot: token.physicalRoot,
					observations,
					entries,
					preparations,
				};
				const references = new Map<Sha256Digest, ArtifactReference>();
				for (const [, entry] of entries)
					for (const reference of entry.content?.chunks ?? [])
						if (reference) references.set(reference.digest, reference);
				for (const preparation of preparations) references.set(preparation.value.digest, preparation.value);
				token.view!.assertComplete(true);
				if (!this.open || !observations.length) throw new Error("persistent input owner changed");
				return (await this.persistentInputs!.store.putInputVersion(payload, [...references.values()])).id;
			} finally {
				proof.clear();
			}
		});
	}
	/** Cold recovery grants a new input lease only after content integrity and current query validation. */
	async restore(
		record: StoredInputVersion,
		retainBytes: number,
		signal?: AbortSignal,
	): Promise<ResourceVersionToken | undefined> {
		if (!this.open || !this.persistentInputs || this.snapshotExcludes.size)
			throw new Error("persistent input owner changed");
		await this.assertPersistentStore();
		const { store, limits } = this.persistentInputs;
		return store.read(async (signal) => {
			const payload = parsePersistentResourcePayload(record.payload, this.root);
			const proofBytes = payload.observations.reduce(
				(sum, entry) => sum + observationBytes(dependencyKey(entry), entry),
				0,
			);
			if (proofBytes > retainBytes) throw resourceBudgetError("resource_snapshot_budget_exceeded");
			const contents = this.persistentInputs!.contents ?? new ContentChunkPool(store, limits);
			const observations = new ResourceObservations(
				this.persistentInputs!.memory,
				payload.observations.map((observation) => [dependencyKey(observation), observation]),
			);
			let view: ResourceReadView;
			try {
				view = new ResourceReadView(retainBytes - proofBytes, {
					boundary: {
						root: this.root,
						physicalRoot: payload.physicalRoot,
						dependency: dependencyKey({ path: this.root, scope: "resolution" }),
					},
					contents,
					memory: this.persistentInputs!.memory,
				});
			} catch (error) {
				observations.clear();
				throw error;
			}
			this.references++;
			const release = releaseOnce(() => this.releaseCapture(observations, view));
			try {
				const references = new Map<Sha256Digest, number>();
				for (const [target, entry] of payload.entries) {
					const content = entry.content && (await contents.restore(entry.content, undefined, signal));
					for (const reference of content?.manifest.chunks ?? [])
						if (reference) references.set(reference.digest, reference.size);
					view.capture(target, {
						...entry,
						computation: entry.computation && TimelineInterval.restore(entry.computation),
						...(content ? { content } : {}),
					} as CapturedResource);
					view.assertComplete();
				}
				for (const preparation of payload.preparations) validateArtifact(preparation.value, references);
				if (
					record.artifacts.length !== references.size ||
					record.artifacts.some((reference) => references.get(reference.digest) !== reference.size)
				)
					throw new Error("persistent input artifact closure mismatch");
				await view.restorePreparations(payload.preparations, store, signal);
				view.seal();
				const token: ResourceVersionToken = {
					root: this.root,
					physicalRoot: payload.physicalRoot,
					observations,
					epoch: this.epoch,
					watching: false,
					preciseContent: Object.freeze([]),
					manager: this,
					view,
					inputView: new WeakRef(view),
					release,
				};
				const proof = new ResourceReadView(retainBytes, { verifyingInputs: true });
				try {
					if ((await this.inspect(token, false, proof)).expired) {
						await release();
						return;
					}
				} finally {
					await proof.dispose();
				}
				signal.throwIfAborted();
				if (!this.open) throw new Error("persistent input owner changed");
				return token;
			} catch (error) {
				await release();
				throw error;
			}
		}, signal);
	}
	/** Discover current input closures by workspace or exact preparation. The caller owns every yielded token. */
	async *recover(
		retainBytes: number | (() => number),
		options: {
			readonly maxCandidates?: number;
			readonly signal?: AbortSignal;
			readonly target?: string;
			readonly preparation?: PreparationLookup;
		} = {},
	): AsyncGenerator<{
		readonly id: Sha256Digest;
		readonly token: ResourceVersionToken;
	}> {
		if (!this.open || !this.persistentInputs || this.snapshotExcludes.size)
			throw new Error("persistent input owner changed");
		const budget = () => {
			const bytes = typeof retainBytes === "number" ? retainBytes : retainBytes();
			if (!Number.isFinite(bytes) || bytes < 0) throw new Error("resource_snapshot_budget_invalid");
			return bytes;
		};
		budget();
		const { store } = this.persistentInputs;
		const { signal } = options;
		const target =
			options.target === undefined ? undefined : filesystemPathKey(path.resolve(this.root, options.target));
		signal?.throwIfAborted();
		const binding = options.preparation?.binding;
		if (options.preparation ? !binding || !store.findPreparedInputCandidates : this.persistentInputs.inputs) return;
		await this.assertPersistentStore();
		const limit = options.maxCandidates ?? Math.min(store.limits.maxCertificates, speculativeConfig().filesystem.maxHistoryEntries);
		const preparation = options.preparation;
		const candidates = preparation
			? await store.findPreparedInputCandidates(
					{ root: this.root, binding: binding!, prepKey: digestObject(preparation.key), limit },
					signal,
				)
			: store.candidatesInputVersions(this.root, limit, signal);
		for await (const record of candidates) {
			signal?.throwIfAborted();
			store.signal.throwIfAborted();
			if (!this.open) throw new Error("persistent input owner changed");
			const payload = record.payload as Partial<PersistentResourcePayload> | null;
			if (payload?.root !== this.root) continue;
			if (preparation && !payload.preparations?.some((item) =>
				item.binding === preparation.binding && item.key === preparation.key)) continue;
			if (
				!preparation &&
				target !== undefined &&
				Array.isArray(payload.entries) &&
				Array.isArray(payload.preparations) &&
				!payload.entries.some(
					(entry) =>
						Array.isArray(entry) &&
						typeof entry[0] === "string" &&
						(entry[0] === target || (entry[1]?.type === "alias" && containsFilesystemPath(entry[0], target))),
				) &&
				!payload.preparations.some(
					(preparation) =>
						!preparation ||
						typeof preparation.resource !== "string" ||
						containsFilesystemPath(preparation.resource, target),
				)
			)
				continue;
			let token: ResourceVersionToken | undefined;
			try {
				token = await this.restore(record, budget(), signal);
				signal?.throwIfAborted();
				store.signal.throwIfAborted();
				if (!this.open) throw new Error("persistent input owner changed");
			} catch (error) {
				await token?.release();
				signal?.throwIfAborted();
				store.signal.throwIfAborted();
				if (!this.open) throw error;
				continue; // An unavailable cache record cannot hide another independently valid input closure.
			}
			if (token) yield Object.freeze({ id: record.id, token });
		}
	}
	/** Resolve existing ancestors before creating the cache: persistent capture must not write into its source. */
	async assertPersistentStore(): Promise<void> {
		if (!this.persistentInputs || this.persistentInputs.internalCache) return;
		await withFilesystemReadView(undefined, async (filesystem) => {
			const root = await filesystem.realpath(this.root);
			const { path: resolved } = await resolveFilesystemDestination(
				this.persistentInputs!.store.root,
				filesystem,
			);
			if (containsFilesystemPath(root, resolved) || containsFilesystemPath(resolved, root))
				throw new Error("persistent input cache must be outside its source tree");
		});
	}
	/** Own an evaluated query's evidence without keeping its input buffers or source branch alive. */
	retain(
		token: ResourceVersionToken,
		metadataBytes?: number,
		memory = this.persistentInputs?.memory,
	): ResourceVersionToken {
		if (!this.open || token.manager !== this || token.root !== this.root || !token.observations.size)
			throw new Error("resource_version_owner_changed");
		const observations = new ResourceObservations(memory, token.observations);
		let view: ResourceReadView | undefined;
		try {
			view =
				metadataBytes === undefined || !token.view?.retained
					? undefined
					: token.view.retainMetadata(observations, metadataBytes, memory);
		} catch (error) {
			observations.clear();
			throw error;
		}
		this.references++;
		return {
			...token,
			observations,
			view,
			watching: false,
			preciseContent: Object.freeze([]),
			release: releaseOnce(() => this.releaseCapture(observations, view)),
		};
	}
	private async captureToken(
		dependencies: ReadonlyArray<ResourceDependency> | undefined,
		retainBytes?: number,
		providedInputs?: ReadonlyMap<string, ResourceInput>,
	): Promise<ResourceVersionToken> {
		if (!this.open) throw new Error("resource_version_manager_closed");
		if (this.snapshotExcludes.size && retainBytes !== undefined)
			throw new Error("resource_filtered_snapshot_not_readable");
		const observations = new ResourceObservations(this.persistentInputs?.memory);
		let precise: Awaited<ReturnType<ResourceVersionManager["acquirePreciseWatches"]>> | undefined;
		this.references++;
		let view: ResourceReadView | undefined;
		const observing = dependencies !== undefined && !providedInputs;
		const release = releaseOnce(() => this.releaseCapture(observations, view, precise));
		try {
			await ensureNativeFilesystem();
			if (retainBytes !== undefined) await this.assertPersistentStore();
			if (observing) {
				await (this.ready ??= this.startWatching());
				await this.watcher?.settle?.();
			}
			const physicalRoot = await fingerprintIO(() =>
				withFilesystemReadView(undefined, (filesystem) => filesystem.realpath(this.root)),
			);
			const capture = async (requested: ReadonlyArray<ResourceDependency>) => {
				const normalized = [
					...new Map(
						requested.map(({ path: target, scope }) => {
							const dependency = {
								path: path.resolve(this.root, target),
								scope,
							};
							if (!containsFilesystemPath(this.root, dependency.path))
								throw new Error(`resource dependency escapes workspace: ${target}`);
							return [dependencyKey(dependency), dependency];
						}),
					).values(),
				].filter((dependency) => !observations.has(dependencyKey(dependency)));
				if (!normalized.length) return;
				if (observing && this.status === "watching") precise = await this.acquirePreciseWatches(normalized);
				for (const observation of await fingerprintDependencies(
					normalized,
					physicalRoot,
					this.snapshotExcludes,
					view,
					observing && !this.snapshotExcludes.size ? { root: this.root, observations } : undefined,
					providedInputs,
					this.persistentInputs?.inputs,
				))
					observations.set(dependencyKey(observation), observation);
			};
			const boundary = { path: this.root, scope: "resolution" as const };
			const boundaryKey = dependencyKey(boundary);
			view =
				retainBytes === undefined
					? undefined
					: new ResourceReadView(retainBytes, {
							load: dependencies ? undefined : (dependency) => capture([dependency]),
							boundary: { root: this.root, physicalRoot, dependency: boundaryKey },
							observeObject: dependencies
								? undefined
								: (target, content) => {
										if (
											!containsFilesystemPath(this.root, target) ||
											!containsFilesystemPath(physicalRoot, target) ||
											filesystemPathKey(target) !== filesystemPathKey(content.realPath) ||
											!(content.stat.isDirectory() ? content.entries : content.stat.isFile())
										)
											return;
										const dependency = {
											path: target,
											scope: content.entries ? ("names" as const) : ("content" as const),
										};
										const key = dependencyKey(dependency);
										view!.captureObject(target, content, key);
										const { value, stamp } = content.entries
											? fingerprintDirectory(content.stat, content.realPath, content.entries)
											: fingerprintFile(content);
										observations.set(key, {
											...dependency,
											stamp,
											fingerprint: digest({
												path: filesystemPathKey(target),
												scope: dependency.scope,
												value,
											}),
										});
									},
							contents:
								retainBytes !== undefined && this.persistentInputs
									? (this.persistentInputs.contents ??
											new ContentChunkPool(this.persistentInputs.store, this.persistentInputs.limits))
									: undefined,
							memory: this.persistentInputs?.memory,
						});
			if (view)
				observations.set(boundaryKey, {
					...boundary,
					stamp: filesystemPathKey(physicalRoot),
					fingerprint: digest({
						path: filesystemPathKey(this.root),
						scope: "resolution",
						value: filesystemPathKey(physicalRoot),
					}),
				});
			if (dependencies) await capture(dependencies);
			if (observing) {
				await watcherTurn();
				await this.watcher?.settle?.();
			}
			if (!this.open) throw new Error("resource_version_manager_closed");
			const retained = view?.retained ? view : undefined;
			if (dependencies) retained?.seal();
			return {
				root: this.root,
				physicalRoot,
				observations,
				epoch: this.epoch,
				watching: Boolean(observing && this.status === "watching"),
				preciseContent: Object.freeze(precise?.paths ?? []),
				manager: this,
				...(retained ? { view: retained, inputView: new WeakRef(retained) } : {}),
				release,
			};
		} catch (error) {
			await release();
			throw error;
		}
	}
	private releaseCapture(
		observations: Map<string, ResourceObservation>,
		view?: ResourceReadView,
		precise?: {
			release: () => void;
		},
	) {
		const finish = () => {
			observations.clear();
			precise?.release();
			if (--this.references === 0 && !this.preciseWatches.size) this.onIdle?.();
		};
		const pending = view?.dispose();
		return pending ? pending.then(finish) : finish();
	}
	/** Adoption waits on these few checks, so their I/O never queues behind a whole-tree scan's waiters. */
	async validate(token: ResourceVersionToken, checkStamps = false): Promise<ResourceVersionValidation> {
		return priorityIO.run(true, () => this.inspect(token, false, undefined, checkStamps));
	}
	async seal(token: ResourceVersionToken): Promise<ResourceVersionValidation> {
		return this.inspect(token, true);
	}
	private async inspect(
		token: ResourceVersionToken,
		sealing: boolean,
		restoredInputs?: ResourceReadView,
		checkStamps = sealing,
	): Promise<ResourceVersionValidation> {
		if (!this.open || token.manager !== this || token.root !== this.root)
			return validation("resource_version_owner_changed");
		try {
			if (sealing) token.view?.seal();
			else token.view?.assertComplete(true);
			if (sealing && this.snapshotExcludes.size) throw new Error("resource_filtered_snapshot_not_observable");
			// Host windows need eager binding evidence; Windows can restore a junction without changing its stamps.
			if (
				sealing &&
				(process.platform === "win32" || !token.observations.has(`binding:${filesystemPathKey(this.root)}`))
			)
				throw new Error("resource_path_binding_window_unprovable");
			if (!token.observations.size) throw new Error("resource_dependencies_unproven");
			// Watcher delivery fences host execution windows; future adoption uses the exact fingerprints below.
			if (sealing) await watcherTurn();
			const watcherFailure = this.invalidation(token, sealing);
			if (watcherFailure) return validation(watcherFailure, "watcher");
			const current = await fingerprintDependencies(
				[...token.observations.values()].filter((entry) => sealing || entry.scope !== "binding"),
				token.physicalRoot,
				this.snapshotExcludes,
				restoredInputs,
				undefined, undefined, this.persistentInputs?.inputs,
			);
			if (sealing) await watcherTurn();
			const lateFailure = this.invalidation(token, sealing);
			if (lateFailure) return validation(lateFailure, "watcher");
			const changed = current
				.filter((entry) => {
					const captured = token.observations.get(dependencyKey(entry));
					return (
						entry.fingerprint !== captured?.fingerprint ||
						(checkStamps && (!entry.stamp || !captured?.stamp || entry.stamp !== captured.stamp))
					);
				})
				.map(dependencyKey);
			const expired =
				!current.length ||
				changed.length > 0 ||
				(!!restoredInputs && !token.view!.sameInputs(restoredInputs));
			const reason = sealing ? "resource_observation_window_changed" : "resource_fingerprint_changed";
			return {
				...validation(expired ? reason : undefined, "exact", current),
				...(!sealing && changed.length ? { changed } : {}),
			};
		} catch {
			const reason = sealing ? "resource_observation_window_unprovable" : "resource_validation_failed";
			return validation(reason);
		}
	}
	private invalidation(token: ResourceVersionToken, sealing: boolean): string | undefined {
		if (!this.open || token.manager !== this || token.root !== this.root)
			return "resource_version_owner_changed";
		if (!sealing || !token.watching) return undefined; // Future reuse compares semantics; events only disprove a host execution window.
		if (this.status !== "watching" || this.changesSince(token).uncertain)
			return "resource_observation_window_unprovable";
		const precise = new Set(token.preciseContent.map(filesystemPathKey));
		const changed = this.events.some(
			(event) =>
				event.epoch > token.epoch &&
				[...token.observations.values()].some((dependency) => affects(dependency, event, precise)),
		);
		return changed ? "resource_observation_window_changed" : undefined;
	}
	changesSince(token: ResourceVersionToken): ResourceChangeSet {
		if (token.manager !== this || token.root !== this.root || !token.watching || this.status !== "watching")
			return { uncertain: true, paths: [] };
		const oldest = this.events[0]?.epoch ?? this.epoch;
		if (token.epoch < oldest && this.events.length >= speculativeConfig().filesystem.maxHistoryEntries)
			return { uncertain: true, paths: [] };
		// Excluded segments (a workspace's .git) are never snapshotted: their churn is no change to prepare for.
		const events = this.events.filter(
			(event) =>
				event.epoch > token.epoch &&
				!relativeFilesystemPath(this.root, event.path)
					?.split(/[\\/]/u)
					.some((segment) => this.snapshotExcludes.has(segment)),
		);
		return {
			uncertain: events.some((event) => event.type === "unknown"),
			paths: [...new Set(events.map((event) => event.path))],
		};
	}
	close() {
		this.status = "closed";
		this.watcher?.close();
		this.watcher = undefined;
		for (const precise of this.preciseWatches.values()) precise.watcher.close();
		this.preciseWatches.clear();
		this.events.length = 0;
	}
	private changed(changedPath: string, type: ResourceEvent["type"]) {
		if (!this.open) return;
		if (type === "unknown") this.status = "degraded";
		const absolute = path.resolve(changedPath);
		const event = { epoch: ++this.epoch, path: absolute, type };
		this.events.push(event);
		if (this.events.length > speculativeConfig().filesystem.maxHistoryEntries)
			this.events.splice(0, this.events.length - speculativeConfig().filesystem.maxHistoryEntries);
	}
	private async acquirePreciseWatches(dependencies: ReadonlyArray<ResourceDependency>) {
		return withFilesystemReadView(
			undefined,
			async (filesystem) => {
				const paths: string[] = [];
				for (const target of new Set(
					dependencies
						.filter((dependency) => dependency.scope === "content")
						.map((dependency) => dependency.path),
				)) {
					const key = filesystemPathKey(target);
					const existing = this.preciseWatches.get(key);
					if (existing) {
						existing.references++;
						paths.push(target);
						continue;
					}
					try {
						let descriptor: number | undefined;
						let watcher: FSWatcher;
						try {
							if (nativeFilesystem) descriptor = filesystem.open(target, 0x200000);
							watcher = watch(descriptor === undefined ? target : `/proc/self/fd/${descriptor}`, (event) =>
								this.changed(target, event),
							);
						} finally {
							if (descriptor !== undefined) closeSync(descriptor);
						}
						watcher.on("error", () => {
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
			},
			async (result) => result.release(),
		);
	}
}
const managers = new Map<string, ResourceVersionManager>();
const persistentManagers = new WeakMap<ResourceInputPersistence, Map<string, ResourceVersionManager>>();
/** One call's finite input leases; records, proof checks and publication stay with the existing manager/store. */
export class ResourceInputSession {
	private readonly lifecycle = new RuntimeLifecycleLane();
	private readonly roots = new Map<string, ResourceVersionManager>();
	private readonly published = new WeakSet<ResourceVersionToken>();
	private readonly persistence: ResourceInputPersistence;
	readonly memory: ResourceInputMemory;
	readonly contents: ContentChunkPool;
	readonly configuration: ResourceInputPersistence;
	readonly maxBytes: number;
	constructor(configuration: ResourceInputPersistence, maxBytes: number) {
		if (!Number.isFinite(maxBytes) || maxBytes <= 0) throw new Error("resource_snapshot_budget_invalid");
		this.configuration = configuration;
		this.maxBytes = maxBytes;
		this.memory = new ResourceInputMemory(maxBytes);
		this.contents = new ContentChunkPool(configuration.store, {
			...configuration.limits,
			maxStoredBytes: Math.min(
				configuration.limits?.maxStoredBytes ?? DEFAULT_CONTENT_LIMITS.maxStoredBytes,
				configuration.store.limits.maxBytes,
			),
		});
		this.lifecycle.own({ dispose: this.contents.retain() });
		this.persistence = {
			...configuration,
			contents: this.contents,
			memory: this.memory,
		};
	}
	get bytes(): number {
		return this.memory.bytes;
	}
	get remainingBytes(): number {
		return this.memory.remainingBytes;
	}
	run<Value>(operation: () => Promise<Value>): Promise<Value> {
		return this.lifecycle.admit(operation);
	}
	retain(token: ResourceVersionToken, metadataBytes?: number): ResourceVersionToken {
		return token.manager.retain(token, metadataBytes, this.memory);
	}
	private root(root: string) {
		if (this.lifecycle.sealed) throw new Error("resource_input_session_closed");
		const key = path.resolve(root);
		let manager = this.roots.get(key);
		if (!manager) {
			manager = resourceManager(key, this.persistence);
			this.lifecycle.own({ dispose: manager.acquire() });
			this.roots.set(key, manager);
		}
		return manager;
	}
	async capture(
		action: ActionKey | undefined,
		root: string,
		semantics: ActionSemanticsRegistry,
		maxBytes: number,
		providedInputs?: ReadonlyMap<string, ResourceInput>,
	): Promise<ResourceVersionToken> {
		this.root(root);
		return captureResourceVersion(
			action,
			root,
			semantics,
			Math.min(maxBytes, this.remainingBytes),
			providedInputs,
			this.persistence,
		);
	}
	private fileCandidates(root: string, signal: AbortSignal) {
		const manager = this.root(root);
		const store = this.configuration.store;
		let catalog: ReturnType<typeof store.inputPathIndex> | undefined;
		return async (target: string) => {
			if (!catalog) {
				const limit = Math.min(store.limits.maxCertificates, speculativeConfig().filesystem.maxHistoryEntries);
				const pending = manager
					.assertPersistentStore()
					.then(() => store.inputPathIndex(manager.root, limit, signal))
					.catch((error) => {
						if (catalog === pending) catalog = undefined;
						throw error;
					});
				catalog = pending;
			}
			const lookup = await catalog;
			return lookup(filesystemPathKey(path.resolve(manager.root, target)));
		};
	}
	/** A native reader verifies its own live file digest. It only needs that file's immutable blocks and receipt. */
	fileContentReader(root: string, signal: AbortSignal) {
		const candidates = this.fileCandidates(root, signal);
		const store = this.configuration.store;
		const load = async (id: Sha256Digest) => {
			const record = await store.getInputVersion(id, signal);
			if (!record) return;
			const sizes = new Map(record.artifacts.map((ref) => [ref.digest, ref.size]));
			const entries = parsePersistentResourcePayload(record.payload, root).entries;
			return new Map(entries.filter(([, file]) =>
				file.content?.chunks.every((ref) => !ref || sizes.get(ref.digest) === ref.size)));
		};
		// Consecutive paths often belong to one multi-file record. Retain at most that one metadata row.
		let last: { id: Sha256Digest; files: ReturnType<typeof load> } | undefined;
		return async function* (target: string) {
			const name = filesystemPathKey(target);
			for (const id of await candidates(name)) {
				signal.throwIfAborted();
				if (last?.id !== id) last = { id, files: load(id) };
				try {
					const file = (await last.files)?.get(name);
					if (file?.content) yield { manifest: file.content, computation: TimelineInterval.restore(file.computation) };
				} catch {
					signal.throwIfAborted();
					store.signal.throwIfAborted();
				}
			}
		};
	}
	/** Concurrent reads in one evaluation share discovery and a single validated lease per closure. */
	reader(root: string, signal: AbortSignal, retain = (token: ResourceVersionToken) => { this.lifecycle.own({ dispose: token.release }); }) {
		const manager = this.root(root);
		const store = this.configuration.store;
		const candidates = this.fileCandidates(root, signal);
		const restored = new Map<Sha256Digest, Promise<ResourceVersionToken | undefined>>();
		const session = this;
		const restore = async (id: Sha256Digest) => {
			const record = await store.getInputVersion(id, signal);
			if (!record) return;
			try {
				const token = await manager.restore(record, session.remainingBytes, signal);
				if (token) retain(token);
				return token;
			} catch {
				signal.throwIfAborted();
				store.signal.throwIfAborted();
				// An invalid historical input leaves the current filesystem authoritative.
			}
		};
		return async function* (target?: string, preparation?: PreparationLookup) {
			if (target === undefined || preparation || session.persistence.inputs) {
				for await (const { token } of manager.recover(() => session.remainingBytes, { signal, target, preparation })) {
					retain(token);
					yield token;
				}
				return;
			}
			for (const id of await candidates(target)) {
				signal.throwIfAborted();
				let pending = restored.get(id);
				if (!pending) restored.set(id, (pending = restore(id)));
				const token = await pending;
				if (token) yield token;
			}
		};
	}
	async publish(
		token: ResourceVersionToken,
		signal?: AbortSignal,
		inputs: readonly ResourceVersionToken[] = [],
	): Promise<boolean> {
		if (
			signal?.aborted ||
			this.lifecycle.sealed ||
			this.published.has(token) ||
			this.bytes > this.maxBytes ||
			!token.view?.retained ||
			token.view.contents !== this.contents ||
			!(token.view.resources.length || token.view.hasPreparedInputs)
		)
			return false;
		try {
			await token.manager.persist(token, inputs);
			this.published.add(token);
			return true;
		} catch {
			return false;
		}
	}
	dispose(): Promise<void> {
		return this.lifecycle.dispose(() => this.roots.clear());
	}
}
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
	persistence?: ResourceInputPersistence,
) {
	const dependencies = providedInputs
		? [...providedInputs].map(([path, input]): ResourceDependency => ({
				path,
				scope:
					input === null || inputContent(input)
						? "content"
						: "names" in input && input.names
							? "names"
							: "type",
			}))
		: action
			? resourceDependencies(action, root, actionSemantics)
			: undefined;
	if (dependencies?.length === 0 || (!dependencies && retainBytes === undefined))
		throw new Error("resource_dependencies_unproven");
	return resourceManager(root, persistence).capture(dependencies, retainBytes, providedInputs);
}
/** Shared notification cursor for preparation only; it cannot validate or seal an action's inputs. */
export function observeResourceChanges(root: string): Promise<ResourceVersionToken> {
	return resourceManager(root).observeChanges();
}
function resourceManager(root: string, persistence?: ResourceInputPersistence): ResourceVersionManager {
	const normalized = path.resolve(root);
	let scope = persistence ? persistentManagers.get(persistence) : managers;
	if (!scope) {
		scope = new Map();
		persistentManagers.set(persistence!, scope);
	}
	let manager = scope.get(normalized);
	if (!manager) {
		manager = new ResourceVersionManager(normalized, {
			watch: path.dirname(normalized) !== normalized,
			persistentInputs: persistence,
			onIdle: () => {
				if (scope.get(normalized) === manager) {
					manager!.close();
					scope.delete(normalized);
				}
			},
		});
		scope.set(normalized, manager);
	}
	return manager;
}
export async function validateResourceVersion(token: unknown, checkStamps = false): Promise<ResourceVersionValidation> {
	if (!Array.isArray(token))
		return isResourceVersionToken(token)
			? token.manager.validate(token, checkStamps)
			: validation("resource_version_missing");
	if (token.length === 1) return validateResourceVersion(token[0], checkStamps);
	const checked: ResourceVersionValidation[] = [];
	const groups = new Map<
		ResourceVersionManager,
		ResourceVersionToken & {
			observations: Map<string, ResourceObservation>;
		}
	>();
	try {
		if (!token.length) throw new Error("resource_version_missing");
		for (const source of token) {
			if (!isResourceVersionToken(source)) throw new Error("resource_version_missing");
			source.view?.assertComplete(true);
			let group = groups.get(source.manager);
			if (!group) groups.set(source.manager, (group = { ...source, observations: new Map() }));
			if (group.root !== source.root || group.physicalRoot !== source.physicalRoot)
				throw new Error("resource_version_conflict");
			for (const [key, entry] of source.observations) {
				const previous = group.observations.get(key);
				if (previous && (previous.fingerprint !== entry.fingerprint || (checkStamps && previous.stamp !== entry.stamp)))
					throw new Error("resource_version_conflict");
				group.observations.set(key, entry);
			}
		}
		await withFilesystemReadView(undefined, async () => {
			for (const group of groups.values()) checked.push(await group.manager.validate(group, checkStamps));
		});
		const expired = checked.find((result) => result.expired);
		if (expired)
			return {
				...validation(expired.reason, "exact", checked),
				changed: [...new Set(checked.flatMap((result) => result.changed ?? []))],
			};
		for (const source of token as ResourceVersionToken[]) source.view?.assertComplete(true);
		return validation(undefined, "exact", checked);
	} catch (error) {
		return validation(
			error instanceof Error ? error.message : "resource_validation_failed",
			"exact",
			checked,
		);
	}
}
export function invalidateResourceInputs(
	tokens: readonly ResourceVersionToken[],
	paths: readonly string[],
): readonly string[] {
	const dependencies = new Set<string>();
	const precise = new Set<string>();
	for (const token of tokens)
		for (const [key, dependency] of token.observations) {
			if (
				paths.some((changed) =>
					dependency.scope === "resolution"
						? containsFilesystemPath(changed, dependency.path)
						: affects(dependency, { path: changed, type: "rename", epoch: 0 }, precise),
				)
			)
				dependencies.add(key);
		}
	return [...new Set(tokens.flatMap((token) => token.view?.invalidate(dependencies) ?? []))];
}
export function resourceProofBytes(versions: readonly Pick<ResourceVersionToken, "observations">[]): number {
	let bytes = 0;
	for (const token of versions)
		for (const [key, entry] of token.observations) bytes += observationBytes(key, entry);
	return bytes;
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
		token.observations instanceof Map &&
		Array.isArray(token.preciseContent) &&
		typeof token.release === "function" &&
		token.manager instanceof ResourceVersionManager
	);
}
export const dependencyKey = (dependency: ResourceDependency) =>
	`${dependency.scope}:${filesystemPathKey(dependency.path)}`;
function affects(dependency: ResourceDependency, event: ResourceEvent, preciseContent: ReadonlySet<string>) {
	if (dependency.scope === "binding")
		return event.type === "rename" && filesystemPathKey(dependency.path) === filesystemPathKey(event.path);
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
		return (
			event.type !== "change" &&
			(dependencyPath === changed || dependencyPath === filesystemPathKey(path.dirname(event.path)))
		);
	}
	return true;
}
type FingerprintResult = {
	readonly value: unknown;
	readonly stamp: string;
	readonly bytesRead: number;
	readonly filesRead: number;
};
export function resourceStatSignature(stat: BigIntStats, target: string): string {
	return stat.isDirectory() ? digest([statStamp(stat), []]) : digest(["file", statStamp(stat), filesystemPathKey(target)]);
}
function fingerprintFile(content: StableFilesystemCapture) {
	return {
		value: {
			type: "file",
			mode: Number(content.stat.mode),
			size: content.bytesRead,
			hash: content.hash,
			resolved: filesystemPathKey(content.realPath),
		},
		stamp: resourceStatSignature(content.stat, content.realPath),
	};
}
function fingerprintDirectory(
	stat: BigIntStats,
	realPath: string,
	names: readonly string[],
	children: readonly {
		name: string;
		value: unknown;
		stamp: string;
	}[] = [],
) {
	return {
		value: {
			type: "directory",
			mode: Number(stat.mode),
			resolved: filesystemPathKey(realPath),
			order: names,
			children: children.map(({ name, value }) => ({ name, value })),
		},
		stamp: digest([statStamp(stat), children.map(({ name, stamp }) => [name, stamp])]),
	};
}
/** A sealed names view reserves its first verified kernel image, just as write/edit reserve file pins. */
function directoryObjectSlot(names: readonly string[], view?: ResourceReadView) {
	return view?.canRetainObject
		? {
				object: null,
				objectBytes: names.reduce((sum, name) => sum + Math.ceil((20 + Buffer.byteLength(name)) / 8) * 8, 48),
			}
		: {};
}
async function fingerprintDependencies(
	dependencies: ReadonlyArray<ResourceDependency>,
	realRoot: string,
	excludes: ReadonlySet<string>,
	view?: ResourceReadView,
	bindings?: {
		readonly root: string;
		readonly observations: Map<string, ResourceObservation>;
	},
	providedInputs?: ReadonlyMap<string, ResourceInput>,
	inputs?: ResourceInputPersistence["inputs"],
	filesystem?: FilesystemReadView,
): Promise<
	(ResourceObservation & {
		readonly bytesRead: number;
		readonly filesRead: number;
	})[]
> {
	if (!filesystem)
		return withFilesystemReadView(undefined, (active) =>
			fingerprintDependencies(dependencies, realRoot, excludes, view, bindings, providedInputs, inputs, active),
		);
	const files = new Map<string, Promise<FingerprintResult>>();
	const nearestExisting = missingResourceResolver(realRoot, filesystem);
	const aliases = new Map<
		string,
		Map<
			string,
			{
				paths: Set<string>;
				links: number;
			}
		>
	>();
	const recordBinding = (target: string, captured: { info: BigIntStats; link?: string }) => {
		if (!bindings || (!captured.info.isDirectory() && captured.link === undefined)) return;
		const dependency = { path: target, scope: "binding" as const };
		bindings.observations.set(dependencyKey(dependency), {
			...dependency,
			fingerprint: "binding",
			stamp: digest([statStamp(captured.info), captured.link]),
		});
	};
	const entries = new Map<string, ReturnType<typeof captureFilesystemEntry>>();
	const capture = (target: string) => {
		const key = filesystemPathKey(target);
		const previous = entries.get(key);
		if (previous) return previous;
		const pending = fingerprintIO(() => captureFilesystemEntry(target, undefined, filesystem))
			.then((entry) => {
				// Eager captures own namespace evidence; ordinary validation joins only pending reads.
				recordBinding(target, entry);
				return entry;
			})
			.finally(() => {
				if (!bindings) entries.delete(key);
			});
		entries.set(key, pending);
		return pending;
	};
	if (bindings) await capture(bindings.root);
	return mapFilesystem(dependencies, async (dependency) => {
		if (dependency.scope === "binding") return fingerprintBinding(dependency, filesystem);
		const key = dependencyKey(dependency);
		if (dependency.scope === "tree_content" || dependency.scope === "tree_entries")
			aliases.set(key, new Map());
		const { value, ...metrics } =
			dependency.scope === "resolution"
				? await fingerprintIO(async () => {
						const resolved = filesystemPathKey(await filesystem.realpath(dependency.path));
						return {
							value: resolved,
							stamp: resolved,
							bytesRead: 0,
							filesRead: 0,
						};
					})
				: await fingerprintPath(dependency.path, dependency.scope, key);
		const groups = [...(aliases.get(key)?.values() ?? [])]
			.map((group) =>
				Object.freeze({
					links: group.links,
					paths: Object.freeze([...group.paths].sort()),
				}),
			)
			.sort((left, right) => (left.paths[0]! < right.paths[0]! ? -1 : 1));
		const topology = groups.length ? { aliases: Object.freeze(groups) } : {};
		return {
			...dependency,
			...topology,
			fingerprint: digest({
				path: filesystemPathKey(dependency.path),
				scope: dependency.scope,
				value,
				...topology,
			}),
			...metrics,
		};
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
			if (bindings)
				for await (const entry of walkFilesystemPath(target, {
					capture,
					followFinal: followsResource(scope),
					view: filesystem,
				})) {
					if (entry.info && !entry.info.isDirectory() && entry.link === undefined && !entry.terminal) break;
				}
			captured = await capture(target);
		} catch (error) {
			if (!missingResource(error)) throw error;
			if (providedInputs && providedInputs.get(filesystemPathKey(target)) !== null) throw error;
			const nearest = await nearestExisting(target);
			view?.capture(target, {
				type: "missing",
				realPath: nearest.realPath,
				dependency,
				computation: TimelineInterval.own(new TimelineInterval(startedAt, performance.now())),
			});
			return {
				value: {
					exists: false,
					error: errorCode(error),
					resolved: nearest.realPath,
				},
				stamp: digest([filesystemPathKey(target), nearest.fingerprint]),
				bytesRead: 0,
				filesRead: 0,
			};
		}
		const { info, link } = captured;
		const realTarget = info.isSymbolicLink()
			? path.join(
					parentReal ?? (await fingerprintIO(() => filesystem!.realpath(path.dirname(target)))),
					path.basename(target),
				)
			: parentReal === undefined
				? await fingerprintIO(() => filesystem!.realpath(target))
				: path.join(parentReal, path.basename(target));
		// An entry name joined to a walked directory stays where the directory was proven to be.
		if (parentReal === undefined) assertInside(realRoot, realTarget);
		const identity = filesystemPathKey(realTarget);
		const cacheable = (scope === "content" && info.isFile()) || (scope === "names" && info.isDirectory());
		const token = !providedInputs && cacheable ? await inputs?.({ path: realTarget, scope }, info) : undefined;
		const hot = token?.view?.snapshotInput(realTarget, view);
		const supplied = providedInputs?.get(filesystemPathKey(target)) ?? hot;
		const suppliedContent = inputContent(supplied);
		if (
			providedInputs &&
			!(suppliedContent
				? info.isFile()
				: supplied && ("type" in supplied ? specialFileType(info) === supplied.type : info.isDirectory()))
		)
			throw new Error(suppliedContent ? "resource_input_not_regular" : "resource_input_type_changed");
		const metadata = { dependency, computation: captured.computation };
		const retainEntry = (entry: CapturedResource, reserved = 0) =>
			view?.capture(target, { realPath: realTarget, ...metadata, metadata, ...entry }, reserved);
		const grouping = aliases.get(dependency);
		if (grouping && info.isFile() && info.nlink > 1n) {
			const key = `${info.dev}:${info.ino}`;
			const links = Number(info.nlink);
			const previous = grouping.get(key);
			if (!Number.isSafeInteger(links) || (previous && previous.links !== links))
				throw new Error(`resource_file_changed:${target}`);
			const group = previous ?? { paths: new Set<string>(), links };
			group.paths.add(identity);
			grouping.set(key, group);
		}
		if (ancestors.has(identity)) throw new Error(`resource_symlink_cycle:${target}`);
		if (info.isSymbolicLink()) {
			let source: string | undefined;
			const links: unknown[] = [];
			if (followsResource(scope))
				await fingerprintIO(async () => {
					for await (const entry of walkFilesystemPath(target, {
						view: filesystem,
					})) {
						source = entry.path;
						if (entry.link !== undefined)
							links.push([filesystemPathKey(source), entry.link, statStamp(entry.info!)]);
					}
				});
			const followed =
				source === undefined
					? undefined
					: await fingerprintPath(
							source,
							scope,
							dependency,
							new Set(ancestors).add(identity),
							descend || scope === "tree_content" || scope === "tree_entries",
						);
			retainEntry({
				type: "alias",
				target: source && filesystemPathKey(source),
				link: link!,
				metadata: { ...metadata, size: Number(info.size) },
			});
			return {
				value: {
					type: "symlink",
					link,
					size: Number(info.size),
					mode: Number(info.mode),
					resolved: identity,
					target: followed?.value,
				},
				stamp: digest(["symlink", link, statStamp(info), links, followed?.stamp]),
				bytesRead: followed?.bytesRead ?? 0,
				filesRead: followed?.filesRead ?? 0,
			};
		}
		if (
			["stat", "type", "entry", "exists"].includes(scope) ||
			(scope === "entries" && !descend) ||
			(["names", "entries", "tree_entries"].includes(scope) && !info.isDirectory())
		) {
			const after = parentReal === undefined ? await fingerprintIO(() => filesystem!.stat(target)) : info;
			if (!sameFilesystemIdentity(info, after)) throw new Error(`resource_file_changed:${target}`);
			const size = ["stat", "entry"].includes(scope) ? Number(after.size) : undefined;
			const computation = supplied && "computation" in supplied ? supplied.computation : captured.computation;
			retainEntry({
				type: after.isDirectory() ? "directory" : after.isFile() ? "file" : "special",
				computation,
				metadata: { dependency, computation, size },
			});
			return {
				value: {
					type: specialFileType(after),
					mode: Number(after.mode),
					resolved: identity,
					size,
				},
				stamp: digest([statStamp(after), identity]),
				bytesRead: 0,
				filesRead: 0,
			};
		}
		if (info.isFile()) {
			const key = JSON.stringify([
				filesystemPathKey(target),
				identity,
				statStamp(info),
				String(info.uid),
				String(info.gid),
			]);
			const existing = files.get(key);
			if (existing) return { ...(await existing), bytesRead: 0, filesRead: 0 };
			const pending = (async () => {
				// Join concurrent reads of this identity; a settled capture stands for a later read only below, by the file's identity.
				const provided = suppliedContent;
				const chunked = !provided && view?.contents;
				const reserved = chunked
					? 96 + Math.ceil(Number(info.size) / speculativeConfig().filesystem.chunkBytes) * 112
					: provided
						? contentMemory(provided)
						: Number(info.size);
				const retain = !view?.verifyingInputs && (view?.reserve(reserved) ?? false);
				if (!retain && (chunked || (provided && (!hot || (view && !view.verifyingInputs)))))
					throw resourceBudgetError("resource_snapshot_budget_exceeded");
				const bytes = provided && (provided instanceof ChunkedContent ? provided : Buffer.from(provided));
				// Supplied bytes own data, never a host observation window; adoption validates them exactly.
				// A file unchanged since an earlier read of it, by the racy-git rule, needs no read unless its bytes are retained.
				const streamed =
					chunked &&
					(await chunked.capture((onChunk) =>
						fingerprintIO(() =>
							captureStableFile(target, chunked.limits.maxLogicalBytes, false, {
								stat: info,
								realPath: realTarget,
								view: filesystem,
								onChunk,
							}),
						),
					));
				const content: StableFilesystemCapture = streamed
					? streamed.capture
					: bytes
						? {
								content: bytes instanceof ChunkedContent ? undefined : bytes,
								bytesRead: bytes.length,
								hash: contentDigest(bytes).slice(7),
								stat: info,
								realPath: realTarget,
								computation: supplied && "computation" in supplied ? supplied.computation : undefined,
							}
						: (!retain && cachedCapture(info, realTarget)) ||
							(await fingerprintIO(() =>
								captureStableFile(target, retain ? Number(info.size) : undefined, retain, {
									stat: info,
									realPath: realTarget,
									retainObject: retain && view!.canRetainObject,
									view: filesystem,
								}),
							));
				assertInside(realRoot, content.realPath);
				retainEntry(
					{
						type: "file",
						content: view?.verifyingInputs ? undefined : streamed ? streamed.content : (bytes ?? content.content),
						computation: content.computation,
						version: view?.verifyingInputs ? { hash: content.hash, size: content.bytesRead } : undefined,
						object:
							content.object ??
							(bytes instanceof Buffer && retain && view!.canRetainObject ? null : undefined),
						realPath: content.realPath,
					},
					retain ? reserved : 0,
				);
				return {
					...fingerprintFile(content),
					bytesRead: bytes || content.shared ? 0 : content.bytesRead,
					filesRead: bytes || content.shared ? 0 : 1,
				};
			})().finally(() => files.delete(key));
			files.set(key, pending);
			return pending;
		}
		if (!info.isDirectory() || scope === "content")
			throw new Error(`unsupported_resource_type:${specialFileType(info)}:${target}`);
		if (descend && (scope === "tree_content" || scope === "tree_entries")) {
			const tree = await new WorkspaceStructureCapture(target, {
				view: filesystem,
				exclude: [...excludes],
				layered: true,
			}).captureVersions();
			if (!tree.snapshot.complete) throw new Error("resource_tree_budget_exceeded");
			for (const [relative, { captured }] of tree.entries) {
				const current = path.join(target, relative);
				entries.set(filesystemPathKey(current), Promise.resolve(captured));
				recordBinding(current, captured);
			}
			const children = await mapFilesystem([...tree.entries.keys()].sort(), async (relative) => ({
				name: relative,
				...(await fingerprintPath(
					path.join(target, relative),
					scope,
					dependency,
					relative ? new Set(ancestors).add(identity) : ancestors,
					false,
					relative ? path.dirname(path.join(realTarget, relative)) : parentReal,
				)),
			}));
			return {
				value: {
					type: "tree",
					entries: children.map(({ name, value }) => ({ name, value })),
				},
				stamp: digest(children.map(({ name, stamp }) => [name, stamp])),
				bytesRead: children.reduce((total, child) => total + child.bytesRead, 0),
				filesRead: children.reduce((total, child) => total + child.filesRead, 0),
			};
		}
		if (supplied && "names" in supplied && supplied.names) {
			retainEntry({
				type: "directory",
				entries: supplied.names,
				computation: supplied.computation,
				metadata: { dependency, computation: supplied.computation },
				...directoryObjectSlot(supplied.names, view),
			});
			return {
				...fingerprintDirectory(info, realTarget, supplied.names),
				bytesRead: 0,
				filesRead: 0,
			};
		}
		const directory = await captureFilesystemDirectory(target, {
			view: filesystem,
			stat: info,
			io: fingerprintIO,
			visit: async (entries) => {
				const selected =
					excludes.size && (scope === "tree_content" || scope === "tree_entries")
						? entries.filter((entry) => !excludes.has(entry.name))
						: entries;
				const descendants = new Set(ancestors).add(identity);
				const children =
					!descend || scope === "names"
						? []
						: await mapFilesystem(
								[...selected].sort((left, right) => left.name.localeCompare(right.name)),
								async (entry) => {
									const child = await fingerprintPath(
										path.join(target, entry.name),
										scope,
										dependency,
										descendants,
										scope !== "entries",
										realTarget,
									);
									return { name: entry.name, ...child };
								},
							);
				return { names: selected.map((entry) => entry.name), children };
			},
		});
		const { names, children } = directory.value!;
		retainEntry({
			type: "directory",
			entries: names,
			computation: directory.computation,
			...directoryObjectSlot(names, view),
		});
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
		const { info, link } = await fingerprintIO(() =>
			captureFilesystemEntry(dependency.path, "identity", filesystem),
		);
		stamp = digest([statStamp(info), link]);
	} catch (error) {
		if (!missingResource(error)) throw error;
		stamp = errorCode(error);
	}
	return {
		...dependency,
		fingerprint: "binding",
		stamp,
		bytesRead: 0,
		filesRead: 0,
	};
}
function specialFileType(value: FilesystemTypeEvidence) {
	const kind = filesystemEntryType(value);
	return kind === "char" ? "character_device" : kind === "block" ? "block_device" : kind;
}
function missingResourceResolver(realRoot: string, filesystem: FilesystemReadView) {
	// Negative queries commonly share ancestors. Join only concurrent reads in this proof batch;
	// settled observations are never cached for another query or Actor validation.
	type Resolution = {
		readonly realPath: string;
		readonly fingerprint: string;
	};
	const pending = new Map<string, Promise<Resolution>>();
	const resolve = (target: string): Promise<Resolution> => {
		const current = path.resolve(target);
		const existing = pending.get(current);
		if (existing) return existing;
		const task = (async () => {
			try {
				const real = await fingerprintIO(() => filesystem.realpath(current));
				const stat = await fingerprintIO(() => filesystem.stat(current));
				assertInside(realRoot, real);
				return {
					realPath: filesystemPathKey(real),
					fingerprint: digest([filesystemPathKey(real), statStamp(stat)]),
				};
			} catch (error) {
				if (!missingResource(error)) throw error;
			}
			const parent = path.dirname(current);
			if (parent === current) throw new Error(`resource_path_unresolved:${target}`);
			const ancestor = await resolve(parent);
			return {
				realPath: filesystemPathKey(path.join(ancestor.realPath, path.basename(current))),
				fingerprint: ancestor.fingerprint,
			};
		})().finally(() => pending.delete(current));
		pending.set(current, task);
		return task;
	};
	return resolve;
}
function statStamp(stat: BigIntStats): string {
	return [
		stat.dev,
		stat.ino,
		stat.mode,
		stat.nlink,
		stat.rdev,
		stat.size,
		stat.mtimeNs,
		stat.ctimeNs,
		stat.birthtimeNs,
	].join(":");
}
function digest(value: unknown): string {
	return hash("sha256", JSON.stringify(value));
}
function assertInside(realRoot: string, target: string): void {
	if (!containsFilesystemPath(realRoot, target))
		throw new Error(`resource_symlink_escapes_workspace:${target}`);
}
function validation(
	reason?: string,
	mode: ResourceValidationMetrics["mode"] = "exact",
	observed: ReadonlyArray<Pick<ResourceValidationMetrics, "bytesRead" | "filesRead">> = [],
): ResourceVersionValidation {
	return {
		expired: reason !== undefined,
		...(reason === undefined ? {} : { reason }),
		mode,
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
const priorityIO = new AsyncLocalStorage<true>();
export const fingerprintIO = (() => {
	let active = 0;
	const waiting: Array<() => void> = [];
	const priority: Array<() => void> = [];
	return async <Value>(task: () => Promise<Value>): Promise<Value> => {
		if (active < speculativeConfig().filesystem.concurrency) active++;
		else await new Promise<void>((resolve) => (priorityIO.getStore() ? priority : waiting).push(resolve));
		try {
			return await task();
		} finally {
			const next = priority.shift() ?? waiting.shift();
			if (next) next();
			else active--;
		}
	};
})();

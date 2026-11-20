import { asRecord, deepFreeze, isFiniteNumber, isRecord, isSafeCount, positiveCount } from "./primitives.ts";
import { AsyncLocalStorage } from "node:async_hooks";
import type { ProvenanceTaint } from "./provenance-certificate.ts";
import type { PatternAwarePresetID } from "./pattern-aware.ts";
type ReadonlyConfig<Value> = Value extends object
	? { readonly [Key in keyof Value]: ReadonlyConfig<Value[Key]> }
	: Value;
export type SpeculativeConfig = ReadonlyConfig<typeof DEFAULT_SPECULATIVE_CONFIG>;
export const DEFAULT_SPECULATIVE_CONFIG = deepFreeze({
	drafter: {
		drafterMaxDepth: 3,
		drafterDeterministicCandidates: 1,
		drafterTemperatureMin: 0.7,
		drafterTemperatureMax: 0.7,
		drafterPatternHints: false,
	},
	nativeBuild: {
		domainValues: 4096,
		domainLimit: 256,
		domainRecords: 65536,
		domainRoots: 4,
		domainAncestors: 64,
		domainEventBytes: 4096,
		domainInputBytes: 2 * 1024 * 1024,
		domainInputFrames: 256,
		domainTraceBytes: 512 * 1024 * 1024,
		domainRingBytes: 16 * 1024 * 1024,
		domainExchangeMs: 2000,
		domainServerMs: 1000,
		domainPollMs: 100,
		domainRetirePollUs: 1000,
		domainParentDepth: 256,
		domainUpdateAttempts: 32,
		domainPendingObjects: 8,
		domainDrainAttempts: 8,
		domainLinks: 64,
		heldStringBytes: 8192,
		heldBridgeBytes: 1024 * 1024,
		heldAuxiliaryBytes: 4096,
		heldRouteFields: 32,
		heldVectorEntries: 1048576,
		heldLineBytes: 32768,
		heldPollMs: 5000,
		heldReadBytes: 65536,
	},
	selfSpeculation: {
		enabled: false,
		endpoint: "http://127.0.0.1:8000",
		requestIDField: "request_id",
		candidatePath: "/self-speculation/candidates",
		forkPath: "/self-speculation/fork",
		clearPath: "/self-speculation/clear",
		capabilitiesPath: "/self-speculation/capabilities",
		timeoutMs: 2000,
		maxCandidates: 8,
		maxDraftTokens: 28,
		actorProfile: "tagged_json",
		draftFormat: "auto",
		draftBoundary: "auto",
		forkEnabled: true,
		forkActionEnabled: true,
		forkActionMinConfidence: 0.9,
		forkTransport: "provider" as "provider" | "sidecar" | "drafter",
		forkTemperature: 0,
		forkMaxAttempts: 5,
		forkRetryStreamUpdates: 50,
		forkBoundaryStreamUpdates: 10,
		forkDecoder: "auto",
		forkForcedPrefix: "auto",
		requireLogprobs: false,
	},
	pattern: {
		enabled: true,
		presets: [] as readonly PatternAwarePresetID[],
		multiStepEnabled: true,
		maxContextLength: 4,
		beamWidth: 4,
		maxPredictionDepth: 6,
		maxFutureGap: 2,
		futureGapCoverage: 0.25,
		decayHalfLifeEvents: 2048,
		minOccurrences: 2,
		maxPatterns: 4096,
	},
	subcomputation: {
		exposeToControlledProcesses: false,
		pythonImports: true,
		maxStepResultBytes: 16 * 1024 * 1024,
		maxStepObjectNodes: 65536,
		maxStepObjectDepth: 64,
		persistTimelineIntervals: true,
		socketRequestBytes: 24 * 1024 * 1024,
		socketTimeoutMs: 30000,
		maxPendingRequests: 64,
	},
	processReuse: {
		maxObservedInputs: 8192,
		invocationPathBytes: 8192,
		startupTimeoutMs: 10000,
		terminateGraceMs: 1000,
		diagnosticTimeoutMs: 2000,
		hostOutputBytes: 16 * 1024 * 1024,
		tracePreviewBytes: 1024 * 1024,
		diagnosticCharacters: 4096,
		interpreterImages: 16,
		interpreterProgramHeaders: 64,
		maxInheritedDescriptors: 64,
		contextKeyBytes: 64 * 1024,
		endpointCharacters: 4096,
		syscallNameCharacters: 64,
		diagnosticSyscalls: 16,
		persistCompletedCertificates: true,
		maxOutputEvents: 65536,
		maxArguments: 4096,
		maxArgumentBytes: 1048576,
		sensitivePaths: [
			"~/.ssh",
			"~/.gnupg",
			"~/.aws",
			"~/.azure",
			"~/.kube",
			"~/.docker/config.json",
			"~/.config/gcloud",
			"~/.config/gh/hosts.yml",
			"~/.git-credentials",
			"~/.netrc",
			"~/.npmrc",
			"~/.pypirc",
			"~/.pi/agent/auth.json",
			"~/.codex/auth.json",
		],
		volatilePaths: ["/proc", "/sys", "/dev", "/run", "/tmp", "/var/tmp"],
		contentAddressedPaths: [] as string[],
		kernelConfigurationPaths: [] as string[],
		stableDevices: ["/dev/null", "/dev/tty", "/dev/zero", "/dev/full"],
		traceOutputBytes: 33554432,
		privateFileBytes: 33554432,
		privateTreeBytes: 134217728,
		actorObservationMode: "provenance" as "none" | "provenance",
		environmentMode: "exact" as "exact" | "allowlist" | "denylist" | "isolated",
		environmentKeys: [] as string[],
		isolatedEnvironment: {} as Record<string, string>,
		replayAcceptedTaints: [] as ProvenanceTaint[],
		maxProcesses: 64,
		maxRequestBytes: 4 * 1024 * 1024,
		maxCaptureBytes: 512 * 1024 * 1024,
		maxCapturedOutputBytes: 32 * 1024 * 1024,
		outputChunkBytes: 1024 * 1024,
		learnedLaunches: 64,
		diagnosticReasons: 4,
		excludedPaths: [] as string[],
	},
	filesystem: {
		windowsObjectLeaseCommand: ["python"],
		objectLeaseTimeoutMs: 10000,
		objectLeaseStartupTimeoutMs: 1000,
		objectLeaseIdleMs: 30000,
		objectLeaseRetryMs: 30000,
		maxWindowsLeaseRequests: 64,
		timestampRestoreBatch: 1024,
		utilityOutputBytes: 4 * 1024 * 1024,
		asyncHashThresholdBytes: 65536,
		timestampQuietByFilesystem: {
			"0xef53": 100,
			"0x58465342": 100,
			"0x9123683e": 100,
			"0xf2f52010": 100,
		} as Record<string, number>,
		directoryReadBytes: 32768,
		directoryMetadataBytes: 524288,
		maxManifestChunks: 65536,
		maxLogicalBytes: 2147483648,
		maxStoredBytes: 1073741824,
		maxOpenChunks: 1024,
		overlayReadyMs: 5000,
		overlayExitMs: 2000,
		diagnosticBytes: 16384,
		overlayRequests: 32,
		overlayResolved: 8,
		overlayDegraded: 16,
		overlayNegativeMs: 30000,
		overlayPollMs: 10,
		concurrency: 12,
		timestampQuietMs: 2000,
		clockAdvanceTimeoutMs: 2000,
		clockPollMs: 1,
		maxDigestEntries: 65536,
		maxSymlinkDepth: 40,
		stabilityAttempts: 3,
		sandboxDriver: "in_memory" as "in_memory" | "overlayfs" | "git",
		maxPinnedObjects: 64,
		maxHistoryEntries: 4096,
		maxTreeEntries: 100000,
		maxTreeBytes: 512 * 1024 * 1024,
		excludes: [".git"],
		chunkBytes: 1024 * 1024,
		maxReadBytes: 16 * 1024 * 1024,
	},
	storage: {
		...({} as { root?: string }),
		gcIntervalMs: 300000,
		orphanGraceMs: 300000,
		maxCertificates: 4096,
		maxBytes: 2 * 1024 * 1024 * 1024,
		sqliteTimeoutMs: 1000,
		sqliteRetryInitialMs: 1,
		sqliteRetryMaxMs: 16,
		sqliteRetryMultiplier: 2,
		maxRecordBytes: 8 * 1024 * 1024,
	},
	search: {
		braceExpandMax: 10000,
		executable: "",
		maxInputBytes: 8 * 1024 * 1024,
		maxEntries: 65536,
		maxResultBytes: 1024 * 1024,
		maxDirectoryEntries: 256,
		imageSniffBytes: 4100,
		displayBytes: 50 * 1024,
		lineCharacters: 500,
		readOffset: 1,
		readLimit: 2000,
		grepLimit: 100,
		findLimit: 1000,
		lsLimit: 500,
	},
	prediction: {
		concurrentProposalPolicy: "first_produced" as "first_produced" | "all",
		sessions: 64,
		calibrationBuckets: 128,
		calibrationWindow: 32,
		calibrationSuccessPrior: 1,
		calibrationFailurePrior: 1,
		decoderEvidenceCapacity: 4,
		decoderEvidenceMinimum: 2,
		actionEvidenceCapacity: 2,
		actionEvidenceMinimum: 1,
		actionModelEntries: 128,
		bindingConstantMinimum: 4,
		bindingSupportMultiplier: 2,
		sampleMultiplier: 4,
		outputDiscriminants: [] as string[],
		maxHints: 4,
		failedCommands: 4,
		retryProbability: 0.25,
		initialProbability: 0.5,
		smoothing: 0.5,
		maxBindingVariants: 32,
		persistIntervalMs: 30000,
		maxValueCharacters: 4096,
		maxValueItems: 256,
		structuralDepth: 4,
	},
	runtime: {
		strictHostVersionCheck: false,
		hostVersions: ["0.84.1"],
		enabled: false,
		reuseEnabled: true,
		drafterEnabled: true,
		prepareObservedOperations: false,
		candidateLimit: 2,
		maxConcurrentActions: 8,
		completedProcesses: 64,
		inputDistanceRange: 1024,
		recentEvents: 50,
		resourceCacheMaxEntries: 512,
		resourceCacheMaxBytes: 268435456,
		eventHistory: 256,
		hotFraction: 0.8,
		preparedOperations: 128,
		preparedCommands: 4,
		logCharacters: 120,
		sourceFailureCharacters: 200,
	},
});
export type SpeculativeConfigInput = {
	readonly [K in keyof SpeculativeConfig]?: SpeculativeConfig[K] extends object
		? Partial<SpeculativeConfig[K]>
		: SpeculativeConfig[K];
};
export const PROVENANCE_TAINTS = Object.freeze([
	"network",
	"ipc",
	"clock",
	"random",
	"pid_observation",
	"descriptor_observation",
	"interactive_io",
	"untracked_fd",
	"confinement_observation",
	"unsupported_syscall",
	"escaped_sandbox",
	"mutable_input",
	"trace_incomplete",
] as const);
const choices: Readonly<Record<string, readonly string[]>> = {
	"processReuse.actorObservationMode": ["none", "provenance"],
	"processReuse.environmentMode": ["exact", "allowlist", "denylist", "isolated"],
	"prediction.concurrentProposalPolicy": ["all", "first_produced"],
	"selfSpeculation.forkTransport": ["provider", "sidecar", "drafter"],
	"filesystem.sandboxDriver": ["in_memory", "overlayfs", "git"],
};
const positiveFields = new Set([
	"filesystem.concurrency",
	"filesystem.directoryMetadataBytes",
	"filesystem.directoryReadBytes",
	"filesystem.chunkBytes",
	"filesystem.stabilityAttempts",
	"filesystem.objectLeaseTimeoutMs",
	"filesystem.objectLeaseStartupTimeoutMs",
	"filesystem.maxWindowsLeaseRequests",
	"subcomputation.maxPendingRequests",
	"subcomputation.socketTimeoutMs",
	"storage.sqliteTimeoutMs",
	"storage.sqliteRetryInitialMs",
	"storage.sqliteRetryMaxMs",
	"storage.sqliteRetryMultiplier",
]);
const resolvedConfigs = new WeakSet<object>([DEFAULT_SPECULATIVE_CONFIG]);
export function resolveSpeculativeConfig(input?: SpeculativeConfigInput): SpeculativeConfig {
	if (input === undefined) return DEFAULT_SPECULATIVE_CONFIG;
	if (resolvedConfigs.has(input)) return input as SpeculativeConfig;
	const fail = (field: string): never => {
		throw new Error(`Invalid speculative configuration: ${field}`);
	};
	const envKey = (name: string) => name.length > 0 && !/[=\0]/u.test(name);
	const copy = (value: unknown, expected: unknown, field: string): unknown => {
		if (value === undefined || value === expected) return expected;
		if (typeof expected === "number") {
			const minimum = field.startsWith("nativeBuild.") || positiveFields.has(field) ? 1 : 0;
			if (!isFiniteNumber(value) || value < minimum) return fail(field);
			if (
				Number.isInteger(expected) &&
				field !== "selfSpeculation.forkTemperature" &&
				!Number.isSafeInteger(value)
			)
				return fail(field);
			if (field.endsWith("Ms") && value > 2147483647) return fail(field);
			if (/(?:Probability|hotFraction|futureGapCoverage|forkActionMinConfidence)$/.test(field) && value > 1)
				return fail(field);
		} else if (Array.isArray(expected)) {
			if (!Array.isArray(value) || value.some((item) => typeof item !== "string" || item.includes("\0")))
				return fail(field);
			if (
				field === "processReuse.replayAcceptedTaints" &&
				!value.every((item) => PROVENANCE_TAINTS.includes(item))
			)
				return fail(field);
			if (field === "processReuse.environmentKeys" && !value.every(envKey)) return fail(field);
			if (field === "filesystem.windowsObjectLeaseCommand" && (!value.length || !value.every(Boolean)))
				return fail(field);
			return Object.freeze([...value]);
		} else if (isRecord(expected)) {
			if (!isRecord(value)) return fail(field);
			if (field === "processReuse.isolatedEnvironment" || field === "filesystem.timestampQuietByFilesystem") {
				for (const [key, entry] of Object.entries(value)) {
					const valid =
						field === "processReuse.isolatedEnvironment"
							? envKey(key) && typeof entry === "string" && !entry.includes("\0")
							: /^(?:0x[\da-f]+|\d+)$/iu.test(key) && isSafeCount(entry);
					if (!valid) return fail(`${field}.${key}`);
				}
				return Object.freeze({ ...value });
			}
			const result = { ...expected };
			for (const [key, entry] of Object.entries(value)) {
				const name = field ? `${field}.${key}` : key;
				if (name === "storage.root") {
					if (entry === undefined) continue;
					if (typeof entry !== "string" || !path.isAbsolute(entry)) return fail(name);
					result[key] = entry;
				} else {
					if (!Object.hasOwn(expected, key)) return fail(name);
					result[key] = copy(entry, expected[key], name);
				}
			}
			return Object.freeze(result);
		} else if (
			typeof value !== typeof expected ||
			(choices[field] && !choices[field].includes(String(value)))
		)
			return fail(field);
		return value;
	};
	const config = copy(input, DEFAULT_SPECULATIVE_CONFIG, "") as SpeculativeConfig;
	const { storage, prediction, filesystem, nativeBuild } = config;
	if (storage.sqliteRetryInitialMs > storage.sqliteRetryMaxMs) fail("storage.sqliteRetryMaxMs");
	if (prediction.calibrationSuccessPrior + prediction.calibrationFailurePrior <= 0)
		fail("prediction calibration prior");
	if (nativeBuild.domainValues < 8 || nativeBuild.domainValues * 8 + 16 > 2147483647)
		fail("nativeBuild.domainValues");
	if (nativeBuild.domainRingBytes < 4096 || Math.log2(nativeBuild.domainRingBytes) % 1)
		fail("nativeBuild.domainRingBytes");
	if (filesystem.chunkBytes < 4 || filesystem.chunkBytes > filesystem.maxReadBytes)
		fail("filesystem.chunkBytes");
	resolvedConfigs.add(config);
	return config;
}
const configuration = new AsyncLocalStorage<SpeculativeConfiguration>();
const configurationScopes = new WeakMap<SpeculativeConfig, SpeculativeConfiguration>();
/** Owned by a Host or service; async callbacks retain this reference across settings updates. */
export class SpeculativeConfiguration {
	current: SpeculativeConfig;
	constructor(input?: SpeculativeConfigInput) {
		this.current = resolveSpeculativeConfig(input);
	}
	update(input?: SpeculativeConfigInput): void {
		this.current = resolveSpeculativeConfig(input);
	}
	run<T>(operation: () => T): T {
		return configuration.run(this, operation);
	}
	bind<T extends object>(target: T): T {
		const bound = new Set<PropertyKey>();
		for (
			let owner: object | null = target;
			owner && owner !== Object.prototype;
			owner = Object.getPrototypeOf(owner)
		) {
			for (const key of Reflect.ownKeys(owner)) {
				if (key === "constructor" || bound.has(key)) continue;
				bound.add(key);
				const method = Object.getOwnPropertyDescriptor(owner, key)?.value;
				if (typeof method !== "function") continue;
				const prototype = owner === target ? undefined : owner;
				Object.defineProperty(target, key, {
					configurable: true,
					writable: true,
					value: (...args: unknown[]) =>
						(configuration.getStore() ?? this).run(() =>
							Reflect.apply(prototype ? Reflect.get(prototype, key, target) : method, target, args),
						),
				});
			}
		}
		return target;
	}
}
export const currentSpeculativeConfiguration = (): SpeculativeConfiguration =>
	configuration.getStore() ?? new SpeculativeConfiguration();
export const speculativeConfig = (): SpeculativeConfig =>
	configuration.getStore()?.current ?? DEFAULT_SPECULATIVE_CONFIG;
export function withSpeculativeConfig<T>(
	config: SpeculativeConfig | SpeculativeConfiguration,
	operation: () => T,
): T {
	if (config instanceof SpeculativeConfiguration) return config.run(operation);
	let scope = configurationScopes.get(config);
	if (!scope) {
		scope = new SpeculativeConfiguration(config);
		configurationScopes.set(config, scope);
	}
	return scope.run(operation);
}
export function nativeBuildDefinitions(config: SpeculativeConfig = speculativeConfig()): string[] {
	const entries = {
		...config.nativeBuild,
		heldOutputEvents: config.processReuse.maxOutputEvents,
		heldOutputBytes: config.processReuse.maxCapturedOutputBytes,
	};
	return Object.entries(entries).map(
		([key, value]) => `-DPI_${key.replace(/[A-Z]/gu, (letter) => "_" + letter).toUpperCase()}=${value}`,
	);
}
export function projectProcessEnvironment(
	environment: Readonly<Record<string, string>>,
	policy: Pick<
		SpeculativeConfig["processReuse"],
		"environmentMode" | "environmentKeys" | "isolatedEnvironment"
	> = speculativeConfig().processReuse,
): Readonly<Record<string, string>> {
	const { environmentMode: mode, environmentKeys, isolatedEnvironment } = policy;
	const keys = mode === "allowlist" || mode === "denylist" ? new Set(environmentKeys) : undefined;
	const entries = Object.entries(mode === "isolated" ? isolatedEnvironment : environment);
	return Object.freeze(
		Object.fromEntries(
			entries.filter(
				([key]) =>
					key !== "PI_SUBCOMPUTE_SOCKET" &&
					key !== "PI_SUBCOMPUTE_CONTEXT" &&
					key !== "PI_PYTHON_COMPILE_SOCKET" &&
					key !== "PI_PYTHON_COMPILE_CONTEXT" &&
					(!keys || keys.has(key) === (mode === "allowlist")),
			),
		),
	);
}
import { getAgentDir } from "@earendil-works/pi-coding-agent";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import type { SpeculativeAgentSettingsInput } from "./agent-integration.ts";
export function positiveInteger<F extends number | undefined>(
	value: unknown,
	fallback: F,
	minimum = 1,
): number | F {
	const integer = typeof value === "number" ? Math.floor(value) : NaN;
	return isSafeCount(integer, minimum) ? integer : fallback;
}
export function nonNegativeInteger(value: unknown, fallback: number): number {
	return positiveInteger(value, fallback, 0);
}
/** Node timers otherwise turn an overflowing delay into a 1 ms timeout. */
export function milliseconds<F extends number | undefined>(value: unknown, fallback: F): number | F {
	return isSafeCount(value, 0, 2147483647) ? value : fallback;
}
export const positiveMilliseconds = <F extends number | undefined>(value: unknown, fallback: F): number | F =>
	milliseconds(value, fallback) || fallback;
export function nonNegativeNumber<F extends number | undefined>(value: unknown, fallback: F): number | F {
	return isFiniteNumber(value) && value >= 0 ? value : fallback;
}
export function probability<F extends number | undefined>(value: unknown, fallback: F): number | F {
	return isFiniteNumber(value) && value >= 0 && value <= 1 ? value : fallback;
}
export function booleanOr(value: unknown, fallback: boolean): boolean {
	return typeof value === "boolean" ? value : fallback;
}
/** Defaults provide the shape; only exceptional fields need their own parser. */
export function settingsParser<Defaults extends Record<string, unknown>>(
	schema: Defaults,
	fields: Partial<{
		readonly [K in keyof Defaults]: (value: unknown, fallback: Defaults[K]) => Defaults[K];
	}> = {},
) {
	const parse = (input?: Readonly<Record<string, unknown>>, base?: Defaults): Defaults =>
		Object.fromEntries(
			Object.entries(schema).map(([key, fallback]) => {
				const value = input && Object.hasOwn(input, key) ? input[key] : base?.[key];
				const parser = fields[key];
				if (parser) return [key, parser(value, fallback as Defaults[string])];
				if (typeof fallback === "boolean") return [key, booleanOr(value, fallback)];
				if (typeof fallback === "number") return [key, positiveInteger(value, fallback)];
				return [key, typeof value === "string" && value.trim() ? value.trim() : fallback];
			}),
		) as Defaults;
	return { defaults: schema, parse };
}
export interface ExecutionRoutingSettings {
	readonly primary?: boolean;
	readonly nativeFallback?: boolean;
}
export interface SpeculativeActionPackageSettings extends SpeculativeAgentSettingsInput {
	readonly config?: SpeculativeConfigInput;
	readonly draftModel?: string;
	readonly executionStoreMaxEntries?: number;
	readonly executionStoreMaxBytes?: number;
	readonly executionRouting?: ExecutionRoutingSettings;
	/** Captured searches share a qualified Actor/producer contract by default; native opts out. */
	readonly searchExecution?: "native" | "captured";
}
export type SpeculativeSettingsScope = "global" | "project";
type SettingsOverlay = Record<string, unknown>;
/** Extension-owned configuration; Pi's settings schema remains untouched. */
export class SpeculativeActionSettingsStore {
	private readonly layers: Partial<Record<SpeculativeSettingsScope, SettingsOverlay>> = {};
	private scopeValue: SpeculativeSettingsScope = "global";
	private writeQueue: Promise<void> = Promise.resolve();
	/** Hand-edited files that failed to parse: ignored when read and never replaced by a save. */
	private readonly unreadable = new Set<string>();
	readonly cwd: string;
	readonly agentDirectory: string;
	constructor(cwd: string, agentDirectory = getAgentDir()) {
		this.cwd = cwd;
		this.agentDirectory = agentDirectory;
	}
	/** An untrusted checkout cannot configure the extension; a trusted one still cannot redirect conversation or keys. */
	async load(trusted = true): Promise<void> {
		const read = (file: string) => readSettings(file, this.unreadable);
		[this.layers.global, this.layers.project] = await Promise.all([
			read(this.globalPath),
			trusted ? read(this.projectPath).then(userScoped) : undefined,
		]);
		this.scopeValue = this.layers.project || this.unreadable.has(this.projectPath) ? "project" : "global";
	}
	get scope(): SpeculativeSettingsScope {
		return this.scopeValue;
	}
	setScope(scope: SpeculativeSettingsScope): void {
		this.scopeValue = scope;
	}
	effective(): SpeculativeActionPackageSettings | undefined {
		return this.editable("project");
	}
	editable(scope = this.scopeValue): SpeculativeActionPackageSettings | undefined {
		return applyOverlay(this.layers.global, scope === "project" ? this.layers.project : undefined);
	}
	/** Each scope persists only its differences from the layer below: defaults for global, global for project. */
	setEffective(
		value: SpeculativeActionPackageSettings,
		inherited = this.scopeValue === "project" ? this.editable("global") : undefined,
	): void {
		const overlay = diffRecord((inherited as SettingsOverlay) ?? {}, value as SettingsOverlay);
		this.layers[this.scope] = this.scope === "project" ? userScoped(overlay) : overlay;
		this.persistSelected();
	}
	clear(): void {
		delete this.layers[this.scope];
		this.persistSelected();
	}
	private persistSelected(): void {
		const snapshot = this.layers[this.scope];
		const target = this.scopeValue === "project" ? this.projectPath : this.globalPath;
		this.writeQueue = this.writeQueue
			.catch(() => undefined)
			.then(async () => {
				if (
					this.unreadable.delete(target) &&
					(await readSettings(target, this.unreadable), this.unreadable.has(target))
				)
					throw new Error(`${target} is not valid JSON; fix or remove it before saving`);
				await (await import("./filesystem-evidence.ts")).writeJsonFile(target, snapshot, 2);
			});
		void this.writeQueue.catch(() => undefined);
	}
	flush(): Promise<void> {
		return this.writeQueue;
	}
	private get globalPath(): string {
		return path.join(this.agentDirectory, "speculative-action.json");
	}
	private get projectPath(): string {
		return path.join(this.cwd, ".pi", "speculative-action.json");
	}
}
async function readSettings(file: string, unreadable: Set<string>): Promise<SettingsOverlay | undefined> {
	try {
		const parsed: unknown = JSON.parse((await readFile(file, "utf8")).replace(/^﻿/u, ""));
		return isRecord(parsed) && Object.keys(parsed).length > 0 ? parsed : undefined;
	} catch (error) {
		if (error instanceof SyntaxError) return void unreadable.add(file);
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
}
/** Network endpoints and credential sources remain user-scoped. */
function userScoped(project: SettingsOverlay | undefined): SettingsOverlay | undefined {
	if (!project) return;
	const filter = (layer: SettingsOverlay) => {
		if (!isRecord(layer.selfSpeculation)) return layer;
		const { endpoint, apiKeyEnv, ...selfSpeculation } = layer.selfSpeculation;
		return { ...layer, selfSpeculation };
	};
	return filter(isRecord(project.config) ? { ...project, config: filter(project.config) } : project);
}
function applyOverlay(
	base: SettingsOverlay | undefined,
	overlay: SettingsOverlay | undefined,
): SettingsOverlay | undefined {
	if (!base && !overlay) return undefined;
	const result: SettingsOverlay = Object.assign(Object.create(null), structuredClone(base));
	for (const [key, value] of Object.entries(overlay ?? {})) {
		if (value === null) {
			delete result[key];
		} else if (isRecord(value)) {
			const nested = applyOverlay(isRecord(result[key]) ? result[key] : undefined, value);
			if (nested) result[key] = nested;
			else delete result[key];
		} else {
			result[key] = structuredClone(value);
		}
	}
	return Object.keys(result).length ? { ...result } : undefined;
}
function diffRecord(base: SettingsOverlay, target: SettingsOverlay): SettingsOverlay | undefined {
	const patch: SettingsOverlay = Object.create(null);
	for (const key of new Set([...Object.keys(base), ...Object.keys(target)])) {
		const before = Object.hasOwn(base, key) ? base[key] : undefined;
		const after = target[key];
		if (!Object.hasOwn(target, key)) patch[key] = null;
		else if (!isDeepStrictEqual(before, after))
			patch[key] = isRecord(before) && isRecord(after) ? diffRecord(before, after) : structuredClone(after);
	}
	return Object.keys(patch).length ? { ...patch } : undefined;
}
export interface DrafterToolDefinition {
	readonly name: string;
	readonly description?: string;
	readonly inputSchema?: unknown;
}
export type DrafterRequestSettings = Readonly<typeof DRAFTER_DEFAULTS>;
const { defaults: DRAFTER_DEFAULTS, parse: parseDrafterSettings } = settingsParser(
	DEFAULT_SPECULATIVE_CONFIG.drafter,
	{
		drafterMaxDepth: nonNegativeInteger,
		drafterDeterministicCandidates: nonNegativeInteger,
		drafterTemperatureMin: nonNegativeNumber,
		drafterTemperatureMax: nonNegativeNumber,
	},
);
export const DEFAULTS = {
	...DEFAULT_SPECULATIVE_CONFIG.runtime,
	...DRAFTER_DEFAULTS,
};
export const clampCandidateLimit = positiveCount;
export function normalizeDrafterRequestSettings(value: unknown): DrafterRequestSettings {
	const result = parseDrafterSettings(asRecord(value), speculativeConfig().drafter);
	const lower = result.drafterTemperatureMin;
	const upper = result.drafterTemperatureMax;
	return {
		...result,
		drafterTemperatureMin: Math.min(lower, upper),
		drafterTemperatureMax: Math.max(lower, upper),
	};
}
/** Stratify non-deterministic requests across the configured range for any proposal count. */
export function drafterRequestTemperature(
	proposalIndex: number,
	proposalCount: number,
	settings: DrafterRequestSettings,
): number {
	const count = clampCandidateLimit(proposalCount);
	const index = Math.max(0, Math.min(count - 1, Math.floor(proposalIndex)));
	const deterministic = Math.min(count, settings.drafterDeterministicCandidates);
	if (index < deterministic) return 0;
	const stochasticCount = count - deterministic;
	if (stochasticCount === 1) return (settings.drafterTemperatureMin + settings.drafterTemperatureMax) / 2;
	return (
		settings.drafterTemperatureMin +
		((settings.drafterTemperatureMax - settings.drafterTemperatureMin) * (index - deterministic)) /
			(stochasticCount - 1)
	);
}

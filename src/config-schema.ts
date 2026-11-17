import { AsyncLocalStorage } from "node:async_hooks";
import type { ProvenanceTaint } from "./provenance-certificate.ts";
import type { PatternAwarePresetID } from "./pattern-aware.ts";
type ReadonlyConfig<Value> = Value extends object
	? { readonly [Key in keyof Value]: ReadonlyConfig<Value[Key]> }
	: Value;
export type SpeculativeConfig = ReadonlyConfig<typeof DEFAULT_SPECULATIVE_CONFIG>;
export const DEFAULT_SPECULATIVE_CONFIG = freezeConfig({
	drafter: {
		drafterMaxDepth: 3,
		drafterDeterministicCandidates: 1,
		drafterTemperatureMin: 0.7,
		drafterTemperatureMax: 0.7,
		drafterPatternHints: false,
	},
	scheduling: {
		gpuProbeBytes: 64 * 1024,
		failureEntries: 1024,
		maxCpuID: 1048575,
		maxAffinityCPUs: 16384,
		resourcePollIntervalMs: 250,
		gpuPollIntervalMs: 1000,
		gpuProbeTimeoutMs: 1000,
		failureThreshold: 2,
		failureRetryDecisions: 4,
		heavyCpu: 2,
		lightCpu: 1,
		heavyMemoryBytes: 64 * 1024 * 1024,
		lightMemoryBytes: 8 * 1024 * 1024,
		heavyIo: 0.25,
		lightIo: 0.125,
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
	explicitOnly: true,
	subcomputation: {
		exposeToControlledProcesses: false,
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
		actorObservationMode: "none" as "none" | "provenance",
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
		maxWindowsLeaseRequests: 64,
		timestampRestoreBatch: 1024,
		utilityOutputBytes: 4 * 1024 * 1024,
		asyncHashThresholdBytes: 65536,
		timestampQuietByFilesystem: {} as Record<string, number>,
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
		executable: "",
		braceExpandMax: 10000,
		maxInputBytes: 8 * 1024 * 1024,
		maxEntries: 65536,
		maxResultBytes: 1024 * 1024,
		maxDirectoryEntries: 4096,
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
		maxBindingVariants: 32,
		persistIntervalMs: 30000,
		maxValueCharacters: 4096,
		maxValueItems: 256,
		structuralDepth: 4,
		smoothing: 0.5,
	},
	runtime: {
		strictHostVersionCheck: false,
		hostVersions: ["0.84.1"],
		enabled: false,
		reuseEnabled: true,
		drafterEnabled: true,
		prepareObservedOperations: true,
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
function freezeConfig<Value>(value: Value): Value {
	if (value && typeof value === "object") {
		for (const child of Object.values(value)) freezeConfig(child);
		Object.freeze(value);
	}
	return value;
}
export type SpeculativeConfigInput = {
	readonly [K in keyof SpeculativeConfig]?: SpeculativeConfig[K] extends object
		? Partial<SpeculativeConfig[K]>
		: SpeculativeConfig[K];
};
export function resolveSpeculativeConfig(input: SpeculativeConfigInput = {}): SpeculativeConfig {
	const source = input as Record<string, unknown>;
	const defaults = DEFAULT_SPECULATIVE_CONFIG as unknown as Record<string, unknown>;
	const fail = (field: string): never => {
		throw new Error("Invalid speculative configuration: " + field);
	};
	for (const key of Object.keys(source)) if (!(key in defaults)) fail(key);
	const copy = (value: unknown, expected: unknown, field: string): unknown => {
		if (value === undefined) value = expected;
		if (typeof expected === "number") {
			if (typeof value !== "number" || !Number.isFinite(value) || value < 0) fail(field);
			if (
				![
					"runtime.hotFraction",
					"scheduling.heavyIo",
					"scheduling.lightIo",
					"drafter.drafterTemperatureMin",
					"drafter.drafterTemperatureMax",
					"prediction.retryProbability",
					"prediction.initialProbability",
					"prediction.smoothing",
					"pattern.futureGapCoverage",
					"selfSpeculation.forkActionMinConfidence",
					"selfSpeculation.forkTemperature",
				].includes(field) &&
				!Number.isSafeInteger(value)
			)
				fail(field);
			if (field.endsWith("Ms") && (value as number) > 2147483647) fail(field);
			return value;
		}
		if (Array.isArray(expected)) {
			if (!Array.isArray(value) || value.some((item) => typeof item !== "string" || item.includes("\0")))
				fail(field);
			return Object.freeze([...(value as string[])]);
		}
		if (expected && typeof expected === "object") {
			if (!value || typeof value !== "object" || Array.isArray(value)) fail(field);
			const object = value as Record<string, unknown>;
			const shape = expected as Record<string, unknown>;
			if (field === "processReuse.isolatedEnvironment") {
				for (const [key, entry] of Object.entries(object))
					if (!key || /[=\0]/u.test(key) || typeof entry !== "string" || entry.includes("\0")) fail(field);
				return Object.freeze({ ...object });
			}
			if (field === "filesystem.timestampQuietByFilesystem") {
				for (const [key, entry] of Object.entries(object)) {
					if (!/^(?:0x[\da-f]+|\d+)$/iu.test(key)) fail(field);
					copy(entry, 0, field + "." + key);
				}
				return Object.freeze({ ...object });
			}
			for (const key of Object.keys(object))
				if (!(key in shape) && !(field === "storage" && key === "root")) fail(field + "." + key);
			const result = Object.fromEntries(
				Object.entries(shape).map(([key, fallback]) => [
					key,
					copy(object[key], fallback, field ? field + "." + key : key),
				]),
			);
			if (field === "storage" && object.root !== undefined) {
				if (typeof object.root !== "string" || !path.isAbsolute(object.root)) fail("storage.root");
				result.root = object.root;
			}
			return Object.freeze(result);
		}
		if (typeof value !== typeof expected) fail(field);
		return value;
	};
	const output = copy(source, defaults, "") as SpeculativeConfig;
	const taints: readonly ProvenanceTaint[] = [
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
	];
	if (output.processReuse.replayAcceptedTaints.some((taint) => !taints.includes(taint)))
		fail("processReuse.replayAcceptedTaints");
	if (output.processReuse.environmentKeys.some((key) => !key || /[=\0]/u.test(key)))
		fail("processReuse.environmentKeys");
	if (
		!["none", "provenance"].includes(output.processReuse.actorObservationMode) ||
		!["exact", "allowlist", "denylist", "isolated"].includes(output.processReuse.environmentMode) ||
		!["all", "first_produced"].includes(output.prediction.concurrentProposalPolicy) ||
		!["provider", "sidecar", "drafter"].includes(output.selfSpeculation.forkTransport) ||
		!["in_memory", "overlayfs", "git"].includes(output.filesystem.sandboxDriver)
	)
		fail("process or filesystem policy");
	const positive = [
		...Object.values(output.nativeBuild),
		output.filesystem.concurrency,
		output.filesystem.directoryMetadataBytes,
		output.filesystem.chunkBytes,
		output.filesystem.directoryReadBytes,
		output.filesystem.stabilityAttempts,
		output.filesystem.objectLeaseTimeoutMs,
		output.filesystem.maxWindowsLeaseRequests,
		output.subcomputation.maxPendingRequests,
		output.subcomputation.socketTimeoutMs,
		output.storage.sqliteTimeoutMs,
		output.storage.sqliteRetryInitialMs,
		output.storage.sqliteRetryMaxMs,
		output.storage.sqliteRetryMultiplier,
	];
	if (positive.some((value) => !Number.isSafeInteger(value) || value <= 0)) fail("positive capacity");
	if (
		!output.filesystem.windowsObjectLeaseCommand.length ||
		output.filesystem.windowsObjectLeaseCommand.some((part) => !part || part.includes("\0"))
	)
		fail("filesystem.windowsObjectLeaseCommand");
	if (output.storage.sqliteRetryInitialMs > output.storage.sqliteRetryMaxMs) fail("storage.sqliteRetryMaxMs");
	if (output.prediction.calibrationSuccessPrior + output.prediction.calibrationFailurePrior <= 0)
		fail("prediction calibration prior");
	if (output.nativeBuild.domainValues < 8 || output.nativeBuild.domainValues * 8 + 16 > 2147483647)
		fail("nativeBuild.domainValues");
	if (Math.log2(output.nativeBuild.domainRingBytes) % 1 || output.nativeBuild.domainRingBytes < 4096)
		fail("nativeBuild.domainRingBytes");
	if (output.filesystem.chunkBytes < 4 || output.filesystem.chunkBytes > output.filesystem.maxReadBytes)
		fail("filesystem.chunkBytes");
	if (
		output.runtime.hotFraction > 1 ||
		output.prediction.retryProbability > 1 ||
		output.prediction.initialProbability > 1 ||
		output.pattern.futureGapCoverage > 1 ||
		output.selfSpeculation.forkActionMinConfidence > 1
	)
		fail("probability");
	return output;
}
const configuration = new AsyncLocalStorage<SpeculativeConfig>();
export const speculativeConfig = (): SpeculativeConfig =>
	configuration.getStore() ?? DEFAULT_SPECULATIVE_CONFIG;
export function withSpeculativeConfig<T>(config: SpeculativeConfig, operation: () => T): T {
	return configuration.run(config, operation);
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
	if (policy.environmentMode === "isolated") return Object.freeze({ ...policy.isolatedEnvironment });
	const keys = new Set(policy.environmentKeys);
	return Object.freeze(
		Object.fromEntries(
			Object.entries(environment).filter(
				([key]) =>
					policy.environmentMode === "exact" ||
					(policy.environmentMode === "allowlist" ? keys.has(key) : !keys.has(key)),
			),
		),
	);
}
import { getAgentDir } from "@earendil-works/pi-coding-agent";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import type { SpeculativeAgentSettingsInput } from "./agent-integration.ts";
import { isFiniteNumber, isRecord, isSafeCount, positiveCount } from "./primitives.ts";
export function positiveInteger<F extends number | undefined>(value: unknown, fallback: F): number | F {
	return typeof value === "number" && Number.isSafeInteger(Math.floor(value)) && value >= 1
		? Math.floor(value)
		: fallback;
}
export function nonNegativeInteger(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isSafeInteger(Math.floor(value)) && value >= 0
		? Math.floor(value)
		: fallback;
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
/** Every stored field declares its parser; unknown fields never enter the normalized result. */
export function settingsParser<Defaults extends Record<string, unknown>, Key extends keyof Defaults>(
	schema: Defaults,
	fields: { readonly [K in Key]: (value: unknown, fallback: Defaults[K]) => Defaults[K] },
) {
	type Settings = Pick<Defaults, Key>;
	const keys = Object.keys(fields) as Key[];
	const defaults = Object.fromEntries(keys.map((key) => [key, schema[key]])) as Settings;
	const parse = (input?: Readonly<Record<string, unknown>>): Settings => {
		const result = {} as Settings;
		for (const key of keys) result[key] = fields[key](input?.[String(key)], defaults[key]);
		return result;
	};
	return { defaults, parse };
}
export type SettingInputResult<T> =
	| {
			readonly ok: true;
			readonly value: T;
	  }
	| {
			readonly ok: false;
			readonly error: string;
	  };
export interface SettingInputDescriptor<T> {
	readonly title: string;
	readonly format: (value: T) => string;
	readonly parse: (input: string) => SettingInputResult<T>;
}
interface NumericInputOptions {
	readonly error?: string;
	readonly format?: (value: number) => string;
	readonly transform?: (value: number) => number;
}
export function settingInput<T>(
	title: string,
	format: (value: T) => string,
	parse: (input: string) => SettingInputResult<T>,
): SettingInputDescriptor<T> {
	return Object.freeze({ title, format, parse });
}
function numericInput(
	title: string,
	valid: (value: number) => boolean,
	error: string,
	options: NumericInputOptions = {},
): SettingInputDescriptor<number> {
	return settingInput(title, options.format ?? String, (input) => {
		const value = Number(input.trim());
		const result = options.transform?.(value) ?? value;
		return input.trim() && Number.isFinite(value) && valid(value) && Number.isFinite(result) && valid(result)
			? { ok: true, value: result }
			: { ok: false, error: options.error ?? error };
	});
}
export function positiveIntegerInput(
	title: string,
	options: NumericInputOptions = {},
): SettingInputDescriptor<number> {
	return numericInput(
		title,
		(value) => isSafeCount(value, 1),
		`${title} must be a positive integer.`,
		options,
	);
}
export function nonNegativeIntegerInput(title: string): SettingInputDescriptor<number> {
	return numericInput(title, (value) => isSafeCount(value), `${title} must be a non-negative integer.`);
}
export function millisecondsInput(title: string): SettingInputDescriptor<number> {
	return numericInput(
		title,
		(value) => milliseconds(value, -1) >= 1,
		`${title} must be an integer from 1 to 2147483647.`,
	);
}
export function nonNegativeNumberInput(title: string): SettingInputDescriptor<number> {
	return numericInput(title, (value) => value >= 0, `${title} must be a non-negative number.`);
}
export function probabilityInput(
	title: string,
	options: NumericInputOptions = {},
): SettingInputDescriptor<number> {
	return numericInput(
		title,
		(value) => value >= 0 && value <= 1,
		`${title} must be between 0 and 1.`,
		options,
	);
}
export function nonEmptyTextInput(title: string): SettingInputDescriptor<string> {
	return settingInput(title, String, (input) => {
		const value = input.trim();
		return value ? { ok: true, value } : { ok: false, error: `${title} cannot be empty.` };
	});
}
export function optionalTextInput(title: string): SettingInputDescriptor<string | undefined> {
	return settingInput(
		title,
		(value) => value ?? "",
		(input) => ({ ok: true, value: input.trim() || undefined }),
	);
}
/** Admission and resource sampling hints; none of these fields grant execution or reuse authority. */
export type SchedulingSettings = Readonly<typeof SCHEDULING_DEFAULTS>;
export const { defaults: SCHEDULING_DEFAULTS, parse: parseSchedulingSettings } = settingsParser(
	DEFAULT_SPECULATIVE_CONFIG.scheduling,
	{
		resourcePollIntervalMs: positiveMilliseconds,

		gpuPollIntervalMs: positiveMilliseconds,

		gpuProbeTimeoutMs: positiveMilliseconds,

		failureThreshold: positiveInteger,

		failureRetryDecisions: positiveInteger,

		heavyCpu: positiveInteger,

		lightCpu: positiveInteger,

		heavyMemoryBytes: positiveInteger,

		lightMemoryBytes: positiveInteger,

		heavyIo: probability,

		lightIo: probability,
	},
);
export function normalizeSchedulingSettings(input?: Readonly<Record<string, unknown>>) {
	return parseSchedulingSettings({
		...speculativeConfig().scheduling,
		...input,
	});
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
	private global: SettingsOverlay | undefined;
	private project: SettingsOverlay | undefined;
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
		[this.global, this.project] = await Promise.all([
			read(this.globalPath),
			trusted ? read(this.projectPath).then(userScoped) : undefined,
		]);
		this.scopeValue = this.project || this.unreadable.has(this.projectPath) ? "project" : "global";
	}
	get scope(): SpeculativeSettingsScope {
		return this.scopeValue;
	}
	setScope(scope: SpeculativeSettingsScope): void {
		this.scopeValue = scope;
	}
	effective(): SpeculativeActionPackageSettings | undefined {
		return applyOverlay(this.global, this.project);
	}
	editable(scope = this.scopeValue): SpeculativeActionPackageSettings | undefined {
		return applyOverlay(this.global, scope === "project" ? this.project : undefined);
	}
	/** Each scope persists only its differences from the layer below: defaults for global, global for project. */
	setEffective(
		value: SpeculativeActionPackageSettings,
		inherited = this.scopeValue === "project" ? this.editable("global") : undefined,
	): void {
		const overlay = diffRecord((inherited as SettingsOverlay) ?? {}, value as SettingsOverlay);
		if (this.scopeValue === "project") this.project = userScoped(overlay);
		else this.global = overlay;
		this.persistSelected();
	}
	clear(): void {
		if (this.scopeValue === "project") this.project = undefined;
		else this.global = undefined;
		this.persistSelected();
	}
	private persistSelected(): void {
		const snapshot = structuredClone(this.scopeValue === "project" ? this.project : this.global);
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
	const nested = project?.selfSpeculation;
	if (!project || !isRecord(nested)) return project;
	const { endpoint: _endpoint, apiKeyEnv: _apiKeyEnv, ...selfSpeculation } = nested;
	return { ...project, selfSpeculation };
}
function applyOverlay(
	base: SettingsOverlay | undefined,
	overlay: SettingsOverlay | undefined,
): SettingsOverlay | undefined {
	if (!base && !overlay) return undefined;
	const result: SettingsOverlay = structuredClone(base) ?? {};
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
	return Object.keys(result).length ? result : undefined;
}
function diffRecord(base: SettingsOverlay, target: SettingsOverlay): SettingsOverlay | undefined {
	const result: SettingsOverlay = {};
	for (const key of new Set([...Object.keys(base), ...Object.keys(target)])) {
		const baseHas = Object.hasOwn(base, key);
		const targetHas = Object.hasOwn(target, key);
		if (!targetHas) {
			if (baseHas) result[key] = null;
			continue;
		}
		const before = base[key];
		const after = target[key];
		if (isDeepStrictEqual(before, after)) continue;
		if (isRecord(before) && isRecord(after)) {
			const nested = diffRecord(before, after);
			if (nested) result[key] = nested;
		} else {
			result[key] = structuredClone(after);
		}
	}
	return Object.keys(result).length ? result : undefined;
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
		/** Successor batches predicted from private observations while earlier actions execute. */
		drafterMaxDepth: nonNegativeInteger,

		/** Number of leading Drafter requests sent at temperature zero. */
		drafterDeterministicCandidates: nonNegativeInteger,

		/** Inclusive temperature range stratified across the remaining requests. */
		drafterTemperatureMin: nonNegativeNumber,

		drafterTemperatureMax: nonNegativeNumber,

		/** Show the Drafter the calls PatternAware expects next (for A/B; off by default). */
		drafterPatternHints: booleanOr,
	},
);
export const DEFAULTS = {
	...DEFAULT_SPECULATIVE_CONFIG.runtime,
	...DRAFTER_DEFAULTS,
};
export const clampCandidateLimit = positiveCount;
export function normalizeDrafterRequestSettings(value: unknown): DrafterRequestSettings {
	const result = parseDrafterSettings({
		...speculativeConfig().drafter,
		...(value && typeof value === "object" ? (value as Record<string, unknown>) : {}),
	});
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

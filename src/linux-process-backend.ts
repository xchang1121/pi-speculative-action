import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	BorderedLoader,
	createLocalBashOperations,
	truncateToVisualLines,
} from "@earendil-works/pi-coding-agent";
import { AsyncLocalStorage } from "node:async_hooks";
import { spawn, type ChildProcess } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { on, once } from "node:events";
import type { BigIntStats } from "node:fs";
import { constants as fsConstants, readFileSync } from "node:fs";
import {
	access,
	chmod,
	copyFile,
	link,
	lstat,
	mkdir,
	mkdtemp,
	open,
	readdir,
	readFile,
	readlink,
	realpath,
	rm,
	rmdir,
	stat,
	writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { finished } from "node:stream/promises";
import { StringDecoder } from "node:string_decoder";
import { fileURLToPath } from "node:url";
import { stripVTControlCharacters } from "node:util";
import { ResourceObjectInputs } from "./agent-execution-world.ts";
import {
	currentSpeculativeConfiguration,
	projectProcessEnvironment,
	speculativeConfig,
} from "./config-schema.ts";
import { controlledRuntimeEnvironment, controlledSubcomputeChannel, withSubcomputeScope } from "./subcompute.ts";
import {
	captureFileContent,
	ContentChunkPool,
	contentDigest,
	publishContent,
	restoreArtifactContent,
	type FileContentVersion,
	writeContent,
} from "./content-chunks.ts";
import type {
	ExecutionResourceContext,
	ExecutionScope,
	ExecutionWorldStorageControl,
	ToolProcessInvocation,
	WorldReuseMetrics,
} from "./execution-world.ts";
import {
	EffectCommitFailure,
	emptyWorldReuseMetrics,
	isPoisonedEffectCommit,
	snapshotExecutionScope,
} from "./execution-world.ts";
import {
	assertNoSymlinkPath,
	captureFileDigest,
	captureFilesystemEntry,
	captureStableFile,
	FILESYSTEM_METADATA_FIELDS,
	filesystemMetadataDigest,
	filesystemMountTopology,
	filesystemObservationDigest,
	FilesystemReadView,
	hashExecutableFile,
	mapFilesystem,
	withFilesystemReadView,
	sameFilesystemIdentity,
	settledFilesystemStat,
	sharedWalk,
} from "./filesystem-evidence.ts";
import type {
	HeldExecClock,
	HeldExecDecision,
	HeldExecProcess,
	HeldExecSnapshot,
	HeldExecTiming,
} from "./linux-held-exec.ts";
import {
	executionDomainCollector,
	executionDomainProof,
	inheritedTracer,
	inspectHeldExecProcess,
	LinuxHeldExecBoundary,
	resolveLinuxExecHelper,
	writeProcessChannel,
} from "./linux-held-exec.ts";
import type {
	ProcessExecutionRequest,
	ProcessExecutor,
	ProcessRouteSnapshot,
} from "./linux-process-world.ts";
import { definedProcessEnvironment } from "./linux-process-world.ts";
import type { Sha256Digest } from "./primitives.ts";
import {
	releaseOnce,
	ignoreMissing,
	digestObject,
	errorMessage,
	execHostText,
	isMissing as missing,
	containsFilesystemPath as pathContains,
	relativeFilesystemPath,
	resolveHostExecutable,
	sha256Digest,
	slash,
	stableEqual,
} from "./primitives.ts";
import type { ProcessExecutionContext, ProcessLaunchOptions } from "./process-context.mjs";
import {
	byteStreamContextKey,
	captureProcessContext,
	processLaunchOptions,
	routedProcessIdentity,
	validProcessContext,
} from "./process-context.mjs";
import type { ProcessExecutionBinding, ProcessHandoff, ProcessReusePlan } from "./process-reuse.ts";
import {
	captureAbsenceDependency,
	captureDirectoryDependency,
	captureFileDependency,
	captureMetadataDependency,
	captureSymlinkDependency,
	observeProcessInputs,
	ProcessHandoffOwnership,
	ProcessHandoffRegistry,
	ProcessReusePlanner,
	sameScope,
	validateDynamicDependencyCertificate,
} from "./process-reuse.ts";
import {
	configureNativeFilesystem,
	nativeFilesystem,
	nativeProcessLibrary,
	qualifyNativeStartup,
	spawnControlled,
} from "./process-startup.mjs";
import type {
	DependencyRole,
	DynamicDependency,
	DynamicDependencyCertificate,
	ExecPrototype,
	ExitOutcome,
	OrderedEffectEvent,
	ProcessProducerProof,
	ProcessProvenanceCertificate,
	ProcessResultRecord,
	ProvenanceTaint,
	WorkspaceEffectState,
} from "./provenance-certificate.ts";
import {
	byteStreamPrototype,
	certificateReplayable,
	createExecPrototype,
	dynamicDependencyIdentity,
	processWeakKey,
	sealProcessCertificate,
} from "./provenance-certificate.ts";
import type { ProvenanceStoreOptions, VerifiedArtifactClosure } from "./reuse-store.ts";
import { ProvenanceCertificateStore } from "./reuse-store.ts";
import { RuntimeLifecycleLane, waitForCompletion } from "./runtime-lifecycle.ts";
import { cause, zeroValidationMetrics, type ResourceValidation } from "./settlement.ts";
import type { ExecutionObservation, TracedWrite, TraceTail } from "./execution-observer.ts";
import { executionRecords, installedImageStart, observeExecution, traceTail, UTSNAME_BYTES, writesWithin } from "./execution-observer.ts";
import type { TimelineDependency } from "./task-timing.ts";
import { TimelineInterval } from "./task-timing.ts";
import type {
	SandboxDirectoryState,
	SandboxWorkspaceChange,
	SandboxWorkspaceContext,
	WorkspaceStructureSnapshot,
	WorkspaceRegularDelta,
	WorkspaceTransactionCapture,
	WorkspaceTransactionDelta,
	WorkspaceTransactionDiff,
	WorkspaceTransactionOwnership,
} from "./workspace-sandbox.ts";
import {
	diffWorkspaceStructures,
	captureWorkspaceStructureEntry,
	ExecutionPathProjection,
	projectFileMutation,
	assertFileAliases,
	createParentDirectories,
	matchesSandboxBaseline,
	readRegularState,
	directoryEntriesDigest,
	sandboxWorkspaceChanges,
	snapshotDependency,
	workspaceInputVersion,
	WorkspaceSandboxService,
	WorkspaceStructureCapture,
} from "./workspace-sandbox.ts";
export interface LinuxEnvironmentReport {
	readonly text: string;
	readonly summary: string;
	readonly warnings: boolean;
}
/** os-release is data: never source it, expand variables, or evaluate command substitutions. */
export function parseLinuxOsRelease(text: string): Readonly<Record<string, string>> {
	const result: Record<string, string> = Object.create(null);
	for (const line of text.split(/\r?\n/)) {
		const match = /^([A-Z][A-Z0-9_]*)=(.*)$/.exec(line.trim());
		if (!match) continue;
		const [key, raw] = match;
		let value = raw!;
		if (value.startsWith('"') || value.startsWith("'")) {
			const quote = value[0]!;
			if (!value.endsWith(quote) || value.length < 2) continue;
			value = value.slice(1, -1);
			if (quote === '"') value = value.replace(/\\(["\\$`])/g, "$1");
		} else if (/\s|["']/.test(value)) continue;
		result[key!] = clean(value);
	}
	return result;
}
/** Inventory plus the runtime's own bounded admission/observer probes. Every temporary owner is closed. */
export async function inspectLinuxEnvironment(root = process.cwd()): Promise<LinuxEnvironmentReport> {
	if (process.platform !== "linux") {
		const text = "Linux environment checks require Pi running inside Linux or WSL 2.";
		return { text, summary: text, warnings: true };
	}
	const release = parseLinuxOsRelease(await readFile("/etc/os-release", "utf8").catch(() => ""));
	const bin = path.join(os.homedir(), ".local", "bin");
	const host = `${release.PRETTY_NAME ?? release.ID ?? "Linux"}; ${process.arch}; ${clean(os.release())}`;
	const lines = ["Linux environment Doctor", `Host: ${host}`];
	const issues: string[] = [];
	const check = async <Value>(
		label: string,
		inspect: () => Promise<Value>,
		describe: (value: Value) => string = String,
	) => {
		try {
			const value = await inspect();
			lines.push(`${label}: ${clean(describe(value))}`);
			return value;
		} catch (error) {
			const detail = `${label}: ${clean(errorMessage(error))}`;
			issues.push(detail);
			lines.push(detail);
			return undefined;
		}
	};
	const program = async (
		name: string,
		alternates: readonly string[] = [],
		fallbacks: readonly string[] = [],
		explicit?: string,
		version = false,
	) => {
		// Toolchain shims dispatch by argv[0]; retain the selected symlink when testing launchability.
		const file = await resolveHostExecutable(explicit, name, fallbacks, alternates, version);
		if (!(await stat(file)).isFile()) throw new Error("not a regular executable");
		if (version) await query(file, ["--version"]);
		return file;
	};
	lines.push("Build tools: launchability only; compilation/linking is verified by setup:linux.");
	let compiler: string | undefined;
	for (const [label, name, alternates, fallbacks] of [
		["C compiler", "cc", ["gcc", "clang"], []],
		["Clang BPF compiler", "clang", [], []],
		["Git", "git", [], []],
		["Cargo", "cargo", [], [path.join(os.homedir(), ".cargo/bin/cargo")]],
		["Rust compiler", "rustc", [], [path.join(os.homedir(), ".cargo/bin/rustc")]],
		["make", "make", [], []],
		["tar", "tar", [], []],
		["xz", "xz", [], []],
	] as const) {
		const file = await check(label, () => program(name, alternates, fallbacks, undefined, true));
		if (name === "cc") compiler = file;
	}
	if (compiler)
		for (const [library, header] of [
			["libc.a", ""],
			["libbpf.so", "bpf/libbpf.h"],
			["libelf.so", "libelf.h"],
			["libz.so", "zlib.h"],
		])
			await check(library!, async () => {
				const file = (await query(compiler!, [`-print-file-name=${library}`])).trim();
				if (!path.isAbsolute(file) || !(await stat(file)).isFile())
					throw new Error("development library not located; install its development package");
				if (header) await access(path.join("/usr/include", header), fsConstants.R_OK);
				return `${file}; linking not tested here`;
			});
	await check("cgroup v2", () =>
		access("/sys/fs/cgroup/cgroup.controllers", fsConstants.R_OK).then(() => "present"),
	);
	await check("Kernel BTF", () =>
		access("/sys/kernel/btf/vmlinux", fsConstants.R_OK).then(
			() => "present; hooks checked by the execution domain",
		),
	);
	if (process.arch !== "x64")
		await check("Process architecture", async () => {
			throw new Error("x86-64 required");
		});
	lines.push("Installed components: locating an executable does not qualify its behavior.");
	for (const [label, name, explicit, fallback] of [
		["Sandlock", "pi-speculative-sandlock", process.env.PI_SPEC_SANDLOCK, "pi-speculative-sandlock"],
		["Execution domain launcher", "pi-speculative-domain", undefined, "pi-speculative-domain"],
	] as const)
		await check(label, () => program(name, [], [path.join(bin, fallback)], explicit));
	const helper = await check("Native helper", () => resolveLinuxExecHelper(process.env.PI_SPEC_HELD_EXEC));
	if (helper) {
		const collector = await check(
			"Execution domain",
			() => executionDomainCollector(helper),
			() => "active; protocol and source fingerprint match",
		);
		if (collector)
			await check("Workspace admission", async () => {
				collector.check(root);
				return "available; operation dependencies and effects remain checked at use";
			});
		else lines.push("Workspace admission: untested (execution domain unavailable)");
		await check(
			"Native startup observation",
			() => qualifyNativeStartup(helper),
			() => "qualified; process state and ownership remain checked at use",
		);
	}
	lines.push("Optional OverlayFS: Git workspace fallback remains available.");
	await check("fuse-overlayfs", () =>
		program("fuse-overlayfs", [], [path.join(bin, "fuse-overlayfs")], undefined, true),
	);
	await check("FUSE mount helper", () => program("fusermount3", ["fusermount"], [], undefined, true));
	await check("/dev/fuse", () =>
		access("/dev/fuse", fsConstants.R_OK | fsConstants.W_OK).then(
			() => "read/write access; mount qualification remains separate",
		),
	);
	lines.push(
		"WSL requires WSL 2 and a Linux-native checkout. Rebuild helpers after upgrading; start Pi through the foreground execution domain and keep the separately qualified observer.",
		"Use Refresh and show capabilities for enabled provider qualification. Missing evidence declines reuse; ordinary tools still execute. No packages, persistent services or permissions were changed.",
	);
	return {
		text: lines.join("\n"),
		summary: `Host: ${host}\n${issues.length ? issues.join("\n") : "Prerequisite and admission checks passed; individual operations still require proof."}`,
		warnings: issues.length > 0,
	};
}
function query(file: string, args: readonly string[]): Promise<string> {
	return execHostText(file, args, {
		cwd: "/",
		env: {
			...process.env,
			RUSTUP_AUTO_INSTALL: "0",
			CARGO_NET_OFFLINE: "true",
		},
		timeout: speculativeConfig().processReuse.diagnosticTimeoutMs,
		killSignal: "SIGKILL",
		maxBuffer: speculativeConfig().filesystem.diagnosticBytes,
	});
}
function sanitizeDiagnosticText(text: string): string {
	return stripVTControlCharacters(text).replace(/[\x00-\x08\x0b-\x1f\x7f]/g, "");
}
function clean(text: string): string {
	return sanitizeDiagnosticText(text).slice(0, speculativeConfig().processReuse.diagnosticCharacters);
}
function requireIdleLinuxContext(ctx: ExtensionContext, action: string, linuxMessage: string): boolean {
	if (process.platform !== "linux") {
		ctx.ui.notify(linuxMessage, "warning");
		return false;
	}
	if (!ctx.isIdle()) {
		ctx.ui.notify(`Wait for the current task to finish before ${action}.`, "warning");
		return false;
	}
	return true;
}
function ensureStillIdle(ctx: ExtensionContext, message: string): boolean {
	if (ctx.isIdle()) return true;
	ctx.ui.notify(`The task is now running. ${message} after it finishes.`, "warning");
	return false;
}
/** Show host prerequisites separately from the controller's authoritative runtime diagnostics. */
export async function checkLinuxEnvironment(
	ctx: ExtensionContext,
	refresh: () => Promise<void>,
): Promise<void> {
	if (
		!requireIdleLinuxContext(
			ctx,
			"checking the Linux environment",
			"Linux environment checks require Pi running inside Linux or WSL 2. Open this menu there.",
		)
	)
		return;
	const report = await inspectLinuxEnvironment(ctx.cwd);
	ctx.ui.notify(report.text, report.warnings ? "warning" : "info");
	if (!ensureStillIdle(ctx, "Refresh execution diagnostics")) return;
	await refresh();
}
/** The installer and its native sources ship in src for both TypeScript and dist entry points. */
export async function installLinuxDependencies(
	ctx: ExtensionContext,
	refresh: () => Promise<void>,
): Promise<void> {
	if (
		!requireIdleLinuxContext(
			ctx,
			"installing Linux dependencies",
			"Linux dependencies must be installed from Pi running inside Linux or WSL 2. Open this menu there.",
		)
	)
		return;
	const report = await inspectLinuxEnvironment(ctx.cwd);
	if (!ensureStillIdle(ctx, "Retry installation")) return;
	if (
		!(await ctx.ui.confirm(
			"Install / update Linux dependencies?",
			`${report.summary}\n\nDownload and build the packaged Sandlock and process helpers, plus optional fuse-overlayfs, in ~/.local/bin. ` +
				"Requires Git, C/Rust toolchains, Clang, libbpf/libelf/zlib development files, make, tar and xz; FUSE needs fusermount and /dev/fuse. " +
				"Missing prerequisites may leave individual components unavailable. System packages and permissions are not changed. Building may take several minutes.",
		))
	)
		return;
	if (!ensureStillIdle(ctx, "Retry installation")) return;
	const script = fileURLToPath(new URL("../src/setup-linux-process-backend.mjs", import.meta.url));
	const quote = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`;
	const result = await ctx.ui.custom<{
		output: string;
		failed: boolean;
		cancelled: boolean;
	}>((tui, theme, _keys, done) => {
		const loader = new BorderedLoader(tui, theme, "Installing Linux dependencies…");
		const lifetime = new AbortController();
		const decoder = new StringDecoder("utf8");
		const signal = AbortSignal.any([loader.signal, lifetime.signal]);
		let output = "";
		const append = (text: string) => {
			output = (output + sanitizeDiagnosticText(text.replace(/\r/g, "\n"))).slice(-16384);
			tui.requestRender();
		};
		loader.addChild({
			render: (width) => truncateToVisualLines(output, 10, width).visualLines,
			invalidate() {},
		});
		void createLocalBashOperations({ shellPath: "/bin/sh" })
			.exec(`exec ${quote(process.execPath)} ${quote(script)}`, path.dirname(script), {
				signal,
				onData: (data) => append(decoder.write(data)),
			})
			.then(
				({ exitCode }) => (exitCode === 0 ? undefined : `Installer exited with ${exitCode ?? "a signal"}.`),
				errorMessage,
			)
			.then((failure) => {
				append(decoder.end());
				if (failure !== undefined) append(`\n${failure}\n`);
				done({ output, failed: failure !== undefined, cancelled: signal.aborted });
			});
		return {
			render: (width) => loader.render(width),
			invalidate: () => loader.invalidate(),
			handleInput: (data) => loader.handleInput(data),
			dispose() {
				lifetime.abort();
				loader.dispose();
			},
		};
	});
	const partial = /\bunavailable\b/i.test(result.output);
	const status = result.cancelled
		? "Installation cancelled; completed components remain installed."
		: result.failed
			? "Linux dependency installation failed."
			: partial
				? "Installation finished with unavailable components."
				: "Linux dependency installation finished.";
	const detail = result.output.trim().split("\n").slice(-14).join("\n");
	ctx.ui.notify(
		`${status}\n${detail}\nInstaller output describes installation checks; runtime qualification is reported separately. Restart Pi to load updated helpers and recheck workspace drivers.`,
		result.failed && !result.cancelled ? "error" : result.cancelled || partial ? "warning" : "info",
	);
	await refresh();
}
// Certificates are meaningful only under the implementation that collected and authorized their proof.
const OBSERVER_FINGERPRINT = digestObject(
	[
		"linux-process-backend",
		"execution-observer",
		"process-reuse",
		"reuse-store",
		"provenance-certificate",
		"filesystem-evidence",
		"primitives",
		"workspace-sandbox",
	]
		.map((name) =>
			sha256Digest(
				readFileSync(new URL(`./${name}.${import.meta.url.endsWith(".ts") ? "ts" : "js"}`, import.meta.url)),
			),
		)
		.concat(
			["c", "h"].map((extension) =>
				sha256Digest(readFileSync(new URL(`../src/linux-execution-domain.${extension}`, import.meta.url))),
			),
		),
);
const POLICY_ID = "sandlock-virtual-root-transparent-exec-creation-mode";
const LEAF_POLICY_ID = "sandlock-virtual-workspace-leaf-creation-mode";
/** The consumer launcher places a dependency inside its existing traced parent; metadata owns neither one. */
const processDependencyPIDs = new WeakMap<TimelineDependency, number>();
/** Native inputs consumed by this exact one-shot execution; they still prohibit any later replay. */
const acceptedReplayTaints = () => speculativeConfig().processReuse.replayAcceptedTaints;
export interface LinuxProcessBackendOptions {
	readonly storeRoot: string;
	readonly store?: ProvenanceStoreOptions;
	readonly sandlockBinary?: string;
	readonly heldExecBinary?: string;
	readonly heldExecObserverBinary?: string;
	/** Additional host paths that speculative processes must never read. */
	readonly deniedPaths?: readonly string[];
}
export interface CompletedProcessReplayOptions {
	readonly sourceRoot: string;
	readonly invocation: (request: ProcessExecutionRequest) => ToolProcessInvocation | undefined;
}
export interface LinuxProcessBackendStatus {
	readonly state: "ready" | "unavailable";
	readonly detail: string;
	readonly fingerprint?: string;
	readonly sandlockBinary?: string;
}
export type LinuxProcessReuseMetrics = WorldReuseMetrics;
type CountedReuseMetric = Exclude<keyof WorldReuseMetrics, "lastError">;
type MutableLinuxProcessReuseMetrics = {
	-readonly [Key in CountedReuseMetric]: number;
} & {
	lastError?: string;
};
export interface LinuxProcessSession {
	readonly executor: ProcessExecutor;
	readonly computationDependencies: () => readonly TimelineDependency[];
	/** Captured exec units in dispatcher arrival order, still speculative until the enclosing branch is adopted. */
	readonly executionBindings: () => readonly ProcessExecutionBinding[];
	/** Execute one retained exec unit in this fresh sandbox; its output is not the enclosing tool's result. */
	readonly executeBinding: (binding: ProcessExecutionBinding) => Promise<{
		readonly output: readonly BufferedOutput[];
		readonly exit: ExitOutcome;
	}>;
	readonly ownership: ProcessHandoffOwnership;
	readonly metrics: () => LinuxProcessReuseMetrics;
	/** Seal the session's owned workspace interval before validation. */
	readonly seal: (changes: readonly SandboxWorkspaceChange[]) => Promise<readonly SandboxWorkspaceChange[]>;
	/** Revalidate every observed input immediately before Actor adoption. */
	readonly validate: () => Promise<ResourceValidation>;
	readonly close: () => Promise<void>;
	/** Release retained session evidence after its ownership is no longer needed. */
	readonly dispose?: () => Promise<void>;
}
type ReadyBackend = Awaited<ReturnType<LinuxProcessReuseBackend["probe"]>>;
interface ExecMount {
	readonly virtualPath: string;
	readonly hostPath: string;
}
interface SandboxMount extends ExecMount {
	readonly readOnly: boolean;
}
interface ProcessArguments extends ProcessLaunchOptions {
	readonly argv0: string;
	readonly args: readonly string[];
	readonly cwd: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly invocation?: readonly [path: string, comm: string];
	readonly imageSource?: string;
	readonly streams?: true; // inherited pipes the launcher holds and settles
}
function boundEnvironment(environment: ProcessArguments["environment"]) {
	const channel = controlledSubcomputeChannel(environment);
	if (!channel) return { environment };
	const { PI_SUBCOMPUTE_SOCKET: _socket, PI_SUBCOMPUTE_CONTEXT: _token, ...variables } = environment;
	return { environment: variables, stepIdentity: channel.identity };
}
type BoundProcessInvocation = ProcessArguments & {
	readonly stepIdentity?: string;
	readonly sourceRoot: string;
	readonly executable: string;
	readonly producer?: ProcessProducerProof;
};
export type NativeProcessInvocation = BoundProcessInvocation;
interface BufferedOutput {
	readonly fd: 1 | 2;
	readonly data: Buffer;
}
interface DispatcherResponse {
	readonly held?: HeldExecDecision;
	readonly kind: "hit" | "executed" | "bypass";
	readonly output?: readonly BufferedOutput[];
	readonly exit?: ExitOutcome;
	/** Present only after a sealed result was registered; independent of optional disk publication. */
	readonly reusable?: boolean;
}
type WriterTraceState =
	| { readonly kind: "traced"; readonly tail: TraceTail }
	| { readonly kind: "settled"; readonly writes: readonly TracedWrite[]; readonly unsafe?: boolean };
/** A brokered run writing into a session's workspace: its launcher's pid and, once known, what it and the runs brokered from its tree wrote. */
interface SessionWriter {
	readonly channel?: ReturnType<typeof controlledSubcomputeChannel>;
	readonly id: string;
	readonly owner: string;
	readonly startedAt: number;
	endedAt?: number;
	trace: WriterTraceState;
	readonly pid?: number;
	pids?: readonly number[];
	closedPids?: readonly number[];
	written?: readonly string[];
	external?: readonly string[];
	descendants?: readonly SessionWriter[];
	observed?: ReadonlyMap<string, number>;
	evidence?: DynamicDependencyCertificate;
	observation?: ExecutionObservation;
}
async function readWriterTrace(
	writer: SessionWriter,
): Promise<{ readonly writes: readonly TracedWrite[]; readonly unsafe?: boolean }> {
	const state = writer.trace;
	if (state.kind === "settled") return state;
	try {
		return await state.tail.read();
	} catch (error) {
		if (writer.trace.kind === "settled") return writer.trace;
		throw error;
	}
}
interface DomainExecution {
	readonly channel?: ReturnType<typeof controlledSubcomputeChannel>;
	readonly pid: number;
	readonly trace: TraceTail;
	readonly endpoints: readonly [string, string];
	readonly resume: () => void;
	readonly exit: Promise<number>;
}
const workspaceWriters = new WeakMap<SandboxWorkspaceContext["transactions"], Set<SessionWriter>>();
type SessionAdmissionState =
	| { readonly kind: "unverified" }
	| {
			readonly kind: "verified";
			readonly proof: ReturnType<typeof executionDomainProof>;
			readonly valid: () => boolean;
			readonly result: ResourceValidation;
	  }
	| { readonly kind: "disposed" };
type ActiveSession = Awaited<ReturnType<LinuxProcessReuseBackend["allocateSession"]>>["session"];
type PrivatePrestate = Awaited<ReturnType<typeof privateBranch>> & {
	readonly startedAt: number;
	versions?: Awaited<ReturnType<WorkspaceStructureCapture["captureVersions"]>>;
};
const privateEntry = (before: PrivatePrestate | undefined, target: string) =>
	before?.versions?.entries.get(target.slice(1));
const privateFile = (before: PrivatePrestate | undefined, target: string) =>
	before?.versions?.snapshot.complete ? before.versions.files.get(target.slice(1)) : undefined;
interface SpawnOutcome {
	readonly code: number | null;
	readonly signal: NodeJS.Signals | null;
	readonly output: readonly BufferedOutput[];
	readonly captureComplete?: boolean;
}
type ReadyProcessPlan = Exclude<
	ProcessReusePlan,
	{
		kind: "miss";
	}
> & {
	readonly validate: () => boolean;
};
type ActorImageRecorder = (
	pid: number,
	prototype: ExecPrototype,
	image: Awaited<ReturnType<typeof inspectHeldExecProcess>>,
	installed: { dev: bigint; ino: bigint },
	clock: HeldExecClock,
) => Promise<void>;
const orderedBindings = (bindings: ReadonlyMap<number, ProcessExecutionBinding>) =>
	Object.freeze([...bindings].sort(([left], [right]) => left - right).map(([, binding]) => binding));
interface NativeObservation {
	readonly scope?: ExecutionScope;
	ordering: Int32Array;
	closed: boolean;
	readonly learn: boolean;
	pending: number;
	readonly reuseEnabled: boolean;
	readonly learned: Map<string, number>;
	readonly inputs?: ExecutionResourceContext["inputs"];
	readonly bindings: Map<number, ProcessExecutionBinding>;
	readonly computations: TimelineDependency[];
	readonly heldClock: ActorHeldClock;
	readonly metrics: MutableLinuxProcessReuseMetrics;
	claims?: Int32Array;
	effects?: Pick<WorkspaceSandboxService, "commitDelta">;
}
/** Linux-only process substrate. Unavailable dependencies remove the route instead of weakening it. */
export class LinuxProcessReuseBackend {
	readonly configuration = currentSpeculativeConfiguration();
	private readonly observations = new AsyncLocalStorage<NativeObservation>();
	/** Keep actual completed launches and acknowledged adoptions in their enclosing native call's order. */
	async observeBindings<Value>(
		scope: ExecutionScope,
		execute: () => Promise<Value>,
		observe: (
			bindings: readonly ProcessExecutionBinding[],
			computations: readonly TimelineDependency[],
		) => void,
		learn = false,
		inputs?: ExecutionResourceContext["inputs"],
		reuseEnabled = true,
	): Promise<Value> {
		const observation: NativeObservation = {
			scope: snapshotExecutionScope(scope),
			ordering: new Int32Array(new SharedArrayBuffer(4)),
			closed: false,
			learn,
			reuseEnabled,
			pending: 0,
			learned: new Map(),
			inputs: reuseEnabled ? inputs : undefined,
			bindings: new Map<number, ProcessExecutionBinding>(),
			computations: [] as TimelineDependency[],
			heldClock: new ActorHeldClock(),
			metrics: emptyWorldReuseMetrics(),
		};
		try {
			return await this.observations.run(observation, execute);
		} finally {
			observation.closed = true;
			try {
				observe(
					orderedBindings(observation.bindings),
					Object.freeze([...observation.computations, ...observation.heldClock.dependencies()]),
				);
			} catch {
				/* Learning cannot replace the native result or error. */
			}
		}
	}
	readonly store: ProvenanceCertificateStore;
	readonly planner: ProcessReusePlanner;
	readonly storage: ExecutionWorldStorageControl;
	private readonly options: LinuxProcessBackendOptions;
	private ready?: Promise<ReadyBackend>;
	private heldExec?: {
		readonly controller: AbortController;
		readonly ready: Promise<LinuxHeldExecBoundary>;
		status: ProcessRouteSnapshot;
	};
	private nativeRetirement?: Promise<void>;
	private readonly handoffs: ProcessHandoffRegistry<NativeProcessInvocation>;
	private readonly counters: MutableLinuxProcessReuseMetrics = emptyWorldReuseMetrics();
	private readonly actorCounters: MutableLinuxProcessReuseMetrics = emptyWorldReuseMetrics();
	private readonly replayWorkspace: WorkspaceSandboxService;
	private disposal?: Promise<void>;
	private producers = 0;
	constructor(options: LinuxProcessBackendOptions) {
		this.configuration.bind(this);
		this.options = {
			sandlockBinary: process.env.PI_SPEC_SANDLOCK,
			heldExecBinary: process.env.PI_SPEC_HELD_EXEC,
			heldExecObserverBinary: process.env.PI_SPEC_HELD_EXEC_OBSERVER,
			...options,
		};
		configureNativeFilesystem(this.options.heldExecBinary);
		this.store = new ProvenanceCertificateStore(options.storeRoot, options.store);
		this.replayWorkspace = new WorkspaceSandboxService(this.store);
		this.planner = new ProcessReusePlanner({ store: this.store });
		this.handoffs = new ProcessHandoffRegistry(
			this.store,
			Math.min(speculativeConfig().processReuse.maxCaptureBytes, this.store.limits.maxBytes),
		);
		this.configureHandoffs();
		this.storage = this.configuration.bind({
			configure: ({ maxEntries, maxBytes }) => {
				this.store.configure({ maxCertificates: maxEntries, maxBytes });
				this.configureHandoffs();
			},
			maintain: async (operation) => {
				this.handoffs.clearCompleted();
				const result = await (operation === "gc" ? this.store.gc() : this.store.clear());
				return {
					removedEntries: result.removedCertificates + result.removedInputVersions,
					removedArtifacts: result.removedArtifacts,
					removedBytes: result.removedBytes,
				};
			},
		});
	}
	private configureHandoffs() {
		this.handoffs.configure(
			Math.min(speculativeConfig().runtime.completedProcesses, this.store.limits.maxCertificates),
			Math.min(speculativeConfig().processReuse.maxCaptureBytes, this.store.limits.maxBytes),
		);
	}
	private async topLevelPrototype(
		invocation: ToolProcessInvocation,
		request: ProcessExecutionRequest,
		projection: ExecutionPathProjection,
		environment = definedProcessEnvironment(request.environment),
	) {
		return topLevelProcessPrototype(
			invocation,
			request,
			environment,
			projection,
			await this.resolvePlatformFingerprint(),
		);
	}
	private async appendOutputJournal(journal: OrderedEffectEvent[], events: readonly BufferedOutput[]) {
		for (const event of events)
			journal.push({
				sequence: journal.length,
				kind: "output",
				fd: event.fd,
				data: await this.store.artifacts.put(event.data),
			});
	}
	private publishCompletedCertificate(
		input: import("./provenance-certificate.ts").ProcessCertificateInput,
		allowByteStream = true,
	) {
		const bytesOnly =
			allowByteStream &&
			input.result.streamMetadataObserved === false &&
			input.result.journal.every((event) => event.kind === "output") &&
			byteStreamPrototype(input.prototype);
		return this.planner.publishCompleted(
			sealProcessCertificate(bytesOnly ? { ...input, prototype: bytesOnly } : input),
			acceptedReplayTaints(),
		);
	}
	async check(refresh = false, sourceRoot?: string): Promise<LinuxProcessBackendStatus> {
		if (process.platform !== "linux") return { state: "unavailable", detail: "Linux host required" };
		if (refresh) this.ready = undefined;
		try {
			const ready = await this.resolveReady();
			ready.collector.check(sourceRoot);
			return {
				state: "ready",
				detail: "Completed process provenance and result replay ready",
				fingerprint: ready.fingerprint,
				sandlockBinary: ready.sandlock,
			};
		} catch (error) {
			return { state: "unavailable", detail: errorMessage(error) };
		}
	}
	async fingerprint(): Promise<string> {
		return (await this.resolveReady()).fingerprint;
	}
	/** Aggregate backend counters retained for qualification and low-level diagnostics. */
	metrics(): LinuxProcessReuseMetrics {
		const metrics = { ...this.counters };
		for (const key of Object.keys(metrics) as (keyof WorldReuseMetrics)[])
			if (key !== "lastError") metrics[key] += this.actorCounters[key];
		metrics.lastError ??= this.actorCounters.lastError;
		return Object.freeze(metrics);
	}
	producerMetrics(): LinuxProcessReuseMetrics {
		return { ...this.counters };
	}
	/** Actor-path counters, excluding child reuse performed inside speculative worlds. */
	actorMetrics(): LinuxProcessReuseMetrics {
		return Object.freeze({ ...this.actorCounters });
	}
	/** The issued operation owns its launch context independently of the enclosing tool's implementation. */
	bindingWorkspace(binding: ProcessExecutionBinding): string {
		return this.boundInvocation(binding, binding.scope).sourceRoot;
	}
	private boundInvocation(
		binding: ProcessExecutionBinding,
		scope: ExecutionScope | undefined,
	): BoundProcessInvocation {
		const invocation = this.handoffs.resolveBinding(binding, scope);
		if (!invocation) throw new Error("process execution binding is unavailable in this scope");
		return invocation;
	}
	executionBindings(scope: ExecutionScope): readonly ProcessExecutionBinding[] {
		return this.handoffs.bindings(scope);
	}
	/** Whether a run of a learned launch would redo work: no result of it, this session's or stored, holds on the workspace now. */
	async bindingStale(binding: ProcessExecutionBinding): Promise<boolean> {
		const invocation = this.handoffs.resolveBinding(binding, binding.scope);
		if (!invocation) return true;
		const stale = await this.store
			.read(async (signal) => {
				const projection = new ExecutionPathProjection({
					sourceRoot: invocation.sourceRoot,
					workspaceRoot: invocation.sourceRoot,
				});
				const { output } = await TimelineInterval.collect(() =>
					this.boundRequest(binding, invocation, projection, signal),
				);
				const key = processWeakKey(output.prototype);
				const checked = new Set<Sha256Digest>();
				const currentInputs = observeProcessInputs(key, (evidence) =>
					TimelineInterval.collect(() =>
						validateDynamicDependencyCertificate(
							evidence,
							// A recorded run suppresses repeated preparation while its inputs hold, including after failure.
							{ acceptedTaints: evidence.taints, signal },
						),
					).then(
						(result) => result.output,
						() => undefined,
					),
				);
				for (const candidates of [
					this.handoffs.results(key, binding.scope),
					this.store.candidatesByWeakKey(key, invocation.executable, checked, signal),
				])
					for await (const certificate of candidates) {
						if (checked.has(certificate.id)) continue;
						checked.add(certificate.id);
						if ((await currentInputs(certificate.dependencyCertificate))?.strongKey === certificate.strongKey)
							return false;
					}
				return true;
			})
			.catch(() => true);
		return stale;
	}
	/** Keep possible publication visible through preparation, execution, and final evidence capture. */
	async withProducer<Value>(run: () => Promise<Value>): Promise<Value> {
		this.producers++;
		try {
			return await run();
		} finally {
			this.producers--;
		}
	}
	get hasReusableWork(): boolean {
		return this.producers > 0 || this.handoffs.hasResults;
	}
	private heldBoundary(): Promise<LinuxHeldExecBoundary> {
		if (this.disposal || this.nativeRetirement)
			return Promise.reject(new Error("native process observation is retiring"));
		if (!this.heldExec) {
			const controller = new AbortController();
			const entry: NonNullable<LinuxProcessReuseBackend["heldExec"]> = {
				controller,
				status: { state: "probing", detail: "Checking native process startup" },
				ready: this.resolveReady()
					.then(() =>
						LinuxHeldExecBoundary.open({
							storeRoot: this.options.storeRoot,
							reservedRoot: this.options.store?.reservedRoot,
							binary: this.options.heldExecBinary,
							observerBinary: this.options.heldExecObserverBinary,
							signal: AbortSignal.any([this.store.signal, controller.signal]),
						}),
					)
					.then(
						(boundary) => {
							entry.status = {
								state: "ready",
								detail: "Native child startup and validated result reuse",
							};
							return boundary;
						},
						(error) => {
							entry.status = {
								state: "unavailable",
								detail: errorMessage(error),
							};
							throw error;
						},
					),
			};
			this.heldExec = entry;
		}
		return this.heldExec.ready;
	}
	nativeDiagnostics(enabled = true): ProcessRouteSnapshot {
		if (this.disposal || process.platform !== "linux")
			return {
				state: "unavailable",
				detail: this.disposal ? "Native route disposed" : "Linux or WSL 2 required",
			};
		if (!enabled) return { state: "disabled", detail: "Native process reuse is disabled" };
		return this.nativeRetirement
			? { state: "probing", detail: "Draining native observations" }
			: (this.heldExec?.status ?? {
					state: "idle",
					detail: "Checked when native work is learned or reusable evidence exists",
				});
	}
	async refreshNativeObservation(enabled = true): Promise<void> {
		await this.resetNativeObservation();
		if (enabled && !this.disposal && process.platform === "linux")
			await this.heldBoundary().catch(() => undefined);
	}
	private nativeObservation(sourceRoot: string, scope?: ExecutionScope, signal?: AbortSignal) {
		return {
			sourceRoot,
			scope: snapshotExecutionScope(scope),
			signal,
			descriptors: !!this.observations.getStore()?.learn || this.hasReusableWork,
			decide: (process: HeldExecProcess) => this.decideHeldExec(process, process.scope),
		};
	}
	/** Observe the native launches of the actual callback, independently of its tool name or process adapter. */
	async observeNative<Value>(
		sourceRoot: string,
		scope: ExecutionScope | undefined,
		signal: AbortSignal | undefined,
		execute: () => Promise<Value>,
	): Promise<Value> {
		let boundary: LinuxHeldExecBoundary | undefined;
		const owner = snapshotExecutionScope(scope);
		const observation = this.observations.getStore();
		try {
			if (
				!this.disposal &&
				process.platform === "linux" &&
				((observation?.learn && !observation.closed && sameScope(observation.scope, owner)) ||
					this.hasReusableWork ||
					(await this.store.mayHaveCertificates(undefined, signal)) ||
					this.hasReusableWork)
			) {
				const ready = await waitForCompletion(this.heldBoundary(), signal);
				if (ready.status === "completed") boundary = ready.value;
			}
		} catch {
			/* Optional native observation leaves the actual callback authoritative. */
		}
		const options = this.nativeObservation(path.resolve(sourceRoot), owner, signal);
		return boundary ? boundary.observe(options, execute) : execute();
	}
	resetNativeObservation(): Promise<void> {
		if (this.nativeRetirement) return this.nativeRetirement;
		const heldExec = this.heldExec;
		this.heldExec = undefined;
		heldExec?.controller.abort(new Error("held-exec initialization retired"));
		return (this.nativeRetirement = Promise.resolve(heldExec?.ready)
			.then(
				(boundary) => boundary?.close(),
				() => undefined,
			)
			.finally(() => {
				this.nativeRetirement = undefined;
			}));
	}
	/** Replay completed results and optionally record the Actor's actual execution. */
	completedReplayExecutor(host: ProcessExecutor, options: CompletedProcessReplayOptions): ProcessExecutor {
		const sourceRoot = path.resolve(options.sourceRoot);
		const execute = (request: ProcessExecutionRequest) =>
			LinuxHeldExecBoundary.command(() => host.execute(request));
		const acceptProducer = (producer: ProcessProducerProof) =>
			actorReplayProducer(producer, sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
		return this.configuration.bind({
			execute: async (request) => {
				if (
					this.disposal ||
					this.observations.getStore()?.reuseEnabled === false ||
					process.platform !== "linux" ||
					!pathContains(sourceRoot, request.cwd)
				)
					return execute(request);
				this.add(undefined, "wholeCommandRequests");
				const result = await TimelineInterval.outside(async () => {
					let committed = false;
					let plan: Awaited<ReturnType<LinuxProcessReuseBackend["plan"]>>;
					try {
						request.signal?.throwIfAborted();
						const invocation = options.invocation(request);
						if (!invocation || !(await this.store.mayHaveCertificates(invocation.shell, request.signal)))
							return;
						assertInvocationMatches(invocation, request);
						const projection = new ExecutionPathProjection({
							sourceRoot,
							workspaceRoot: sourceRoot,
						});
						const prototype = (
							await TimelineInterval.collect(() => this.topLevelPrototype(invocation, request, projection))
						).output;
						plan = await this.planPrototype(prototype, projection, acceptProducer, {
							signal: request.signal,
						});
						if (this.disposal || !plan?.certificate.result.exit) return;
						request.signal?.throwIfAborted();
						const replay = this.prepareReplay(plan, projection);
						if (!replay.validate()) return;
						await replay.commitEffects();
						committed = true;
						for (const event of replay.output) request.onData(event.data);
						TimelineInterval.use(replay.computation.computation);
						this.add(undefined, "wholeCommandReusedProcessMs", replay.observedMs);
						this.add(undefined, "wholeCommandHits");
						return { exitCode: replay.exit.kind === "code" ? replay.exit.code : null };
					} catch (error) {
						this.setError(undefined, `actor_replay:${errorMessage(error)}`);
						if (committed || isPoisonedEffectCommit(error)) throw error;
					} finally {
						await plan?.artifacts.dispose();
					}
				});
				if (result) return result;
				this.add(undefined, "wholeCommandMisses");
				if (speculativeConfig().processReuse.actorObservationMode === "provenance") {
					const observed = await this.captureActorExecution(request, options);
					if (observed) return observed;
				}
				return execute(request);
			},
		});
	}
	private async captureActorExecution(
		request: ProcessExecutionRequest,
		options: CompletedProcessReplayOptions,
	): Promise<
		| {
				exitCode: number | null;
		  }
		| undefined
	> {
		const invocation = options.invocation(request);
		if (!invocation) return;
		const channel = controlledSubcomputeChannel(request.environment);
		let recording: ReturnType<ReadyBackend["collector"]["open"]> | undefined;
		let proof: ReturnType<typeof executionDomainProof> | undefined;
		let output: SpawnOutcome | undefined;
		let started = false;
		try {
			assertInvocationMatches(invocation, request);
			const ready = await this.resolveReady();
			const projection = new ExecutionPathProjection({
				sourceRoot: options.sourceRoot,
				workspaceRoot: options.sourceRoot,
			});
			const prototype = await this.topLevelPrototype(invocation, request, projection);
			// Watch from before execution; post-exit hashes alone cannot establish what the process read.
			proof = executionDomainProof(ready.collector, options.sourceRoot);
			if (proof.reason) throw new Error(proof.reason);
			recording = ready.collector.open();
			await using trace = traceTail(executionRecords(recording.trace()), request.cwd);
			const command = recording.launch([invocation.shell, ...shellArguments(invocation, request.command)]);
			const boundary = await this.heldBoundary();
			const began = performance.now();
			let outputEndpoints: readonly [string, string] | undefined;
			const native = this.nativeObservation(options.sourceRoot, this.observations.getStore()?.scope, request.signal);
			const dispatcherExecutable = await realpath(ready.dispatcher).catch(() => ready.dispatcher);
			const shellExecutable = await realpath(invocation.shell).catch(() => invocation.shell);
			let rootPid: number | undefined;
			let rootShellContinued = false;
			const replayed = new Set<number>();
			// The installed child, its exit and all evidence come from this one execution domain.
			const record: ActorImageRecorder = async (pid, child, image, installed, clock) => {
				if (pid === rootPid && image.executable === shellExecutable) return;
				try {
					const events = (await trace.read()).journal(pid);
					const endpoints = image.context.outputEndpoints;
					if (!events || !endpoints.every((name) => name === "/dev/null" || /^(socket|pipe):\[\d+\]$/.test(name)))
						return;
					const start = installedImageStart(events, installed);
					const terminal = events.at(-1);
					if (
						start < 0 ||
						terminal?.name !== "terminated" ||
						terminal.result === undefined ||
						(terminal.result & 127) !== 0 ||
						events.slice(0, start).some((event) => event.name === "terminated")
					) return;
					let observed = await observeExecution(trace, image.executable, image.cwd, {
						execution: { pid, start },
						captureOutput: true,
						outputEndpoints: endpoints,
						outputFlags: image.outputFlags ?? [0, 0],
						platformFingerprint: child.platformFingerprint,
						controlledSocket: channel?.socket,
						guardFilesystemSemanticsWithin: [options.sourceRoot],
					});
					if (observed.pids?.some((member) => replayed.has(member))) return;
					if (channel) observed = await channel.observe(observed);
					await this.publishActorProcess(
						child, observed, { kind: "code", code: terminal.result >> 8 },
						new TimelineInterval(clock.startedAt, clock.completedAt), proof!, request.signal, channel,
					);
				} catch (error) {
					this.setError(undefined, `actor_child:${errorMessage(error)}`);
				}
			};
			output = await boundary.trace(
				{
					...native,
					descriptors: true,
					decide: async (held) => {
						rootPid ??= held.pid;
						if (held.pid === rootPid && !rootShellContinued) {
							const exe = await readlink(`/proc/${held.pid}/exe`).catch(() => undefined);
							if (exe === dispatcherExecutable) return { kind: "continue" };
							if (exe === shellExecutable) {
								rootShellContinued = true;
								return { kind: "continue" };
							}
						}
						const decision = await this.decideHeldExec(held, held.scope, record, channel?.identity);
						if (decision.kind !== "replay") return decision;
						return {
							...decision,
							adopted: () => {
								replayed.add(held.pid);
								return decision.adopted?.();
							},
						};
					},
				},
				() => {
					started = true;
					return runSpawn([ready.dispatcher, "--output-channel=3", ...command], {
						...request,
						held: true,
						streamBeyondCaptureLimit: true,
						onOutputEndpoints: (endpoints) => {
							outputEndpoints = endpoints;
						},
						...(invocation.commandTransport === "stdin" ? { stdin: Buffer.from(request.command) } : {}),
					});
				},
			);
			const ended = performance.now();
			let observation = await observeExecution(trace, invocation.shell, request.cwd, {
				platformFingerprint: prototype.platformFingerprint,
				captureOutput: true,
				outputEndpoints,
				controlledSocket: channel?.socket,
				guardFilesystemSemanticsWithin: [options.sourceRoot],
			});
			observation = verifyProcessOutput(observation, output.output);
			if (channel) observation = await channel.observe(observation);
			await using files = this.fileInputs(
				options.sourceRoot, request.signal ?? this.store.signal, this.observations.getStore()?.inputs,
			);
			const workspaceFiles = files && await withFilesystemReadView(undefined, (view) =>
				this.captureActorFiles(observation, options.sourceRoot, files, proof!, view));
			if (output.captureComplete !== false && !observation.pids?.some((pid) => replayed.has(pid)))
				await this.publishActorProcess(prototype, observation, exitOutcome(output),
					new TimelineInterval(began, ended), proof, request.signal, channel, workspaceFiles, true);
		} catch (error) {
			this.setError(undefined, `actor_observation:${errorMessage(error)}`);
			if (started && !output) throw error;
		} finally {
			proof?.close();
			recording?.close();
		}
		return output ? { exitCode: output.code } : undefined;
	}
	private async publishActorProcess(
		prototype: ExecPrototype,
		observation: ExecutionObservation,
		exit: ExitOutcome,
		computation: TimelineInterval,
		proof: ReturnType<typeof executionDomainProof>,
		signal: AbortSignal | undefined,
		channel: ReturnType<typeof controlledSubcomputeChannel>,
		files?: ReadonlyMap<string, FileContentVersion>,
		whole = false,
	) {
		if (!speculativeConfig().processReuse.persistCompletedCertificates) return;
		const reasons = [
			...observation.incompleteReasons,
			...observation.taints.filter((taint) => !acceptedReplayTaints().includes(taint)),
			...(observation.written?.length || observation.external?.length ? ["unsealed_writes"] : []),
			...(observation.locks?.length ? ["unsealed_locks"] : []),
			...(observation.metadataChanges?.length ? ["metadata_changed"] : []),
		];
		if (!observation.complete || reasons.length) {
			this.setError(undefined, `actor_${whole ? "command" : "child"}_unpublished:${reasons.join(",")}`, "tainted");
			return;
		}
		const { dependencies, taints } = await captureActorDependencies(
			observation,
			sensitivePaths(this.options.storeRoot, this.options.deniedPaths),
			signal,
			files,
		);
		const lateTaints = taints.filter((taint) => !acceptedReplayTaints().includes(taint));
		if (lateTaints.length) {
			this.setError(undefined, `actor_${whole ? "command" : "child"}_unpublished:${lateTaints.join(",")}`, "tainted");
			return;
		}
		const validate = proof.seal(dependencies, (target) => target);
		if (!validate()) return;
		await this.store.withPublication(async () => {
			const journal: OrderedEffectEvent[] = [];
			await this.appendOutputJournal(journal, observation.output ?? []);
			await channel?.validate();
			if (!validate()) return;
			const published = await this.publishCompletedCertificate(
				{
					prototype,
					producer: {
						observer: { provider: "execution_domain", fingerprint: OBSERVER_FINGERPRINT },
						execution: { authority: "actor" },
					},
					dependencyCertificate: { complete: true, dependencies, taints },
					result: {
						replayProfile: "buffered_noninteractive",
						journal,
						exit,
						streamMetadataObserved: observation.streamMetadataObserved,
						observedProcessMs: computation.completedAt - computation.startedAt,
						computation: TimelineInterval.serialize(computation),
					},
				},
				!whole,
			);
			if (published) this.add(undefined, whole ? "wholeCommandPublished" : "published");
		});
	}
	private async captureActorFiles(
		observation: ExecutionObservation,
		root: string,
		files: ResourceObjectInputs,
		proof: ReturnType<typeof executionDomainProof>,
		view: FilesystemReadView,
	) {
		const captured = new Map<string, FileContentVersion>();
		const denied = sensitivePaths(this.options.storeRoot, this.options.deniedPaths);
		const candidates: { target: string; file: FileContentVersion; dependency: DynamicDependency }[] = [];
		const seen = new Set<string>();
		for (const input of observation.paths) {
			if (input.role === "metadata" || input.lookupFailure || !pathContains(root, input.path)) continue;
			try {
				const target = await view.realpath(input.path);
				if (!pathContains(root, target) || denied.some((dir) => pathContains(dir, target)) || seen.has(target))
					continue;
				seen.add(target);
				const stat = await view.stat(target, false);
				const limit = Math.min(files.remainingBytes, speculativeConfig().filesystem.maxReadBytes);
				if (!stat.isFile() || stat.size > BigInt(limit)) continue;
				const file = (await files.borrow(target, stat, files.contents)) ??
					(await captureFileContent(target, limit, files.contents, { stat, realPath: target, view }));
				const dependency = { kind: "file", path: target, role: input.role, contentDigest: contentDigest(file.content) } as const;
				// This file read owns its own snapshot. Crediting it to the process also requires continuity since launch.
				files.remember(target, file);
				candidates.push({ target, file, dependency });
			} catch {
				this.store.signal.throwIfAborted();
			}
		}
		if (candidates.length) {
			if (proof.seal(candidates.map((item) => item.dependency), (name) => name)()) {
				for (const { target, file } of candidates) captured.set(target, file);
			} else if (candidates.length > 1) {
				for (const { target, file, dependency } of candidates)
					if (proof.seal([dependency], (name) => name)()) captured.set(target, file);
			}
		}
		await this.publishFileInputs(files);
		return captured;
	}
	private async publishFileInputs(files: ResourceObjectInputs, session?: ActiveSession) {
		this.add(session, "fileInputHits", files.hits);
		this.add(session, "fileInputsPublished", await TimelineInterval.outside(() => files.publish().catch(() => 0)));
	}
	fileInputs(workspace: string, signal: AbortSignal, lookup?: ExecutionResourceContext["inputs"]) {
		const limits = this.configuration.current.runtime;
		return limits.resourceCacheMaxEntries && limits.resourceCacheMaxBytes
			? new ResourceObjectInputs(workspace, this.store, signal, lookup)
			: undefined;
	}
	async open(
		input: Pick<ExecutionResourceContext, "onOperationAdopted" | "acceptOperationScope"> & {
			readonly inputs?: ResourceObjectInputs;
			readonly sourceRoot: string;
			readonly workspace: SandboxWorkspaceContext;
			readonly invocation: ToolProcessInvocation | ProcessExecutionBinding;
			readonly scope?: ExecutionScope;
			readonly signal?: AbortSignal;
		},
	): Promise<LinuxProcessSession> {
		return this.withProducer(async (): Promise<LinuxProcessSession> => {
			const { session, ready, readyAt, controller } = await this.allocateSession(input);
			let executionState: "idle" | "tool" | "operation" = "idle";
			this.producers++;
			let dispatch: Promise<void> | undefined;
			const execute = <Value>(
				kind: "tool" | "operation",
				operation: () => Promise<Value>,
			): Promise<Value> => {
				if (
					session.lifecycle.sealed ||
					session.sealing.promise ||
					executionState === "operation" ||
					(executionState !== "idle" && executionState !== kind)
				)
					return Promise.reject(new Error("process session execution boundary is already consumed"));
				executionState = kind;
				return session.lifecycle.admit(() =>
					measureProcess(session, async () => {
						session.signal?.throwIfAborted();
						const dispatchStartedAt = performance.now();
						// Bound operations can launch reusable children too; interception excludes their own image below.
						await (dispatch ??= this.createProcessInterposition(session, ready).then(
							async (interposition) => {
								session.signal?.throwIfAborted();
								session.interposition = interposition;
								const dispatch = new TimelineInterval(dispatchStartedAt, performance.now());
								const computation = new TimelineInterval(
									creation.startedAt,
									dispatch.completedAt,
									[ownedDependency(creation), ownedDependency(dispatch)],
									[],
								);
								TimelineInterval.group(computation, session);
								// Only recorded preparation cuts its caller's clock; work between creation and dispatch belongs to that caller.
								session.preparation = { computation, shared: [creation, dispatch] };
								session.computations.push({ ...session.preparation, owned: true });
							},
						));
						session.sealing.privateSince ??= Date.now();
						await (session.sealing.transaction ??= session.workspace.transactions.begin());
						return operation();
					}),
				);
			};
			const creation = new TimelineInterval(readyAt, performance.now());
			let validating: Promise<ResourceValidation> | undefined;
			const currentAdmission = () => session.sealing.admission;
			const admissionFailure = (
				status: "stale" | "indeterminate",
				code: string,
				detail?: string,
				metrics = zeroValidationMetrics(),
			): ResourceValidation => ({
				status,
				cause: detail === undefined ? cause("freshness", code) : { stage: "freshness", code, detail },
				metrics,
			});
			const inspectAdmission = async (): Promise<ResourceValidation> => {
				const initial = currentAdmission();
				if (initial.kind === "disposed") return admissionFailure("indeterminate", "process_session_disposed");
				try {
					ready.collector.check();
				} catch (error) {
					return admissionFailure("indeterminate", "execution_domain_unavailable", errorMessage(error));
				}
				if (initial.kind === "verified") {
					if (initial.valid()) return { ...initial.result, metrics: zeroValidationMetrics() };
					initial.proof.close();
					session.sealing.admission = { kind: "unverified" };
				}
				const evidence = session.sealing.topLevelEvidence;
				const paths =
					evidence?.dependencies.flatMap((dependency) =>
						"path" in dependency
							? [
									dependency.path,
									...(dependency.kind === "file"
										? (dependency.aliases ?? [])
										: dependency.kind === "symlink"
											? [path.resolve(path.dirname(dependency.path), dependency.target)]
											: []),
								]
							: [],
					) ?? [];
				const policy = (mounts: string) =>
					JSON.stringify([
						filesystemMountTopology(mounts, [session.sourceRoot], true),
						filesystemMountTopology(mounts, paths),
					]);
				const proof = executionDomainProof(ready.collector, session.sourceRoot);
				try {
					if (policy(await readFile("/proc/self/mountinfo", "utf8")) !== policy(session.sourceMounts))
						return admissionFailure("stale", "source_mount_policy_changed");
					const result = await validateTransferredProcessEvidence(
						evidence,
						session.incompleteReasons,
						await session.hostReads,
					);
					if (result.status !== "valid") return result;
					if (currentAdmission().kind === "disposed")
						return admissionFailure("indeterminate", "process_session_disposed");
					const valid = proof.seal(evidence!.dependencies, (target) => target);
					if (!valid())
						return admissionFailure("stale", "process_inputs_changed_during_admission", proof.reason, result.metrics);
					// The session owns both this proof and the prepared result; its final commit
					// rechecks continuity instead of repeating the same dependency reads.
					session.sealing.admission = { kind: "verified", proof, valid, result };
					return result;
				} finally {
					const settled = currentAdmission();
					if (settled.kind !== "verified" || settled.proof !== proof) proof.close();
				}
			};
			const validate = () =>
				(validating ??= inspectAdmission().finally(() => {
					validating = undefined;
				}));
			const seal = () =>
				(session.sealing.promise ??= this.withProducer(async () => {
					await session.lifecycle.drain();
					try {
						const external = await privateCommits(
							session,
							session.sealing.privateSince ?? 0,
							sessionWriters(session),
						);
						if (!external.complete)
							throw new Error(
								`unrepresentable writes outside the workspace: external_write:${[...external.before].find(([, value]) => !value)?.[0]}`,
							);
						const changes = [...(await sealSessionEvidence(session, external)), ...external.changes];
						await validate();
						return changes;
					} finally {
						if (session.inputs) await this.publishFileInputs(session.inputs, session);
					}
				}));
			const result: LinuxProcessSession = {
				ownership: session.ownership,
				computationDependencies: () => Object.freeze([...session.computations]),
				executionBindings: () => orderedBindings(session.executionBindings),
				executor: {
					execute: (request) =>
						execute("tool", async () => {
							const { invocation } = session;
							if (!invocation) throw new Error("process session has no whole-tool invocation");
							if (session.lifecycle.sealed) throw new Error("Linux process session is closed");
							assertInvocationMatches(invocation, request);
							const physicalCwd = session.projection.toPhysical(path.resolve(request.cwd));
							if (
								!pathContains(session.sourceRoot, path.resolve(request.cwd)) ||
								!physicalCwd ||
								!pathContains(session.workspace.sandboxRoot, physicalCwd)
							)
								throw new Error("process cwd is unmapped");
							const environment = definedProcessEnvironment(request.environment);
							const { output: prototype, computation: preparation } = await TimelineInterval.outside(() =>
								TimelineInterval.measure(() =>
									this.topLevelPrototype(invocation, request, session.projection, environment),
								),
							);
							this.add(session, "wholeCommandRequests");
							const result = await this.executeRequest(
								session,
								{
									argv0: invocation.shell,
									args: shellArguments(invocation, request.command),
									cwd: physicalCwd,
									environment,
									outputRoute: [1, 2],
								},
								invocation.shell,
								-session.metrics.wholeCommandRequests,
								{ prototype, preparation, outlet: request },
							);
							if (result.kind === "hit") for (const event of result.output ?? []) request.onData(event.data);
							return {
								exitCode: result.exit?.kind === "code" ? result.exit.code : null,
							};
						}),
				},
				executeBinding: (binding) =>
					withSubcomputeScope(() => execute("operation", async () => {
						const invocation = this.boundInvocation(binding, session.scope);
						if (
							invocation.sourceRoot !== session.sourceRoot ||
							(invocation.producer && !stableEqual(session.nestedProducer, invocation.producer))
						)
							throw new Error("process execution binding is unavailable in this scope");
						const {
							output: { request, executable, prototype },
							computation: preparation,
						} = await TimelineInterval.outside(() =>
							TimelineInterval.measure(() =>
								this.boundRequest(binding, invocation, session.projection, session.signal, true),
							),
						);
						const resultsBefore = session.preparedResults;
						this.add(session, "requests");
						const result = await this.executeRequest(
							session,
							request,
							executable,
							session.metrics.requests,
							{ prototype, preparation },
						);
						session.signal.throwIfAborted();
						if (
							!invocation.producer &&
							result.kind === "executed" &&
							result.reusable === false &&
							session.preparedResults === resultsBefore
						) {
							throw new Error(
								`bound process preparation produced no reusable result${session.metrics.lastError ? `: ${session.metrics.lastError}` : ""}`,
							);
						}
						return {
							output: result.output ?? [],
							exit: result.exit!,
						};
					})),
				metrics: () => Object.freeze({ ...session.metrics }),
				seal,
				validate,
				close: () =>
					session.lifecycle.close(async () => {
						controller.abort(new Error("Linux process session closed"));
						try {
							await dispatch?.catch(() => undefined);
							await seal().catch(() => undefined);
						} finally {
							try {
								session.releaseVersions?.();
								session.privateStructure.clear();
								session.workspaceDomain?.close();
								session.domain.close();
							} finally {
								this.producers--;
							}
						}
					}),
				dispose: async () => {
					if (session.sealing.admission.kind === "verified") session.sealing.admission.proof.close();
					session.sealing.admission = { kind: "disposed" };
					try {
						await result.close();
					} finally {
						await validating?.catch(() => undefined);
						await session.hostReads?.then(
							(view) => view.close(),
							() => undefined,
						);
					}
				},
			};
			return result;
		});
	}
	private async allocateSession(input: Parameters<LinuxProcessReuseBackend["open"]>[0]) {
		if (this.disposal) throw new Error("Linux process backend is disposed");
		const ready = await TimelineInterval.outside(
			async () => (await TimelineInterval.collect(() => this.resolveReady())).output,
		);
		const readyAt = performance.now();
		input.signal?.throwIfAborted();
		const sourceRoot = path.resolve(input.sourceRoot);
		const sourceMounts = await readFile("/proc/self/mountinfo", "utf8");
		if (
			input.workspace.metadataMounts !== undefined &&
			filesystemMountTopology(input.workspace.metadataMounts, [sourceRoot], true) !==
				filesystemMountTopology(sourceMounts, [sourceRoot], true)
		)
			throw new Error("source mount policy changed during workspace capture");
		const projection = new ExecutionPathProjection({
			sourceRoot,
			workspaceRoot: input.workspace.sandboxRoot,
		});
		const invocation = "environment" in input.invocation ? input.invocation : undefined;
		const bound =
			"environment" in input.invocation ? undefined : this.boundInvocation(input.invocation, input.scope);
		if (bound && bound.sourceRoot !== sourceRoot) throw new Error("bound process workspace changed");
		const token = randomBytes(32).toString("hex");
		const processRoot = await mkdtemp(path.join(input.workspace.processRoot, "session-"));
		const boundary = await this.heldBoundary();
		const deniedPaths = sensitivePaths(this.options.storeRoot, this.options.deniedPaths).filter(
			(target) =>
				!pathContains(input.workspace.sandboxRoot, target) &&
				!pathContains(input.workspace.processRoot, target),
		);
		const producer = speculativeProducerProof(ready, deniedPaths, POLICY_ID);
		const nestedProducer = speculativeProducerProof(ready, deniedPaths, LEAF_POLICY_ID);
		const controller = new AbortController();
		const gitDirectory = await lstat(path.join(sourceRoot, ".git")).then(
			(info) => (info.isDirectory() ? path.join(sourceRoot, ".git") : undefined),
			() => undefined,
		);
		const domain = ready.collector.open();
		const upper = path.join(processRoot, "private", "writes", "upper");
		let workspaceDomain: typeof domain | undefined;
		let metadata: ReturnType<typeof executionRecords>;
		let releaseVersions: (() => void) | undefined;
		try {
			metadata = executionRecords(domain.trace(ready.sandlock), upper);
			if (input.workspace.structure.retain) workspaceDomain = ready.collector.open();
			await mkdir(upper, { recursive: true, mode: 0o700 });
			if (input.workspace.metadataImage) {
				// The COW root is bookkeeping storage. A process statting / observes the host root.
				const rows = JSON.parse(await readFile(input.workspace.metadataImage, "utf8"));
				const roots = await Promise.all([lstat(upper, { bigint: true }), lstat("/", { bigint: true })]);
				rows.push([
					...roots.map((info) => FILESYSTEM_METADATA_FIELDS.map((field) => String(info[field]))),
					0,
				]);
				await writeFile(input.workspace.metadataImage, JSON.stringify(rows));
			}
			domain.watch([path.join(processRoot, "private")]);
			if (workspaceDomain) releaseVersions = input.workspace.structure.retain!(workspaceDomain);
		} catch (error) {
			workspaceDomain?.close();
			domain.close();
			throw error;
		}
		const session = {
			token,
			processRoot,
			sourceRoot,
			sourceMounts,
			projection,
			deniedPaths,
			producer,
			nestedProducer,
			boundary,
			// The workspace's own repository stays read-only instead of exposing the snapshot's repository.
			...(gitDirectory ? { gitDirectory } : {}),
			workspace: input.workspace,
			inputs: input.inputs,
			invocation,
			scope: snapshotExecutionScope(input.scope),
			interposition: undefined as string | undefined,
			signal: AbortSignal.any([controller.signal, ...(input.signal ? [input.signal] : [])]),
			lifecycle: new RuntimeLifecycleLane(),
			ownership: new ProcessHandoffOwnership(input.onOperationAdopted, input.acceptOperationScope),
			// Completed reusable children and retained live frontiers produced by this preparation.
			preparedResults: 0,
			executionBindings: new Map<number, ProcessExecutionBinding>(),
			computations: [] as TimelineDependency[],
			// One physical session setup, shared by every result that depended on it.
			preparation: undefined as TimelineDependency | undefined,
			incompleteReasons: new Set<string>(),
			// Concurrent writers retain their live trace or settled writes for ownership attribution.
			writers: workspaceWriterRecords(input.workspace),
			fileEvidence: new WeakMap<object, SessionWriter>(),
			metrics: emptyWorldReuseMetrics() as MutableLinuxProcessReuseMetrics,
			// Children write into their parent's captured output sockets.
			outputs: {
				topLevel: undefined as readonly [string, string] | undefined,
				nested: new Set<readonly [string, string]>(),
			},
			sealing: {
				admission: { kind: "unverified" } as SessionAdmissionState,
				transaction: undefined as Promise<WorkspaceTransactionCapture> | undefined,
				capture: undefined as Promise<WorkspaceTransactionDelta> | undefined,
				promise: undefined as Promise<readonly SandboxWorkspaceChange[]> | undefined,
				topLevelEvidence: undefined as DynamicDependencyCertificate | undefined,
				privateSince: undefined as number | undefined,
			},
			hostReads: undefined as Promise<FilesystemReadView> | undefined,
			inputProofs: sharedWalk((key: string) =>
				captureInputProof(path.join(processRoot, "private", "writes", "inputs", key)),
			),
			domain,
			metadata,
			workspaceDomain,
			releaseVersions,
			privateStructure: new WorkspaceStructureCapture(
				upper,
				{
					exclude: [],
					content: {
						pool: input.workspace.contents,
						maxFileBytes: speculativeConfig().processReuse.privateFileBytes,
						maxBytes: speculativeConfig().processReuse.privateTreeBytes,
					},
				},
				domain,
			),
		};
		return { session, ready, readyAt, controller };
	}
	dispose(): Promise<void> {
		return (this.disposal ??= Promise.resolve().then(() => {
			const handoffs = this.handoffs.dispose();
			return this.resetNativeObservation().finally(() =>
				Promise.all([this.replayWorkspace.dispose(), this.store.dispose(), handoffs]),
			);
		}));
	}
	private async resolveReady(): Promise<ReadyBackend> {
		if (this.disposal) throw new Error("Linux process backend is disposed");
		this.ready ??= TimelineInterval.collect(() => this.probe()).then((result) => result.output);
		return this.ready;
	}
	private async resolvePlatformFingerprint(): Promise<Sha256Digest> {
		const native =
			nativeFilesystem ?? nativeProcessLibrary(await resolveLinuxExecHelper(this.options.heldExecBinary));
		const uts = Buffer.alloc(UTSNAME_BYTES);
		if (native(-16, uts) !== 0) throw new Error("cannot read the kernel UTS identity");
		return sha256Digest(uts);
	}
	private async probe() {
		if (process.platform !== "linux") throw new Error("Linux host required");
		if (!this.options.store?.reservedRoot)
			await mkdir(this.options.storeRoot, { recursive: true, mode: 0o700 });
		await chmod(this.options.storeRoot, 0o700);
		const [sandlock, dispatcher] = await Promise.all([
			resolveHostExecutable(this.options.sandlockBinary, "pi-speculative-sandlock", [
				path.join(os.homedir(), ".local", "bin", "pi-speculative-sandlock"),
			]),
			resolveLinuxExecHelper(this.options.heldExecBinary),
		]);
		// The exact confinement and tracer builds, not their versions: a patched fix changes what their certificates mean.
		nativeProcessLibrary(dispatcher);
		const [sandlockCheck, sandlockBuild, platformFingerprint] = await Promise.all([
			execHostText(sandlock, ["check"]),
			hashExecutableFile(sandlock),
			this.resolvePlatformFingerprint(),
		]);
		if (!sandlockCheck.includes("Status:         OK"))
			throw new Error("Sandlock kernel protections are unavailable");
		const collector = await executionDomainCollector(dispatcher);
		const mountProbe = await mkdtemp(path.join(os.tmpdir(), "pi-process-view-probe-"));
		let executionContext: ProcessExecutionContext | undefined;
		try {
			const logicalRoot = path.join(mountProbe, "logical");
			const physicalRoot = path.join(mountProbe, "physical");
			await Promise.all([mkdir(logicalRoot), mkdir(physicalRoot)]);
			const routing = path.join(mountProbe, "routing");
			const launcher = path.join(routing, "dispatcher");
			await mkdir(routing);
			await copyFile(dispatcher, launcher);
			await chmod(launcher, 0o755);
			await writeFile(path.join(physicalRoot, "script-position"), "#!/bin/sh\nexit 42\n", { mode: 0o700 });
			const metadataImage = path.join(physicalRoot, "metadata.json");
			const metadataStat = await lstat(path.join(physicalRoot, "script-position"), { bigint: true });
			const metadataRow = FILESYSTEM_METADATA_FIELDS.map((field) => String(metadataStat[field]));
			await writeFile(metadataImage, JSON.stringify([[metadataRow, metadataRow, 0]]), { mode: 0o600 });
			const command: [string, ...string[]] = [
				sandlock,
				...sandboxPolicyArguments(
					logicalRoot,
					[],
					[physicalRoot],
					[
						{
							virtualPath: logicalRoot,
							hostPath: physicalRoot,
							readOnly: false,
						},
						{ virtualPath: routing, hostPath: routing, readOnly: true },
					],
					[
						{ virtualPath: "/", hostPath: launcher },
						{ virtualPath: launcher, hostPath: launcher },
					],
					path.join(physicalRoot, "private"),
				),
				"--metadata-image",
				metadataImage,
				"--preserve-fd",
				"3",
				"--",
				launcher,
				"--probe-context",
				logicalRoot,
				path.join(logicalRoot, "script-position"),
			];
			const outcome = await runSpawn(command, {
				cwd: physicalRoot,
				environment: definedProcessEnvironment(process.env),
				onControl: (channel) => {
					const capture = (async () => {
						let bytes = Buffer.alloc(0);
						for await (const [chunk] of on(channel, "data", {
							close: ["end", "close"],
						})) {
							bytes = Buffer.concat([bytes, chunk]);
							if (bytes.length > 4) throw new Error("invalid context probe pid");
							if (bytes.length < 4) continue;
							const pid = bytes.readInt32LE();
							executionContext = await captureProcessContext(
								pid,
								(await readdir(`/proc/${pid}/fd`)).filter((name) => /^[012]$/.test(name)),
							);
							await writeProcessChannel(channel, Buffer.from("R"));
							channel.end();
							return;
						}
						throw new Error("context probe closed before capture");
					})();
					void capture.catch(() => channel.destroy());
					return () => capture;
				},
			});
			if (outcome.signal || outcome.code !== 0)
				throw new Error("process execution context probe did not finish");
			if (!validProcessContext(executionContext))
				throw new Error("process execution context probe returned invalid data");
		} catch (cause) {
			throw new Error(
				`process execution/metadata policy probe failed; rebuild with npm run setup:linux${await inheritedTracer()}`,
				{ cause },
			);
		} finally {
			await rm(mountProbe, { recursive: true, force: true });
		}
		const observerFingerprint = OBSERVER_FINGERPRINT;
		const fingerprint = digestObject({
			observerFingerprint,
			kernel: collector.fingerprint,
			policy: POLICY_ID,
			sandlock: sandlockBuild,
			platformFingerprint,
			executionContext: sha256Digest(executionContext.key),
			arch: process.arch,
			deniedPaths: sensitivePaths(this.options.storeRoot, this.options.deniedPaths),
		});
		return {
			sandlock,
			fingerprint,
			observerFingerprint,
			executionContext,
			dispatcher,
			collector,
		};
	}
	private async dispatchHeld(
		session: ActiveSession,
		owner: SessionWriter,
		held: HeldExecProcess,
	): Promise<HeldExecDecision> {
		const native = { kind: "continue" } as const;
		if (owner.trace.kind !== "traced" || !session.interposition || session.lifecycle.sealed) return native;
		const source = owner.trace.tail;
		const receipt = await readFile(session.interposition + ".route." + held.pid, "utf8").catch(ignoreMissing);
		if (!receipt) return native;
		const route = receipt.split("\0");
		if (route[0] !== "B") return native;
		const target = route[1];
		if (
			route.length !== 6 ||
			route.at(-1) !== "" ||
			!target?.startsWith("/") ||
			!route.at(-2)?.startsWith("/")
		)
			throw new Error("invalid exec route");
		const image = await open(`/proc/${held.pid}/exe`, "r");
		try {
			const imageSource = `/proc/${process.pid}/fd/${image.fd}`;
			const actual = await readlink(imageSource);
			const upper = privateUpper(session);
			if (
				session.projection.toLogical(actual.startsWith(`${upper}/`) ? actual.slice(upper.length) : actual) !==
				target
			)
				throw new Error("exec route image changed");
			const executable = session.projection.toPhysical(
				(await image.stat()).nlink ? target : target.replace(/ \(deleted\)$/, ""),
			)!;
			const snapshot = await inspectHeldExecProcess(held.pid, executable);
			const cwd = session.projection.toPhysical(route.at(-2)!);
			if (
				!cwd ||
				snapshot.invocation[0] !== route[2] ||
				snapshot.invocation[1] !== Buffer.from(route[3]!).subarray(0, 15).toString()
			)
				throw new Error("exec context changed");
			const { context, environment, invocation, argv } = snapshot;
			const args = argv.slice(1);
			this.add(session, "requests");
			const requestID = session.metrics.requests;
			const ready = await this.resolveReady();
			const eligibility = eligibleRequest(session, context, cwd, args, ready.executionContext);
			if (typeof eligibility === "string") {
				this.setError(session, `process_bypass:${eligibility}`, "bypasses");
				return native;
			}
			const resumed = Promise.withResolvers<void>();
			const exited = Promise.withResolvers<number>();
			const start = (await source.read()).journal(held.pid)?.length;
			if (start === undefined) return native;
			const domain = {
				channel: owner.channel,
				pid: held.pid,
				trace: traceTail(source, session.projection.toLogical(cwd), { execution: { pid: held.pid, start } }),
				endpoints: context.outputEndpoints,
				resume: resumed.resolve,
				exit: exited.promise,
			};
			const completed = this.executeRequest(
				session,
				{ argv0: argv[0]!, args, cwd, environment, invocation, imageSource, ...eligibility },
				executable,
				requestID,
				{ domain },
			);
			const running = session.lifecycle.track(completed);
			return await Promise.race([
				running.then((result) => result.held ?? native),
				resumed.promise.then(
					(): HeldExecDecision => ({
						kind: "continue",
						observeCompletion: async (_, clock) => {
							try {
								const events = (await source.read()).journal(held.pid);
								const status = events?.findLast((event) => event.name === "terminated")?.result;
								if (!clock || status === undefined) throw new Error("process_exit_unproven");
								exited.resolve(status);
							} catch (error) {
								exited.reject(error);
							}
							await running;
						},
					}),
				),
			]);
		} finally {
			await image.close();
		}
	}
	private async executeRequest(
		session: ActiveSession,
		request: ProcessArguments,
		executable: string,
		requestID: number,
		options: { domain: DomainExecution } | {
			prototype: ExecPrototype;
			preparation: TimelineInterval;
			outlet?: ProcessExecutionRequest;
		},
	): Promise<DispatcherResponse> {
		const domain = "domain" in options ? options.domain : undefined;
		const outlet = "outlet" in options ? options.outlet : undefined;
		return measureProcess(
			session,
			async () => {
				const { outputRoute } = request;
				const channel = controlledSubcomputeChannel(request.environment) ?? domain?.channel;
				const signal = AbortSignal.any([session.signal, ...(outlet?.signal ? [outlet.signal] : [])]);
				signal.throwIfAborted();
				const captured = "prototype" in options
					? { output: options.prototype, computation: options.preparation }
					: await TimelineInterval.outside(() =>
							TimelineInterval.measure(() => this.prototype(session.projection, request, executable, channel?.identity)),
						);
				const { computation: preparation, output: observedPrototype } = captured;
				const weakKey = processWeakKey(observedPrototype);
				const executablePath = observedPrototype.executablePath;
				const acquired = await TimelineInterval.outside(() =>
					this.handoffs.acquire({
						key: weakKey,
						signal,
						scope: session.scope,
						producer: { ownership: session.ownership, executablePath },
						lookup: (live, excludedCertificates, artifacts) =>
							this.planPrototype(
								observedPrototype,
								session.projection,
								(candidate) => stableEqual(outlet ? session.producer : session.nestedProducer, candidate),
								{ session, live, excludedCertificates, artifacts, signal },
							),
					}),
				);
				if (acquired.kind === "hit") {
					const plan = acquired.plan;
					const binding = acquired.producer?.binding;
					return TimelineInterval.outside(async (): Promise<DispatcherResponse> => {
						const { certificate } = plan;
						const replay = this.prepareReplay(plan, session.projection, {
							workspace: this.replayWorkspace,
							privateStorage: path.join(session.processRoot, "private"),
							acquired,
						});
						const { output, computation } = replay;
						const paths = certificate.result.journal.flatMap((event) =>
							event.kind === "workspace" ? [event.path] : [],
						);
						const startedAt = Date.now();
						const writer: SessionWriter = {
							id: `${session.token}:${requestID}`,
							owner: session.token,
							startedAt,
							pid: domain?.pid,
							evidence: certificate.dependencyCertificate,
							external: paths.filter((target) => !pathContains(session.sourceRoot, target)),
							written: paths
								.filter((target) => pathContains(session.sourceRoot, target))
								.map((target) => slash(path.relative(session.sourceRoot, target))),
							trace: {
								kind: "settled",
								writes: [
									{
										at: startedAt,
										opened: true,
										paths: paths.flatMap((target) => [target, session.projection.toPhysical(target)!]),
									},
								],
							},
							observed: new Map(
								certificate.dependencyCertificate.dependencies.flatMap((dependency) =>
									"path" in dependency && pathContains(session.sourceRoot, dependency.path)
										? [[slash(path.relative(session.sourceRoot, dependency.path)), startedAt] as const]
										: [],
								),
							),
						};
						const commit = async () => {
							session.writers.add(writer);
							await replay
								.commitEffects()
								.then(
									() => acquired.settle?.(true, true),
									(error) => {
										writer.observed = undefined;
										acquired.settle?.(
											!(error instanceof EffectCommitFailure && error.disposition === "recoverable"),
										);
										throw error;
									},
								)
								.finally(() => {
									writer.endedAt = Date.now();
								});
							if (outlet) this.add(session, "wholeCommandHits");
							else this.recordHit(acquired.producer?.scope, acquired.joined, session);
							recordProcessDependency(session, computation, domain?.pid);
							if (certificate.result.observedProcessMs !== undefined)
								this.add(session, outlet ? "wholeCommandReusedProcessMs" : "reusedProcessMs", replay.observedMs);
							if (!outlet && binding && this.handoffs.resolveBinding(binding, session.scope))
								session.executionBindings.set(requestID, binding);
						};
						if (domain && replay.exit.kind === "code")
							return {
								kind: "hit",
								held: {
									kind: "replay",
									validate: replay.validate,
									output,
									exitCode: replay.exit.code,
									commit,
									settle: (consumed) => acquired.settle?.(consumed),
								},
							};
						try {
							if (domain || !replay.validate()) return { kind: "bypass" };
							await commit();
							return { kind: "hit", output, exit: replay.exit };
						} finally {
							acquired.settle?.(false);
						}
					}).catch((error) => {
						acquired.settle?.(false);
						throw error;
					});
				}
				if (acquired.kind !== "work") throw new Error("process work reservation failed");
				this.add(session, outlet ? "wholeCommandMisses" : "misses");
				try {
					TimelineInterval.group(preparation, session);
					recordProcessDependency(session, ownedDependency(preparation), domain?.pid);
					const work = acquired.work;
					const ready = await this.resolveReady();
					const started = performance.now();
					const transaction = await session.workspace.transactions.begin();
					let outcome: SpawnOutcome | undefined;
					let reusable: boolean | undefined;
					let transactionFinishing = false;
					let releaseRunning: (() => void) | undefined;
					let inputCheck: Promise<boolean> | undefined;
					let stage = "capture";
					let dependencyCertificate: DynamicDependencyCertificate | undefined;
					let certificateID: Sha256Digest | undefined;
					let unattributed: ExecutionObservation | undefined;
					let writer: SessionWriter | undefined;
					const failureDetail = (error: unknown) =>
						`${errorMessage(error)}; process=${JSON.stringify({
							stage,
							requestID,
							weakKey,
							scope: session.scope,
							workspace: session.workspace.sandboxRoot,
							executable: observedPrototype.executablePath,
							certificateID,
							complete: dependencyCertificate?.complete,
							taints: dependencyCertificate?.taints,
						})}`;
					try {
						const privateBefore: PrivatePrestate = {
							startedAt: Date.now(),
							...(await privateBranch(path.join(session.processRoot, "private"))),
						};
						privateBefore.versions = await session.privateStructure.captureVersions();
						const image = session.projection.toLogical(executable);
						const dispatcher = session.interposition!;
						const launchPath = request.imageSource ?? image;
						const logicalCwd = session.projection.toLogical(request.cwd);
						let rootPid: number | undefined;
						const tail =
							domain?.trace ??
							traceTail(session.metadata, logicalCwd, { rootPid: () => rootPid, launchPath });
						const changedInput = async () => {
							const changes = session.workspace.sourceChanges?.();
							if (!changes?.paths.length || !transaction.readBefore) return false;
							let inputs: Set<string> | undefined;
							for (const changed of changes.paths) {
								const relative = relativeFilesystemPath(session.sourceRoot, changed);
								if (
									relative === undefined ||
									session.deniedPaths.some((denied) => pathContains(denied, changed))
								)
									continue;
								inputs ??= new Set(
									(
										await observeExecution(tail, image, logicalCwd, {
											platformFingerprint: observedPrototype.platformFingerprint,
											controlledSocket: channel?.socket,
											launchPath,
											previewBytes: speculativeConfig().processReuse.tracePreviewBytes,
											dispatcherSocket: session.boundary.endpoint,
										})
									).paths
										.filter((observed) => observed.role === "input")
										.map((observed) => path.resolve(observed.path)),
								);
								if (!inputs.has(changed)) continue;
								const before = await transaction.readBefore(
									slash(relative),
									speculativeConfig().processReuse.maxRequestBytes,
								);
								if (!before) continue;
								// Only observed inputs need comparison; the transaction prestate includes predecessor effects.
								await assertNoSymlinkPath(session.sourceRoot, changed);
								const current = await captureStableFile(
									changed,
									speculativeConfig().processReuse.maxRequestBytes,
								);
								this.add(undefined, "validationFilesRead", current.shared ? 0 : 1);
								this.add(undefined, "validationBytesRead", current.shared ? 0 : current.bytesRead);
								if (sha256Digest(before) === `sha256:${current.hash}`) continue;
								this.setError(undefined, `actor_running_input_changed:${changed}`);
								return true;
							}
							return false;
						};
						const advancing = (advance = async () => true) =>
							this.handoffs.observeRunning(
								weakKey,
								work,
								async () =>
									!(await (inputCheck ??= changedInput()
										.catch(() => true)
										.finally(() => {
											inputCheck = undefined;
										}))) && advance(),
							);
						releaseRunning = advancing();
						const command: [string, ...string[]] = [
							ready.sandlock,
							// A nested child's own children are brokered as the top level's are: a build tool's compilers run in its trace otherwise.
							...sandboxPolicyArguments(
								logicalCwd,
								session.deniedPaths,
								[session.workspace.sandboxRoot, ...(channel ? [channel.socket] : [])],
								[
									{
										virtualPath: session.sourceRoot,
										hostPath: session.workspace.sandboxRoot,
										readOnly: false,
									},
									{
										virtualPath: path.dirname(dispatcher),
										hostPath: path.dirname(dispatcher),
										readOnly: true,
									},
									...(channel?.readOnlyDirectories ?? []).map((dir) => ({ virtualPath: dir, hostPath: dir, readOnly: true })),
									...(session.gitDirectory
										? [
												{
													virtualPath: session.gitDirectory,
													hostPath: session.gitDirectory,
													readOnly: true,
												},
											]
										: []),
								],
								[
									{ virtualPath: "/", hostPath: dispatcher },
									{ virtualPath: dispatcher, hostPath: dispatcher },
									...(request.imageSource ? [{ virtualPath: launchPath, hostPath: launchPath }] : []),
								],
								path.join(session.processRoot, "private"),
							),
							...(session.workspace.metadataImage
								? ["--metadata-image", session.workspace.metadataImage]
								: []),
							...(outlet?.timeout !== undefined
								? ["--timeout", String(Math.max(1, Math.ceil(outlet.timeout)))]
								: []),
							"--",
							dispatcher,
							request.closeStdin ? "--exec-closed-input" : "--exec",
							outputRoute.join("") +
								(request.stackLimit !== undefined ? `:${request.stackLimit}` : "") +
								(request.outputFlags ? `,${request.outputFlags.join(",")}` : ""),
							request.argv0,
							launchPath,
							...request.args,
						];
						const processStarted = performance.now();
						session.writers.add(
							(writer = {
								channel,
								id: `${session.token}:${requestID}`,
								owner: session.token,
								startedAt: Date.now(),
								trace: { kind: "traced", tail },
								...(domain ? { pid: domain.pid } : {}),
							}),
						);
						stage = "execution";
						let outputEndpoints = domain?.endpoints;
						let completedObservation: ExecutionObservation | undefined;
						const proofOwners = new Set<SessionWriter>();
						const observe = () =>
							completedObservation
								? Promise.resolve(completedObservation)
								: observeExecution(tail, image, logicalCwd, {
										platformFingerprint: observedPrototype.platformFingerprint,
										controlledSocket: channel?.socket,
										launchPath,
										brokeredWrites: (pid) => brokeredWrites(session, pid, writer),
										privateUpper: privateUpper(session),
										dispatcherSocket: session.boundary.endpoint,
										fileEvidence: (line) => {
											const owner = session.fileEvidence.get(line);
											if (!owner || owner === writer) return false;
											proofOwners.add(owner);
											return true;
										},
										...(domain
											? {
													execution: { pid: domain.pid, start: 0 },
													captureOutput: true as const,
													outputFlags: request.outputFlags ?? ([0, 0] as const),
												}
											: {}),
										...(outputEndpoints ? { outputEndpoints, captureOutput: true as const } : {}),
										guardFilesystemSemanticsWithin: [session.workspace.sandboxRoot, session.sourceRoot],
									});
						if (domain) {
							domain.resume();
							const exited = await waitForCompletion(domain.exit, session.signal);
							session.signal.throwIfAborted();
							if (exited.status !== "completed") throw new Error("execution domain closed");
							writer.endedAt = Date.now();
							completedObservation = await observe();
							const signal = exited.value & 127;
							outcome = {
								code: signal ? null : exited.value >> 8,
								signal: signal
									? ((Object.entries(os.constants.signals).find(
											([, number]) => number === signal,
										)?.[0] as NodeJS.Signals) ?? "SIGKILL")
									: null,
								output: completedObservation.output ?? [],
							};
						} else {
							outcome = await session.boundary
								.trace(
									{
										sourceRoot: session.sourceRoot,
										scope: session.scope,
										signal,
										descriptors: false,
										decide: (held) => this.dispatchHeld(session, writer!, held),
									},
									() =>
										runSpawn([ready.dispatcher, "--output-channel=3", ...session.domain.launch(command)], {
											held: true,
											onSpawn: (child) => {
												rootPid = child.pid;
											},
											onOutputEndpoints: (endpoints) => {
												session.outputs.nested.add((outputEndpoints = endpoints));
												session.outputs.topLevel = endpoints;
											},
											cwd: request.cwd,
											environment: request.environment,
											signal: AbortSignal.any([
												session.signal,
												work.signal,
												...(outlet?.signal ? [outlet.signal] : []),
											]),
											onData: outlet?.onData,
											timeout: outlet?.timeout,
											...(outlet && session.invocation?.commandTransport === "stdin"
												? { stdin: Buffer.from(outlet.command, "utf8") }
												: {}),
										}),
								)
								.finally(() => {
									writer!.endedAt = Date.now();
								});
						}
						if (!domain && outputEndpoints) {
							session.outputs.nested.delete(outputEndpoints);
							completedObservation = verifyProcessOutput(await observe(), outcome.output);
							if (completedObservation.complete && completedObservation.output)
								outcome = { ...outcome, output: completedObservation.output };
						}
						// Replays write each event to the target's own descriptor, whichever outlet this route gave it.
						if (!domain)
							outcome = {
								...outcome,
								output: outcome.output.map((event) => ({
									...event,
									fd: outputRoute[0] === event.fd ? 1 : 2,
								})),
							};
						const observedProcessMs = Math.max(0, performance.now() - processStarted);
						releaseRunning();
						try {
							transactionFinishing = true;
							const observing = observe();
							const roots = [session.workspace.sandboxRoot, session.sourceRoot];
							const own = writer!;
							const named = (target: string) =>
								roots.flatMap((root) =>
									pathContains(root, target) ? [slash(path.relative(root, target))] : [],
								)[0];
							const ownership = () =>
								observing.then(async (observation) => {
									own.pids = observation.pids;
									own.closedPids =
										observation.complete &&
										!observation.taints.some((taint) =>
											["network", "ipc", "unsupported_syscall"].includes(taint),
										)
											? observation.pids
											: undefined;
									// When it last looked at each workspace name; one the trace cannot place counts as seen at its end.
									const looked = new Map<string, number>();
									for (const [target, at] of (await tail.read()).seen) {
										const name = named(target);
										if (name !== undefined) looked.set(name, Math.max(looked.get(name) ?? 0, at));
									}
									// A run brokered from its own tree, which its trace holds as a launcher, wrote on its behalf.
									const traced = new Set(observation.pids);
									const children = sessionWriters(session).filter(
										(other) => other !== own && other.pid !== undefined && traced.has(other.pid),
									);
									const descendants = (own.descendants = [
										...new Set(children.flatMap((child) => [child, ...(child.descendants ?? [])])),
									]);
									own.written = observation.written ?? [];
									own.observed = new Map(
										observation.paths
											.flatMap(({ path: target }) => named(target) ?? [])
											.map((name) => [name, looked.get(name) ?? own.endedAt!] as const),
									);
									return writerOwnership(session, [own, ...descendants]);
								});
							const captures = [transaction.finish(ownership), observing] as const;
							const [delta, observation] = await Promise.all(captures).catch(async (error: unknown) => {
								// Both captures own live workspace/trace resources until they settle.
								stage = (await Promise.allSettled(captures))
									.flatMap((result, index) =>
										result.status === "rejected"
											? [index === 0 ? "transaction_capture" : "trace_capture"]
											: [],
									)
									.join("+");
								throw error;
							});
							own.written ??= observation.written ?? [];
							own.external = observation.external ?? [];
							if (observation.incompleteReasons.length) {
								this.setError(session, `trace:${observation.incompleteReasons.join(",")}`);
								for (const reason of observation.incompleteReasons)
									session.incompleteReasons.add(`nested_trace:${reason}`);
							}
							if (!delta.complete) {
								stage = "transaction_capture";
								unattributed = observation;
								throw new Error(`workspace transaction is incomplete: ${delta.reason}`);
							}
							const { before, after } = delta;
							stage = "workspace_effects";
							const effects = diffWorkspaceStructures(before, after, delta.changes, session.projection);
							if (!effects.complete) unattributed = observation;
							stage = "dependencies";
							const sources = evidenceSources([...(own.descendants ?? []), ...proofOwners]);
							const observed = [
								channel ? await channel.observe(observation) : observation,
								...sources.flatMap((child) => child.observation ?? []),
							];
							const external = await privateCommits(
								session,
								own.startedAt,
								[own, ...(own.descendants ?? [])],
								privateBefore,
							);
							const evidence = await captureDependencies(
								session,
								before,
								observed,
								effects,
								external,
								sources.flatMap((child) => child.evidence ?? []),
								privateBefore,
							);
							if (evidence.incompleteReasons.length)
								this.setError(session, `evidence:${evidence.incompleteReasons.join(",")}`);
							const taints = new Set<ProvenanceTaint>(evidence.taints);
							dependencyCertificate = {
								complete: evidence.complete,
								dependencies: evidence.dependencies,
								taints: [...taints],
							};
							stage = "artifacts";
							await this.store.withPublication(async () => {
								const journal: OrderedEffectEvent[] = [];
								for (const { logicalPath, change } of [
									...effects.effects,
									...external.changes.map((change) => ({
										logicalPath: slash(change.target),
										change,
									})),
								]) {
									const state = async (side: "before" | "after"): Promise<WorkspaceEffectState> => {
										if (change.kind === "directory")
											return change[side]
												? {
														kind: "directory",
														...directoryState(change[side]!),
													}
												: { kind: "absent" };
										const content = change[side];
										const mode = change[side === "before" ? "beforeMode" : "afterMode"];
										if (content === undefined) return { kind: "absent" };
										if (mode === undefined) throw new Error("transaction file mode is unavailable");
										return {
											kind: "file",
											data: await publishContent(content, this.store),
											mode,
											...(side === "after" && change.afterModified ? { modified: change.afterModified } : {}),
										};
									};
									journal.push({
										sequence: journal.length,
										kind: "workspace",
										path: logicalPath,
										before: await state("before"),
										after: await state("after"),
										...(change.kind !== "directory" ? projectFileMutation(change) : {}),
									});
								}
								await this.appendOutputJournal(journal, outcome!.output);
								const computation = TimelineInterval.current(work.startedAt, performance.now(), [
									...(preparation ? [ownedDependency(preparation)] : []),
									...(session.preparation ? [session.preparation] : []),
									...session.computations.filter((input) => {
										const pid = processDependencyPIDs.get(input);
										return pid !== undefined && observation.pids?.includes(pid);
									}),
								]);
								TimelineInterval.group(computation, session);
								recordProcessDependency(session, ownedDependency(computation), domain?.pid);
								const result: ProcessResultRecord = {
									replayProfile: "buffered_noninteractive",
									streamMetadataObserved: observation.streamMetadataObserved,
									observedProcessMs,
									journal,
									exit: exitOutcome(outcome!),
									computation: TimelineInterval.serialize(computation),
								};
								stage = "certificate";
								let input: import("./provenance-certificate.ts").ProcessCertificateInput = {
									prototype: observedPrototype,
									producer: outlet ? session.producer : session.nestedProducer,
									dependencyCertificate: dependencyCertificate!,
									result,
								};
								const transferable = certificateReplayable(input, acceptedReplayTaints());
								// A child can read an earlier child's private output. Its parent can prove that input from the combined
								// transaction even when this child, considered alone against the host, could not.
								if (!evidence.sourceComplete && observation.complete && external.complete)
									own.observation = observation;
								else {
									own.evidence = dependencyCertificate;
									for (const file of tail.records)
										for (const line of file.lines)
											if (!session.fileEvidence.has(line)) session.fileEvidence.set(line, own);
								}
								if (taints.size) {
									if (!outlet) this.add(session, "tainted");
									// Transferable input categories must not hide existing failures. Diagnostics never change sealed evidence.
									const reasons = (stage: string, values: readonly string[]) =>
										values
											.slice(0, speculativeConfig().processReuse.diagnosticReasons)
											.map((value) => `${stage}:${value}`)
											.concat(
												values.length > speculativeConfig().processReuse.diagnosticReasons
													? [`${stage}:more_reasons`]
													: [],
											);
									const detail = [
										`tainted:${[...taints].join(",")}`,
										...(observation.unsupportedSyscalls?.length
											? [
													`syscalls:${observation.unsupportedSyscalls.join(",")}${observation.unsupportedSyscallsTruncated ? ",…" : ""}`,
												]
											: []),
										...reasons("trace", observation.incompleteReasons),
										...reasons("dependency", evidence.incompleteReasons),
									].join("; ");
									if (!session.metrics.lastError || (!outlet && !transferable))
										this.setError(session, detail);
								}
								stage = "certificate_publication";
								await channel?.validate();
								if (speculativeConfig().processReuse.persistCompletedCertificates)
									await this.publishCompletedCertificate(input);
								stage = "handoff_registration";
								if (await this.handoffs.publish(weakKey, work, input, computation))
									this.add(session, outlet ? "wholeCommandPublished" : "published");
								const { streams: _streams, imageSource: _imageSource, ...invocation } = request;
								const binding =
									!outlet &&
									this.handoffs.bind(weakKey, work, {
										...invocation,
										...boundEnvironment(invocation.environment),
										cwd: logicalCwd,
										executable: image,
										sourceRoot: session.sourceRoot,
										outputRoute,
										producer: session.nestedProducer,
									});
								if (binding) session.executionBindings.set(requestID, binding);
								if (!outlet) {
									// A sealed frontier or input seed remains useful even when its result cannot enter history.
									reusable = transferable;
									if (reusable) session.preparedResults++;
								}
							}, true);
						} catch (error) {
							// The process already ran. Certificate failure must never cause dispatcher fallback/re-execution.
							const detail = failureDetail(error);
							if (!outlet || !session.incompleteReasons.size)
								this.setError(session, `post_execution_capture:${detail}`);
							// Its command captures the effects whole: what it observed joins the command's evidence, measured from the command's start.
							if (unattributed?.complete && !unattributed.incompleteReasons.length)
								writer!.observation = unattributed;
							else session.incompleteReasons.add(`nested_capture:${detail}`);
						}
						const exit = exitOutcome(outcome);
						return {
							kind: "executed",
							output: outcome.output,
							exit,
							...(reusable !== undefined ? { reusable } : {}),
						};
					} catch (error) {
						if (stage !== "capture" || !domain || session.signal.aborted || work.signal.aborted)
							throw error;
						this.setError(session, failureDetail(error), "bypasses");
						return { kind: "bypass" };
					} finally {
						releaseRunning?.();
						await inputCheck;
						const durationMs = Math.max(0, performance.now() - started);
						if (!outlet) this.add(session, "executionMs", durationMs);
						if (!transactionFinishing)
							await transaction.abort(stage === "capture" ? true : undefined).catch(() => undefined);
						if (writer) {
							writer.endedAt ??= Date.now();
							if (writer.trace.kind === "traced") {
								const activeTail = writer.trace.tail;
								writer.trace = await activeTail.read().then(
									(snapshot): WriterTraceState => ({
										kind: "settled",
										writes: snapshot.writes,
										unsafe: snapshot.unsafe,
									}),
									(): WriterTraceState => ({
										kind: "settled",
										writes: [{ at: writer!.startedAt, opened: true }],
									}),
								);
								await activeTail[Symbol.asyncDispose]();
							}
						}
					}
				} finally {
					this.handoffs.complete(weakKey, acquired.work);
				}
			},
			domain?.pid,
		);
	}
	private prepareReplay(
		plan: ReadyProcessPlan,
		projection: ExecutionPathProjection,
		options: {
			workspace?: Pick<WorkspaceSandboxService, "commitDelta">;
			privateStorage?: string;
			acquired?: {
				readonly producer?: ProcessHandoff;
				readonly waiting?: readonly TimelineInterval[];
			};
		} = {},
	) {
		const { artifacts, certificate, validate } = plan;
		return {
			validate,
			exit: certificate.result.exit,
			observedMs: certificate.result.observedProcessMs ?? 0,
			output: loadOutputEvents(artifacts, certificate.result.journal),
			computation: reusedComputation(certificate, options.acquired),
			commitEffects: () =>
				replayFilesystemEffects(
					options.workspace ?? this.replayWorkspace,
					this.store,
					artifacts,
					certificate.result.journal,
					projection,
					options.privateStorage,
				),
		};
	}
	private async planPrototype(
		prototype: ExecPrototype,
		projection: ExecutionPathProjection,
		accept: (producer: ProcessProducerProof) => boolean,
		options: Parameters<LinuxProcessReuseBackend["plan"]>[4],
	) {
		const exact = await this.plan(
			processWeakKey(prototype),
			prototype.executablePath,
			projection,
			accept,
			options,
		);
		if (exact || options?.live) return exact;
		const bytesOnly = byteStreamPrototype(prototype);
		return bytesOnly
			? this.plan(processWeakKey(bytesOnly), prototype.executablePath, projection, accept, options)
			: undefined;
	}
	private async plan(
		weakKey: Sha256Digest,
		executablePath: string,
		projection: ExecutionPathProjection,
		acceptProducer: (producer: ProcessProducerProof) => boolean,
		{
			session,
			live,
			artifacts,
			excludedCertificates,
			signal = session?.signal,
		}: {
			session?: ActiveSession;
			live?: readonly ProcessProvenanceCertificate[];
			artifacts?: ReadonlyMap<Sha256Digest, VerifiedArtifactClosure>;
			excludedCertificates?: ReadonlySet<Sha256Digest>;
			signal?: AbortSignal;
		} = {},
	): Promise<ReadyProcessPlan | undefined> {
		if (!live && !(await this.store.hasWeakKey(weakKey, executablePath, signal))) return;
		await using privateReads = session ? FilesystemReadView.ambient() : undefined;
		let proof = session
			? undefined
			: executionDomainProof((await this.resolveReady()).collector, projection.sourceRoot);
		try {
			// Proof reads belong to the caller's excluded lookup interval, even when file capture emits owned receipts.
			const plan = (
				await TimelineInterval.collect(() =>
					this.planner.plan({
						weakKey,
						executablePath,
						acceptProducer,
						excludedCertificates,
						signal,
						contract: {
							sink: "buffered",
							orderedJournal: true,
							transactionalEffects: true,
						},
						validation: {
							resolvePath: (logicalPath) => projection.toPhysical(logicalPath),
							acceptedTaints: acceptedReplayTaints(),
							...(session
								? {
										readView: (physical: string) =>
											projection.isWorkspacePhysical(physical) ? privateReads : undefined,
									}
								: {}),
						},
						...(live
							? {
									live: {
										certificate: live,
										artifacts,
										acceptedTaints: acceptedReplayTaints(),
									},
								}
							: {}),
					}),
				)
			).output;
			for (const [metric, value] of [
				["validationCandidates", plan.lookup.candidateCertificates],
				["validationPathsets", plan.lookup.pathsetsValidated],
				["validationFilesRead", plan.lookup.filesRead],
				["validationBytesRead", plan.lookup.bytesRead],
				["validationArtifactsLoaded", plan.lookup.artifactsLoaded],
				["validationArtifactBytesRead", plan.lookup.artifactBytesRead],
			] as const) {
				this.add(session, metric, value);
			}
			if (plan.kind === "miss" && plan.lookup.candidateCertificates > 0) {
				const detail = `reuse_miss:${plan.reasons.join(",")}${plan.changedDependencies?.length ? `:${plan.changedDependencies.join(",")}` : ""}`;
				if (session) this.setError(session, detail);
				else this.setError(undefined, `actor_${detail}`);
			}
			if (plan.kind === "miss") return;
			const validate =
				proof?.seal(plan.certificate.dependencyCertificate.dependencies, (target) =>
					projection.toPhysical(target),
				) ?? (() => true);
			if (!validate()) {
				this.setError(session, `input_admission:${proof?.reason}`);
				await plan.artifacts.dispose();
				return;
			}
			if (proof) {
				// This closure belongs to this lookup alone; its existing artifact lifetime also owns the proof.
				const dispose = plan.artifacts.dispose.bind(plan.artifacts);
				const { close } = proof;
				Object.defineProperty(plan.artifacts, "dispose", {
					value: () => dispose().finally(close),
				});
				proof = undefined;
			}
			return { ...plan, validate };
		} finally {
			proof?.close();
		}
	}
	private async decideHeldExec(process: HeldExecProcess, scope?: ExecutionScope, record?: ActorImageRecorder, parentSteps?: string): Promise<HeldExecDecision> {
		const observation = this.observations.getStore();
		let releaseLearning: (() => void) | undefined;
		const measured = (decision: HeldExecDecision): HeldExecDecision => {
			if (decision.kind === "replay" && observation?.claims) {
				const settle = decision.settle;
				const claims = observation.claims;
				Atomics.add(claims, 0, 1);
				decision = {
					...decision,
					settle: releaseOnce((consumed: boolean) => {
						if (!consumed) Atomics.sub(claims, 0, 1);
						return settle?.(consumed);
					}),
				};
			}
			if (releaseLearning && decision.kind === "continue" && decision.observeCompletion) {
				const complete = decision.observeCompletion;
				const release = releaseLearning;
				releaseLearning = undefined;
				decision = {
					...decision,
					observeCompletion: async (...args) => {
						try {
							await complete(...args);
						} finally {
							release();
						}
					},
				};
			}
			decision =
				decision.kind === "continue"
					? {
							...decision,
							observeCompletion:
								decision.observeCompletion && AsyncLocalStorage.bind(decision.observeCompletion),
						}
					: {
							...decision,
							validate: AsyncLocalStorage.bind(decision.validate),
							commit: AsyncLocalStorage.bind(decision.commit),
							adopted: decision.adopted && AsyncLocalStorage.bind(decision.adopted),
							settle: decision.settle && AsyncLocalStorage.bind(decision.settle),
						};
			return !observation || observation.closed || !sameScope(observation.scope, scope)
				? decision
				: {
						...decision,
						observeTiming: (timing) => {
							if (!observation.closed) observation.heldClock.record(timing);
						},
					};
		};
		const learning =
			observation?.learn &&
			!observation.closed &&
			sameScope(observation.scope, scope) &&
			observation.pending < speculativeConfig().processReuse.learnedLaunches;
		const order = observation ? Atomics.add(observation.ordering, 0, 1) + 1 : 0;
		let settle: ((consumed: boolean) => void | Promise<void>) | undefined;
		this.add(undefined, "requests");
		try {
			process.signal?.throwIfAborted();
			(await this.resolveReady()).collector.check();
			const sourceRoot = path.resolve(process.sourceRoot);
			const installed = `/proc/${process.pid}/exe`;
			const name = await readlink(installed);
			const installedStat = await stat(installed, { bigint: true });
			const executable = installedStat.nlink ? name : name.replace(/ \(deleted\)$/, "");
			const projection = new ExecutionPathProjection({
				sourceRoot,
				workspaceRoot: sourceRoot,
			});
			const executablePath = projection.toLogical(executable);
			if (learning) {
				observation!.pending++;
				releaseLearning = releaseOnce(() => {
					observation!.pending--;
				});
			}
			const available =
				observation?.reuseEnabled !== false &&
				(this.handoffs.mayHaveExecutable(executablePath) ||
					(await this.store.mayHaveCertificates(executablePath, process.signal)) ||
					this.handoffs.mayHaveExecutable(executablePath));
			// Concurrent discovery is bounded, but a full retained set never shuts discovery off for the rest of the call.
			if (!learning && !available && !record) {
				this.add(undefined, "misses");
				return measured({ kind: "continue" });
			}
			const snapshot = await inspectHeldExecProcess(process.pid, executable);
			if (!validProcessContext(snapshot.context)) throw new Error("held process descriptors are not replayable");
			if (!pathContains(sourceRoot, snapshot.cwd)) {
				this.setError(undefined, "actor_bypass:cwd_outside_workspace", "bypasses");
				return measured({ kind: "continue" });
			}
			const digest = (
				await TimelineInterval.collect(() => hashExecutableFile(`/proc/${process.pid}/exe`, process.signal))
			).output;
			const prototype = bufferedProcessPrototype(
				snapshot,
				projection,
				digest,
				await this.resolvePlatformFingerprint(),
				controlledSubcomputeChannel(snapshot.environment)?.identity ?? parentSteps,
			);
			const weakKey = processWeakKey(prototype);
			const continued = () => measured({
				kind: "continue",
				observeCompletion: (learning || record) ? async (durationMs, clock) => {
					if (durationMs === undefined || process.signal?.aborted) return;
					if (learning && !observation!.closed && snapshot.outputRoute) {
						const { argv, context: _context, ...invocation } = snapshot;
						const binding = this.handoffs.observe(weakKey, executablePath, scope!, {
							...invocation, ...boundEnvironment(invocation.environment), argv0: argv[0]!, args: argv.slice(1),
							cwd: projection.toLogical(snapshot.cwd), executable: executablePath, sourceRoot,
							outputRoute: snapshot.outputRoute,
						}, durationMs);
						if (binding) {
							const limit = Math.min(this.store.limits.maxCertificates, speculativeConfig().processReuse.learnedLaunches);
							const victim = observation!.learned.has(weakKey) ? weakKey :
								observation!.bindings.size >= limit ? observation!.learned.keys().next().value : undefined;
							if (victim !== undefined) {
								observation!.bindings.delete(observation!.learned.get(victim)!);
								observation!.learned.delete(victim);
							}
							if (observation!.bindings.size < limit) {
								observation!.bindings.set(order, binding);
								observation!.learned.set(weakKey, order);
							}
						}
					}
					if (clock && snapshot.outputRoute) await record?.(process.pid, prototype, snapshot, installedStat, clock);
				} : undefined,
			});
			if (!available) {
				this.add(undefined, "misses");
				return continued();
			}
			const accepted = (producer: ProcessProducerProof) =>
				actorReplayProducer(producer, sensitivePaths(this.options.storeRoot, this.options.deniedPaths));
			const result = await this.handoffs.acquire({
				key: weakKey,
				scope,
				signal: process.signal,
				lookup: (live, excludedCertificates, artifacts) =>
					this.planPrototype(prototype, projection, accepted, {
						live,
						artifacts,
						excludedCertificates,
						signal: process.signal,
					}),
			});
			const acquired = result.kind === "hit" ? result : undefined;
			settle = acquired?.settle;
			const { plan } = acquired ?? {};
			if (!acquired || !plan || plan.certificate.result.exit.kind !== "code") {
				this.add(undefined, "misses");
				return continued();
			}
			const replay = this.prepareReplay(plan, projection, {
				workspace: observation?.effects ?? this.replayWorkspace,
				acquired,
			});
			const { output, computation } = replay;
			const decision = measured({
				kind: "replay",
				exitCode: (replay.exit as { kind: "code"; code: number }).code,
				validate: replay.validate,
				settle,
				output,
				commit: async () => {
					try {
						process.signal?.throwIfAborted();
						await replay.commitEffects();
					} catch (error) {
						this.setError(undefined, `actor_child_commit:${errorMessage(error)}`);
						throw error;
					}
				},
				adopted: () => {
					const binding = acquired.producer?.binding;
					if (observation && !observation.closed && sameScope(observation.scope, scope)) {
						if (
							binding &&
							this.handoffs.resolveBinding(binding, scope) &&
							observation.bindings.size < this.store.limits.maxCertificates
						)
							observation.bindings.set(order, binding);
						// Native acknowledgement consumes borrowed inputs; lookup alone is validation.
						observation.computations.push(computation);
					}
					this.recordHit(acquired.producer?.scope, acquired.joined, undefined, scope);
					this.add(undefined, "reusedProcessMs", replay.observedMs);
					if (scope)
						acquired.producer?.ownership.adopted({
							scope,
							id: process.id,
							sequence: order,
							backend: "linux_process_reuse",
							operationIdentity: weakKey,
							executionMs: replay.observedMs,
							computation,
						});
				},
			});
			settle = undefined;
			return decision;
		} catch (error) {
			this.setError(undefined, `actor_child:${errorMessage(error)}`, "bypasses");
			return measured({ kind: "continue" });
		} finally {
			try {
				await settle?.(false);
			} finally {
				releaseLearning?.();
			}
		}
	}
	private add(session: ActiveSession | undefined, metric: CountedReuseMetric, value = 1): void {
		if (session) this.counters[metric] += value;
		(session?.metrics ?? this.actorCounters)[metric] += value;
		const observation = !session && this.observations.getStore();
		if (observation && !observation.closed) observation.metrics[metric] += value;
	}
	private recordHit(
		producer: ExecutionScope | undefined,
		joined: boolean,
		session?: ActiveSession,
		scope: ExecutionScope | undefined = session?.scope,
	): void {
		for (const metric of [
			"hits",
			...(joined ? (["joinedHits"] as const) : []),
			producer && scope
				? sameScope(producer, scope)
					? "sameTurnHits"
					: "crossTurnHits"
				: "unattributedHits",
		] as const)
			this.add(session, metric);
	}
	private setError(session: ActiveSession | undefined, detail: string, countedAs?: CountedReuseMetric): void {
		if (countedAs) this.add(session, countedAs);
		const limit = speculativeConfig().processReuse.diagnosticCharacters;
		detail = detail.length > limit ? `${detail.slice(0, Math.max(0, limit - 1))}…` : detail;
		this.counters.lastError = detail;
		(session?.metrics ?? this.actorCounters).lastError = detail;
		const observation = !session && this.observations.getStore();
		if (observation && !observation.closed) observation.metrics.lastError = detail;
	}
	private async createProcessInterposition(session: ActiveSession, ready: ReadyBackend) {
		const dispatcher = path.join(session.processRoot, "routing", "dispatcher");
		session.signal?.throwIfAborted();
		await mkdir(path.dirname(dispatcher));
		await copyFile(ready.dispatcher, dispatcher);
		await chmod(dispatcher, 0o755);
		return dispatcher;
	}
	/** Retain the learned launch authority and the input evidence used for local preparation. */
	private async boundRequest(
		binding: ProcessExecutionBinding,
		invocation: BoundProcessInvocation,
		projection: ExecutionPathProjection,
		signal: AbortSignal,
		launch = false,
	) {
		const cwd = projection.toPhysical(invocation.cwd);
		const executable = projection.toPhysical(invocation.executable);
		if (!cwd || !executable) throw new Error("bound process paths are unmapped");
		const environment = launch && invocation.stepIdentity
			? { ...invocation.environment, ...await controlledRuntimeEnvironment(invocation.environment) }
			: invocation.environment;
		const request = { ...invocation, cwd, environment };
		const identity = launch ? controlledSubcomputeChannel(environment)?.identity : invocation.stepIdentity;
		const prototype = await this.prototype(projection, request, executable, identity);
		if (processWeakKey(prototype) !== binding.key) throw new Error("bound process execution context changed");
		signal.throwIfAborted();
		return { request, executable, prototype };
	}
	private async prototype(
		projection: ExecutionPathProjection,
		request: ProcessArguments,
		executable: string,
		stepIdentity = controlledSubcomputeChannel(request.environment)?.identity,
	): Promise<ExecPrototype> {
		const [digest, ready] = await Promise.all([
			hashExecutableFile(request.imageSource ?? executable),
			this.resolveReady(),
		]);
		return bufferedProcessPrototype(
			{
				...request,
				executable,
				argv: [request.argv0, ...request.args],
				context: routedProcessIdentity(ready.executionContext, request),
			},
			projection,
			digest,
			await this.resolvePlatformFingerprint(),
			stepIdentity,
		);
	}
}
function bufferedProcessPrototype(
	snapshot: HeldExecSnapshot,
	projection: ExecutionPathProjection,
	executableDigest: Sha256Digest,
	platformFingerprint: string,
	stepIdentity?: string,
) {
	const { context } = snapshot;
	const contextDigest = (key: string) => stepIdentity ? digestObject([key, stepIdentity]) : sha256Digest(key);
	const streamKey = byteStreamContextKey(context);
	return createExecPrototype({
		executablePath: projection.toLogical(snapshot.executable),
		invocation: snapshot.invocation,
		executableDigest,
		argv: snapshot.argv,
		logicalCwd: projection.toLogical(snapshot.cwd),
		environment: snapshot.environment,
		umask: context.umask,
		processContextDigest: contextDigest(context.key),
		...(streamKey ? { byteStreamContextDigest: contextDigest(streamKey) } : {}),
		stdin: { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: context.descriptorTypes.map((type, fd) => ({
			fd,
			type,
			flagsDigest: sha256Digest(`${context.key}\0${fd}`),
			...(fd === 0 ? { eof: true } : {}),
		})),
		platformFingerprint,
	});
}
const ownedDependency = (computation: TimelineInterval): TimelineDependency => ({
	computation,
	owned: true,
	shared: [computation],
});
function recordProcessDependency(session: ActiveSession, dependency: TimelineDependency, pid?: number) {
	if (pid !== undefined) processDependencyPIDs.set(dependency, pid);
	session.computations.push(dependency);
}
async function measureProcess<T>(session: ActiveSession, execute: () => Promise<T>, pid?: number) {
	const first = session.computations.length;
	const result = await TimelineInterval.measure(execute, () =>
		session.computations.slice(first).filter((input) => processDependencyPIDs.get(input) === pid),
	);
	recordProcessDependency(session, ownedDependency(result.computation), pid);
	return result.output;
}
/** Optimistic execution boundaries: control wins over sibling overlap; original producers keep their own clocks. */
class ActorHeldClock {
	private readonly spans: HeldExecClock[][] = [[], []];
	record(timing: HeldExecTiming): void {
		for (const [index, clock] of [
			{ startedAt: timing.requestedAt, completedAt: timing.completedAt },
			timing.native,
		].entries()) {
			if (!clock || clock.completedAt <= clock.startedAt) continue;
			const spans = this.spans[index]!;
			let start = clock.startedAt;
			let end = clock.completedAt;
			let left = 0;
			let right = spans.length;
			while (left < right) {
				const middle = (left + right) >>> 1;
				if (spans[middle]!.completedAt < start) left = middle + 1;
				else right = middle;
			}
			while (right < spans.length && spans[right]!.startedAt <= end) {
				start = Math.min(start, spans[right]!.startedAt);
				end = Math.max(end, spans[right++]!.completedAt);
			}
			spans.splice(left, right - left, { startedAt: start, completedAt: end });
		}
	}
	dependencies(): TimelineDependency[] {
		const result: TimelineDependency[] = [];
		for (const [index, spans] of this.spans.entries())
			if (spans.length) {
				const computation = new TimelineInterval(
					spans[0]!.startedAt,
					spans.at(-1)!.completedAt,
					index ? result : [],
					index ? spans : undefined,
				);
				result.push({
					computation,
					...(index ? { owned: true } : { overhead: true }),
					shared: spans,
				});
			}
		return result;
	}
}
function reusedComputation(
	certificate: ProcessProvenanceCertificate,
	acquired?: {
		readonly producer?: ProcessHandoff;
		readonly waiting?: readonly TimelineInterval[];
	},
): TimelineDependency {
	const computation =
		acquired?.producer?.computation ??
		TimelineInterval.restore(certificate.result.computation) ??
		TimelineInterval.unknownReuse(`${certificate.id}:${certificate.createdAt}`);
	return { computation, shared: acquired?.waiting, reused: true };
}
async function sealSessionEvidence(
	session: ActiveSession,
	external: Awaited<ReturnType<typeof privateCommits>>,
) {
	const capture = await captureSessionInterval(session);
	session.sealing.topLevelEvidence = {
		complete: false,
		dependencies: [],
		taints: ["trace_incomplete"],
	};
	if (!capture.complete) {
		session.incompleteReasons.add(`top_capture:${capture.reason}`);
		return [];
	}
	const effects = diffWorkspaceStructures(capture.before, capture.after, capture.changes, session.projection);
	if (!effects.complete) {
		session.incompleteReasons.add(`top_effects:${effects.reason ?? "incomplete"}`);
		throw new Error(`top-level workspace effects are incomplete: ${effects.reason ?? "unknown"}`);
	}
	const committedChanges = sandboxWorkspaceChanges(session.sourceRoot, effects);
	try {
		const writers = evidenceSources(sessionWriters(session));
		const evidence = await captureDependencies(
			session,
			capture.before,
			writers.flatMap((writer) => writer.observation ?? []),
			effects,
			external,
			writers.flatMap((writer) => writer.evidence ?? []),
		);
		for (const reason of evidence.incompleteReasons) session.incompleteReasons.add(`top_evidence:${reason}`);
		session.sealing.topLevelEvidence = {
			...evidence,
			complete: evidence.complete && session.incompleteReasons.size === 0,
		};
	} catch (error) {
		session.incompleteReasons.add(`top_seal:${errorMessage(error)}`);
	}
	return committedChanges;
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
async function captureDependencies(
	session: ActiveSession,
	snapshot: WorkspaceStructureSnapshot,
	observations: readonly ExecutionObservation[],
	effects: WorkspaceTransactionDiff,
	external: Awaited<ReturnType<typeof privateCommits>>,
	runs: readonly DynamicDependencyCertificate[] = [],
	privateBefore?: PrivatePrestate,
) {
	if (!effects.complete) throw new Error(`workspace effects are incomplete: ${effects.reason}`);
	await using privateReads = FilesystemReadView.ambient();
	const hostReads = await (session.hostReads ??= FilesystemReadView.open());
	const hostEntry = sharedWalk((target) => captureFilesystemEntry(target, undefined, hostReads));
	// The pathname walk resolves links once. Qualified prefixes are shared by every terminal input in this cut.
	const hostInput = sharedWalk(
		async (target: string): Promise<Awaited<ReturnType<typeof hostEntry>> | undefined> => {
			if (speculativeConfig().processReuse.volatilePaths.some((root) => pathContains(root, target))) return;
			const captured = await hostEntry(target).catch(ignoreMissing);
			if (
				!captured ||
				(captured.info.uid !== 0n && captured.info.uid !== BigInt(process.getuid?.() ?? -1)) ||
				captured.link !== undefined ||
				(captured.info.mode & 18n) !== 0n ||
				(!captured.info.isFile() && !captured.info.isDirectory())
			)
				return;
			return target === path.dirname(target) || (await hostInput(path.dirname(target)))
				? captured
				: undefined;
		},
	);
	const observed = observations.flatMap((part) => part.paths);
	const locks = observations.flatMap((part) => part.locks ?? []);
	const written = observations.flatMap((part) => part.written ?? []);
	// Original versions belong to the transaction; a later pathname is never its input source.
	const captureBefore = sharedWalk(async (relative: string) => {
		const structure = snapshot.entries.get(relative);
		if (!structure || structure.kind !== "file") return structure;
		const content = await workspaceInputVersion(snapshot, relative);
		if (!content || content.byteLength !== structure.size)
			throw new Error(`transaction input version is unavailable: ${relative}`);
		return { ...structure, digest: contentDigest(content) };
	});
	const before = (physicalPath: string) => {
		const relative = relativeFilesystemPath(snapshot.root, physicalPath);
		return relative === undefined
			? Promise.reject(new Error(`workspace dependency escapes snapshot: ${physicalPath}`))
			: captureBefore(relative);
	};
	// A name it created itself depends only on having been free, not on everything else its directory held.
	const created = new Set(written.map((name) => path.resolve(session.workspace.sandboxRoot, name)));
	const parentWrites = new Set([...created].map((name) => path.dirname(name)));
	const exclusions = (target: string) =>
		path.resolve(target) === path.resolve(session.workspace.sandboxRoot)
			? session.workspace.observationExcludes
			: undefined;
	const workspaceDependency = async (
		physical: string,
		logical: string,
		role: DependencyRole,
		listed = true,
	) => {
		const [entry, parent] = await Promise.all([
			before(physical),
			path.resolve(physical) === path.resolve(session.workspace.sandboxRoot) ||
			created.has(path.resolve(physical))
				? undefined
				: before(path.dirname(physical)),
		]);
		return snapshotDependency(
			logical,
			entry?.kind === "file" && entry.aliases
				? {
						...entry,
						aliases: entry.aliases.map((name) => session.projection.toLogical(name)).sort(),
					}
				: entry,
			parent,
			role,
			{
				excludedEntries: exclusions(physical),
				parentExcludedEntries: exclusions(path.dirname(physical)),
				listed,
			},
		);
	};
	const dependencies = new Map<string, DynamicDependency | undefined>();
	const taints = new Set<ProvenanceTaint>([...observations, ...runs].flatMap((part) => part.taints));
	const incompleteReasons = new Set(observations.flatMap((part) => part.incompleteReasons));
	let complete = observations.every((part) => part.complete);
	if (!complete) taints.add("trace_incomplete");
	const add = (dependency: DynamicDependency | undefined, reason = "dependency_unavailable") => {
		if (!dependency) {
			incompleteReasons.add(reason);
			return;
		}
		const identity = dynamicDependencyIdentity(dependency);
		const merged = mergeDynamicDependency(dependencies.get(identity), dependency);
		if (merged === "conflict") incompleteReasons.add(`dependency_changed_during_execution:${identity}`);
		else dependencies.set(identity, merged);
	};
	// Rebase owned inputs; metadata survives unless its private object vanished within this transaction.
	const changed = new Set(effects.effects.map((effect) => effect.logicalPath));
	const retainedExternal = new Set(
		external.changes.filter((change) => change.after).map((change) => change.target),
	);
	let privateWrites: Promise<readonly TracedWrite[] | undefined> | undefined;
	const privateObjectUnmoved = async (target: string) => {
		// A rename/link of this name or an ancestor can retain its object elsewhere.
		// Unrelated multi-path writes do not extend the lifetime of this private object.
		const writes = await (privateWrites ??= Promise.all(sessionWriters(session).map(readWriterTrace)).then((traces) =>
			traces.some((trace) => trace.unsafe) ? undefined : traces.flatMap((trace) => trace.writes),
		));
		return (
			writes !== undefined &&
			!writes.some(
				(write) =>
					!write.paths ||
					(write.paths.length > 1 &&
						write.paths.some(
							(name) =>
								pathContains(name, target) || pathContains(name, path.join(privateUpper(session), target)),
						)),
			)
		);
	};
	const vanishedPrivateObject = async (target: string) =>
		external.before.get(target)?.kind === "absence" &&
		!retainedExternal.has(target) &&
		(await privateObjectUnmoved(target));
	for (const target of observations.flatMap((part) => part.metadataChanges ?? []))
		if (!(await vanishedPrivateObject(target))) {
			taints.add("mutable_input");
			incompleteReasons.add(`metadata_changed:${target}`);
		}
	const atStart = sharedWalk(async (dependency: DynamicDependency) => {
		if (dependency.kind !== "lock" && external.before.has(dependency.path)) {
			const initial = external.before.get(dependency.path);
			// Only a private object created AND removed by this run can discard its metadata.
			// A retained output's identity must still agree with the object the Actor will see.
			if (dependency.kind !== "metadata" || (await vanishedPrivateObject(dependency.path))) return initial;
		}
		const physical = session.projection.toPhysical(dependency.path);
		const relative = physical && relativeFilesystemPath(snapshot.root, physical);
		if (
			!physical ||
			relative === undefined ||
			session.workspace.observationExcludes.includes(relative.split(path.sep)[0]!)
		)
			return dependency;
		const entry = snapshot.entries.get(relative);
		const up = relativeFilesystemPath(snapshot.root, path.dirname(physical));
		const parent = up === undefined ? undefined : snapshot.entries.get(up);
		if (
			dependency.kind === "metadata" &&
			parentWrites.has(physical) &&
			entry?.kind === "directory" &&
			entry.observed
		)
			return {
				...dependency,
				digest: filesystemObservationDigest(
					session.workspace.projectMetadata?.(entry.observed) ?? entry.observed,
					dependency.fields,
				),
			};
		switch (dependency.kind) {
			case "metadata":
				if (entry || changed.has(dependency.path) || !created.has(physical)) return dependency;
				break;
			case "absence":
				if (
					!entry &&
					(dependency.parentEntriesDigest === undefined ||
						(parent?.kind === "directory" && parent.entriesDigest === dependency.parentEntriesDigest))
				)
					return dependency;
				break;
			case "directory":
				if (
					entry?.kind === "directory" &&
					entry.metadataDigest === dependency.metadataDigest &&
					(dependency.entriesDigest ?? entry.entriesDigest) === entry.entriesDigest
				)
					return dependency;
				break;
			case "symlink":
				if (entry?.kind === "symlink" && entry.target === dependency.target) return dependency;
				break;
			case "lock":
				if (entry?.kind === "file") return dependency;
				break;
			default:
				if (
					!changed.has(dependency.path) &&
					entry?.kind === "file" &&
					entry.metadataDigest === dependency.metadataDigest
				)
					return dependency;
		}
		return workspaceDependency(
			physical,
			dependency.path,
			dependency.kind === "file" ? dependency.role : "input",
			dependency.kind === "directory" && !!dependency.entriesDigest,
		);
	}, JSON.stringify);
	// Kernel pathname walk over the baseline: links are recorded and expanded in place and `..` leaves the directory
	// actually reached. Sandlock collapses `..` first, so another result means the sandbox read another object.
	const walk = sharedWalk(async ([logical, follow, absent = false]: readonly [string, boolean, boolean?]) => {
		const pending = logical.split("/").filter(Boolean);
		const links: DynamicDependency[] = [];
		let current = "/";
		let hostComplete = true;
		for (let segment = pending.shift(); segment !== undefined; segment = pending.shift()) {
			if (segment === "..") {
				current = path.posix.dirname(current);
				continue;
			}
			let next = path.posix.join(current, segment);
			// The repository is the workspace's own, mounted read-only (also when reached through the overlay): the host walk captures it.
			const repository = session.projection.isWorkspacePhysical(next)
				? session.projection.toLogical(next)
				: next;
			const inRepository = session.gitDirectory && pathContains(session.gitDirectory, repository);
			if (inRepository) next = repository;
			const physical =
				!inRepository && pathContains(session.sourceRoot, next)
					? session.projection.toPhysical(next)
					: undefined;
			const captured =
				!physical && !speculativeConfig().processReuse.volatilePaths.some((root) => pathContains(root, next))
					? await hostEntry(next).catch(ignoreMissing)
					: undefined;
			const entry = physical
				? snapshot.entries.get(relativeFilesystemPath(snapshot.root, physical)!)
				: captured?.link !== undefined
					? { kind: "symlink", target: captured.link }
					: captured?.info.isDirectory()
						? { kind: "directory" }
						: captured?.info.isFile()
							? { kind: "file" }
							: undefined;
			if (captured?.info.isDirectory()) hostComplete &&= !!(await hostInput(next));
			if (entry?.kind === "file" && pending.length)
				return { path: next, links, hostComplete, blocked: true as const };
			// A failed lookup needs both its original prefixes and the normalized target to retain their observed states.
			if (absent && pending.includes("..") && entry?.kind !== "symlink") {
				const prefix = physical
					? await workspaceDependency(physical, next, "input", false)
					: await captureDirectoryDependency(next, next, false, [], false, undefined, hostReads).catch(
							(error) =>
								missing(error)
									? captureAbsenceDependency(
											path.join(privateUpper(session), next),
											next,
											false,
											[],
											undefined,
											privateReads,
										)
									: undefined,
						);
				if (
					prefix?.kind !== "directory" &&
					(prefix?.kind !== "absence" || created.has(physical ?? next) || own.has(next))
				)
					return;
				add(prefix);
			}
			if (entry?.kind !== "symlink" || (!pending.length && !follow)) {
				current = next;
				continue;
			}
			if (captured)
				hostComplete &&=
					(captured.info.uid === 0n || captured.info.uid === BigInt(process.getuid?.() ?? -1)) &&
					!!(await hostInput(path.dirname(next)));
			const target = entry.target!;
			if (
				links.push({
					kind: "symlink",
					path: next,
					target,
					targetDigest: sha256Digest(target),
				}) > speculativeConfig().filesystem.maxSymlinkDepth
			)
				return undefined;
			pending.unshift(...target.split("/").filter(Boolean));
			if (target.startsWith("/")) current = "/";
		}
		return { path: current, links, hostComplete };
	}, JSON.stringify);
	const own = new Set([...external.before.keys()].flatMap((target) => [target, path.dirname(target)]));
	// A lock on a file it made itself no one else could have held; on one that was there, another holder would have refused it.
	for (const lock of locks)
		if (
			await (session.projection.isWorkspacePhysical(lock.path)
				? before(lock.path)
				: lstat(lock.path).catch(() => undefined))
		) {
			add({
				kind: "lock",
				path: session.projection.isWorkspacePhysical(lock.path)
					? session.projection.toLogical(lock.path)
					: slash(lock.path),
				exclusive: lock.exclusive,
			});
		}
	const pending = [...observed];
	const seenImages = new Set<string>();
	const hostPaths = new Map<
		string,
		{
			physical: string;
			role: DependencyRole;
			listed: boolean;
			absent: boolean;
			safe: boolean;
		}
	>();
	for (let item = pending.shift(); item; item = pending.shift()) {
		const absent = item.role !== "metadata" && item.lookupFailure === "ENOENT";
		const follow = item.role !== "metadata" || item.followSymlinks;
		const parents = item.path.split("/").includes("..");
		// Metadata already contains the returned ABI fields; only `..` needs a normalized-lookup comparison.
		const walked =
			item.role === "metadata" && !parents
				? {
						path: path.posix.normalize(item.path),
						links: [],
						hostComplete: true,
						blocked: undefined,
					}
				: await walk([item.path, follow, absent]);
		// A blocking file must reproduce the observed ENOTDIR; every other walk must reach the same object after normalization.
		const normalized =
			!walked?.blocked && parents ? await walk([path.posix.normalize(item.path), follow]) : walked;
		if (
			!walked ||
			(walked.blocked && (item.role === "metadata" || item.lookupFailure !== "ENOTDIR")) ||
			(normalized?.blocked && !walked.blocked) ||
			walked.path !== normalized?.path
		) {
			add(undefined, `pathname_walk:${item.path}`);
			continue;
		}
		for (const link of item.role === "metadata" ? [] : walked.links) add(link);
		const observedPath = item.role === "metadata" ? path.resolve(item.path) : walked.path;
		if (session.deniedPaths.some((denied) => pathContains(denied, observedPath))) {
			taints.add("escaped_sandbox");
			incompleteReasons.add(`denied:${observedPath}`);
			continue;
		}
		const physical =
			pathContains(session.sourceRoot, observedPath) &&
			!(session.gitDirectory && pathContains(session.gitDirectory, observedPath))
				? (session.projection.toPhysical(observedPath) ?? observedPath)
				: observedPath;
		const contentAddressed = await captureContentAddressedDependency(physical, item.role, hostReads);
		if (contentAddressed) {
			add(contentAddressed);
			continue;
		}
		// What it wrote outside the workspace, and the directories holding those names, rest on its effects' baselines.
		if (own.has(physical) && (item.role !== "metadata" || !external.before.has(physical))) continue;
		const special = classifyObservedSpecialPath(observedPath, undefined, item.role !== "metadata");
		if (special) {
			if (special !== "ignored") taints.add(special);
			continue;
		}
		if (item.role === "metadata") {
			const { role, path: _path, afterWrite, ...metadata } = item;
			const dependency = {
				...metadata,
				kind: role,
				path: session.projection.isWorkspacePhysical(physical)
					? session.projection.toLogical(physical)
					: slash(physical),
			};
			const input = await atStart(dependency);
			const identity = dynamicDependencyIdentity(dependency);
			// A missing slot needs these fields bound before the write, possibly by a brokered child.
			if (afterWrite && input?.kind === "metadata") {
				if (!dependencies.has(identity)) dependencies.set(identity, undefined);
			} else add(input);
			continue;
		}
		// The kernel itself opens a script's interpreter and an ELF's loader, which no traced syscall names.
		const interpreter =
			item.role === "executable" &&
			seenImages.size < speculativeConfig().processReuse.interpreterImages &&
			!seenImages.has(physical) &&
			seenImages.add(physical)
				? await imageInterpreter(physical)
				: undefined;
		if (interpreter?.startsWith("/")) pending.push({ path: interpreter, role: "executable" });
		if (session.projection.isWorkspacePhysical(physical)) {
			const logical = session.projection.toLogical(physical);
			const dep = await workspaceDependency(physical, logical, item.role, !!item.listed);
			add(dep);
			if (dep?.kind === "file" && session.inputs) {
				const original = session.workspace.baselineFile?.(path.relative(session.sourceRoot, logical));
				if (original && contentDigest(original.content) === dep.contentDigest) {
					if (session.inputs.remember(logical, original) && original.shared) TimelineInterval.use(original.computation);
				}
			}
			continue;
		}
		const key = `${item.role}\0${physical}`;
		const previous = hostPaths.get(key);
		hostPaths.set(key, {
			physical,
			role: item.role,
			listed: !!item.listed || !!previous?.listed,
			absent: absent && previous?.absent !== false,
			safe: previous?.safe !== false && walked.hostComplete,
		});
	}
	// Capture independent external inputs concurrently, preserving trace order.
	{
		const images = await privateInputImages(
			session,
			new Set([...hostPaths.values()].map((item) => item.physical)),
			privateBefore,
		);
		for (const captured of await mapFilesystem(
			[...hostPaths.values()],
			async ({ physical, role, listed, absent, safe }) => {
				try {
					if (images.has(physical))
						return {
							physical,
							value: images
								.get(physical)
								?.map((dependency) => (dependency.kind === "file" ? { ...dependency, role } : dependency)),
						};
					const captured = safe && !absent ? await hostInput(physical) : undefined;
					const dependency =
						absent || captured
							? await captureHostObjectDependency(
									physical,
									slash(physical),
									role,
									listed,
									captured?.info,
									hostReads,
								)
							: undefined;
					return { physical, value: dependency && [dependency] };
				} catch (error) {
					return { physical, error };
				}
			},
		)) {
			if ("error" in captured) {
				taints.add("trace_incomplete");
				add(undefined, `capture:${captured.physical}:${errorMessage(captured.error)}`);
			} else if (captured.value) for (const dependency of captured.value) add(dependency);
			else {
				taints.add("mutable_input");
				add(undefined, `mutable:${captured.physical}`);
			}
		}
	}
	for (const effect of effects.effects) {
		const physical = session.projection.toPhysical(effect.logicalPath);
		if (!physical) {
			add(undefined, `effect_unmapped:${effect.logicalPath}`);
			continue;
		}
		add(await workspaceDependency(physical, effect.logicalPath, "input"));
	}
	for (const [target, dependency] of external.before) add(dependency, `external_write:${target}`);
	const rebased = (
		await mapFilesystem([...new Set(runs.flatMap((run) => run.dependencies))], atStart)
	).flatMap((dependency) => dependency ?? (add(undefined, "start_state_unavailable"), []));
	for (const [identity, dependency] of dependencies)
		if (!dependency && !rebased.some((dependency) => dynamicDependencyIdentity(dependency) === identity))
			add(undefined, `metadata_after_write:${identity}`);
	const sourceComplete = complete && incompleteReasons.size === 0;
	complete &&= runs.every((run) => run.complete);
	for (const dependency of rebased) add(dependency);
	await hostReads.verify();
	return {
		complete: complete && incompleteReasons.size === 0,
		sourceComplete,
		dependencies: Object.freeze([...dependencies.values()].flatMap((value) => value ?? [])),
		taints: Object.freeze([...taints]),
		incompleteReasons: [...incompleteReasons],
	};
}
/** Also what memory allocators (jemalloc, glibc) read at startup to size their mappings. */
async function replayFilesystemEffects(
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
function directoryState({ entriesDigest, mode, uid, gid }: SandboxDirectoryState) {
	return { entriesDigest, mode, uid, gid };
}
/** Output events carry the target's own descriptor. */
function loadOutputEvents(artifacts: VerifiedArtifactClosure, journal: readonly OrderedEffectEvent[]) {
	return journal.flatMap((event) =>
		event.kind === "output" ? [{ fd: event.fd, data: artifacts.read(event.data) }] : [],
	);
}
function sandboxPolicyArguments(
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
/** What the brokered run a traced launcher `pid` started wrote, with its own descendants; a run never answers for itself. */
function brokeredWrites(session: ActiveSession, pid: number, self?: SessionWriter) {
	const writers = sessionWriters(session).filter((writer) => writer !== self && writer.pid === pid);
	return writers.length && writers.every((writer) => writer.written)
		? [
				...new Set(
					writers.flatMap((writer) =>
						[writer, ...(writer.descendants ?? [])].flatMap((child) => [
							...(child.written ?? []),
							...(child.external ?? []),
						]),
					),
				),
			]
		: undefined;
}
const sessionWriters = (session: ActiveSession) =>
	[...session.writers].filter((writer) => writer.owner === session.token);
function evidenceSources(records: readonly SessionWriter[]): SessionWriter[] {
	const included = new Set(records.flatMap((record) => (record.evidence ? (record.descendants ?? []) : [])));
	return [...new Set(records)].filter((record) => !included.has(record));
}
function workspaceWriterRecords(workspace: SandboxWorkspaceContext) {
	const writers = workspaceWriters.get(workspace.transactions) ?? new Set<SessionWriter>();
	workspaceWriters.set(workspace.transactions, writers);
	return writers;
}
type WorkspaceWriteInquiry = {
	readonly exclude: readonly string[];
	readonly targets: ReadonlySet<string>;
	readonly from: number;
	readonly until: number;
	readonly owner?: string;
	readonly processes?: ReadonlySet<number>;
};
/** The workspace owner can inspect caller-owned native traces even while that caller is blocked. */
export async function observeWorkspaceWrites(
	workspace: SandboxWorkspaceContext,
	input: WorkspaceWriteInquiry,
): Promise<boolean> {
	const writers = workspaceWriterRecords(workspace);
	const completed = new Map<string, SessionWriter[]>();
	for (const writer of writers)
		if (writer.endedAt !== undefined && writer.endedAt < input.from)
			for (const pid of writer.closedPids ?? []) {
				const key = `${writer.owner}:${pid}`;
				const records = completed.get(key) ?? [];
				records.push(writer);
				completed.set(key, records);
			}
	for (const other of writers)
		if (
			!input.exclude.includes(other.id) &&
			other.startedAt <= input.until &&
			(other.endedAt ?? Infinity) >= input.from
		) {
			const { writes: tail } = await readWriterTrace(other);
			// Shared parent traces retain the child's original opens after its complete tree has exited.
			if (
				(other.endedAt ?? Infinity) >= input.from &&
				writesWithin(
					tail.filter(
						(write) =>
							(other.owner !== input.owner || !input.processes?.has(write.pid!)) &&
							!completed
								.get(`${other.owner}:${write.pid}`)
								?.some((record) => write.at >= record.startedAt && write.at <= record.endedAt!),
					),
					input.targets,
					input.from,
					input.until,
				)
			)
				return true;
		}
	return false;
}
function writerOwnership(
	session: ActiveSession,
	writers: readonly SessionWriter[],
): WorkspaceTransactionOwnership | undefined {
	if (writers.some((writer) => !writer.written || !writer.observed || writer.endedAt === undefined)) return;
	const observed = new Map<string, number>();
	const startedAt = Math.min(...writers.map((writer) => writer.startedAt));
	for (const writer of writers)
		for (const [name, at] of writer.observed!) observed.set(name, Math.max(observed.get(name) ?? 0, at));
	return {
		written: new Set(writers.flatMap((writer) => writer.written!)),
		observed,
		endedAt: Math.max(0, ...writers.map((writer) => writer.endedAt!)),
		interfered: (paths, until) => {
			const inquiry = {
				exclude: writers.map((writer) => writer.id),
				owner: session.token,
				processes: new Set(writers.flatMap((writer) => writer.pids ?? [])),
				from: startedAt,
				until,
				targets: new Set(
					[...paths].flatMap((name) =>
						[session.workspace.sandboxRoot, session.sourceRoot].map((root) => path.posix.join(root, name)),
					),
				),
			};
			return observeWorkspaceWrites(session.workspace, inquiry);
		},
	};
}
function captureSessionInterval(session: ActiveSession): Promise<WorkspaceTransactionDelta> {
	return (session.sealing.capture ??= Promise.resolve(session.sealing.transaction)
		.then((capture) =>
			capture
				? capture.finish(async () => writerOwnership(session, sessionWriters(session)))
				: { complete: false as const, changes: [], reason: "not_run" },
		)
		.catch((error) => ({
			complete: false,
			changes: [],
			reason: errorMessage(error),
		})));
}
const privateUpper = (session: ActiveSession) => path.join(session.processRoot, "private", "writes", "upper");
/** A session's private branch under `storage`: its upper directory, its whiteout log and the host paths that log hides. */
async function privateBranch(storage: string) {
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
async function hostRoot(target: string) {
	for (let directory = path.dirname(target); ; directory = path.dirname(directory)) {
		if (
			(await lstat(directory).catch(() => undefined))?.isDirectory() ||
			directory === path.dirname(directory)
		)
			return directory;
	}
}
/** The execution domain owns each original input proof; working versions are checked separately at each boundary. */
async function captureInputProof(directory: string) {
	const fields = FILESYSTEM_METADATA_FIELDS;
	const decode = (values: unknown) => {
		if (
			!Array.isArray(values) ||
			values.length !== fields.length ||
			values.some((value) => typeof value !== "string" || !/^-?\d{1,40}$/.test(value))
		)
			throw new Error("invalid input image identity");
		return Object.fromEntries(fields.map((field, index) => [field, BigInt(values[index])])) as Record<
			(typeof fields)[number],
			bigint
		>;
	};
	const row = JSON.parse(await readFile(path.join(directory, "proof.json"), "utf8")) as {
		path: string;
		physical: unknown;
		original: unknown;
	};
	const original = decode(row.original);
	const physical = decode(row.physical);
	if (
		!path.isAbsolute(row.path) ||
		(original.mode & 61440n) !== 32768n ||
		original.nlink !== 1n ||
		original.size < 0n ||
		original.size > BigInt(speculativeConfig().processReuse.privateFileBytes)
	)
		throw new Error("invalid input image proof");
	const baseline = await captureFileDigest(path.join(directory, "baseline"), Number(original.size));
	if (baseline.stat.size !== original.size) throw new Error("input image owner changed");
	return { path: row.path, original, physical, hash: baseline.hash };
}
/** Native images prove first reads; retained preimages prove earlier writes, with traced interference excluded. */
async function privateInputImages(
	session: ActiveSession,
	needed: ReadonlySet<string>,
	before?: PrivatePrestate,
	effects = false,
) {
	const root = path.join(session.processRoot, "private", "writes", "inputs");
	const upper = privateUpper(session);
	const predecessors = new Set<string>();
	const file = (
		target: string,
		hash: string,
		metadata: Pick<BigIntStats, "mode" | "uid" | "gid" | "size" | "nlink">,
	): DynamicDependency => {
		if (privateFile(before, target)) predecessors.add(target);
		return {
			kind: "file",
			path: slash(target),
			role: "input",
			contentDigest: `sha256:${hash}`,
			metadataDigest: filesystemMetadataDigest(metadata),
		};
	};
	const capture = sharedWalk(async (key: string) => {
		const row = await session.inputProofs(key);
		const { original, physical } = row;
		const previous = privateFile(before, row.path);
		if (previous)
			return previous.stat.dev === physical.dev && previous.stat.ino === physical.ino
				? file(row.path, previous.hash, {
						...previous.stat,
						nlink: original.nlink + previous.stat.nlink - physical.nlink,
					})
				: undefined;
		const working = await captureFileDigest(
			path.join(root, key, "working"),
			speculativeConfig().processReuse.privateFileBytes,
		);
		if (working.stat.dev !== physical.dev || working.stat.ino !== physical.ino)
			throw new Error("input image owner changed");
		return !privateEntry(before, row.path) &&
			working.hash === row.hash &&
			sameFilesystemIdentity(working.stat, physical)
			? file(row.path, row.hash, original)
			: undefined;
	});
	const images = new Map<string, DynamicDependency[] | undefined>();
	session.metadata.read();
	const indices = session.metadata.inputs;
	await mapFilesystem([...needed], async (target) => {
		const index = indices.get(target);
		if (!index) return;
		if (!index.image || !index.sealed) throw new Error("unsealed input image");
		const input = await capture(index.image);
		images.set(
			target,
			input
				? [
						...index.links.map(
							([path, target]): DynamicDependency => ({
								kind: "symlink",
								path,
								target,
								targetDigest: sha256Digest(target),
							}),
						),
						input,
					]
				: undefined,
		);
	});
	await mapFilesystem(
		[...needed].filter((target) => !images.has(target)),
		async (target) => {
			if (
				privateEntry(before, target)?.captured.info.isFile() ||
				(await lstat(path.join(upper, target)).then(
					(info) => info.isFile(),
					(error) => {
						if (!missing(error)) throw error;
						return false;
					},
				))
			) {
				const previous =
					privateEntry(before, target)?.captured.info.nlink === 1n && privateFile(before, target);
				images.set(target, previous ? [file(target, previous.hash, previous.stat)] : undefined);
			}
		},
	);
	if (!effects && predecessors.size && (await privateInterference(session, predecessors, before!.startedAt)))
		for (const [target, dependencies] of images)
			if (dependencies?.some((dependency) => dependency.kind === "file" && predecessors.has(dependency.path)))
				images.set(target, undefined);
	return images;
}
/** Rebase every owned writer, including children retained as certificates, onto this run's private or host prestate. */
async function privateCommits(
	session: ActiveSession,
	since: number,
	owned: readonly SessionWriter[],
	prestate?: PrivatePrestate,
) {
	const { upper, deleted } = await privateBranch(path.join(session.processRoot, "private"));
	const after = await session.privateStructure.captureVersions();
	const entries = new Map(after.snapshot.entries);
	const deltas: WorkspaceRegularDelta[] = [];
	const before = new Map<string, DynamicDependency | undefined>();
	const targets = new Set(
		owned.flatMap((writer) => writer.external ?? []).map((name) => path.posix.resolve(name)),
	);
	for (const target of targets) {
		if (speculativeConfig().processReuse.stableDevices.includes(target)) continue;
		const relative = target.slice(1);
		const own = after.entries.get(relative);
		const prior = privateEntry(prestate, target);
		const initial =
			prior?.captured ??
			(prestate?.deleted(target)
				? undefined
				: await captureFilesystemEntry(target, "directory").catch(ignoreMissing));
		before.set(target, undefined);
		if (
			(!prior && initial && Number(initial.info.ctimeMs) > since) ||
			[own?.captured, initial].some((entry) => entry && !entry.info.isFile() && !entry.info.isDirectory())
		)
			continue;
		const beforeEntry = prior?.entry ?? (initial && captureWorkspaceStructureEntry(initial));
		const beforeFile =
			privateFile(prestate, target) ??
			(!prior && initial?.info.isFile()
				? await captureFileContent(
						target,
						speculativeConfig().processReuse.maxCaptureBytes,
						session.workspace.contents,
						{ stat: initial.info },
					)
				: undefined);
		const afterFile = after.files.get(relative);
		if ((initial?.info.isFile() && !beforeFile) || (own?.captured.info.isFile() && !afterFile)) continue;
		before.set(
			target,
			snapshotDependency(
				target,
				beforeEntry?.kind === "file" ? { ...beforeEntry, digest: `sha256:${beforeFile!.hash}` } : beforeEntry,
				undefined,
			),
		);
		if (
			(!own && !deleted(target)) ||
			(own?.entry.kind === "directory" && beforeEntry?.kind === "directory") ||
			(afterFile &&
				beforeFile &&
				afterFile.hash === beforeFile.hash &&
				own!.captured.info.mode === initial!.info.mode &&
				own!.captured.info.mtimeNs === initial!.info.mtimeNs)
		)
			continue;
		if (beforeEntry) entries.set(relative, beforeEntry);
		else entries.delete(relative);
		if (own?.entry.kind !== "directory" && beforeEntry?.kind !== "directory")
			deltas.push({
				relativePath: relative,
				before: beforeFile?.content,
				beforeMode: beforeEntry?.kind === "file" ? beforeEntry.mode : undefined,
				after: afterFile?.content,
				afterMode: own?.entry.kind === "file" ? own.entry.mode : undefined,
			});
	}
	const diff = diffWorkspaceStructures(
		{ ...after.snapshot, entries },
		after.snapshot,
		deltas,
		new ExecutionPathProjection({ sourceRoot: "/", workspaceRoot: upper }),
	);
	const changes = diff.complete
		? await Promise.all(
				sandboxWorkspaceChanges("/", diff).map(async (change): Promise<SandboxWorkspaceChange> => {
					const root = await hostRoot(change.target);
					const address = { root, resource: slash(path.relative(root, change.target)) };
					if (change.kind === "directory" && change.after)
						return { ...change, ...address, operation: "mkdir" };
					return { ...change, ...address };
				}),
			)
		: [];

	if (prestate)
		for (const [target, dependencies] of await privateInputImages(
			session,
			new Set([...before.keys()].filter((target) => privateFile(prestate, target))),
			prestate,
			true,
		)) {
			before.set(
				target,
				dependencies?.find((dependency) => dependency.kind === "file" && dependency.path === target),
			);
		}
	if (
		prestate &&
		before.size &&
		(await privateInterference(session, new Set(before.keys()), prestate.startedAt, owned))
	)
		for (const target of before.keys()) before.set(target, undefined);
	return { changes, before, complete: diff.complete && [...before.values()].every(Boolean) };
}
/** The entry is flushed before a syscall runs; a partial entry seen at capture start stays unsafe after completion. */
async function privateInterference(
	session: ActiveSession,
	paths: ReadonlySet<string>,
	since: number,
	owned: readonly SessionWriter[] = [],
) {
	const targets = new Set<string>();
	const until = Date.now();
	const processes = new Set(owned.flatMap((writer) => writer.pids ?? []));
	for (const target of paths)
		for (let name = target; ; name = path.dirname(name)) {
			targets.add(name);
			targets.add(path.join(privateUpper(session), name));
			if (name === "/") break;
		}
	for (const writer of sessionWriters(session))
		if (!owned.includes(writer) && (writer.endedAt ?? Infinity) >= since) {
			const tail = await readWriterTrace(writer);
			if (
				tail.unsafe ||
				writesWithin(
					tail.writes.filter(
						(write) => !processes.has(write.pid!) && (!write.paths || write.at >= since || write.mapped),
					),
					targets,
					since,
					until,
				)
			)
				return true;
		}
	return false;
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
function speculativeProducerProof(ready: ReadyBackend, deniedPaths: readonly string[], policy = POLICY_ID) {
	return Object.freeze({
		observer: { provider: "execution_domain", fingerprint: ready.observerFingerprint },
		execution: {
			authority: "speculative",
			confinement: {
				provider: "sandlock",
				fingerprint: digestObject({ policy, deniedPaths }),
			},
		},
	} satisfies ProcessProducerProof);
}
/** Compare the kernel journal with each actual pipe before publishing its cross-stream order. */
function verifyProcessOutput(
	observation: ExecutionObservation,
	delivered: readonly BufferedOutput[],
): ExecutionObservation {
	const digests = (events: readonly BufferedOutput[]) => {
		const hashes = [createHash("sha256"), createHash("sha256")];
		for (const { fd, data } of events) hashes[fd - 1]!.update(data);
		return hashes.map((hash) => hash.digest("hex"));
	};
	if (observation.output && stableEqual(digests(observation.output), digests(delivered))) return observation;
	return {
		...observation,
		complete: false,
		taints: [...new Set([...observation.taints, "trace_incomplete" as const])],
		incompleteReasons: [...observation.incompleteReasons, "output_bytes_unproven"],
	};
}
async function runSpawn(
	[executable, ...args]: readonly [string, ...string[]],
	options: Pick<ProcessExecutionRequest, "cwd" | "environment" | "signal" | "timeout"> & {
		readonly stdin?: Buffer;
		readonly streamBeyondCaptureLimit?: boolean;
		readonly held?: boolean;
		readonly onData?: (data: Buffer) => void;
		readonly onSpawn?: (child: ChildProcess) => void;
		readonly onOutputEndpoints?: (endpoints: readonly [string, string]) => void;
		readonly onControl?: (
			channel: import("node:stream").Duplex,
			wake: () => boolean,
		) => () => void | Promise<void>;
	},
): Promise<SpawnOutcome> {
	options.signal?.throwIfAborted();
	const child = (options.held ? spawnControlled : spawn)(executable, args, {
		cwd: options.cwd,
		env: options.environment,
		detached: true,
		stdio: [
			options.stdin ? "pipe" : "ignore",
			"pipe",
			"pipe",
			...(options.onControl ? ["pipe" as const] : []),
			...(options.onOutputEndpoints ? ["pipe" as const] : []),
		],
	});
	options.onSpawn?.(child);
	const output: BufferedOutput[] = [];
	const limits = speculativeConfig().processReuse;
	let capturedBytes = 0;
	let captureComplete = true;
	const append = (fd: 1 | 2, value: Buffer) => {
		options.onData?.(value);
		if (!captureComplete) return;
		capturedBytes += value.length;
		if (capturedBytes > limits.maxCapturedOutputBytes || output.length >= limits.maxOutputEvents) {
			captureComplete = false;
			output.length = 0;
			if (!options.streamBeyondCaptureLimit) terminate();
			return;
		}
		const event = { fd, data: Buffer.from(value) } as const;
		const previous = output.at(-1);
		if (previous?.fd === fd && previous.data.byteLength + event.data.byteLength <= limits.outputChunkBytes) {
			output[output.length - 1] = {
				fd,
				data: Buffer.concat([previous.data, event.data]),
			};
		} else output.push(event);
	};
	const endpoints = options.onOutputEndpoints
		? (async () => {
				const channel = child.stdio.at(-1) as import("node:stream").Duplex;
				let bytes = Buffer.alloc(0);
				for await (const chunk of channel) {
					bytes = Buffer.concat([bytes, chunk]);
					if (bytes.length > 16) throw new Error("invalid output endpoint record");
					if (bytes.length === 16) {
						options.onOutputEndpoints!([
							`socket:[${bytes.readBigUInt64LE(0)}]`,
							`socket:[${bytes.readBigUInt64LE(8)}]`,
						]);
						channel.end("R");
					}
				}
				if (bytes.length !== 16) throw new Error("output endpoints are unavailable");
			})()
		: Promise.resolve();
	const drained = Promise.all([
		endpoints,
		...[child.stdout!, child.stderr!].map((source, index) => {
			source.on("data", (value: Buffer) => append(index === 0 ? 1 : 2, value));
			return finished(source, {
				readable: true,
				writable: false,
				cleanup: true,
			});
		}),
	]);
	void drained.catch(() => undefined);
	if (options.stdin) child.stdin?.end(options.stdin);
	const terminate = () => {
		if (!child.pid) return;
		try {
			process.kill(-child.pid, "SIGKILL");
		} catch {
			child.kill("SIGKILL");
		}
	};
	options.signal?.addEventListener("abort", terminate, { once: true });
	let timedOut = false;
	const timeout =
		options.timeout === undefined
			? undefined
			: setTimeout(
					() => {
						timedOut = true;
						terminate();
					},
					Math.max(1, options.timeout * 1000),
				);
	const completed: Promise<Pick<SpawnOutcome, "code" | "signal">> = once(child, "close").then(
		([code, signal]) => ({ code, signal }),
	);
	let releaseControl: (() => void | Promise<void>) | undefined;
	try {
		if (options.onControl)
			releaseControl = options.onControl(child.stdio[3] as import("node:stream").Duplex, () =>
				child.kill("SIGUSR2"),
			);
		const [result] = await Promise.all([completed, drained]);
		if (options.signal?.aborted) throw new Error("aborted");
		if (timedOut) throw new Error(`timeout:${options.timeout}`);
		if (!captureComplete && !options.streamBeyondCaptureLimit)
			throw new Error("process_output_budget_exceeded");
		return { ...result, output, captureComplete };
	} catch (error) {
		terminate();
		await completed.catch(() => undefined);
		throw error;
	} finally {
		clearTimeout(timeout);
		options.signal?.removeEventListener("abort", terminate);
		await releaseControl?.();
	}
}
function eligibleRequest(
	session: ActiveSession,
	context: ProcessExecutionContext,
	cwd: string,
	args: readonly string[],
	expectedContext: ProcessExecutionContext,
) {
	if (!validProcessContext(context)) return "process_context_unsupported";
	if (!pathContains(session.workspace.sandboxRoot, cwd)) return "cwd_outside_workspace";
	if (args.length > speculativeConfig().processReuse.maxArguments) return "argument_count_limit";
	if (
		args.reduce((sum, value) => sum + Buffer.byteLength(value), 0) >
		speculativeConfig().processReuse.maxArgumentBytes
	)
		return "argument_bytes_limit";
	if (!session.outputs.topLevel) return "output_endpoint_capture_missing";
	if (context.outputEndpoints.some((endpoint) => !endpoint)) return "request_output_endpoint_missing";
	// The launch key below still checks a discarded descriptor's type and flags. Both outlets belong to one captured process.
	const endpoints =
		[session.outputs.topLevel, ...session.outputs.nested].find((pair) =>
			context.outputEndpoints.every(
				(endpoint) => pair.includes(endpoint) || endpoint === "/dev/null",
			),
		) ?? session.outputs.topLevel;
	const launch = processLaunchOptions(context, endpoints);
	if (!launch) return "output_endpoint_mismatch";
	const routed = routedProcessIdentity(expectedContext, launch);
	if (context.launchKey !== routed.launchKey) return "launch_key_mismatch";
	if (context.umask !== routed.umask) return "umask_mismatch";
	return launch;
}
function shellArguments(invocation: ToolProcessInvocation, command: string) {
	return invocation.commandTransport === "argv"
		? [...invocation.shellArgs, command]
		: [...invocation.shellArgs];
}
async function topLevelProcessPrototype(
	invocation: ToolProcessInvocation,
	request: ProcessExecutionRequest,
	environment: Readonly<Record<string, string>>,
	projection: ExecutionPathProjection,
	platformFingerprint: Sha256Digest,
) {
	return createExecPrototype({
		executablePath: invocation.shell,
		executableDigest: await hashExecutableFile(invocation.shell, request.signal),
		argv: [invocation.shell, ...shellArguments(invocation, request.command)],
		logicalCwd: projection.toLogical(projection.toPhysical(request.cwd) ?? request.cwd),
		environment,
		umask: process.umask(),
		processContextDigest: digestObject({
			subcomputations: controlledSubcomputeChannel(request.environment)?.identity,
			limits: sha256Digest(await readFile("/proc/self/limits", { signal: request.signal })),
			credentials: {
				uid: process.getuid?.(),
				euid: process.geteuid?.(),
				gid: process.getgid?.(),
				egid: process.getegid?.(),
				groups: process.getgroups?.(),
			},
			scheduler: {
				cpuCount: os.availableParallelism(),
				timeout: request.timeout ?? null,
			},
			signals: "node-default",
		}),
		stdin:
			invocation.commandTransport === "stdin"
				? { type: "bytes", digest: sha256Digest(request.command), eof: true }
				: { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [
			{
				fd: 0,
				type: invocation.commandTransport === "stdin" ? "pipe" : "device",
				flagsDigest: digestObject({ mode: "read" }),
				eof: true,
			},
			...[1, 2].map((fd) => ({
				fd,
				type: "pipe" as const,
				flagsDigest: digestObject({ mode: "write", sink: "buffered" }),
			})),
		],
		platformFingerprint,
	});
}
async function captureHostObjectDependency(
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
async function captureContentAddressedDependency(
	target: string,
	role: DependencyRole | "metadata",
	view: FilesystemReadView,
	settings = speculativeConfig().processReuse,
): Promise<DynamicDependency | undefined> {
	if (role === "metadata" || !settings.contentAddressedPaths.some((root) => pathContains(root, target))) return;
	const info = await view.stat(target, false).catch(ignoreMissing);
	if (!info?.isFile() || !settledFilesystemStat(info, Date.now(), target)) return;
	const fields = ["dev", "ino", "mode", "size", "mtimeNs", "ctimeNs"] as const;
	return { kind: "metadata", path: slash(target), followSymlinks: false, fields, digest: filesystemObservationDigest(info, fields) };
}
function mergeDynamicDependency(
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
function classifyObservedSpecialPath(
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
	if (/^\/proc\/(?:self|thread-self|\d+)(?:\/|$)/.test(observedPath))
		return observedPath.endsWith("/limits") ? "confinement_observation" : "pid_observation";
	// Reopening an inherited descriptor reaches what that descriptor carries, like a jobserver pipe, not a host file.
	if (/^\/dev\/fd\/\d+$/.test(observedPath)) return "descriptor_observation";
	if (/^\/dev\/u?random$/.test(observedPath)) return "random";
	if (
		/^\/proc\/sys(?:\/|$)|^\/sys\/(?:devices\/system\/cpu|fs\/cgroup)(?:\/|$)|^\/proc\/(?:meminfo|version|version_signature|cpuinfo|stat|loadavg|uptime)$/.test(
			observedPath,
		)
	)
		return "clock";
	return undefined;
}
async function captureActorDependencies(
	observation: ExecutionObservation,
	deniedPaths: readonly string[],
	signal?: AbortSignal,
	files?: ReadonlyMap<string, FileContentVersion>,
): Promise<{ dependencies: DynamicDependency[]; taints: ProvenanceTaint[] }> {
	const dependencies = new Map<string, DynamicDependency>();
	const taints = new Set<ProvenanceTaint>(observation.taints);
	const settings = speculativeConfig().processReuse;
	await using view = await FilesystemReadView.open();
	const add = (dependency: DynamicDependency) => {
		const key = dynamicDependencyIdentity(dependency);
		const merged = mergeDynamicDependency(dependencies.get(key), dependency);
		if (merged === "conflict") throw new Error(`actor_dependency_conflict:${key}`);
		dependencies.set(key, merged);
	};
	const prefixes = new Map<string, Promise<DynamicDependency | undefined>>();
	const symlinkAt = (prefix: string) => {
		let pending = prefixes.get(prefix);
		if (!pending)
			prefixes.set(
				prefix,
				(pending = view
					.stat(prefix, false)
					.catch(ignoreMissing)
					.then((info) => (info?.isSymbolicLink() ? captureSymlinkDependency(prefix, prefix, view) : info && undefined))),
			);
		return pending;
	};
	const relevant: ExecutionObservation["paths"][number][] = [];
	for (const input of observation.paths) {
		signal?.throwIfAborted();
		const observedPath = path.resolve(input.path);
		if (deniedPaths.some((denied) => pathContains(denied, observedPath)))
			throw new Error(`actor_denied_input:${observedPath}`);
		// These links are resolved in the tracee's filesystem context, which ends at exit.
		if (/^\/proc\/(?:self|thread-self|\d+)\/(?:task\/\d+\/)?(?:cwd|root|fd|map_files)\//.test(input.path) ||
			/^\/dev\/fd\/\d+\//.test(input.path))
			throw new Error(`actor_unresolved_process_path:${input.path}`);
		const special = classifyObservedSpecialPath(observedPath, settings);
		if (special) {
			if (special !== "ignored") taints.add(special);
			continue;
		}
		relevant.push(input);
	}
	const captured = await mapFilesystem(
		relevant,
		async (input) => {
			signal?.throwIfAborted();
			const observedPath = path.resolve(input.path);
			const items: DynamicDependency[] = [];
			let sharedComputation: TimelineInterval | undefined;
			// Record every symlink in the observed pathname before following its final object.
			let prefix = path.parse(input.path).root;
			for (const segment of input.path.slice(prefix.length).split(path.sep).filter(Boolean)) {
				prefix = path.join(prefix, segment);
				const link = await symlinkAt(prefix);
				if (link) {
					const resolved = await view.realpath(prefix).catch(ignoreMissing);
					if (resolved && deniedPaths.some((denied) => pathContains(denied, resolved)))
						throw new Error(`actor_denied_input:${resolved}`);
					items.push(link);
				}
			}
			const contentAddressed = await captureContentAddressedDependency(observedPath, input.role, view, settings);
			if (contentAddressed) {
				items.push(contentAddressed);
				return { items, sharedComputation };
			}
			const info = await view
				.stat(input.path, input.role === "metadata" ? input.followSymlinks : true)
				.catch(ignoreMissing);
			if (!info) {
				const absence = await captureHostObjectDependency(input.path, input.path, "input", false, undefined, view);
				if (!absence) throw new Error(`actor_absence_changed:${input.path}`);
				items.push(absence);
			} else if (input.role === "metadata") {
				const item = await captureMetadataDependency(
					input.path,
					input.path,
					input.followSymlinks,
					input.fields,
					view,
				);
				if (item.digest !== input.digest) throw new Error(`actor_metadata_changed:${input.path}`);
				items.push(item);
			} else if (info.isDirectory())
				items.push(
					(await captureHostObjectDependency(input.path, input.path, input.role, !!input.listed, info, view, {}))!,
				);
			else if (info.isFile()) {
				const target = await view.realpath(input.path);
				if (deniedPaths.some((denied) => pathContains(denied, target)))
					throw new Error(`actor_denied_input:${target}`);
				const file = files?.get(target);
				if (file && sameFilesystemIdentity(info, file.stat)) {
					items.push({
						kind: "file",
						path: target,
						role: input.role,
						contentDigest: contentDigest(file.content),
						metadataDigest: filesystemMetadataDigest(info),
					});
					if (file.shared) sharedComputation = file.computation;
				} else {
					items.push(
						(await captureHostObjectDependency(target, target, input.role, false, info, view, {
							signal,
							maxFileBytes: settings.maxCaptureBytes,
						}))!,
					);
				}
			} else throw new Error(`actor_special_input:${input.path}`);
			return { items, sharedComputation };
		},
		signal,
	);
	for (const { items, sharedComputation } of captured) {
		for (const item of items) add(item);
		if (sharedComputation) TimelineInterval.use(sharedComputation);
	}
	return { dependencies: [...dependencies.values()], taints: [...taints].sort() };
}
function actorReplayProducer(producer: ProcessProducerProof, deniedPaths: readonly string[]) {
	if (
		producer.observer.provider !== "execution_domain" ||
		producer.observer.fingerprint !== OBSERVER_FINGERPRINT
	)
		return false;
	const confinement = producer.execution.authority === "actor" ? undefined : producer.execution.confinement;
	return (
		!confinement ||
		(confinement.provider === "sandlock" &&
			[POLICY_ID, LEAF_POLICY_ID].some(
				(policy) => confinement.fingerprint === digestObject({ policy, deniedPaths }),
			))
	);
}
function exitOutcome(outcome: SpawnOutcome): ExitOutcome {
	if (!outcome.signal) return { kind: "code", code: outcome.code ?? 125 };
	return {
		kind: "signal",
		signal: os.constants.signals[outcome.signal] ?? 9,
		coreDumped: false,
	};
}
function assertInvocationMatches(invocation: ToolProcessInvocation, request: ProcessExecutionRequest) {
	const channel = controlledSubcomputeChannel(request.environment);
	const matches = {
		command: request.command === invocation.command,
		cwd: path.resolve(request.cwd) === path.resolve(invocation.cwd),
		timeout: request.timeout === invocation.timeout,
		environment: stableEqual(
			projectProcessEnvironment(definedProcessEnvironment(request.environment)),
			projectProcessEnvironment({ ...invocation.environment, ...channel?.environment(invocation.environment) }),
		),
	};
	for (const [field, valid] of Object.entries(matches))
		if (!valid) throw new Error(`process ${field} differs from the action execution context`);
}
export async function validateTransferredProcessEvidence(
	evidence: DynamicDependencyCertificate | undefined,
	incompleteReasons: Iterable<string> = [],
	readView?: FilesystemReadView,
): Promise<ResourceValidation> {
	if (!evidence) {
		return {
			status: "indeterminate",
			cause: cause("freshness", "process_evidence_missing"),
			metrics: zeroValidationMetrics(),
		};
	}
	const blockingTaints = evidence.taints.filter((taint) => !acceptedReplayTaints().includes(taint));
	const validation = await validateDynamicDependencyCertificate(
		{ ...evidence, taints: blockingTaints },
		{ maxFileBytes: speculativeConfig().filesystem.maxLogicalBytes, readView },
	);
	const metrics = {
		bytesRead: validation.bytesRead,
		filesRead: validation.filesRead,
		mode: "exact" as const,
	};
	if (validation.status === "valid") return { status: "valid", metrics };
	if (validation.status === "stale")
		return {
			status: "stale",
			cause: {
				stage: "freshness",
				code: "process_dependency_changed",
				detail: validation.changed.join(","),
			},
			metrics,
		};
	return {
		status: "indeterminate",
		cause: {
			stage: "freshness",
			code: "process_provenance_indeterminate",
			detail: [validation.reason, ...incompleteReasons].join(","),
		},
		metrics,
	};
}
function sensitivePaths(storeRoot: string, additional: readonly string[] | undefined) {
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

/** Compatibility aggregate. Prefer the narrow ./core and ./process-reuse package entries. */
export * from "./core.ts";
export * from "./process-reuse.ts";

export {
	BenefitGate,
	type BenefitDecision,
	type BenefitDecisionReason,
	type BenefitGatePolicy,
	type BenefitGateSnapshot,
	type BenefitObservation,
	DEFAULT_BENEFIT_GATE_POLICY,
	ForkBenefitGate,
	type ForkBenefitDecision,
	type ForkBenefitDecisionReason,
	type ForkBenefitGatePolicy,
	type ForkBenefitGateSnapshot,
	type ForkBenefitObservation,
} from "./fork-benefit-gate.ts";
export {
	type ActionKeyMismatchReason,
	actionKeyMismatchReason,
	actionKeyProjectionPartitions,
	actionKeyProjects,
	BASH_TAIL_LINES_ACTION_KEY_PROJECTOR,
	type BashTailLinesView,
	bashTailLinesView,
	buildPiActionKey,
	FIND_DEFAULT_LIMIT,
	GREP_DEFAULT_LIMIT,
	inferredActionEffect,
	KEYABLE_TOOLS,
	LS_DEFAULT_LIMIT,
	normalizeReadLimit,
	normalizeReadOffset,
	normalizeRelativeRoot,
	OBSERVATION_ACTION_TOOLS,
	READ_DEFAULT_LIMIT,
	READ_DEFAULT_OFFSET,
	type ReadActionRange,
	readActionRange,
	UNBOUNDED_ACTION_TOOLS,
	WORKSPACE_MUTATION_ACTION_TOOLS,
} from "./action-semantics.ts";
export {
	createResourceSnapshotExecutionWorld,
	type SpeculativeAgentExecutionWorld,
	type SpeculativeToolExecutionContext,
} from "./agent-execution-world.ts";
export {
	type ActionDrafterGateSnapshot,
	type CreateSpeculativeActionHostOptions,
	createSpeculativeActionHost,
	type DraftOptionsContext,
	patternPlanActionID,
	type SpeculativeActionHost,
	type SpeculativeAgentPreflightContext,
	type SpeculativeAgentSettingsInput,
	type SpeculativeToolExecutionInput,
} from "./agent-integration.ts";
export {
	clampCandidateLimit,
	DEFAULTS,
	type LegacySpeculativeToolGroups,
	normalizeSpeculativeToolSelection,
	type SpeculativeToolSelectionInput,
} from "./common.ts";
export { calculateContextTokens as usageTokenCount } from "@earendil-works/pi-agent-core";
export type { ActionReuseKind, ExecutionScope } from "./execution-world.ts";
export {
	createSpeculativeActionExtension,
	type EffectiveSpeculativeActionSettings,
	formatSpeculativeActionEvent,
	formatSpeculativeActionStatus,
	normalizeSpeculativeActionSettings,
	resolveSpeculativeDraftModel,
	type SpeculativeActionExtensionDependencies,
	type SpeculativeActionMetrics,
	type SpeculativeSettingsStore,
} from "./extension.ts";
export {
	acquirePatternAwareStore,
	applyBindings,
	applyBindingsVariants,
	asPatternAwareRuntimeContext,
	inferBindings,
	PATTERN_AWARE_DEFAULTS,
	type PatternAwareActionSemantics,
	type PatternAwareBinding,
	type PatternAwareCandidate,
	type PatternAwareContinuation,
	type PatternAwareDependency,
	type PatternAwareDependencySource,
	type PatternAwareEvent,
	type PatternAwareEventInput,
	type PatternAwareEventSignature,
	type PatternAwareFeedback,
	type PatternAwareObservation,
	type PatternAwarePath,
	type PatternAwarePattern,
	type PatternAwareRuntimeContext,
	type PatternAwareSettings,
	PatternAwareStore,
	type PatternAwareStoreLease,
	patternAwareAnalyzerKey,
	patternAwarePersistenceFile,
	patternAwareRuntimeContext,
	patternAwareSettings,
	projectPatternAwareObservation,
} from "./pattern-aware.ts";
export { PI_BASH_TAIL_LINES_PROJECTION_RULE } from "./pi-bash-projection.ts";
export {
	PI_READ_RANGE_PROJECTION_RULE,
	withPiProjectionCoverage,
	withPiReadCoverage,
} from "./pi-read-projection.ts";
export { type PiToolInvocationOptions, resolvePiToolInvocation } from "./pi-tool-invocation.ts";
export {
	LinuxProcessReuseBackend,
	type LinuxProcessBackendOptions,
	type LinuxProcessBackendStatus,
	type LinuxProcessReuseMetrics,
	type LinuxProcessSession,
} from "./linux-process-backend.ts";
export {
	createLinuxProcessExecutionWorld,
	type LinuxProcessExecutionWorldOptions,
} from "./linux-process-world.ts";
export {
	adaptProcessToolOperations,
	ProcessExecutionCoordinator,
	type ProcessExecutionRequest,
	type ProcessExecutionResult,
	type ProcessExecutor,
	type ProcessToolOperations,
} from "./process-execution.ts";
export { isResourceVersionToken } from "./resource-version.ts";
export {
	normalizeSelfSpeculationSettings,
	SELF_SPECULATION_DEFAULTS,
	SelfSpeculationCoordinator,
	type SelfSpeculationCoordinatorOptions,
	type SelfSpeculationCoordinatorSnapshot,
	type SelfSpeculationForkTransport,
	type SelfSpeculationSettings,
	type SelfSpeculationSettingsInput,
	type SelfSpeculationVerificationOutcome,
	type SelfSpeculationVerificationStep,
} from "./self-speculation.ts";
export {
	ACTOR_PROBE_SCHEDULE,
	ActorForkPlanSource,
	createActorForkPlanSource,
	type ActorForkActionBatch,
	type ActorForkActionCall,
	type ActorForkActionEvidence,
	type ActorProbeSchedule,
	type ActorProbeSnapshot,
} from "./actor-fork-plan-source.ts";
export {
	type SpeculativeActionPackageSettings,
	SpeculativeActionSettingsStore,
	type SpeculativeSettingsScope,
} from "./settings-store.ts";
export type { ToolInvocation, ToolProcessInvocation, ToolSettlement } from "./tool-settlement.ts";
export {
	closeWorkspaceSandboxPools,
	commitSandboxDelta,
	createWorkspaceSandbox,
	type PrepareSandboxWorkspaceOptions,
	prepareSandboxWorkspace,
	qualifyWorkspaceSandboxDriver,
	type QualifiedWorkspaceSandboxDriver,
	readSandboxDirectoryState,
	type SandboxDirectoryChange,
	type SandboxDirectoryState,
	type SandboxExecutionDelta,
	type SandboxFileChange,
	type SandboxWorkspaceChange,
	type SandboxWorkspaceContext,
	type SandboxWorkspaceBranchOptions,
	type WorkspaceSandboxOptions,
	type WorkspaceSandboxDriver,
	WorkspaceSandboxService,
	forkSandboxWorkspace,
	withSandboxWorkspace,
	workspaceSandboxFingerprint,
} from "./workspace-sandbox.ts";
export {
	linuxOverlayfsCapability,
	mountLinuxOverlayfs,
	type LinuxOverlayfsCapability,
	type LinuxOverlayfsMount,
	type LinuxOverlayfsOptions,
} from "./linux-overlayfs.ts";
export type {
	WorkspaceRegularDelta,
	WorkspaceStructureDriver,
	WorkspaceTransactionCapture,
	WorkspaceTransactionDelta,
	WorkspaceTransactionDriver,
} from "./workspace-transaction.ts";

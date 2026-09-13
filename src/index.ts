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
} from "./fork-benefit-gate.ts";
export {
	type ActionKeyMismatchReason,
	actionKeyMismatchReason,
	actionKeyProjectionPartitions,
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
export * from "./agent-execution-world.ts";
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
	normalizeSpeculativeToolSelection,
} from "./common.ts";
export { calculateContextTokens as usageTokenCount } from "@earendil-works/pi-agent-core";
export type { ExecutionScope } from "./execution-world.ts";
export * from "./extension.ts";
export * from "./pattern-aware.ts";
export * from "./pi-read-projection.ts";
export { type PiToolInvocationOptions, resolvePiToolInvocation } from "./pi-tool-invocation.ts";
export {
	type CompletedProcessReplayOptions,
	LinuxProcessReuseBackend,
	type LinuxProcessBackendOptions,
	type LinuxProcessBackendStatus,
	type LinuxProcessReuseMetrics,
	type LinuxProcessSession,
} from "./linux-process-backend.ts";
export * from "./linux-process-world.ts";
export {
	adaptProcessToolOperations,
	ProcessExecutionCoordinator,
	type ProcessExecutionRoute,
	type ProcessExecutionRequest,
	type ProcessExecutionResult,
	type ProcessExecutor,
	type ProcessToolOperations,
} from "./process-execution.ts";
export { isResourceVersionToken } from "./resource-version.ts";
export * from "./self-speculation.ts";
export * from "./actor-fork-plan-source.ts";
export * from "./settings-store.ts";
export type { ToolFilesystemOperations, ToolInvocation, ToolProcessInvocation, ToolSettlement } from "./tool-settlement.ts";
export * from "./workspace-sandbox.ts";
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

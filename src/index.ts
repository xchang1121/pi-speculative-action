/** Pi Host API. Runtime and provenance APIs have their own core and process-reuse entries. */

export {
	bindResourceOperation,
	createResourceSnapshotExecutionWorld,
	type ActorToolExecutionContext,
	type AgentExecutionWorld,
	type SpeculativeAgentExecutionWorld,
	type SpeculativeToolExecutionContext,
} from "./agent-execution-world.ts";
export {
	createSpeculativeActionHost,
	patternPlanActionID,
	type ActionDrafterBudgetSnapshot,
	type CreateSpeculativeActionHostOptions,
	type DraftOptionsContext,
	type SpeculativeActionHost,
	type SpeculativeAgentPreflightContext,
	type SpeculativeAgentSettingsInput,
	type SpeculativeToolExecutionInput,
} from "./agent-integration.ts";
export type { AgentRuntime, AgentRuntimeEvents } from "./agent-integration.ts";
export {
	EXACT_TOOL_OBSERVATION,
	registerExecutionWorld,
	type ExecutionWorldEntry,
	type ExecutionWorldQualification,
	type ToolInvocation,
	type ToolObservationContract,
	type ToolProcessInvocation,
	type ToolSettlement,
} from "./execution-world.ts";
export {
	LinuxProcessReuseBackend,
	type LinuxProcessBackendOptions,
	type LinuxProcessBackendStatus,
	type LinuxProcessSession,
} from "./linux-process-backend.ts";
export {
	ProcessExecutionCoordinator,
	adaptProcessToolOperations,
	createCompletedProcessExecutionWorld,
	createLinuxProcessExecutionWorld,
	createRestorableProcessExecutionWorld,
	type LinuxProcessExecutionWorldOptions,
	type ProcessExecutionRequest,
	type ProcessExecutionResult,
	type ProcessExecutor,
	type ProcessToolOperations,
} from "./linux-process-world.ts";
export { resolvePiToolInvocation, type PiToolInvocationOptions } from "./pi-tool-invocation.ts";
export {
	defineSpeculativeStep,
	defineSpeculativeTool,
	type SpeculativeFiles,
	type SpeculativeStep,
	type SpeculativeStepContext,
	type SpeculativeToolContext,
	type SpeculativeToolEffect,
	type SpeculativeWritableFiles,
} from "./speculative-tool.ts";
export { WorkspaceSandboxService, type WorkspaceSandboxOptions } from "./workspace-sandbox.ts";
export { registerSpeculativeTool } from "./agent-integration.ts";
export {
	DEFAULT_SPECULATIVE_CONFIG,
	resolveSpeculativeConfig,
	SpeculativeConfiguration,
	withSpeculativeConfig,
	type SpeculativeConfig,
	type SpeculativeConfigInput,
} from "./config-schema.ts";
export {
	computeStepInView,
	type ComputeStepRequest,
	type ToolFilesystemOperations,
} from "./resource-version.ts";
export { subcomputeEnvironment } from "./subcompute.ts";
export { createControlledBashOperations } from "./linux-process-world.ts";

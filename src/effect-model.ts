/** Atomic guarantees an execution backend may contain, virtualize, or validate. */
export type EffectCapability =
	| "invocation.host_function"
	| "invocation.process"
	| "invocation.workspace_path"
	| "filesystem.read"
	| "filesystem.write"
	| "process.spawn"
	| "network.mediate"
	| "ipc.mediate"
	| "time.virtualize"
	| "random.virtualize"
	| "output.gate"
	| "validation.resource_snapshot";

export interface EffectRequirements {
	readonly capabilities: readonly EffectCapability[];
}

/** `all` is reserved for a runtime-wide sandbox that contains every operation effect. */
export type EffectCapabilities = "all" | readonly EffectCapability[];

export const RESOURCE_OBSERVATION_EFFECTS = effectRequirements(
	"invocation.host_function",
	"filesystem.read",
	"validation.resource_snapshot",
);

export const WORKSPACE_PATH_MUTATION_EFFECTS = effectRequirements(
	"invocation.host_function",
	"invocation.workspace_path",
	"filesystem.read",
	"filesystem.write",
);

export const UNRESTRICTED_PROCESS_EFFECTS = effectRequirements(
	"invocation.process",
	"filesystem.read",
	"filesystem.write",
	"process.spawn",
	"network.mediate",
	"ipc.mediate",
	"time.virtualize",
	"random.virtualize",
	"output.gate",
);

export function effectRequirements(...capabilities: readonly EffectCapability[]): EffectRequirements {
	return Object.freeze({ capabilities: Object.freeze(normalizeCapabilities(capabilities)) });
}

export function effectCapabilitiesCover(
	provided: EffectCapabilities,
	required: EffectRequirements,
): boolean {
	if (provided === "all") return true;
	const available = new Set(provided);
	return required.capabilities.every((capability) => available.has(capability));
}

export function normalizeEffectRequirements(requirements: EffectRequirements): EffectRequirements {
	return effectRequirements(...requirements.capabilities);
}

function normalizeCapabilities(capabilities: readonly EffectCapability[]): EffectCapability[] {
	return [...new Set(capabilities)].sort();
}

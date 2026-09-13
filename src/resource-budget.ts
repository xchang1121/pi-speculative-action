import { positiveCount as units } from "./number-utils.ts";
import type { ActionEffect } from "./action-semantics.ts";
import type { SpeculativeExecution } from "./execution-world.ts";

export type SpeculativeResourceClass = "filesystem" | "workspace" | "process" | "global";

export interface SpeculativeResourceProfile {
	readonly class: SpeculativeResourceClass;
	readonly units: number;
}

export interface SpeculativeResourceBudget {
	readonly total: number;
	readonly classes: Readonly<Record<SpeculativeResourceClass, number>>;
}

export function speculativeResourceBudget(capacity: number): SpeculativeResourceBudget {
	const total = units(capacity);
	return {
		total,
		classes: { filesystem: total, workspace: total, process: total, global: total },
	};
}

export function resourceProfile(execution: SpeculativeExecution): SpeculativeResourceProfile {
	if (execution === "resource_snapshot") return { class: "filesystem", units: 1 };
	if (execution === "workspace_branch") return { class: "workspace", units: 1 };
	return { class: "global", units: 1 };
}

export function actionResourceProfile(effect: ActionEffect | undefined): SpeculativeResourceProfile {
	if (effect === "observation") return { class: "filesystem", units: 1 };
	if (effect === "workspace_mutation") return { class: "workspace", units: 1 };
	return { class: "global", units: 1 };
}

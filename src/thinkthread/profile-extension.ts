import path from "node:path";
import type { ExtensionFactory } from "@earendil-works/pi-coding-agent";
import { inferredActionEffect } from "../action-semantics.ts";
import type { AgentExecutionWorld } from "../agent-execution-world.ts";
import type { SpeculativeActionHost } from "../agent-integration.ts";
import { createSpeculativeActionHost } from "../agent-integration.ts";
import { createSpeculativeActionExtension } from "../extension.ts";
import { SpeculativeActionSettingsStore } from "../settings-store.ts";
import {
	createThinkThreadExecutionWorld,
	type ThinkThreadExecutionWorld,
	type ThinkThreadExecutionWorldOptions,
} from "./execution-world.ts";

export interface ThinkThreadProfileExtensionOptions {
	readonly world?: ThinkThreadExecutionWorldOptions;
	readonly configDirectory?: string;
}

export function createThinkThreadProfileExtension(options: ThinkThreadProfileExtensionOptions = {}): ExtensionFactory {
	const worlds = new WeakMap<AgentExecutionWorld, ThinkThreadExecutionWorld>();
	return createSpeculativeActionExtension({
		createExecutionWorlds: ({ autoResizeImages }) => {
			const world = createThinkThreadExecutionWorld({ autoResizeImages, ...options.world });
			worlds.set(world, world);
			return [world];
		},
		createHost: (sessionID, hostOptions) => {
			const world = hostOptions.executionWorlds?.map((candidate) => worlds.get(candidate))
				.find((candidate) => candidate !== undefined);
			if (!world) throw new Error("ThinkThread execution world was not created for this Pi session");
			return withThinkThreadProfileLifecycle(createSpeculativeActionHost(sessionID, hostOptions), world);
		},
		createSettingsStore: (cwd) =>
			new SpeculativeActionSettingsStore(cwd, resolveConfigDirectory(options.configDirectory)),
	});
}

export function withThinkThreadProfileLifecycle(
	host: SpeculativeActionHost,
	world: ThinkThreadExecutionWorld,
): SpeculativeActionHost {
	const invalidateAfterActorMutation = async (tool: string): Promise<void> => {
		if (inferredActionEffect(tool) === "observation") return;
		// Invalidation clears BASE before releasing its owner. Cleanup failure must not replace Actor output.
		await world.actorFallbackSettled().catch(() => undefined);
	};
	return {
		...host,
		execute: (input, signal, executor) => host.execute(input, signal, async (operation) => {
			try {
				return await executor(operation);
			} finally {
				// Runs only on an Actor miss, before its bound settlement can launch successor actions.
				await invalidateAfterActorMutation(operation.tool);
			}
		}),
		finishTurn: async (...args: Parameters<SpeculativeActionHost["finishTurn"]>) => {
			const [turnID] = args;
			try {
				await host.finishTurn(...args);
			} finally {
				await world.finishTurn(turnID);
			}
		},
	};
}

function resolveConfigDirectory(configDirectory: string | undefined): string {
	const configured = configDirectory ?? process.env.PI_SPECULATIVE_ACTION_CONFIG_DIR;
	if (!configured) throw new Error("PI_SPECULATIVE_ACTION_CONFIG_DIR is required by the ThinkThread profile");
	if (!path.isAbsolute(configured)) {
		throw new Error("PI_SPECULATIVE_ACTION_CONFIG_DIR must be an absolute path");
	}
	return configured;
}

const thinkThreadProfileExtension = createThinkThreadProfileExtension();
export default thinkThreadProfileExtension;

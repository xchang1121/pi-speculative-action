import path from "node:path";
import type { ExtensionFactory } from "@earendil-works/pi-coding-agent";
import { inferredActionEffect } from "../action-semantics.ts";
import type { AgentExecutionWorld } from "../agent-execution-world.ts";
import type { CreateSpeculativeActionHostOptions, SpeculativeActionHost } from "../agent-integration.ts";
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
			return withThinkThreadProfileLifecycle(createSpeculativeActionHost(sessionID, hostOptions), world, hostOptions);
		},
		createSettingsStore: (cwd) =>
			new SpeculativeActionSettingsStore(cwd, resolveConfigDirectory(options.configDirectory)),
	});
}

export function withThinkThreadProfileLifecycle(
	host: SpeculativeActionHost,
	world: ThinkThreadExecutionWorld,
	options: CreateSpeculativeActionHostOptions,
): SpeculativeActionHost {
	const activeTurns = new Set<string>();
	const enabled = () => options.speculativeExecutionWorldEnabled?.(world.id) !== false;
	const invalidateAfterActorMutation = async (tool: string): Promise<void> => {
		if ((activeTurns.size === 0 && !enabled()) || inferredActionEffect(tool) === "observation") return;
		// Invalidation clears BASE before releasing its owner. Cleanup failure must not replace Actor output.
		await world.actorFallbackSettled().catch(() => undefined);
	};
	return {
		...host,
		startTurn: async (...args: Parameters<SpeculativeActionHost["startTurn"]>) => {
			const [input] = args;
			if (!enabled()) return host.startTurn(...args);
			await world.beginTurn(input.turnID).catch(() => undefined);
			activeTurns.add(input.turnID);
			try {
				await host.startTurn(...args);
			} catch (error) {
				activeTurns.delete(input.turnID);
				await world.finishTurn(input.turnID).catch(() => undefined);
				throw error;
			}
		},
		execute: (input, signal, executor) => host.execute(input, signal, async (operation) => {
			try {
				return await executor(operation);
			} finally {
				// Runs only on an Actor miss, before runtime.actual can launch successor actions.
				await invalidateAfterActorMutation(operation.tool);
			}
		}),
		actual: async (...args: Parameters<SpeculativeActionHost["actual"]>) => {
			await invalidateAfterActorMutation(args[0].tool);
			await host.actual(...args);
		},
		finishTurn: async (...args: Parameters<SpeculativeActionHost["finishTurn"]>) => {
			const [turnID] = args;
			try {
				await host.finishTurn(...args);
			} finally {
				if (activeTurns.delete(turnID)) await world.finishTurn(turnID);
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

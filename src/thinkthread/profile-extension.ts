import path from "node:path";
import type { ExtensionFactory } from "@earendil-works/pi-coding-agent";
import { createSpeculativeActionExtension } from "../extension.ts";
import { SpeculativeActionSettingsStore } from "../settings-store.ts";
import {
	createThinkThreadExecutionWorld,
	type ThinkThreadExecutionWorldOptions,
} from "./execution-world.ts";

export interface ThinkThreadProfileExtensionOptions {
	readonly world?: ThinkThreadExecutionWorldOptions;
	readonly configDirectory?: string;
}

export function createThinkThreadProfileExtension(options: ThinkThreadProfileExtensionOptions = {}): ExtensionFactory {
	return createSpeculativeActionExtension({
		createExecutionWorlds: ({ autoResizeImages }) => [createThinkThreadExecutionWorld({ autoResizeImages, ...options.world })],
		createSettingsStore: (cwd) =>
			new SpeculativeActionSettingsStore(cwd, resolveConfigDirectory(options.configDirectory)),
	});
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

import { readFile } from "node:fs/promises";
import path from "node:path";
import { writeJsonFile } from "./filesystem-evidence.ts";
import { isDeepStrictEqual } from "node:util";
import { getAgentDir } from "@earendil-works/pi-coding-agent";
import type { SpeculativeAgentSettingsInput } from "./agent-integration.ts";
import { isRecord } from "./stable-json.ts";

export interface ExecutionRoutingSettings {
	readonly primary?: boolean;
	readonly nativeFallback?: boolean;
}

export interface SpeculativeActionPackageSettings extends SpeculativeAgentSettingsInput {
	readonly draftModel?: string;
	readonly executionStoreMaxEntries?: number;
	readonly executionStoreMaxBytes?: number;
	readonly executionRouting?: ExecutionRoutingSettings;
	/** Explicit Actor and speculative search semantics; native Pi remains the default. */
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
		[this.global, this.project] = await Promise.all([read(this.globalPath), trusted ? read(this.projectPath).then(userScoped) : undefined]);
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
	setEffective(value: SpeculativeActionPackageSettings, inherited = this.scopeValue === "project" ? this.editable("global") : undefined): void {
		const overlay = diffRecord(inherited as SettingsOverlay ?? {}, value as SettingsOverlay);
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
		this.writeQueue = this.writeQueue.catch(() => undefined).then(async () => {
			if (this.unreadable.delete(target) && (await readSettings(target, this.unreadable), this.unreadable.has(target))) throw new Error(`${target} is not valid JSON; fix or remove it before saving`);
			await writeJsonFile(target, snapshot, 2);
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

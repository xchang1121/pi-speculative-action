import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { getAgentDir } from "@earendil-works/pi-coding-agent";
import type { SpeculativeAgentSettingsInput } from "./agent-integration.ts";

export interface ExecutionRoutingSettings {
	readonly primary?: boolean;
	readonly nativeFallback?: boolean;
}

export interface SpeculativeActionPackageSettings extends SpeculativeAgentSettingsInput {
	readonly draftModel?: string;
	readonly executionStoreMaxEntries?: number;
	readonly executionStoreMaxBytes?: number;
	readonly executionRouting?: ExecutionRoutingSettings;
}

export type SpeculativeSettingsScope = "global" | "project";
type SettingsOverlay = Record<string, unknown>;

/** Extension-owned configuration; Pi's settings schema remains untouched. */
export class SpeculativeActionSettingsStore {
	private global: SettingsOverlay | undefined;
	private project: SettingsOverlay | undefined;
	private scopeValue: SpeculativeSettingsScope = "global";
	private writeQueue: Promise<PromiseSettledResult<void>> = Promise.resolve({ status: "fulfilled", value: undefined });

	readonly cwd: string;
	readonly agentDirectory: string;

	constructor(cwd: string, agentDirectory = getAgentDir()) {
		this.cwd = cwd;
		this.agentDirectory = agentDirectory;
	}

	async load(): Promise<void> {
		[this.global, this.project] = await Promise.all([readSettings(this.globalPath), readSettings(this.projectPath)]);
		this.scopeValue = this.project ? "project" : "global";
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

	setEffective(value: SpeculativeActionPackageSettings, inherited = this.editable("global")): void {
		if (this.scopeValue === "project") this.project = diffRecord(inherited as SettingsOverlay ?? {}, value as SettingsOverlay);
		else this.global = structuredClone(value) as SettingsOverlay;
		this.persistSelected();
	}

	clear(): void {
		if (this.scopeValue === "project") this.project = undefined;
		else this.global = undefined;
		this.persistSelected();
	}

	private persistSelected(): void {
		const value = this.scopeValue === "project" ? this.project : this.global;
		const target = this.scopeValue === "project" ? this.projectPath : this.globalPath;
		const snapshot = structuredClone(value);
		this.writeQueue = this.writeQueue.then(() => writeSettings(target, snapshot)).then(
			(value) => ({ status: "fulfilled", value }),
			(reason: unknown) => ({ status: "rejected", reason }),
		);
	}

	async flush(): Promise<void> {
		const result = await this.writeQueue;
		if (result.status === "rejected") throw result.reason;
	}

	private get globalPath(): string {
		return path.join(this.agentDirectory, "speculative-action.json");
	}

	private get projectPath(): string {
		return path.join(this.cwd, ".pi", "speculative-action.json");
	}
}

async function readSettings(file: string): Promise<SettingsOverlay | undefined> {
	try {
		const parsed: unknown = JSON.parse(await readFile(file, "utf8"));
		return isRecord(parsed) && Object.keys(parsed).length > 0 ? parsed : undefined;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT" || error instanceof SyntaxError) return undefined;
		throw error;
	}
}

async function writeSettings(file: string, value: SettingsOverlay | undefined): Promise<void> {
	if (!value) {
		await rm(file, { force: true });
		return;
	}
	await mkdir(path.dirname(file), { recursive: true });
	const temporary = `${file}.${randomUUID()}.tmp`;
	try {
		await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, "utf8");
		await rename(temporary, file);
	} finally {
		await rm(temporary, { force: true }).catch(() => undefined);
	}
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

function isRecord(value: unknown): value is Record<string, unknown> {
	return !!value && typeof value === "object" && !Array.isArray(value);
}

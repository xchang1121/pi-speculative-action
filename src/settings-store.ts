import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
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
	private writeQueue = Promise.resolve();

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
		return mergeSettings(this.global, this.project);
	}

	editable(scope = this.scopeValue): SpeculativeActionPackageSettings | undefined {
		return mergeSettings(this.global, scope === "project" ? this.project : undefined);
	}

	setEffective(value: SpeculativeActionPackageSettings, inherited = this.editable("global")): void {
		if (this.scopeValue === "project") this.project = settingsDiff(inherited as SettingsOverlay | undefined, value);
		else this.global = clone(value) as SettingsOverlay;
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
		const snapshot = clone(value);
		this.writeQueue = this.writeQueue.then(() => writeSettings(target, snapshot));
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
	const temporary = `${file}.${process.pid}.tmp`;
	await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, "utf8");
	await rename(temporary, file);
}

function mergeSettings(
	global: SettingsOverlay | undefined,
	project: SettingsOverlay | undefined,
): SpeculativeActionPackageSettings | undefined {
	const merged = applyOverlay(global, project);
	return merged && Object.keys(merged).length > 0 ? (merged as SpeculativeActionPackageSettings) : undefined;
}

function settingsDiff(
	base: SettingsOverlay | undefined,
	target: SpeculativeActionPackageSettings,
): SettingsOverlay | undefined {
	const difference = diffRecord(base ?? {}, target as SettingsOverlay);
	return Object.keys(difference).length > 0 ? difference : undefined;
}

function applyOverlay(
	base: SettingsOverlay | undefined,
	overlay: SettingsOverlay | undefined,
): SettingsOverlay | undefined {
	if (!base && !overlay) return undefined;
	const result: SettingsOverlay = clone(base) ?? {};
	for (const [key, value] of Object.entries(overlay ?? {})) {
		if (value === null) {
			delete result[key];
		} else if (isRecord(value)) {
			const nested = applyOverlay(isRecord(result[key]) ? result[key] : undefined, value);
			if (nested && Object.keys(nested).length > 0) result[key] = nested;
			else delete result[key];
		} else {
			result[key] = clone(value);
		}
	}
	return result;
}

function diffRecord(base: SettingsOverlay, target: SettingsOverlay): SettingsOverlay {
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
		if (equalValue(before, after)) continue;
		if (isRecord(before) && isRecord(after)) {
			const nested = diffRecord(before, after);
			if (Object.keys(nested).length > 0) result[key] = nested;
		} else {
			result[key] = clone(after);
		}
	}
	return result;
}

function equalValue(left: unknown, right: unknown): boolean {
	if (Object.is(left, right)) return true;
	if (Array.isArray(left) && Array.isArray(right)) {
		return left.length === right.length && left.every((value, index) => equalValue(value, right[index]));
	}
	if (!isRecord(left) || !isRecord(right)) return false;
	const leftKeys = Object.keys(left);
	const rightKeys = Object.keys(right);
	return (
		leftKeys.length === rightKeys.length &&
		leftKeys.every((key) => Object.hasOwn(right, key) && equalValue(left[key], right[key]))
	);
}

function clone<T>(value: T | undefined): T | undefined {
	return value === undefined ? undefined : structuredClone(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return !!value && typeof value === "object" && !Array.isArray(value);
}

import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";

const DATASET_ROWS =
	"https://datasets-server.huggingface.co/rows?dataset=TokenRhythm%2FClaw-SWE-Bench&config=lite&split=test&offset=0&length=100";

export interface DatasetRow {
	readonly instance_id: string;
	readonly repo: string;
	readonly base_commit: string;
	readonly patch: string;
	readonly test_patch: string;
	readonly problem_statement: string;
	readonly language: string;
	readonly source_dataset: string;
	readonly FAIL_TO_PASS: readonly string[];
	readonly PASS_TO_PASS: readonly string[];
}

interface RowsResponse {
	readonly ok: boolean;
	readonly status: number;
	json(): Promise<unknown>;
}

export type DatasetFetch = (url: string) => Promise<RowsResponse>;

/**
 * Resolve one benchmark row, persisting only the validated row rather than a
 * transient dataset-server response. A per-instance cache makes record/replay
 * runs independent of dataset-server availability after their first prepare.
 */
export async function loadDatasetRow(
	instanceID: string,
	cacheFile: string,
	request: DatasetFetch = (url) => fetch(url),
): Promise<DatasetRow> {
	const cached = await readCachedRow(cacheFile);
	if (cached?.instance_id === instanceID) return cached;

	const response = await request(DATASET_ROWS);
	if (!response.ok) throw new Error(`Dataset request failed with HTTP ${response.status}`);
	const value = await response.json();
	const row = findDatasetRow(value, instanceID);
	await writeValidatedRow(cacheFile, row);
	return row;
}

async function readCachedRow(cacheFile: string): Promise<DatasetRow | undefined> {
	try {
		return validDatasetRow(JSON.parse(await readFile(cacheFile, "utf8")));
	} catch (error) {
		if (isMissingFile(error) || error instanceof SyntaxError) return undefined;
		throw error;
	}
}

function findDatasetRow(value: unknown, instanceID: string): DatasetRow {
	if (!value || typeof value !== "object" || !("rows" in value) || !Array.isArray(value.rows)) {
		throw new Error("Dataset response has no rows");
	}
	for (const item of value.rows) {
		if (!item || typeof item !== "object" || !("row" in item)) continue;
		const row = validDatasetRow(item.row);
		if (row?.instance_id === instanceID) return row;
	}
	throw new Error(`Claw-SWE-Bench Lite instance not found: ${instanceID}`);
}

function validDatasetRow(value: unknown): DatasetRow | undefined {
	if (!value || typeof value !== "object") return undefined;
	const row = value as Partial<Record<keyof DatasetRow, unknown>>;
	for (const key of [
		"instance_id",
		"repo",
		"base_commit",
		"patch",
		"test_patch",
		"problem_statement",
		"language",
		"source_dataset",
	] as const) {
		if (typeof row[key] !== "string") return undefined;
	}
	for (const key of ["FAIL_TO_PASS", "PASS_TO_PASS"] as const) {
		if (!Array.isArray(row[key]) || !row[key].every((item) => typeof item === "string")) return undefined;
	}
	return row as DatasetRow;
}

async function writeValidatedRow(cacheFile: string, row: DatasetRow): Promise<void> {
	await mkdir(path.dirname(cacheFile), { recursive: true });
	const temporary = path.join(path.dirname(cacheFile), `.${path.basename(cacheFile)}.${randomUUID()}.tmp`);
	try {
		await writeFile(temporary, `${JSON.stringify(row, null, 2)}\n`, { encoding: "utf8", flag: "wx" });
		await rename(temporary, cacheFile);
	} finally {
		await rm(temporary, { force: true }).catch(() => undefined);
	}
}

function isMissingFile(error: unknown): boolean {
	return Boolean(error && typeof error === "object" && "code" in error && error.code === "ENOENT");
}

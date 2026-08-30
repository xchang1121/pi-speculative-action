import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadDatasetRow, type DatasetFetch, type DatasetRow } from "../bench/swebench-dataset.ts";

const roots: string[] = [];

afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("SWE-bench dataset cache", () => {
	it("persists a validated instance and reuses it without a network request", async () => {
		const cacheFile = await cachePath();
		const request = vi.fn<DatasetFetch>().mockResolvedValue(okResponse([datasetRow()]));

		await expect(loadDatasetRow("axios__axios-5316", cacheFile, request)).resolves.toEqual(datasetRow());
		expect(request).toHaveBeenCalledOnce();
		expect(JSON.parse(await readFile(cacheFile, "utf8"))).toEqual(datasetRow());

		const offline = vi.fn<DatasetFetch>().mockRejectedValue(new Error("offline"));
		await expect(loadDatasetRow("axios__axios-5316", cacheFile, offline)).resolves.toEqual(datasetRow());
		expect(offline).not.toHaveBeenCalled();
	});

	it("repairs malformed cache contents only after a valid response arrives", async () => {
		const cacheFile = await cachePath();
		await writeFile(cacheFile, "{broken", "utf8");
		const request = vi.fn<DatasetFetch>().mockResolvedValue(okResponse([datasetRow()]));

		await expect(loadDatasetRow("axios__axios-5316", cacheFile, request)).resolves.toEqual(datasetRow());
		expect(JSON.parse(await readFile(cacheFile, "utf8"))).toEqual(datasetRow());
	});

	it("does not cache a malformed dataset response", async () => {
		const cacheFile = await cachePath();
		const request = vi.fn<DatasetFetch>().mockResolvedValue(okResponse([{ instance_id: "axios__axios-5316" }]));

		await expect(loadDatasetRow("axios__axios-5316", cacheFile, request)).rejects.toThrow(
			"Claw-SWE-Bench Lite instance not found",
		);
		await expect(readFile(cacheFile, "utf8")).rejects.toMatchObject({ code: "ENOENT" });
	});
});

async function cachePath(): Promise<string> {
	const root = await mkdtemp(path.join(os.tmpdir(), "pi-swebench-dataset-"));
	roots.push(root);
	return path.join(root, "axios__axios-5316.json");
}

function okResponse(rows: unknown[]) {
	return {
		ok: true,
		status: 200,
		async json() {
			return { rows: rows.map((row) => ({ row })) };
		},
	};
}

function datasetRow(): DatasetRow {
	return {
		instance_id: "axios__axios-5316",
		repo: "axios/axios",
		base_commit: "0123456789abcdef",
		patch: "",
		test_patch: "",
		problem_statement: "Fix the regression",
		language: "JavaScript",
		source_dataset: "SWE-bench",
		FAIL_TO_PASS: ["test/failing.js"],
		PASS_TO_PASS: [],
	};
}

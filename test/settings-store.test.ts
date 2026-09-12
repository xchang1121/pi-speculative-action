import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { temporaryDirectories } from "./filesystem.ts";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { SpeculativeActionSettingsStore } from "../src/settings-store.ts";

const directories = temporaryDirectories("pi-spec-settings-");

afterEach(directories.dispose);

describe("extension-owned speculative settings", () => {
	it("persists only the project overlay while preserving global inheritance", async () => {
		const { root, agent, cwd } = await fixture();
		const store = new SpeculativeActionSettingsStore(cwd, agent);
		await store.load();
		store.setEffective({ enabled: true, tools: ["read", "ls"], candidateLimit: 4, patternAware: { beamWidth: 2, enabled: true } });
		await store.flush();
		store.setScope("project");
		store.setEffective({ enabled: true, tools: ["read", "ls"], candidateLimit: 2, patternAware: { enabled: true, beamWidth: 5 } });
		await store.flush();
		expect(JSON.parse(await readFile(path.join(cwd, ".pi", "speculative-action.json"), "utf8"))).toEqual({
			candidateLimit: 2,
			patternAware: { beamWidth: 5 },
		});

		const reloaded = new SpeculativeActionSettingsStore(cwd, agent);
		await reloaded.load();
		expect(reloaded.scope).toBe("project");
		expect(reloaded.effective()).toMatchObject({
			enabled: true,
			candidateLimit: 2,
			patternAware: { enabled: true, beamWidth: 5 },
		});
		await expect(readFile(path.join(root, ".pi", "settings.json"), "utf8")).rejects.toMatchObject({ code: "ENOENT" });
	});

	it("uses explicit tombstones for project removal and clears only the selected layer", async () => {
		const { agent, cwd } = await fixture();
		const store = new SpeculativeActionSettingsStore(cwd, agent);
		await store.load();
		store.setEffective({ enabled: true, candidateLimit: 6, draftModel: "openai/draft" });
		await store.flush();
		store.setScope("project");
		store.setEffective({ enabled: false, candidateLimit: 6 });
		await store.flush();
		expect(store.editable()).toEqual({ enabled: false, candidateLimit: 6 });
		expect(store.editable("global")).toEqual({ enabled: true, candidateLimit: 6, draftModel: "openai/draft" });
		expect(JSON.parse(await readFile(path.join(cwd, ".pi", "speculative-action.json"), "utf8"))).toEqual({
			enabled: false,
			draftModel: null,
		});
		expect(store.effective()).toEqual({ enabled: false, candidateLimit: 6 });
		store.clear();
		await store.flush();
		expect(store.effective()).toMatchObject({ enabled: true, candidateLimit: 6, draftModel: "openai/draft" });
	});

	it("recovers from malformed input and failed publication without poisoning future writes", async () => {
		const { agent, cwd } = await fixture();
		const target = path.join(agent, "speculative-action.json");
		await writeFile(target, "{broken", "utf8");
		const store = new SpeculativeActionSettingsStore(cwd, agent);
		await expect(store.load()).resolves.toBeUndefined();
		expect(store.effective()).toBeUndefined();
		await rm(target);
		await mkdir(target);
		store.setEffective({ enabled: false });
		await expect(store.flush()).rejects.toThrow();
		await rm(target, { recursive: true });
		store.setEffective({ enabled: true });
		await store.flush();
		expect(JSON.parse(await readFile(target, "utf8"))).toEqual({ enabled: true });
	});
});

async function fixture() {
	const root = await directories.create();
	const agent = path.join(root, "agent");
	const cwd = path.join(root, "workspace");
	await Promise.all([mkdir(agent, { recursive: true }), mkdir(cwd, { recursive: true })]);
	return { root, agent, cwd };
}

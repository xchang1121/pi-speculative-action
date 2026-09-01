import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { discoverAndLoadExtensions } from "@earendil-works/pi-coding-agent";
import { describe, expect, test } from "vitest";
import * as coreApi from "../src/core.ts";
import * as packageApi from "../src/index.ts";
import * as processReuseApi from "../src/process-reuse.ts";
import {
	acquirePatternAwareStore,
	captureResourceVersion,
	createSpeculativeActionExtension,
	createSpeculativeActionHost,
	createLinuxProcessExecutionWorld,
	createWorkspaceSandbox,
	EffectTransactionCoordinator,
	ExecutionWorldRouter,
	makeSpeculativeActionRuntime,
	prepareSandboxWorkspace,
	ProcessReusePlanner,
	ProcessExecutionCoordinator,
	ProvenanceCertificateStore,
	qualifyWorkspaceSandboxDriver,
	releaseResourceVersion,
	ToolExecutionGateway,
	WorkspaceSandboxService,
} from "../src/index.ts";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

describe("speculative action package boundary", () => {
	test("exports runtime, learning, resource, and file-mutation entry points", () => {
		for (const exported of [
			makeSpeculativeActionRuntime,
			createSpeculativeActionHost,
			createSpeculativeActionExtension,
			createLinuxProcessExecutionWorld,
			acquirePatternAwareStore,
			captureResourceVersion,
			releaseResourceVersion,
			createWorkspaceSandbox,
			prepareSandboxWorkspace,
			qualifyWorkspaceSandboxDriver,
			EffectTransactionCoordinator,
			ExecutionWorldRouter,
			ToolExecutionGateway,
			ProcessReusePlanner,
			ProcessExecutionCoordinator,
			ProvenanceCertificateStore,
			WorkspaceSandboxService,
		]) {
			expect(exported).toBeTypeOf("function");
		}
		for (const internal of [
			"ActorAction",
			"CandidateExecution",
			"PlanRuntime",
			"PostSettlementQueue",
			"ResultCache",
			"SourceGeneration",
			"SpeculationScheduler",
		]) {
			expect(packageApi).not.toHaveProperty(internal);
		}
	});

	test("keeps the root entry as a compatibility aggregate of the narrow APIs", () => {
		for (const api of [coreApi, processReuseApi]) {
			for (const [name, exported] of Object.entries(api)) expect(packageApi[name]).toBe(exported);
		}
	});

	test("keeps source implementations and Pi dependencies outside the host-neutral runtime", async () => {
		const core = await readModuleClosure(path.join(packageRoot, "src", "core.ts"));
		const processReuse = await readModuleClosure(path.join(packageRoot, "src", "process-reuse.ts"));
		expect(core).not.toMatch(/pattern-aware|@earendil-works\/pi-/);
		expect(processReuse).not.toMatch(/@earendil-works\/pi-/);
		expect(coreApi.makeSpeculativeActionRuntime).toBeTypeOf("function");
		expect(coreApi).not.toHaveProperty("createSpeculativeActionHost");
		expect(coreApi).not.toHaveProperty("PatternAwareStore");
		expect(processReuseApi.ProcessReusePlanner).toBeTypeOf("function");
		expect(processReuseApi).not.toHaveProperty("makeSpeculativeActionRuntime");
	});

	test("declares a source-loadable Pi package with only public host peers", async () => {
		const manifest = JSON.parse(await fs.readFile(path.join(packageRoot, "package.json"), "utf8")) as {
			exports: Record<string, unknown>;
			files: string[];
			repository?: { type?: string; url?: string; directory?: string };
			scripts?: Record<string, string>;
			pi?: { extensions?: string[] };
			peerDependencies?: Record<string, string>;
		};
		expect(Object.keys(manifest.exports).sort()).toEqual([
			".",
			"./core",
			"./extension",
			"./package.json",
			"./pattern-aware",
			"./process-reuse",
		]);
		expect(manifest.files).toEqual(["dist", "src", "README.md", "README-CN.md", "CHANGELOG.md", "LICENSE"]);
		expect(manifest.scripts?.build).toBe(
			"shx rm -rf dist && tsc -p tsconfig.build.json && shx cp src/process-dispatcher.mjs dist/process-dispatcher.mjs && shx cp src/process-namespace-launcher.mjs dist/process-namespace-launcher.mjs",
		);
		expect(Object.keys(manifest.scripts ?? {})).not.toEqual(
			expect.arrayContaining(["build:native", "build:worker", "generate:native-manifest", "smoke:native"]),
		);
		await expect(fs.stat(path.join(packageRoot, "native", "sandbox"))).rejects.toThrow();
		await expect(fs.stat(path.join(packageRoot, "native", "worker"))).rejects.toThrow();
		for (const removed of ["container-sandbox.ts", "native-sandbox.ts", "oci-setup.ts"]) {
			await expect(fs.stat(path.join(packageRoot, "src", removed))).rejects.toThrow();
		}
		expect(manifest.pi?.extensions).toEqual(["./src/extension.ts"]);
		expect(manifest.peerDependencies).toMatchObject({
			"@earendil-works/pi-agent-core": "*",
			"@earendil-works/pi-ai": "*",
			"@earendil-works/pi-coding-agent": "*",
		});
		expect(manifest.repository).toEqual({
			type: "git",
			url: "git+https://github.com/xchang1121/pi.git",
		});
	});

	test("does not resolve implementation or tests through a Pi source checkout", async () => {
		const source = await readTypeScript(path.join(packageRoot, "src"));
		const buildConfig = await fs.readFile(path.join(packageRoot, "tsconfig.build.json"), "utf8");
		const testConfig = await fs.readFile(path.join(packageRoot, "vitest.config.ts"), "utf8");

		expect(source).not.toMatch(/@earendil-works\/pi-[^"']+\/src(?:\/|["'])/);
		expect(source).not.toMatch(/(?:^|["'])(?:\.\.\/){2,}/m);
		expect(buildConfig).not.toMatch(/\.\.\/(?:agent|ai|coding-agent)|"paths"/);
		expect(testConfig).not.toMatch(/\.\.\/(?:agent|ai|telemetry)|alias/);
	});

	test("loads from its own package manifest through Pi's public extension loader", async () => {
		const temporaryRoot = await fs.mkdtemp(path.join(os.tmpdir(), "pi-speculative-package-"));
		const cwd = path.join(temporaryRoot, "workspace");
		const agentDir = path.join(temporaryRoot, "agent");
		await Promise.all([fs.mkdir(cwd), fs.mkdir(agentDir)]);
		try {
			const loaded = await discoverAndLoadExtensions([packageRoot], cwd, agentDir);
			expect(loaded.errors).toEqual([]);
			expect(loaded.extensions).toHaveLength(1);
		} finally {
			await fs.rm(temporaryRoot, { recursive: true, force: true });
		}
	});
});

async function readTypeScript(directory: string): Promise<string> {
	const entries = await fs.readdir(directory, { withFileTypes: true });
	const contents = await Promise.all(
		entries.map(async (entry) => {
			const target = path.join(directory, entry.name);
			if (entry.isDirectory()) return readTypeScript(target);
			return entry.isFile() && entry.name.endsWith(".ts") ? fs.readFile(target, "utf8") : "";
		}),
	);
	return contents.join("\n");
}

async function readModuleClosure(entry: string, seen = new Set<string>()): Promise<string> {
	const target = path.resolve(entry);
	if (seen.has(target)) return "";
	seen.add(target);
	const source = await fs.readFile(target, "utf8");
	const dependencies: string[] = [];
	for (const match of source.matchAll(/(?:from\s+|import\s*\()\s*["'](\.[^"']+)["']/g)) {
		const specifier = match[1];
		if (!specifier) continue;
		const dependency = path.resolve(path.dirname(target), specifier);
		if (dependency.startsWith(path.join(packageRoot, "src"))) dependencies.push(dependency);
	}
	return [source, ...(await Promise.all(dependencies.map((dependency) => readModuleClosure(dependency, seen))))].join(
		"\n",
	);
}

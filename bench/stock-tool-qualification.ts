import assert from "node:assert/strict";
import { lstat, mkdir, mkdtemp, readFile, readdir, readlink, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import {
	createEditToolDefinition, createFindToolDefinition, createGrepToolDefinition,
	createLsToolDefinition, createReadToolDefinition, createWriteToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { PI_ACTION_SEMANTICS } from "../src/action-semantics.ts";
import { createResourceSnapshotExecutionWorld, type SpeculativeAgentExecutionWorld, type SpeculativeToolExecutionContext } from "../src/agent-execution-world.ts";
import { isPoisonedEffectCommit } from "../src/effect-transaction.ts";
import { slash } from "../src/path-utils.ts";
import { withPiProjectionCoverage } from "../src/pi-read-projection.ts";
import { PI_OPERATION_TOOLS, resolvePiToolInvocation } from "../src/pi-tool-invocation.ts";
import { stableValueHash } from "../src/stable-value-hash.ts";
import { runThinkThreadTool } from "../src/thinkthread/tool-runner.ts";
import {
	decodeThinkThreadToolRunnerResponse, encodeThinkThreadToolRunnerResponse, THINKTHREAD_TOOL_RUNNER_VERSION,
	type ThinkThreadToolName,
} from "../src/thinkthread/tool-runner-protocol.ts";
import { ToolExecutionGateway } from "../src/tool-execution-gateway.ts";
import { toolErrorSettlement, type ToolSettlement } from "../src/tool-settlement.ts";
import { createWorkspaceSandbox } from "../src/workspace-sandbox.ts";

export const STOCK_TOOL_CASES = [
	["read", { path: "notes.txt" }],
	["grep", { pattern: "alpha", path: "." }],
	["find", { pattern: "notes.txt", path: "." }],
	["ls", { path: "." }],
	["write", { path: "generated.txt", content: "generated\n" }],
	["edit", { path: "notes.txt", edits: [{ oldText: "beta", newText: "gamma" }] }],
] as const;

/** Owns one fixture and compares all routes at the same cwd/path, without output path rewriting.
 * A supplied primary world is also owned/disposed here. Omission tests only the local wire runner.
 */
export async function qualifyStockTool(
	name: ThinkThreadToolName,
	input: (typeof STOCK_TOOL_CASES)[number][1],
	primary?: { readonly cwd: string; readonly world: SpeculativeAgentExecutionWorld },
) {
	const fixtureParent = path.resolve(primary?.cwd ?? os.tmpdir());
	const root = await mkdtemp(path.join(fixtureParent, "pi-tool-qualification-"));
	assert.equal(path.dirname(root), fixtureParent);
	const cwd = primary?.cwd ?? root;
	const args = { ...input, path: slash(path.join(path.relative(cwd, root), input.path)) };
	const invocation = resolvePiToolInvocation(name, args, { cwd, environment: {} });
	const action = PI_ACTION_SEMANTICS.buildKey(name, args, cwd, "", invocation
		? { fingerprint: stableValueHash(invocation.identity), context: invocation } : undefined);
	assert.ok(action);
	const semantics = PI_ACTION_SEMANTICS.definition(name)!;
	const tools = [
		createReadToolDefinition(cwd), createGrepToolDefinition(cwd), createFindToolDefinition(cwd),
		createLsToolDefinition(cwd), createWriteToolDefinition(cwd), createEditToolDefinition(cwd),
	];
	const definition = tools.find((tool) => tool.name === name)!;
	const tool: AgentTool = {
		...definition,
		execute: (callID, value, signal, onUpdate) =>
			definition.execute(callID, value as never, signal, onUpdate as never, undefined as never),
	};
	const context = { cwd, tool, toolName: name, args, action, callID: `qualify-${name}`, signal: new AbortController().signal,
		executionScope: { sessionID: root, turnID: name } };
	const operation = { tool: name, input: args, action, callID: context.callID };
	let primaryEnabled = false;
	const fallback = createWorkspaceSandbox({ driver: "git" });
	const resources = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, { tools: PI_OPERATION_TOOLS.resources, maxBytes: () => 1024 * 1024 });
	const gateway = new ToolExecutionGateway<SpeculativeToolExecutionContext, ToolSettlement>([
		...(primary ? [primary.world] : []), fallback, resources,
	], (id) => id !== primary?.world.id || primaryEnabled);
	const actor = async (): Promise<ToolSettlement> => {
		try {
			return { result: withPiProjectionCoverage(name, args, await tool.execute(context.callID, args)), isError: false };
		} catch (error) { return toolErrorSettlement(error); }
	};
	const reset = async () => {
		// Only this mkdtemp-owned fixture is reset, never the caller's cwd.
		await rm(root, { recursive: true, force: true });
		await mkdir(path.join(root, "nested"), { recursive: true });
		await writeFile(path.join(root, "notes.txt"), "alpha\nbeta\n");
		await writeFile(path.join(root, "nested", "todo.txt"), "beta\n");
	};
	let poisoned = false;
	try {
		await reset();
		const initial = await workspaceState(root);
		const baselineStarted = performance.now();
		const actorOutput = await actor();
		const actorBaselineMs = performance.now() - baselineStarted;
		const baseline = wire(actorOutput);
		assert.equal(baseline.isError, false, `${name}: Actor baseline failed`);
		const expected = await workspaceState(root);
		const executeRoute = async (requirePrimary: boolean) => {
			await reset();
			primaryEnabled = requirePrimary;
			const preparedAt = performance.now();
			const route = await gateway.resolve({ operation, effect: semantics.effect, requirements: semantics.requirements }, { cwd });
			const preparationMs = performance.now() - preparedAt;
			if (requirePrimary) assert.equal(route?.backend, primary?.world.id, `${name}: primary fell back; not a Runtime pass`);
			else assert.equal(route?.backend, semantics.effect === "workspace_mutation" ? fallback.id : invocation?.filesystem ? resources.id : undefined,
				`${name}: native route differs from its stock fallback`);
			const started = performance.now();
			let output: ToolSettlement;
			let executionMs: number;
			let adoptionMs: number | null = null;
			if (!route) {
				output = await actor();
				executionMs = performance.now() - started;
			} else {
				const branch = await gateway.executeSpeculative(operation, route, () => ({
					...context, action: { ...action, executionFingerprint: route.fingerprint },
				}));
				executionMs = performance.now() - started;
				try {
					assert.deepEqual(await workspaceState(root), initial, `${name}: speculative effects leaked before adoption`);
					const arrived = performance.now();
					assert.equal((await branch.validate()).status, "valid", `${name}: fresh candidate rejected`);
					output = await branch.commit();
					adoptionMs = performance.now() - arrived;
				} finally { await branch.dispose(); }
			}
			assert.deepEqual(wire(output), baseline, `${name}: output differs`);
			assert.deepEqual(await workspaceState(root), expected, `${name}: adopted file effects differ`);
			return { route: route?.backend ?? "Actor only", preparationMs, executionMs, adoptionMs,
				actorReadyAdoptionSpeedup: adoptionMs === null ? null : actorBaselineMs / adoptionMs };
		};
		const native = await executeRoute(false);
		if (primary) {
			const isolated = await executeRoute(true);
			return { tool: name, evidence: "Runtime execution and adoption", actorBaselineMs, native, isolated };
		}
		await reset();
		const output = wire(await runThinkThreadTool({
			version: THINKTHREAD_TOOL_RUNNER_VERSION, tool: name, args, callID: context.callID, autoResizeImages: true,
		}, cwd));
		assert.deepEqual(output, baseline, `${name}: local wire runner output differs`);
		assert.deepEqual(await workspaceState(root), expected, `${name}: local wire runner effects differ`);
		return { tool: name, evidence: "Local wire runner only", actorBaselineMs, native };
	} catch (error) {
		poisoned = isPoisonedEffectCommit(error);
		if (poisoned) console.error(`Indeterminate adoption: retain fixture without further writes at ${root}`);
		throw error;
	} finally {
		try { await gateway.dispose(); }
		finally { if (!poisoned) await rm(root, { recursive: true, force: true }); }
	}
}

function wire(output: ToolSettlement): ToolSettlement {
	return decodeThinkThreadToolRunnerResponse(Buffer.from(encodeThinkThreadToolRunnerResponse(output)));
}

async function workspaceState(root: string, relative = ""): Promise<unknown[]> {
	const result: unknown[] = [];
	for (const name of (await readdir(path.join(root, relative))).sort()) {
		const file = path.join(relative, name);
		const absolute = path.join(root, file);
		const info = await lstat(absolute);
		assert.ok(info.isFile() || info.isDirectory() || info.isSymbolicLink(), `Unexpected special file: ${file}`);
		const kind = info.isSymbolicLink() ? "symlink" : info.isDirectory() ? "directory" : "file";
		result.push({ path: slash(file), kind, mode: info.mode & 0o777,
			content: kind === "file" ? (await readFile(absolute)).toString("base64")
				: kind === "symlink" ? await readlink(absolute) : null });
		if (kind === "directory") result.push(...await workspaceState(root, file));
	}
	return result;
}

import { execFile } from "node:child_process";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { Type } from "typebox";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import { afterEach, expect, test } from "vitest";
import { defineSpeculativeTool, subcomputeEnvironment } from "../src/index.ts";
import { systemHost } from "./agent-system-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";

const directories = temporaryDirectories("pi-subcompute-test-");
afterEach(directories.dispose);
const exec = promisify(execFile);
const javascript = fileURLToPath(new URL("./subcompute-client.mjs", import.meta.url));
const python = fileURLToPath(new URL("./subcompute-client.py", import.meta.url));

test("public tool calls share binary steps across processes, languages and host restarts", async () => {
	const cwd = await directories.create();
	const storeRoot = await directories.create();
	await writeFile(path.join(cwd, "input.bin"), Buffer.from([0, 255, 12, 72]));
	let tsBuilds = 0;
	const inProcess = defineSpeculativeTool({
		name: "typescript_transform",
		label: "TypeScript transform",
		description: "Share the declared transform",
		parameters: Type.Object({ mode: Type.String() }),
		version: "1",
		effect: "actor",
		steps: {},
		execute: async ({ computeStep }, args) => {
			const value = await computeStep(
				{
					namespace: "cross-language.reverse",
					version: "1",
					key: "shared",
					inputResources: [{ path: "input.bin", scope: "content" }],
					envKeys: ["TRANSFORM_MODE"],
					environment: { TRANSFORM_MODE: args.mode },
				},
				async (view) => {
					tsBuilds++;
					return Buffer.from(await view.readFile(path.join(cwd, "input.bin"))).reverse();
				},
			);
			return {
				content: [],
				details: { encoding: "base64", value: value.toString("base64") },
			};
		},
	});
	const tool = {
		name: "external_transform",
		label: "External transform",
		description: "Run an explicit external step",
		parameters: Type.Object({ language: Type.String(), mode: Type.String() }),
		execute: async (_id: string, args: unknown) => {
			const input = args as { language: string; mode: string };
			const environment = await subcomputeEnvironment();
			const program =
				input.language === "python"
					? process.platform === "win32"
						? "python"
						: "python3"
					: process.execPath;
			const script = input.language === "python" ? python : javascript;
			const result = await exec(program, [script], {
				cwd,
				env: { ...process.env, ...environment, TRANSFORM_MODE: input.mode },
				timeout: 15000,
			});
			return {
				content: [{ type: "text" as const, text: result.stdout }],
				details: JSON.parse(result.stdout),
			};
		},
	};
	const open = () =>
		systemHost(
			cwd,
			[tool, inProcess as unknown as AgentTool],
			{
				executionWorlds: [],
				resolveInvocation: () => undefined,
				config: { storage: { root: storeRoot } },
			},
			{ enabled: false },
		);
	let host = open();
	try {
		await host.start();
		const cold = await host.execute(tool.name, {
			language: "javascript",
			mode: "reverse",
		});
		expect(cold.details).toEqual({
			hit: false,
			value: {
				encoding: "base64",
				value: Buffer.from([72, 12, 255, 0]).toString("base64"),
			},
		});
		const warm = await host.execute(tool.name, {
			language: "python",
			mode: "reverse",
		});
		expect(warm.details).toEqual({ ...(cold.details as object), hit: true });
		expect((await host.execute(inProcess.name, { mode: "reverse" })).details).toEqual(
			(cold.details as { value: unknown }).value,
		);
		expect(tsBuilds).toBe(0);
		expect(host.actors.at(-1)?.computation?.subcomputationHits).toBeGreaterThan(0);
		expect(host.actors.at(-1)?.computation?.reusedComputeMs).toBeGreaterThan(0);
		await host.finish(true);
		await host.dispose();
		host = open();
		await host.start();
		const restored = await host.execute(tool.name, {
			language: "javascript",
			mode: "reverse",
		});
		expect(restored.details).toEqual(warm.details);
		expect(host.actors.at(-1)?.computation?.reusedComputeMs).toBeGreaterThan(0);
		await writeFile(path.join(cwd, "input.bin"), Buffer.from([1, 2, 3]));
		const changed = await host.execute(tool.name, {
			language: "python",
			mode: "reverse",
		});
		expect(changed.details).toEqual({
			hit: false,
			value: { encoding: "base64", value: "AwIB" },
		});
		const environment = await host.execute(tool.name, {
			language: "javascript",
			mode: "other",
		});
		expect(environment.details).toMatchObject({ hit: false });
		const warmEnvironment = await host.execute(tool.name, {
			language: "python",
			mode: "other",
		});
		expect(warmEnvironment.details).toMatchObject({ hit: true });
		await host.execute(inProcess.name, { mode: "typescript-produced" });
		expect(tsBuilds).toBe(1);
		expect(
			(
				await host.execute(tool.name, {
					language: "python",
					mode: "typescript-produced",
				})
			).details,
		).toMatchObject({ hit: true });
		const database = new DatabaseSync(path.join(storeRoot, "metadata.sqlite"), {
			readOnly: true,
		});
		try {
			expect(
				Number(database.prepare("SELECT COUNT(*) AS count FROM input_versions").get()!.count),
			).toBeGreaterThan(0);
		} finally {
			database.close();
		}
	} finally {
		await host.dispose();
	}
}, 60000);

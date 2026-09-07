import { spawnSync } from "node:child_process";
import path from "node:path";
import { getShellConfig } from "@earendil-works/pi-coding-agent";
import { expect, it } from "vitest";
import { actionKeyCovers, actionKeyMatch, PI_ACTION_SEMANTICS, RESOURCE_INPUT_ACTION_KEY_PROJECTOR } from "../src/action-semantics.ts";

const shell = (() => { try { return getShellConfig(); } catch { return undefined; } })();
const nativeBash = shell && /(^|[\\/])bash(?:\.exe)?$/i.test(shell.shell) && shell.commandTransport !== "stdin";

it.skipIf(!nativeBash).each([
	["output", '() { printf "tail arguments: %s\\n" "$*"; }', ["tail arguments: -n 3\n", "tail arguments: -n 2\n"], [0, 0]],
	["exit status", '() { printf same; if test "$2" = 3; then return 0; else return 7; fi; }', ["same", "same"], [0, 7]],
] as const)("does not infer Bash equivalence from a suffix when the resolved command changes %s", (_kind, definition, stdout, status) => {
	const commands = ["printf data 2>&1 | tail -n 3", "printf data 2>&1 | tail -n 2"];
	const environment = { ...process.env, BASH_ENV: "", ENV: "", "BASH_FUNC_tail%%": definition };
	const results = commands.map((command) => spawnSync(shell!.shell, [...shell!.args, command], {
		env: environment, encoding: "utf8", windowsHide: true, timeout: 3_000,
	}));
	expect(results.map((result) => result.error)).toEqual([undefined, undefined]);
	expect(results.map((result) => result.stdout)).toEqual(stdout);
	expect(results.map((result) => result.status)).toEqual(status);
	const [producer, actor] = commands.map((command) => PI_ACTION_SEMANTICS.buildKey("bash", { command }, path.resolve("."), "schema", {
		fingerprint: "same-shell-and-environment", context: { process: { command } },
	})!);
	const projectors = PI_ACTION_SEMANTICS.projectors();
	expect(actionKeyMatch(producer!, producer!, projectors)?.kind).toBe("exact");
	// Sealed-input lookup is only a hint to re-execute, never permission to project these outputs.
	const relation = actionKeyMatch(producer!, actor!, projectors);
	if (relation) expect(relation).toMatchObject({ kind: "projected", projector: RESOURCE_INPUT_ACTION_KEY_PROJECTOR.id });
	expect(actionKeyCovers(producer!, actor!, projectors)).toBe(false);
});

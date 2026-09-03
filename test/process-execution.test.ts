import { describe, expect, test } from "vitest";
import { ProcessExecutionCoordinator } from "../src/process-execution.ts";

describe("ProcessExecutionCoordinator", () => {
	test("routes only the dynamic execution scope and restores the host outlet", async () => {
		const calls: string[] = [];
		const coordinator = new ProcessExecutionCoordinator({
			execute: async (request) => {
				calls.push(`host:${request.command}`);
				return { exitCode: 0 };
			},
		});
		const invoke = (command: string) =>
			coordinator.operations.exec(command, "/work", {
				onData: () => undefined,
				env: { PATH: "/bin" },
			});

		await invoke("outside-before");
		await coordinator.runWith(
			{
				execute: async (request) => {
					calls.push(`isolated:${request.command}`);
					return { exitCode: 0 };
				},
			},
			async () => {
				await Promise.resolve();
				await invoke("inside");
			},
		);
		await invoke("outside-after");

		expect(calls).toEqual(["host:outside-before", "isolated:inside", "host:outside-after"]);
	});

	test("keeps the raw Actor outlet when reuse is disabled", async () => {
		const calls: string[] = [];
		let enabled = false;
		const executor = (label: string) => ({
			execute: async () => {
				calls.push(label);
				return { exitCode: 0 };
			},
		});
		const coordinator = new ProcessExecutionCoordinator(executor("raw"), {
			enabled: () => enabled,
			executor: executor("reuse"),
		});
		const invoke = () => coordinator.operations.exec("true", "/work", { onData: () => undefined });

		await invoke();
		enabled = true;
		await invoke();
		await coordinator.runWith(executor("world"), invoke);
		enabled = false;
		await invoke();

		expect(calls).toEqual(["raw", "reuse", "world", "raw"]);
	});
});

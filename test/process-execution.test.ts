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
});

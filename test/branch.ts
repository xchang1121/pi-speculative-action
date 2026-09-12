import type { WorldBranch } from "../src/execution-world.ts";

export function testBranch<Output>(
	output: Output,
	options: Partial<Pick<WorldBranch<Output>, "backend" | "checkpoint" | "resources" | "executionMetrics" | "validate">> & {
		readonly executionFingerprint?: string;
		readonly onCommit?: () => void;
		readonly onDispose?: () => void;
	} = {},
): WorldBranch<Output> {
	return {
		output,
		backend: options.backend ?? "test",
		...(options.checkpoint ? { checkpoint: options.checkpoint } : {}),
		resources: options.resources ?? [],
		capturedBytes: 0,
		executionMetrics: options.executionMetrics ?? {},
		compatibility: {
			status: "compatible",
			backend: options.backend ?? "test",
			executionFingerprint: options.executionFingerprint ?? "",
		},
		...(options.validate ? { validate: options.validate } : {}),
		commit: async () => {
			options.onCommit?.();
			return output;
		},
		dispose: () => options.onDispose?.(),
	};
}

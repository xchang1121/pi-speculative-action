import { configDefaults, defineConfig } from "vitest/config";

// A sibling suite creating fixtures in the observed home directory can invalidate a live process proof.
const domainTests = [
	"bash-closure",
	"extension.system",
	"held-exec",
	"host-closure",
	"linux-environment",
	"linux-reuse-chain",
	"process-provenance.system",
].map((suite) => `test/${suite}.test.ts`);

export default defineConfig({
	test: {
		globals: true,
		environment: "node",
		testTimeout: 30000,
		reporters: process.env.GITHUB_ACTIONS ? ["dot", "github-actions"] : ["dot"],
		silent: "passed-only",
		projects: process.platform === "linux"
			? [
					{
						extends: true,
						test: { name: "independent", exclude: [...configDefaults.exclude, ...domainTests] },
					},
					{
						extends: true,
						test: {
							name: "execution-domain",
							include: domainTests,
							fileParallelism: false,
							sequence: { groupOrder: 1 },
						},
					},
				]
			: undefined,
	},
});

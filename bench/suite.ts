import { spawn } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { summarizeSuite, type SuiteBenchmarkRun, type SuiteBenchmarkSummary } from "./suite-report.ts";

type SuiteFile = Readonly<Record<string, readonly string[]>>;

interface StoredResult {
	readonly metadata: { readonly implementationCommit: string };
	readonly summary: SuiteBenchmarkSummary;
}

const directory = path.dirname(fileURLToPath(import.meta.url));
const parsed = parseSuiteArguments(process.argv.slice(2));
const suites = validateSuites(JSON.parse(await readFile(path.join(directory, "suite.json"), "utf8")));
const instances = suites[parsed.suite];
if (!instances) throw new Error(`Unknown suite ${parsed.suite}; expected ${Object.keys(suites).join(", ")}`);
for (const option of ["--instance", "--output", "--prepare-only"]) {
	if (hasOption(parsed.forwarded, option)) throw new Error(`${option} is controlled by the suite runner`);
}

const label = optionValue(parsed.forwarded, "--label") ?? "baseline";
const outputRoot = path.resolve(
	parsed.outputRoot ?? path.join(os.tmpdir(), "pi-speculative-ablation-suites", `${safeName(label)}-${Date.now()}`),
);
const runner = path.join(directory, "run.ts");
const tsx = fileURLToPath(import.meta.resolve("tsx/cli"));
const runs: SuiteBenchmarkRun[] = [];
await mkdir(outputRoot, { recursive: true });

try {
	for (let repeat = 1; repeat <= parsed.repeats; repeat++) {
		for (const instance of instances) {
			const output = path.join(outputRoot, `repeat-${repeat}`, `${safeName(instance)}.json`);
			let run: SuiteBenchmarkRun = { instance, repeat, output };
			try {
				await mkdir(path.dirname(output), { recursive: true });
				await writeFile(output, "", { flag: "wx" }); // Only this attempt may supply the result, including after failure.
				const failed = await execute(process.execPath, [tsx, runner, ...parsed.forwarded, "--instance", instance, "--output", output])
					.then(() => undefined, (error: unknown) => ({ error }));
				try {
					const result = validateResult(JSON.parse(await readFile(output, "utf8")), output);
					run = { ...run, implementationCommit: result.metadata.implementationCommit, summary: result.summary };
				} catch (error) { if (!failed) throw error; }
				if (failed) throw failed.error;
				const errors = Object.values(run.summary?.benchmarkErrors ?? {});
				if (errors.length) throw new Error(`Benchmark failed: ${errors.join("; ")}`);
			} catch (error) {
				run = { ...run, error: String(error) };
				throw error;
			} finally {
				runs.push(run);
			}
		}
	}
} finally {
	const report = {
		metadata: {
			suite: parsed.suite, label, repeats: parsed.repeats, instances, forwardedArguments: parsed.forwarded,
		},
		...summarizeSuite(runs),
		runOutputs: runs.map(({ instance, repeat, output }) => ({ instance, repeat, output })),
	};
	const reportFile = path.join(outputRoot, "suite-result.json");
	await writeFile(reportFile, `${JSON.stringify(report, null, 2)}\n`, "utf8");
	process.stdout.write(`${JSON.stringify({ output: reportFile, ...report }, null, 2)}\n`);
}

function parseSuiteArguments(args: readonly string[]) {
	const forwarded = [...args];
	const suite = takeOption(forwarded, "--suite") ?? "swe_diverse";
	const repeatsValue = takeOption(forwarded, "--repeats") ?? "1";
	const repeats = Number(repeatsValue);
	if (!Number.isSafeInteger(repeats) || repeats <= 0) throw new Error("--repeats must be a positive integer");
	const outputRoot = takeOption(forwarded, "--output-root");
	return { suite, repeats, outputRoot, forwarded };
}

function takeOption(args: string[], name: string): string | undefined {
	const equals = args.findIndex((argument) => argument.startsWith(`${name}=`));
	if (equals >= 0) return args.splice(equals, 1)[0]!.slice(name.length + 1);
	const index = args.indexOf(name);
	if (index < 0) return undefined;
	const value = args[index + 1];
	if (!value || value.startsWith("--")) throw new Error(`${name} requires a value`);
	args.splice(index, 2);
	return value;
}

function optionValue(args: readonly string[], name: string): string | undefined {
	const equals = args.find((argument) => argument.startsWith(`${name}=`));
	if (equals) return equals.slice(name.length + 1);
	const index = args.indexOf(name);
	return index >= 0 ? args[index + 1] : undefined;
}

function hasOption(args: readonly string[], name: string): boolean {
	return args.includes(name) || args.some((argument) => argument.startsWith(`${name}=`));
}

function validateSuites(value: unknown): SuiteFile {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("suite.json must be an object");
	const suites: Record<string, readonly string[]> = {};
	for (const [name, instances] of Object.entries(value)) {
		if (!Array.isArray(instances) || !instances.length || !instances.every((item) => typeof item === "string")) {
			throw new Error(`Suite ${name} must contain instance names`);
		}
		if (new Set(instances).size !== instances.length) throw new Error(`Suite ${name} contains duplicate instances`);
		suites[name] = instances;
	}
	return suites;
}

function validateResult(value: unknown, file: string): StoredResult {
	if (!value || typeof value !== "object") throw new Error(`Invalid benchmark result ${file}`);
	const result = value as Partial<StoredResult>;
	if (!result.metadata?.implementationCommit || !result.summary || typeof result.summary.patchCandidate !== "boolean") {
		throw new Error(`Incomplete benchmark result ${file}`);
	}
	return result as StoredResult;
}

function execute(file: string, args: readonly string[]): Promise<void> {
	return new Promise((resolve, reject) => {
		const child = spawn(file, args, { stdio: "inherit", env: process.env });
		child.once("error", reject);
		child.once("exit", (code, signal) => {
			if (code === 0) resolve();
			else reject(new Error(`Benchmark runner exited with ${signal ?? code}`));
		});
	});
}

function safeName(value: string): string {
	return value.replaceAll(/[^A-Za-z0-9._-]/g, "_");
}

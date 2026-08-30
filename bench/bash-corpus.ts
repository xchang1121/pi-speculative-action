import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { analyzeBashCorpus, bashCallsFromSWEbenchSummary } from "./bash-corpus-analysis.ts";

const { values } = parseArgs({
	options: {
		summary: { type: "string" },
		cwd: { type: "string", default: "/testbed" },
		"covering-buckets": { type: "string", default: "20,40,80,200,500,1000,2000" },
	},
	strict: true,
});

if (!values.summary) {
	throw new Error(
		"Usage: npm run bench:bash-corpus -- --summary <summary.json> [--covering-buckets 20,40,80]",
	);
}

const summary = JSON.parse(await readFile(values.summary, "utf8")) as unknown;
const buckets = (values["covering-buckets"] ?? "")
	.split(",")
	.map((value) => Number(value.trim()))
	.filter((value) => Number.isSafeInteger(value) && value > 0);
const report = analyzeBashCorpus(bashCallsFromSWEbenchSummary(summary), {
	cwd: values.cwd,
	coveringBuckets: buckets,
});
console.log(JSON.stringify(report, undefined, 2));

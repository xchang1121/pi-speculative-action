import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { analyzeTape, type LlmTape } from "./tape-analysis.ts";

const { values } = parseArgs({
	options: {
		tape: { type: "string" },
		"actor-model": { type: "string" },
		"drafter-model": { type: "string" },
	},
	strict: true,
});

if (!values.tape || !values["actor-model"] || !values["drafter-model"]) {
	throw new Error("Usage: npm run bench:tape -- --tape <path> --actor-model <id> --drafter-model <id>");
}

const tape = JSON.parse(await readFile(values.tape, "utf8")) as LlmTape;
console.log(JSON.stringify(analyzeTape(tape, values["actor-model"], values["drafter-model"]), undefined, 2));

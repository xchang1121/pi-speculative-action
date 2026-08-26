import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { SELF_SPECULATION_DEFAULTS } from "../src/self-speculation.ts";
import {
	analyzeTape,
	analyzeTapeDrafterRace,
	analyzeTapeDrafterWidth,
	analyzeTapeForkGate,
	analyzeTapeReprobe,
	type LlmTape,
} from "./tape-analysis.ts";

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
const actionAnalysis = analyzeTape(tape, values["actor-model"], values["drafter-model"]);
console.log(
	JSON.stringify(
		{
			...actionAnalysis,
			forkGate: analyzeTapeForkGate(tape, values["actor-model"], values["drafter-model"], {
				enabled: SELF_SPECULATION_DEFAULTS.forkGateEnabled,
				minSamples: SELF_SPECULATION_DEFAULTS.forkGateMinSamples,
				windowSize: SELF_SPECULATION_DEFAULTS.forkGateWindowSize,
				minNetBenefitMs: SELF_SPECULATION_DEFAULTS.forkGateMinNetBenefitMs,
				probeInterval: SELF_SPECULATION_DEFAULTS.forkGateProbeInterval,
				failureThreshold: SELF_SPECULATION_DEFAULTS.forkGateFailureThreshold,
			}),
			reprobe: analyzeTapeReprobe(tape, values["actor-model"], values["drafter-model"]),
			drafterWidth: analyzeTapeDrafterWidth(tape, values["actor-model"], values["drafter-model"], [1, 2, 3, 8]),
			drafterRace: analyzeTapeDrafterRace(tape, values["actor-model"], values["drafter-model"], 2),
		},
		undefined,
		2,
	),
);

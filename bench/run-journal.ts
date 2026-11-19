import { appendFileSync, createReadStream } from "node:fs";
import { resolve } from "node:path";
import type { SpeculativeActionEvent } from "../src/settlement.ts";

type Event = SpeculativeActionEvent<string>;
type Entry =
	| { topic: "run_start"; instance: string; implementationCommit: string }
	| { topic: "tool_start" | "tool_end"; id: string; tool: string }
	| { topic: "host"; event: Event }
	| { topic: "heartbeat" | "timeout" | "run_end" };

export function openRunJournal(path: string, instance: string, implementationCommit: string) {
	const write = (entry: Entry, flag = "a") => appendFileSync(path,
		JSON.stringify({ at: performance.timeOrigin + performance.now(), ...entry },
			(_, value) => typeof value === "bigint" ? String(value) : value) + "\n", { flag, mode: 0o600 });
	write({ topic: "run_start", instance, implementationCommit }, "wx");
	const heartbeat = setInterval(() => write({ topic: "heartbeat" }), 10_000).unref();
	return { write, close() { clearInterval(heartbeat); } };
}

/** Process complete newline-delimited records. A killed writer may leave a partial final line. */
export async function recoverRun(path: string) {
	const starts = new Map<string, { tool: string; at: number; end?: number }>();
	const actions = new Map<string, Extract<Event, { type: "actor_action" }>>();
	let started: Extract<Entry, { topic: "run_start" }> | undefined;
	let ended = false, timedOut = false, lastAt = 0, buffer = "";
	for await (const chunk of createReadStream(path, { encoding: "utf8" })) {
		buffer += chunk;
		let newline: number;
		while ((newline = buffer.indexOf("\n")) >= 0) {
			const row: Entry & { at: number } = JSON.parse(buffer.slice(0, newline));
			buffer = buffer.slice(newline + 1);
			lastAt = row.at;
			if (row.topic === "run_start") started = row;
			if (row.topic === "run_end") ended = true;
			if (row.topic === "timeout") timedOut = true;
			if (row.topic === "tool_start") starts.set(row.id, { tool: row.tool, at: row.at });
			if (row.topic === "tool_end") {
				const call = starts.get(row.id);
				if (call) call.end = row.at;
			}
			if (row.topic === "host" && row.event.type === "actor_action")
				actions.set(row.event.settlement.actorAction.id, row.event);
		}
	}
	if (!started) throw new Error("Missing benchmark run start");
	const calls = [...starts].map(([id, start]) => {
		const action = actions.get(id);
		const timing = action?.computation;
		return { id, tool: start.tool, completed: start.end !== undefined,
			waitMs: (start.end ?? lastAt) - start.at, toolComputeMs: timing?.toolComputeMs ?? null,
			hiddenComputeMs: timing?.hiddenComputeIncomplete ? null : timing?.hiddenComputeMs ?? null,
			provider: action?.settlement.provider, source: action?.candidate?.source };
	});
	const measured = calls.filter((c) => c.toolComputeMs !== null && c.hiddenComputeMs !== null);
	const toolComputeMs = measured.reduce((n, c) => n + c.toolComputeMs!, 0);
	const hiddenComputeMs = measured.reduce((n, c) => n + c.hiddenComputeMs!, 0);
	return { instance: started.instance, implementationCommit: started.implementationCommit, ended, timedOut,
		starts: calls.length, timedCalls: measured.length, toolComputeMs: measured.length ? toolComputeMs : null,
		hiddenComputeMs: measured.length ? hiddenComputeMs : null, hiddenFraction: toolComputeMs ? hiddenComputeMs / toolComputeMs : null,
		hits: calls.filter((c) => c.provider?.kind === "speculative").length, calls };
}

if (process.argv[1] && import.meta.filename === resolve(process.argv[1]))
	console.log(JSON.stringify(await recoverRun(process.argv[2]!), null, 2));

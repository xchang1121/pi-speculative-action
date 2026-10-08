import path from "node:path";
import type { PatternAwareEvent } from "./pattern-aware.ts";
import { asRecord } from "./stable-json.ts";

export const PATTERN_AWARE_PRESETS = Object.freeze([
	Object.freeze({ id: "reported-files", label: "Reported files", description: "Read files named by the latest tool results that have not been read yet.", defaultOff: false }),
	Object.freeze({ id: "edited-file", label: "Edited files", description: "Read files the Actor just edited or wrote.", defaultOff: false }),
	Object.freeze({ id: "recent-reads", label: "Recent reads", description: "Prepare another read of recently read files.", defaultOff: false }),
	Object.freeze({ id: "recent-command", label: "Recent command", description: "After edits, prepare stale native work from a command the Actor actually ran.", defaultOff: false }),
	Object.freeze({ id: "reported-lines", label: "Reported lines", description: "Read around line numbers reported by tool results.", defaultOff: true }),
	Object.freeze({ id: "companion-files", label: "Companion files", description: "Read an already observed source or test companion near the current file.", defaultOff: true }),
	Object.freeze({ id: "failure-edits", label: "Edits after failure", description: "After a failed call, inspect recently edited files that have not been read since.", defaultOff: true }),
	Object.freeze({ id: "continue-read", label: "Continue reading", description: "Prepare the next page when a successful read reports more lines.", defaultOff: true }),
	Object.freeze({ id: "retry-failed-command", label: "Retry failed command", description: "After an edit to a reported file, prepare a command the Actor previously ran and that failed.", defaultOff: true }),
] as const);

export type PatternAwarePresetID = typeof PATTERN_AWARE_PRESETS[number]["id"];
export type PatternPresetRead = { kind: string; input: { path: string } & Record<string, unknown>; prior: number };

/** Stateless suggestions from observed facts. The caller owns feedback, canonical coverage and admission. */
export function patternPresetReads(
	history: readonly PatternAwareEvent[],
	presets: readonly PatternAwarePresetID[],
	readIdentity: (target: string) => string | undefined,
): PatternPresetRead[] {
	const last = history.at(-1);
	if (!last) return [];
	const batch = history.filter(event => event.turnID === last.turnID && event.sessionID === last.sessionID);
	const reads = history.filter(event => event.tool === "read" && typeof event.input.path === "string");
	const read = new Set(reads.map(event => readIdentity(String(event.input.path))));
	const ranked: PatternPresetRead[] = [];
	// Preserve the original three relations' order, bounds and priors.
	if (presets.includes("edited-file")) for (const event of [...batch].reverse())
		if ((event.tool === "edit" || event.tool === "write") && typeof event.input.path === "string")
			ranked.push({ kind: "edited", input: { path: event.input.path }, prior: 0.35 });
	const listed = batch.filter(event => event.tool !== "read").flatMap(event => [...(event.outputLocations ?? []).map(location => location.path), ...event.outputPaths ?? []]);
	if (presets.includes("reported-files")) for (const [index, target] of [...new Set(listed)].filter(target => !read.has(readIdentity(target))).slice(0, 8).entries())
		ranked.push({ kind: "listed", input: { path: target }, prior: 0.2 / (index + 1) });
	if (presets.includes("recent-reads")) for (const [index, event] of [...reads].reverse().slice(0, 4).entries())
		ranked.push({ kind: "reread", input: { path: String(event.input.path) }, prior: 0.15 / (index + 1) });

	const scoped = history.filter(event => event.sessionID === last.sessionID);
	const successfulReads = scoped.filter(event => event.tool === "read" && event.outcome === "success" && typeof event.input.path === "string");
	const readSince = (identity: string, sequence: number) => successfulReads.some(event => event.sequence > sequence && readIdentity(String(event.input.path)) === identity);
	if (presets.includes("reported-lines")) {
		const lines = new Map<string, number[]>();
		let count = 0;
		for (const event of [...batch].reverse()) for (const location of event.outputLocations ?? []) {
			if (count >= 8 || typeof location.path !== "string" || !positiveInteger(location.line)) continue;
			const identity = readIdentity(location.path);
			if (!identity) continue;
			const previous = lines.get(identity) ?? [];
			if (previous.some(line => Math.abs(line - location.line) <= 80)) continue;
			previous.push(location.line); lines.set(identity, previous);
			ranked.push({ kind: "reported-line", input: { path: location.path, offset: Math.max(1, location.line - 20), limit: 80 }, prior: 0.18 / ++count });
		}
	}
	if (presets.includes("companion-files")) {
		const known = new Map<string, string>();
		for (const event of [...scoped].reverse()) {
			const targets = [
				...((event.tool === "read" || event.tool === "edit" || event.tool === "write") && typeof event.input.path === "string" ? [event.input.path] : []),
				...(event.outputLocations ?? []).map(location => location.path), ...event.outputPaths ?? [],
			];
			for (const target of targets) {
				if (known.size >= 256 || typeof target !== "string") continue;
				const identity = readIdentity(target);
				if (identity && !known.has(identity)) known.set(identity, target);
			}
		}
		const seen = new Set<string>();
		for (const event of [...batch].reverse()) {
			if (event.outcome !== "success" || !["read", "edit", "write"].includes(event.tool) || typeof event.input.path !== "string") continue;
			const anchor = readIdentity(event.input.path);
			if (!anchor) continue;
			for (const [identity, target] of known) {
				if (seen.size >= 2 || identity === anchor || seen.has(identity) || readSince(identity, event.sequence) || !companions(event.input.path, target)) continue;
				seen.add(identity);
				ranked.push({ kind: "companion", input: { path: target }, prior: 0.12 / seen.size });
			}
		}
	}
	if (presets.includes("failure-edits")) {
		const failure = [...batch].reverse().find(event => event.outcome === "failure");
		const seen = new Set<string>();
		if (failure) for (const event of [...scoped].reverse()) {
			if (seen.size >= 2 || event.sequence >= failure.sequence || event.outcome !== "success" ||
				(event.tool !== "edit" && event.tool !== "write") || typeof event.input.path !== "string") continue;
			const identity = readIdentity(event.input.path);
			if (!identity || seen.has(identity) || readSince(identity, event.sequence)) continue;
			seen.add(identity);
			ranked.push({ kind: "failure-edit", input: { path: event.input.path }, prior: 0.15 / seen.size });
		}
	}
	if (presets.includes("continue-read")) {
		const seen = new Set<string>();
		for (const event of [...batch].reverse()) {
			if (seen.size >= 2 || event.tool !== "read" || event.outcome !== "success" || typeof event.input.path !== "string") continue;
			const output = asRecord(event.output), details = asRecord(output?.details);
			const truncation = asRecord(output?.truncation) ?? asRecord(details?.truncation);
			if (truncation?.truncated !== true || truncation.firstLineExceedsLimit || !positiveInteger(truncation.outputLines) ||
				!positiveInteger(truncation.totalLines) || truncation.totalLines <= truncation.outputLines) continue;
			const offset = event.input.offset ?? 1;
			if (!positiveInteger(offset)) continue;
			const next = offset + truncation.outputLines, identity = readIdentity(event.input.path);
			if (!Number.isSafeInteger(next) || !identity || seen.has(identity) || readSince(identity, event.sequence)) continue;
			seen.add(identity);
			ranked.push({ kind: "continuation", input: { path: event.input.path, offset: next,
				...(positiveInteger(event.input.limit) ? { limit: event.input.limit } : {}) }, prior: 0.15 / seen.size });
		}
	}
	return ranked;
}

function positiveInteger(value: unknown): value is number {
	return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function companionName(target: string) {
	const normalized = path.posix.normalize(target.replace(/\\/gu, "/")), extension = path.posix.extname(normalized);
	if (!extension) return undefined;
	const name = path.posix.basename(normalized, extension), marked = name.replace(/(?:\.(?:tests?|specs?)|_(?:tests?|specs?))$/iu, "").replace(/^(?:tests?|specs?)_/iu, "");
	const stem = marked === name ? name.replace(/(?:Tests?|Specs?)$/u, "") : marked;
	const directory = path.posix.dirname(normalized), scope: string[] = [];
	let testDirectory = false, afterTestDirectory = false;
	for (const part of directory.split("/")) {
		if (isTestDirectory(part)) { testDirectory = true; afterTestDirectory = true; continue; }
		if (afterTestDirectory && /^units?$/iu.test(part)) { afterTestDirectory = false; continue; }
		afterTestDirectory = false;
		scope.push(part);
	}
	return stem ? { directory, scope: path.posix.normalize(scope.join("/")), extension, stem, test: stem !== name || testDirectory } : undefined;
}

const isTestDirectory = (name: string) => /^(?:tests?|__tests__)$/iu.test(name);

/** Both paths already exist in the observed history; names only select a relation, never construct a file. */
function companions(left: string, right: string): boolean {
	const first = companionName(left), second = companionName(right);
	if (!first || !second || first.test === second.test || first.stem !== second.stem || first.extension !== second.extension) return false;
	const test = first.test ? first : second, source = first.test ? second : first;
	return test.scope === source.scope || path.posix.dirname(test.directory) === source.directory ||
		path.posix.dirname(source.directory) === test.directory ||
		path.posix.dirname(test.directory) === path.posix.dirname(source.directory) && isTestDirectory(path.posix.basename(test.directory));
}

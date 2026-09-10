import type { AssistantMessageEvent } from "@earendil-works/pi-ai";

export type ActorStreamPreview =
	| { readonly type: "tool"; readonly tool: string }
	| {
			readonly type: "call";
			readonly call: { readonly id: string; readonly name: string; readonly arguments: Record<string, unknown> };
	  };

type StreamedCall = {
	arguments: string;
	depth: number;
	quoted: boolean;
	escaped: boolean;
	ready: boolean;
	toolPreviewed: boolean;
	callPreviewed: boolean;
};

/** Derives non-authoritative scheduling hints from the same Actor stream in every host integration. */
export class ActorStreamPreviewTracker {
	private readonly calls = new Map<number, StreamedCall>();

	clear(): void {
		this.calls.clear();
	}

	observe(update: AssistantMessageEvent): readonly ActorStreamPreview[] {
		if (update.type === "toolcall_end") {
			const state = this.calls.get(update.contentIndex);
			this.calls.delete(update.contentIndex);
			const { id, name, arguments: args } = update.toolCall;
			return state?.callPreviewed ? [] : [{ type: "call", call: { id, name, arguments: args } }];
		}
		if (update.type !== "toolcall_start" && update.type !== "toolcall_delta") return [];
		const call = update.partial.content[update.contentIndex];
		if (call?.type !== "toolCall") return [];
		const state = this.calls.get(update.contentIndex) ?? {
			arguments: "",
			depth: 0, quoted: false, escaped: false, ready: false,
			toolPreviewed: false,
			callPreviewed: false,
		};
		this.calls.set(update.contentIndex, state);
		if (state.callPreviewed) return [];
		if (update.type === "toolcall_delta") {
			state.arguments += update.delta;
			// Scan only new characters; strict parsing still decides whether the complete input is an object.
			for (const character of update.delta) {
				if (state.escaped) state.escaped = false;
				else if (character === "\\" && state.quoted) state.escaped = true;
				else if (character === '"') state.quoted = !state.quoted;
				else if (!state.quoted) {
					if (character === "{" || character === "[") state.depth++;
					else if (character === "}" || character === "]") state.ready = --state.depth === 0 || state.ready;
				}
			}
		}
		const result: ActorStreamPreview[] = [];
		if (!state.toolPreviewed && call.name) {
			state.toolPreviewed = true;
			result.push({ type: "tool", tool: call.name });
		}
		if (update.type === "toolcall_delta" && state.ready && call.name && call.id) {
			state.ready = false;
			const parsed = jsonObject(state.arguments);
			if (parsed) {
				state.callPreviewed = true;
				state.arguments = "";
				result.push({ type: "call", call: { id: call.id, name: call.name, arguments: parsed } });
			}
		}
		return result;
	}
}

function jsonObject(value: string): Record<string, unknown> | undefined {
	try {
		const parsed: unknown = JSON.parse(value);
		return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
			? (parsed as Record<string, unknown>)
			: undefined;
	} catch {
		return undefined;
	}
}

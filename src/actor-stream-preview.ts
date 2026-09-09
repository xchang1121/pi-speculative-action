import type { AssistantMessageEvent } from "@earendil-works/pi-ai";

export type ActorStreamPreview =
	| { readonly type: "tool"; readonly tool: string }
	| {
			readonly type: "call";
			readonly call: { readonly id: string; readonly name: string; readonly arguments: Record<string, unknown> };
	  };

type StreamedCall = {
	arguments: string;
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
			return state?.callPreviewed
				? []
				: [
						{
							type: "call",
							call: {
								id: update.toolCall.id,
								name: update.toolCall.name,
								arguments: update.toolCall.arguments,
							},
						},
					];
		}
		if (update.type !== "toolcall_start" && update.type !== "toolcall_delta") return [];
		const call = update.partial.content[update.contentIndex];
		if (call?.type !== "toolCall") return [];
		const state = this.calls.get(update.contentIndex) ?? {
			arguments: "",
			toolPreviewed: false,
			callPreviewed: false,
		};
		if (update.type === "toolcall_delta") state.arguments += update.delta;
		this.calls.set(update.contentIndex, state);
		const result: ActorStreamPreview[] = [];
		if (!state.toolPreviewed && call.name) {
			state.toolPreviewed = true;
			result.push({ type: "tool", tool: call.name });
		}
		const parsed = update.type === "toolcall_delta" ? jsonObject(state.arguments) : undefined;
		if (parsed && !state.callPreviewed && call.name && call.id) {
			state.callPreviewed = true;
			result.push({ type: "call", call: { id: call.id, name: call.name, arguments: parsed } });
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

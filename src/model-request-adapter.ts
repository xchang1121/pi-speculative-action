import type { StreamFn } from "@earendil-works/pi-agent-core";
import { createAssistantMessageEventStream, fauxAssistantMessage, type AssistantMessage } from "@earendil-works/pi-ai";
import { modelRequestDomain, type ModelRequestContext, type ModelRequestEvent, type ModelRequestPolicy, type ModelRequestScheduler } from "./model-request.ts";

export function assistantCompletion(message: AssistantMessage | undefined): "visible" | "tool-use" | "reasoning-only" | "empty" | "length" | "failed" {
	if (!message || message.stopReason === "error" || message.stopReason === "aborted") return "failed";
	if (message.stopReason === "length") return "length";
	if (message.content.some(part => part.type === "toolCall")) return "tool-use";
	if (message.content.some(part => part.type === "text" && part.text.trim().length > 0)) return "visible";
	return message.content.some(part => part.type === "thinking") ? "reasoning-only" : "empty";
}

/** Retry incomplete output only; the caller retains its existing task budget and finalization tool policy. */
export async function recoverAssistantCompletion(input: {
	readonly latest: () => AssistantMessage | undefined;
	readonly canContinue: () => boolean;
	readonly recover: (attempt: number, reason: "reasoning-only" | "empty" | "length") => Promise<void>;
	/** null continues until visible completion, external failure or caller cancellation. */
	readonly maxAttempts?: number | null;
}): Promise<number> {
	const limit = input.maxAttempts === null ? Infinity : input.maxAttempts ?? 2;
	if (limit !== Infinity && (!Number.isSafeInteger(limit) || limit < 0)) throw new Error("Invalid completion recovery limit");
	let attempts = 0;
	while (attempts < limit && input.canContinue()) {
		const reason = assistantCompletion(input.latest());
		if (reason !== "reasoning-only" && reason !== "empty" && reason !== "length") break;
		await input.recover(++attempts, reason);
	}
	return attempts;
}

/** Only a proved SDK pre-rejection is excluded from the actual-execution settlement guard. */
export function sdkRejectedToolAttempt(tool: string, activeToolsAtStart: readonly string[], isError: boolean, content: readonly { readonly type: string; readonly text?: string }[]): boolean {
	return !activeToolsAtStart.includes(tool) && isError && content.some(part => part.type === "text" && part.text?.trim() === `Tool ${tool} not found`);
}

/** Relay local events independently from the transport's lifetime so aborted consumers cannot free upstream slots. */
export function scheduledModelStream(original: StreamFn, scheduler: ModelRequestScheduler, input: {
	readonly role: "actor" | "drafter";
	readonly context: () => ModelRequestContext;
	readonly policy: () => ModelRequestPolicy;
	readonly onEvent?: (event: ModelRequestEvent) => void;
}): StreamFn {
	return (model, context, options) => {
		const stream = createAssistantMessageEventStream();
		let ended = false;
		let responseStatus: number | undefined, providerRequestID: string | undefined;
		const failed = (reason: "error" | "aborted", error: unknown) => {
			if (ended) return; ended = true;
			const message = { ...fauxAssistantMessage([], { stopReason: reason }), api: model.api, provider: model.provider, model: model.id,
				errorMessage: reason === "aborted" ? "Consumer cancelled; remote settlement is tracked separately" : String(error) };
			stream.push({ type: "error", reason, error: message }); stream.end(message);
		};
		const abort = () => failed("aborted", options?.signal?.reason);
		options?.signal?.addEventListener("abort", abort, { once: true });
		if (options?.signal?.aborted) abort();
		void scheduler.run({ domain: modelRequestDomain(model.baseUrl), role: input.role, context: input.context(), policy: input.policy(),
			signal: options?.signal, onEvent: input.onEvent,
			execute: async request => {
				const upstream = await original(model, context, { ...options, signal: request.signal,
					maxRetries: options?.maxRetries ?? 0,
					onResponse: async (response, currentModel) => { responseStatus = response.status;
						providerRequestID = response.headers["x-request-id"] ?? response.headers["request-id"];
						await options?.onResponse?.(response, currentModel); },
					headers: { ...options?.headers, "X-Pi-Request-Id": request.id, "X-Pi-Request-Role": input.role } });
				for await (const event of upstream) {
					if (!ended) { stream.push(event); if (event.type === "done" || event.type === "error") ended = true; }
				}
				const message = await upstream.result(); stream.end(message); return message;
			},
			settled: message => ({ remoteFinished: !["error", "aborted"].includes(message.stopReason) || (responseStatus ?? 0) >= 400,
				usage: message.usage, providerRequestID }),
		}).catch(error => failed(options?.signal?.aborted ? "aborted" : "error", error))
			.finally(() => options?.signal?.removeEventListener("abort", abort));
		return stream;
	};
}

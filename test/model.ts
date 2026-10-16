import { fauxAssistantMessage, fauxToolCall, fauxProvider, type Model } from "@earendil-works/pi-ai";

const model = fauxProvider({ api: "openai-responses", provider: "openai",
	models: [{ id: "actor", input: ["text"], contextWindow: 8192, maxTokens: 2048 }] }).getModel();

export function testModel(id = "actor", overrides: Partial<Model<"openai-responses">> = {}): Model<"openai-responses"> {
	return { ...structuredClone(model), id, name: id, api: "openai-responses", baseUrl: "https://example.invalid", ...overrides };
}

export const toolUse = (...args: Parameters<typeof fauxToolCall>) => fauxAssistantMessage(fauxToolCall(...args), { stopReason: "toolUse" });

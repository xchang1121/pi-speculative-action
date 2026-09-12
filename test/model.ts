import type { Model } from "@earendil-works/pi-ai";

export function testModel(id = "actor", overrides: Partial<Model<"openai-responses">> = {}): Model<"openai-responses"> {
	return {
		id,
		name: id,
		api: "openai-responses",
		provider: "openai",
		baseUrl: "https://example.invalid",
		reasoning: false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 8192,
		maxTokens: 2048,
		...overrides,
	};
}

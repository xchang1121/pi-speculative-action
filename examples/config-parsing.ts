import type { TrustedSubcomputeStep } from "@earendil-works/pi-speculative-action";

/** Register with createSpeculativeActionHost(sessionID, { trustedSteps: [parseJsonConfig], ... }). */
export const parseJsonConfig: TrustedSubcomputeStep = {
	namespace: "config.parse",
	implementation: "JSON.parse(UTF-8)",
	acceptShared: () => true,
	async build({ input }) {
		return JSON.parse((await input()).toString("utf8"));
	},
};

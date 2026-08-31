import { describe, expect, it } from "vitest";
import {
	effectCapabilitiesCover,
	effectRequirements,
	UNRESTRICTED_PROCESS_EFFECTS,
	WORKSPACE_PATH_MUTATION_EFFECTS,
} from "../src/effect-model.ts";

describe("effect capability model", () => {
	it("matches backends by guarantee inclusion without observing tool names", () => {
		expect(effectCapabilitiesCover(WORKSPACE_PATH_MUTATION_EFFECTS.capabilities, WORKSPACE_PATH_MUTATION_EFFECTS)).toBe(
			true,
		);
		expect(effectCapabilitiesCover(WORKSPACE_PATH_MUTATION_EFFECTS.capabilities, UNRESTRICTED_PROCESS_EFFECTS)).toBe(
			false,
		);
		expect(effectCapabilitiesCover("all", UNRESTRICTED_PROCESS_EFFECTS)).toBe(true);
	});

	it("normalizes, deduplicates, and freezes requirement descriptors", () => {
		const requirements = effectRequirements("filesystem.write", "filesystem.read", "filesystem.write");

		expect(requirements.capabilities).toEqual(["filesystem.read", "filesystem.write"]);
		expect(Object.isFrozen(requirements)).toBe(true);
		expect(Object.isFrozen(requirements.capabilities)).toBe(true);
	});
});

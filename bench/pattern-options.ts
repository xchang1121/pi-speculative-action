import { PATTERN_AWARE_PRESETS, type PatternAwarePresetID } from "../src/pattern-aware-presets.ts";

/** Omission preserves package defaults; an explicitly empty list disables prebuilt modes. */
export function parsePatternPresets(value: string | undefined): readonly PatternAwarePresetID[] | undefined {
	if (value === undefined) return undefined;
	const selected = new Set(value.trim() ? value.split(",").map(id => id.trim()) : []);
	const known = new Set<string>(PATTERN_AWARE_PRESETS.map(preset => preset.id));
	for (const id of selected) if (!known.has(id))
		throw new Error(`Unknown --pattern-presets ID ${JSON.stringify(id)}; expected ${[...known].join(", ")}`);
	return Object.freeze(PATTERN_AWARE_PRESETS.filter(preset => selected.has(preset.id)).map(preset => preset.id));
}

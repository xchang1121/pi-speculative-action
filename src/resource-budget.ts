import { positiveCount as units } from "./number-utils.ts";

/** Every admitted execution keeps its capacity until physical completion. */
export function fitsResourceBudget(
	entries: Iterable<{ readonly work: { readonly resourceUnits: number } }>,
	incoming: number,
	capacity: number,
): boolean {
	let used = incoming;
	for (const { work } of entries) used += work.resourceUnits;
	return used <= units(capacity);
}

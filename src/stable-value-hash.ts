import { hash } from "node:crypto";
import { stableStringify } from "./stable-json.ts";

/** Stable, compact identity for structured runtime configuration and schemas. */
export function stableValueHash(value: unknown): string {
	return hash("sha256", stableStringify(value)).slice(0, 32);
}

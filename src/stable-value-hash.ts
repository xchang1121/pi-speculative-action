import { createHash } from "node:crypto";
import { stableStringify } from "./stable-json.ts";

/** Stable, compact identity for structured runtime configuration and schemas. */
export function stableValueHash(value: unknown): string {
	return createHash("sha256").update(stableStringify(value)).digest("hex").slice(0, 32);
}

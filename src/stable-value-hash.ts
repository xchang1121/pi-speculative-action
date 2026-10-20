import { hash, webcrypto } from "node:crypto";
import { stableStringify } from "./stable-json.ts";

/** Stable, compact identity for structured runtime configuration and schemas. */
export function stableValueHash(value: unknown): string {
	return hash("sha256", stableStringify(value)).slice(0, 32);
}

export type Sha256Digest = `sha256:${string}`;

export function sha256Digest(value: string | Uint8Array): Sha256Digest {
	return `sha256:${hash("sha256", value)}`;
}

/** The same digest; large content is hashed on the thread pool, leaving the event loop free. */
export const sha256DigestAsync = async (value: Uint8Array): Promise<Sha256Digest> => value.byteLength < 65_536 ? sha256Digest(value)
	: `sha256:${Buffer.from(await webcrypto.subtle.digest("SHA-256", value)).toString("hex")}`;

export function digestObject(value: unknown): Sha256Digest {
	return sha256Digest(stableStringify(value));
}

export function isSha256Digest(value: unknown): value is Sha256Digest {
	return typeof value === "string" && /^sha256:[0-9a-f]{64}$/.test(value);
}

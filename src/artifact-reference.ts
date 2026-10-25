import { isSha256Digest, type Sha256Digest } from "./stable-value-hash.ts";
import { isSafeCount } from "./number-utils.ts";

export interface ArtifactReference { readonly digest: Sha256Digest; readonly size: number; }

/** The same immutable content identity for input versions and effect closures. */
export function validateArtifact(reference: ArtifactReference, sizes?: Map<Sha256Digest, number>): void {
	if (!reference || !isSha256Digest(reference.digest) || !isSafeCount(reference.size)) throw new Error("invalid content artifact");
	const previousSize = sizes?.get(reference.digest);
	if (previousSize !== undefined && previousSize !== reference.size) throw new Error("conflicting content artifact sizes");
	sizes?.set(reference.digest, reference.size);
}

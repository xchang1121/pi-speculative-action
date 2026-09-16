import type { Sha256Digest } from "./provenance-certificate.ts";

interface WorkspaceEntryChange {
	readonly changeDigest: Sha256Digest;
	readonly changeTimeMs: number;
}

export type WorkspaceTreeEntry = WorkspaceEntryChange & (
	| {
			readonly kind: "file";
			/** Owned byte source when an immutable lower entry is carried into a layered view. */
			readonly contentPath?: string;
			readonly digest: Sha256Digest;
			readonly metadataDigest: Sha256Digest;
			readonly mode: number;
			readonly size: number;
			readonly links: number;
	  }
	| {
			readonly kind: "directory";
			readonly entriesDigest: Sha256Digest;
			readonly metadataDigest: Sha256Digest;
			readonly mode: number;
			readonly uid: number;
			readonly gid: number;
	  }
	| {
			readonly kind: "symlink";
			readonly target: string;
			readonly targetDigest: Sha256Digest;
	  }
	| {
			readonly kind: "unsupported";
			readonly type: string;
	  });

export type WorkspaceStructureEntry =
	| Omit<Extract<WorkspaceTreeEntry, { readonly kind: "file" }>, "digest">
	| Exclude<WorkspaceTreeEntry, { readonly kind: "file" }>;

export interface WorkspaceStructureSnapshot {
	readonly root: string;
	readonly entries: ReadonlyMap<string, WorkspaceStructureEntry>;
	readonly files: number;
	readonly bytesRead: number;
	readonly complete: boolean;
}

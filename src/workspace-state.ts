import type { Sha256Digest } from "./provenance-certificate.ts";

export type WorkspaceTreeEntry =
	| {
			readonly kind: "file";
			readonly digest: Sha256Digest;
			readonly metadataDigest: Sha256Digest;
			readonly changeDigest: Sha256Digest;
			readonly changeTimeMs: number;
			readonly mode: number;
			readonly size: number;
			readonly links: number;
			readonly content?: Buffer;
	  }
	| {
			readonly kind: "directory";
			readonly entriesDigest: Sha256Digest;
			readonly metadataDigest: Sha256Digest;
			readonly changeDigest: Sha256Digest;
			readonly changeTimeMs: number;
			readonly mode: number;
	  }
	| {
			readonly kind: "symlink";
			readonly target: string;
			readonly targetDigest: Sha256Digest;
			readonly changeDigest: Sha256Digest;
			readonly changeTimeMs: number;
	  }
	| {
			readonly kind: "unsupported";
			readonly type: string;
			readonly changeDigest: Sha256Digest;
			readonly changeTimeMs: number;
	  };

export type WorkspaceStructureEntry =
	| {
			readonly kind: "file";
			readonly metadataDigest: Sha256Digest;
			readonly changeDigest: Sha256Digest;
			readonly changeTimeMs: number;
			readonly mode: number;
			readonly size: number;
			readonly links: number;
	  }
	| Exclude<WorkspaceTreeEntry, { readonly kind: "file" }>;

export interface WorkspaceSnapshot<Entry> {
	readonly root: string;
	readonly entries: ReadonlyMap<string, Entry>;
	readonly files: number;
	readonly bytesRead: number;
	readonly complete: boolean;
}

export interface WorkspaceTreeSnapshot extends WorkspaceSnapshot<WorkspaceTreeEntry> {}

export interface WorkspaceStructureSnapshot extends WorkspaceSnapshot<WorkspaceStructureEntry> {}

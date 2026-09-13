import type { Sha256Digest } from "./provenance-certificate.ts";

interface WorkspaceEntryChange {
	readonly changeDigest: Sha256Digest;
	readonly changeTimeMs: number;
}

export type WorkspaceTreeEntry = WorkspaceEntryChange & (
	| {
			readonly kind: "file";
			readonly digest: Sha256Digest;
			readonly metadataDigest: Sha256Digest;
			readonly mode: number;
			readonly size: number;
			readonly links: number;
			readonly content?: Buffer;
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
	| Omit<Extract<WorkspaceTreeEntry, { readonly kind: "file" }>, "digest" | "content">
	| Exclude<WorkspaceTreeEntry, { readonly kind: "file" }>;

interface WorkspaceSnapshot<Entry> {
	readonly root: string;
	readonly entries: ReadonlyMap<string, Entry>;
	readonly files: number;
	readonly bytesRead: number;
	readonly complete: boolean;
}

export interface WorkspaceStructureSnapshot extends WorkspaceSnapshot<WorkspaceStructureEntry> {}

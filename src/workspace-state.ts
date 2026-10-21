import type { FilesystemObservationEvidence } from "./filesystem-evidence.ts";
import { digestObject, type Sha256Digest } from "./stable-value-hash.ts";

interface WorkspaceEntryChange { readonly changeDigest: Sha256Digest; readonly changeTimeMs: number; }

export interface WorkspaceDirectoryState {
	readonly entriesDigest: Sha256Digest;
	readonly mode: number;
	readonly uid: number;
	readonly gid: number;
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
			/** Snapshot-local object identity and its closed set of names. */
			readonly object?: string;
			readonly aliases?: readonly string[];
			readonly modified?: string;
			readonly ownership?: string;
	  }
	| ({
			readonly kind: "directory";
			/** Kernel prestate for observations made after this operation changed the directory's entries. */
			readonly observed?: FilesystemObservationEvidence;
			readonly metadataDigest: Sha256Digest;
	  } & WorkspaceDirectoryState)
	| { readonly kind: "symlink"; readonly target: string; readonly targetDigest: Sha256Digest; }
	| { readonly kind: "unsupported"; readonly type: string; });

export type WorkspaceStructureEntry =
	| Omit<Extract<WorkspaceTreeEntry, { readonly kind: "file" }>, "digest">
	| Exclude<WorkspaceTreeEntry, { readonly kind: "file" }>;

export interface WorkspaceStructureSnapshot {
	readonly root: string;
	readonly entries: ReadonlyMap<string, WorkspaceStructureEntry>;
	readonly complete: boolean;
}

const aliasIdentities = new WeakMap<readonly string[], Sha256Digest>();

/** A captured alias group is shared by its names; compare its immutable namespace once. */
export function workspaceAliasIdentity(aliases: readonly string[] | undefined): Sha256Digest | undefined {
	if (!aliases) return;
	const identity = aliasIdentities.get(aliases) ?? digestObject(aliases);
	if (Object.isFrozen(aliases)) aliasIdentities.set(aliases, identity);
	return identity;
}

/** A name refers to an object from either endpoint of the same workspace transaction. */
export interface WorkspaceFileMutation {
	readonly operation?: "write_contents";
	readonly object?: { readonly path: string; readonly before: boolean };
	readonly aliases?: readonly string[];
	/** Closed names of the resulting object, independent of its predecessor's names. */
	readonly afterAliases?: readonly string[];
}

export function projectFileMutation({ operation, object, aliases, afterAliases }: WorkspaceFileMutation, project = (name: string) => name): WorkspaceFileMutation {
	return Object.fromEntries(Object.entries({ operation, object: object && { ...object, path: project(object.path) },
		aliases: aliases?.map(project), afterAliases: afterAliases?.map(project) }).filter(([, value]) => value !== undefined));
}

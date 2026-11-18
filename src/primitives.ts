import type { ExecFileOptions } from "node:child_process";
import { execFile } from "node:child_process";
import { hash, webcrypto } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, realpath } from "node:fs/promises";
import path from "node:path";
import { promisify, types } from "node:util";
import { speculativeConfig } from "./config-schema.ts";
export function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
export function errorDetail(error: unknown): string {
	return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
export function hasErrorCode(error: unknown, code: string): boolean {
	return Boolean(error && typeof error === "object" && "code" in error && error.code === code);
}
export function isMissing(error: unknown): boolean {
	return hasErrorCode(error, "ENOENT");
}
export function ignoreMissing(error: unknown): undefined {
	if (!isMissing(error)) throw error;
}
/** Stable dependency-first traversal. Missing vertices and cycles reject the whole graph. */
export function dependencyOrder<Node>(
	roots: Iterable<Node>,
	dependencies: (node: Node) => Iterable<Node | undefined>,
): Node[] | undefined {
	const states = new Map<Node, boolean>();
	const ordered: Node[] = [];
	for (const root of roots) {
		const pending: Array<readonly [Node | undefined, boolean]> = [[root, false]];
		while (pending.length) {
			const [node, finished] = pending.pop()!;
			if (node === undefined) return undefined;
			if (finished) {
				states.set(node, true);
				ordered.push(node);
				continue;
			}
			const state = states.get(node);
			if (state === true) continue;
			if (state === false) return undefined;
			states.set(node, false);
			pending.push([node, true]);
			const parents = [...dependencies(node)];
			for (let index = parents.length - 1; index >= 0; index--) pending.push([parents[index], false]);
		}
	}
	return ordered;
}
/** Stable, compact identity for structured runtime configuration and schemas. */
export function stableValueHash(value: unknown): string {
	return hash("sha256", stableStringify(value)).slice(0, 32);
}
export type Sha256Digest = `sha256:${string}`;
export function sha256Digest(value: string | Uint8Array): Sha256Digest {
	return `sha256:${hash("sha256", value)}`;
}
/** The same digest; large content is hashed on the thread pool, leaving the event loop free. */
export const sha256DigestAsync = async (value: Uint8Array): Promise<Sha256Digest> =>
	value.byteLength < speculativeConfig().filesystem.asyncHashThresholdBytes
		? sha256Digest(value)
		: `sha256:${Buffer.from(await webcrypto.subtle.digest("SHA-256", value)).toString("hex")}`;
export function digestObject(value: unknown): Sha256Digest {
	return sha256Digest(stableStringify(value));
}
export function isSha256Digest(value: unknown): value is Sha256Digest {
	return typeof value === "string" && /^sha256:[0-9a-f]{64}$/.test(value);
}
/** Validate discrete quantities without coercing, rounding or clamping malformed evidence. */
export function isSafeCount(value: unknown, minimum = 0, maximum = Number.MAX_SAFE_INTEGER): value is number {
	return Number.isSafeInteger(value) && (value as number) >= minimum && (value as number) <= maximum;
}
export function isFiniteNumber(value: unknown): value is number {
	return typeof value === "number" && Number.isFinite(value);
}
/** Clamp measured values; non-finite and missing evidence contributes zero. */
export function nonNegativeFinite(value: unknown): number {
	return isFiniteNumber(value) ? Math.max(0, value) : 0;
}
export function nonNegativeCount(value: unknown): number {
	return isFiniteNumber(value) ? Math.max(0, Math.floor(value)) : 0;
}
export function positiveCount(value: unknown): number {
	return isFiniteNumber(value) ? Math.max(1, Math.floor(value)) : 1;
}
export function finiteNumber(value: unknown): number | undefined {
	return isFiniteNumber(value) ? value : undefined;
}
export function clampProbability(value: number): number {
	return Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
}
/** Copy and increment own data keys; arbitrary names never address inherited properties. */
export function incrementCounts(
	target: Readonly<Record<string, number>>,
	keys: readonly string[],
): Record<string, number> {
	const counts = {
		...Object.fromEntries(keys.map((key) => [key, 0])),
		...target,
	};
	for (const key of keys) counts[key]!++;
	return counts;
}
export const slash = (value: string): string => (path.sep === "/" ? value : value.replaceAll(path.sep, "/"));
/** Normalize a logical resource without folding case. */
export function normalizeLogicalPath(value: string): string {
	const normalized = path.posix.normalize(slash(value));
	return normalized === "/" || /^[A-Za-z]:\/$/.test(normalized) ? normalized : normalized.replace(/\/$/, "");
}
/** Preserve the spelling of a resolved physical path; the backing volume may distinguish case. */
export function filesystemPathKey(value: string): string {
	return slash(path.resolve(value));
}
/** Windows drive letters never distinguish case; directory names may, so they keep their exact spelling. */
export function sameFilesystemPath(left: string, right: string): boolean {
	const drive = (value: string) =>
		process.platform === "win32" ? value.replace(/^[a-z](?=:)/u, (letter) => letter.toUpperCase()) : value;
	return drive(left) === drive(right);
}
/** An absolute POSIX path without an empty, `.` or `..` segment or a trailing slash: between two, containment is a prefix. */
const NORMALIZED_ABSOLUTE = /^(?:\/(?!\.{1,2}(?:\/|$))[^/]+)+$/;
export function relativeFilesystemPath(root: string, target: string): string | undefined {
	if (path.sep === "/" && NORMALIZED_ABSOLUTE.test(root) && NORMALIZED_ABSOLUTE.test(target))
		return target === root ? "" : target.startsWith(`${root}/`) ? target.slice(root.length + 1) : undefined;
	const resolvedRoot = path.resolve(root);
	const resolvedTarget = path.resolve(target);
	const relative = path.relative(resolvedRoot, resolvedTarget);
	if (relative === "") return sameFilesystemPath(resolvedRoot, resolvedTarget) ? "" : undefined;
	if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative))
		return undefined;
	return sameFilesystemPath(path.resolve(resolvedRoot, relative), resolvedTarget) ? relative : undefined;
}
export const containsFilesystemPath = (root: string, target: string): boolean =>
	relativeFilesystemPath(root, target) !== undefined;
export function containsLogicalPath(root: string, target: string): boolean {
	const relative = path.posix.relative(normalizeLogicalPath(root), normalizeLogicalPath(target));
	return (
		relative === "" || (relative !== ".." && !relative.startsWith("../") && !path.posix.isAbsolute(relative))
	);
}
/** Fixed-entry LRU map used for derived state that is always safe to recompute. */
export class BoundedRecencyMap<Key, Value> extends Map<Key, Value> {
	private readonly capacity: number;
	private readonly evicted?: (value: Value) => void;
	constructor(capacity: number, evicted?: (value: Value) => void) {
		super();
		this.capacity = Math.max(1, Math.floor(capacity));
		this.evicted = evicted;
	}
	get(key: Key): Value | undefined {
		const value = super.get(key);
		if (value === undefined && !this.has(key)) return undefined;
		super.delete(key);
		super.set(key, value as Value);
		return value;
	}
	/** Cache factory results, including undefined; factory errors propagate. */
	getOrCreate(key: Key, create: () => Value): Value {
		const cached = this.get(key);
		if (cached !== undefined || this.has(key)) return cached as Value;
		const value = create();
		this.set(key, value);
		return value;
	}
	set(key: Key, value: Value): this {
		super.delete(key);
		super.set(key, value);
		if (!(this.size <= this.capacity)) {
			const [oldest, removed] = this.entries().next().value!;
			super.delete(oldest);
			this.evicted?.(removed);
		}
		return this;
	}
}
/** A field-addressable object; this is not a proof of plain or immutable data. */
export function isRecord(value: unknown): value is Record<string, unknown> {
	return isObject(value) && !Array.isArray(value);
}
export function asRecord(value: unknown): Record<string, unknown> | undefined {
	return isRecord(value) ? value : undefined;
}
/** JSON.stringify with UTF-16-ordered named keys, numeric indices first, and no intermediate object tree. */
export function stableStringify(value: unknown): string {
	return serialize(value) as string;
}
/** Structural equality with stableStringify's object/array semantics, without allocating strings. */
export function stableEqual(left: unknown, right: unknown): boolean {
	if (left === right) return true;
	if (!isObject(left) || !isObject(right)) return false;
	return equalObject(left, right);
}
export function deepFreeze<Value>(value: Value): Value {
	if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
	for (const child of Object.values(value as Record<string, unknown>)) deepFreeze(child);
	return Object.freeze(value);
}
const immutableSnapshots = new WeakSet<object>();
/** Only owned, lossless data trees can carry an immutable execution identity. */
export function isImmutableSnapshot(value: unknown): boolean {
	return isObject(value)
		? immutableSnapshots.has(value)
		: value === undefined ||
				value === null ||
				typeof value === "string" ||
				typeof value === "boolean" ||
				(isFiniteNumber(value) && !Object.is(value, -0));
}
/** Each invocation owns its structured values, including already sealed data. */
export function immutableSnapshot<Value>(value: Value): Value {
	const owned = structuredClone(value);
	const seen = new WeakSet<object>();
	const freeze = (item: unknown): boolean => {
		if (!isObject(item)) return isImmutableSnapshot(item);
		if (seen.has(item)) return false;
		seen.add(item);
		const array = Array.isArray(item);
		const keys = Object.keys(item);
		let immutable = Object.getPrototypeOf(item) === (array ? Array.prototype : Object.prototype);
		if (array && (keys.length !== item.length || keys.some((key) => !isArrayIndex(key)))) immutable = false;
		for (const child of Object.values(item)) if (!freeze(child) || child === undefined) immutable = false;
		Object.freeze(item);
		if (immutable) immutableSnapshots.add(item);
		return immutable;
	};
	freeze(owned);
	return owned;
}
/** Own every enumerable data key, including symbols, without flattening opaque objects or invoking accessors. */
export function cloneSharedData<Value>(value: Value): Value {
	const seen = new WeakMap<object, object>();
	const copy = (item: unknown): unknown => {
		if (typeof item === "function" || typeof item === "symbol") throw new Error("shared_output_not_data");
		if (!isObject(item)) return item;
		if (types.isProxy(item)) throw new Error("shared_output_not_data");
		if (seen.has(item)) return seen.get(item);
		const array = Array.isArray(item);
		if (Object.getPrototypeOf(item) !== (array ? Array.prototype : Object.prototype))
			throw new Error("shared_output_not_data");
		const owned = array ? new Array(item.length) : {};
		seen.set(item, owned);
		for (const key of Reflect.ownKeys(item)) {
			const property = Object.getOwnPropertyDescriptor(item, key)!;
			if (!("value" in property) || (!property.enumerable && !(array && key === "length")))
				throw new Error("shared_output_not_data");
			if (array && key === "length") continue;
			Object.defineProperty(owned, key, {
				value: copy(property.value),
				enumerable: true,
				writable: true,
				configurable: true,
			});
		}
		return owned;
	};
	return copy(value) as Value;
}
function equalObject(left: object, right: object): boolean {
	const leftArray = Array.isArray(left);
	const rightArray = Array.isArray(right);
	if (leftArray || rightArray) {
		if (!leftArray || !rightArray || left.length !== right.length) return false;
		for (let index = 0; index < left.length; index++)
			if (!equalSlot(left[index], right[index], true)) return false;
		return true;
	}
	const leftRecord = left as Record<string, unknown>;
	const rightRecord = right as Record<string, unknown>;
	for (const key of new Set([...Object.keys(leftRecord), ...Object.keys(rightRecord)])) {
		if (!equalSlot(leftRecord[key], rightRecord[key], false)) return false;
	}
	return true;
}
function equalSlot(left: unknown, right: unknown, arraySlot: boolean): boolean {
	if (isObject(left) || isObject(right)) return isObject(left) && isObject(right) && equalObject(left, right);
	return (
		(JSON.stringify(left) ?? (arraySlot ? "null" : undefined)) ===
		(JSON.stringify(right) ?? (arraySlot ? "null" : undefined))
	);
}
function serialize(value: unknown): string | undefined {
	if (!value || typeof value !== "object") return JSON.stringify(value);
	if (Array.isArray(value)) {
		let result = "[";
		for (let index = 0; index < value.length; index++) {
			if (index) result += ",";
			result += serialize(value[index]) ?? "null";
		}
		return `${result}]`;
	}
	const record = value as Record<string, unknown>;
	const keys = Object.keys(record);
	let firstNamed = 0;
	while (firstNamed < keys.length && isArrayIndex(keys[firstNamed]!)) firstNamed++;
	keys.push(...keys.splice(firstNamed).sort());
	let result = "{";
	let first = true;
	for (const key of keys) {
		const item = serialize(record[key]);
		if (item === undefined) continue;
		if (!first) result += ",";
		result += `${JSON.stringify(key)}:${item}`;
		first = false;
	}
	return `${result}}`;
}
function isArrayIndex(value: string) {
	const index = Number(value);
	return isSafeCount(index, 0, 4294967294) && String(index) === value;
}
export function isObject(value: unknown): value is object {
	return value !== null && typeof value === "object";
}
export const execHostFile = promisify(execFile);
/** Resolve a trusted host executable from explicit paths before consulting PATH. */
export async function resolveHostExecutable(
	explicit: string | undefined,
	name: string,
	fallbacks: readonly string[] = [],
	alternateNames: readonly string[] = [],
	preserveSymlink = false,
): Promise<string> {
	const directories = (process.env.PATH ?? "").split(path.delimiter).filter(Boolean);
	for (const candidate of [
		explicit,
		...fallbacks,
		...[name, ...alternateNames].flatMap((name) =>
			directories.map((directory) => path.join(directory, name)),
		),
	])
		if (
			candidate &&
			(await access(candidate, fsConstants.X_OK).then(
				() => true,
				() => false,
			))
		)
			return preserveSymlink ? path.resolve(candidate) : realpath(candidate);
	throw new Error(`Executable not found: ${[name, ...alternateNames].join(" or ")}`);
}
/** Run a host utility with bounded output; callers retain their timeout and environment policy. */
export function execHostText(
	executable: string,
	args: readonly string[],
	options: ExecFileOptions = {},
): Promise<string> {
	return execHostFile(executable, args, {
		maxBuffer: speculativeConfig().processReuse.hostOutputBytes,
		...options,
		encoding: "utf8",
	}).then(
		({ stdout, stderr }) => `${stdout}${stderr}`,
		(error) => {
			if (error.stderr === undefined) throw error;
			throw new Error(`${executable} failed: ${error.stderr.trim() || error.message}`, { cause: error });
		},
	);
}

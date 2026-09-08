import { types } from "node:util";

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

/** Own a structured value before exposing its immutable identity to another lifecycle. */
export function immutableSnapshot<Value>(value: Value): Value {
	const owned = structuredClone(value), seen = new WeakSet<object>();
	const freeze = (item: unknown): void => {
		if (!isObject(item) || seen.has(item)) return;
		seen.add(item);
		for (const child of Object.values(item)) freeze(child);
		Object.freeze(item);
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
		if (Object.getPrototypeOf(item) !== (array ? Array.prototype : Object.prototype)) throw new Error("shared_output_not_data");
		const owned = array ? new Array(item.length) : {};
		seen.set(item, owned);
		for (const key of Reflect.ownKeys(item)) {
			const property = Object.getOwnPropertyDescriptor(item, key)!;
			if (!("value" in property) || (!property.enumerable && !(array && key === "length"))) {
				throw new Error("shared_output_not_data");
			}
			if (array && key === "length") continue;
			Object.defineProperty(owned, key, { value: copy(property.value), enumerable: true, writable: true, configurable: true });
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
		for (let index = 0; index < left.length; index++) {
			if (!equalSlot(left[index], right[index], true)) return false;
		}
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
	return Number.isInteger(index) && index >= 0 && index < 0xffff_ffff && String(index) === value;
}

function isObject(value: unknown): value is object {
	return value !== null && typeof value === "object";
}

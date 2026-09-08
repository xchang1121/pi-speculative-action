/** JSON.stringify with recursively stable object keys and no intermediate object tree. */
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
	keys.push(...keys.splice(firstNamed).sort((left, right) => left.localeCompare(right)));
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

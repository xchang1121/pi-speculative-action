/** Stored values never coerce text; interactive parsers below separately enforce strict input. */
export function positiveInteger<F extends number | undefined>(value: unknown, fallback: F): number | F {
	return typeof value === "number" && Number.isSafeInteger(Math.floor(value)) && value >= 1 ? Math.floor(value) : fallback;
}

export function nonNegativeInteger(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isSafeInteger(Math.floor(value)) && value >= 0 ? Math.floor(value) : fallback;
}

/** Node timers otherwise turn an overflowing delay into a 1 ms timeout. */
export function milliseconds(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 2_147_483_647 ? value : fallback;
}
export const positiveMilliseconds = (value: unknown, fallback: number): number => milliseconds(value, fallback) || fallback;

export function nonNegativeNumber<F extends number | undefined>(value: unknown, fallback: F): number | F {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : fallback;
}

export function probability<F extends number | undefined>(value: unknown, fallback: F): number | F {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1 ? value : fallback;
}

export function booleanOr(value: unknown, fallback: boolean): boolean {
	return typeof value === "boolean" ? value : fallback;
}

/** Every stored field declares its parser; unknown fields never enter the normalized result. */
export function settingsParser<Settings extends Record<string, unknown>>(
	fields: { readonly [Key in keyof Settings]: readonly [Settings[Key], (value: unknown, fallback: Settings[Key]) => Settings[Key]] },
) {
	const keys = Object.keys(fields) as Array<Extract<keyof Settings, string>>;
	const defaults = Object.fromEntries(keys.map(key => [key, fields[key][0]])) as Settings;
	const parse = (input?: Readonly<Record<string, unknown>>): Settings => {
		const result = {} as Settings;
		for (const key of keys) result[key] = fields[key][1](input?.[key], defaults[key]);
		return result;
	};
	return { defaults, parse };
}

export type SettingInputResult<T> =
	| { readonly ok: true; readonly value: T }
	| { readonly ok: false; readonly error: string };

export interface SettingInputDescriptor<T> {
	readonly title: string;
	readonly format: (value: T) => string;
	readonly parse: (input: string) => SettingInputResult<T>;
}

interface NumericInputOptions {
	readonly error?: string;
	readonly format?: (value: number) => string;
	readonly transform?: (value: number) => number;
}

export function settingInput<T>(
	title: string,
	format: (value: T) => string,
	parse: (input: string) => SettingInputResult<T>,
): SettingInputDescriptor<T> {
	return Object.freeze({ title, format, parse });
}

function numericInput(title: string, valid: (value: number) => boolean, error: string, options: NumericInputOptions = {}): SettingInputDescriptor<number> {
	return settingInput(title, options.format ?? String, (input) => {
		const value = Number(input.trim());
		const result = options.transform?.(value) ?? value;
		return input.trim() && Number.isFinite(value) && valid(value) && Number.isFinite(result) && valid(result)
			? { ok: true, value: result } : { ok: false, error: options.error ?? error };
	});
}

export function positiveIntegerInput(title: string, options: NumericInputOptions = {}): SettingInputDescriptor<number> {
	return numericInput(title, (value) => Number.isSafeInteger(value) && value > 0, `${title} must be a positive integer.`, options);
}

export function nonNegativeIntegerInput(title: string): SettingInputDescriptor<number> {
	return numericInput(title, (value) => Number.isSafeInteger(value) && value >= 0, `${title} must be a non-negative integer.`);
}

export function millisecondsInput(title: string, allowZero = false): SettingInputDescriptor<number> {
	return numericInput(title, value => milliseconds(value, -1) >= (allowZero ? 0 : 1), `${title} must be an integer from ${allowZero ? 0 : 1} to 2147483647.`);
}

export function nonNegativeNumberInput(title: string): SettingInputDescriptor<number> {
	return numericInput(title, (value) => value >= 0, `${title} must be a non-negative number.`);
}

export function probabilityInput(title: string, options: NumericInputOptions = {}): SettingInputDescriptor<number> {
	return numericInput(title, (value) => value >= 0 && value <= 1, `${title} must be between 0 and 1.`, options);
}

export function nonEmptyTextInput(title: string): SettingInputDescriptor<string> {
	return settingInput(title, String, (input) => {
		const value = input.trim();
		return value ? { ok: true, value } : { ok: false, error: `${title} cannot be empty.` };
	});
}

export function optionalTextInput(title: string): SettingInputDescriptor<string | undefined> {
	return settingInput(title, value => value ?? "", input => ({ ok: true, value: input.trim() || undefined }));
}

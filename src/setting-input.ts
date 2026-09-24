/** Stored values never coerce text; interactive parsers below separately enforce strict input. */
export function positiveInteger<F extends number | undefined>(value: unknown, fallback: F): number | F {
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

export function nonNegativeInteger(value: unknown, fallback: number): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? Math.floor(value) : fallback;
}

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
	defaults: Settings,
	parsers: { readonly [Key in keyof Settings]: (value: unknown, fallback: Settings[Key]) => Settings[Key] },
): (input: Readonly<Record<string, unknown>> | undefined) => Settings {
	const keys = Object.keys(defaults) as Array<Extract<keyof Settings, string>>;
	return (input) => {
		const result = {} as Settings;
		for (const key of keys) result[key] = parsers[key](input?.[key], defaults[key]);
		return result;
	};
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
		return Number.isFinite(value) && valid(value) ? { ok: true, value: options.transform?.(value) ?? value } : { ok: false, error: options.error ?? error };
	});
}

export function positiveIntegerInput(title: string, options: NumericInputOptions = {}): SettingInputDescriptor<number> {
	return numericInput(title, (value) => Number.isInteger(value) && value > 0, `${title} must be a positive integer.`, options);
}

export function nonNegativeIntegerInput(title: string): SettingInputDescriptor<number> {
	return numericInput(title, (value) => Number.isInteger(value) && value >= 0, `${title} must be a non-negative integer.`);
}

export function nonNegativeNumberInput(title: string): SettingInputDescriptor<number> {
	return numericInput(title, (value) => value >= 0, `${title} must be a non-negative number.`);
}

export function probabilityInput(title: string, options: NumericInputOptions = {}): SettingInputDescriptor<number> {
	return numericInput(title, (value) => value >= 0 && value <= 1, `${title} must be between 0 and 1.`, options);
}

export function optionalPositiveIntegerInput(title: string): SettingInputDescriptor<number | undefined> {
	const required = positiveIntegerInput(title);
	return settingInput(title, (value) => (value === undefined ? "" : required.format(value)), (input) => (input.trim() === "" ? { ok: true, value: undefined } : required.parse(input)));
}

export function nonEmptyTextInput(title: string): SettingInputDescriptor<string> {
	return settingInput(title, String, (input) => {
		const value = input.trim();
		return value ? { ok: true, value } : { ok: false, error: `${title} cannot be empty.` };
	});
}

export function optionalTextInput(title: string): SettingInputDescriptor<string | undefined> {
	return settingInput(
		title,
		(value) => value ?? "",
		(input) => ({ ok: true, value: input.trim() || undefined }),
	);
}

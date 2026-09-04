import path from "node:path";
import { describe, expect, test } from "vitest";
import {
	containsFilesystemPath,
	containsLogicalPath,
	filesystemPathKey,
	relativeFilesystemPath,
} from "../src/path-utils.ts";

describe("filesystem path policy", () => {
	const root = path.resolve("workspace");
	test.each([
		["root", root, true],
		["child", path.join(root, "src", "file.ts"), true],
		["dot-dot name", path.join(root, "..foo"), true],
		["sibling", `${root}-sibling`, false],
		["escape", path.resolve(root, "..", "outside"), false],
	] as const)("classifies %s without prefix matching", (_name, target, expected) => {
		expect(containsFilesystemPath(root, target)).toBe(expected);
		expect(relativeFilesystemPath(root, target) !== undefined).toBe(expected);
	});

	test("preserves physical path case", () => {
		expect(filesystemPathKey(path.join(root, "Case"))).not.toBe(filesystemPathKey(path.join(root, "case")));
	});

	test.runIf(process.platform === "win32")("treats drive, separator, and case aliases conservatively", () => {
		expect(containsFilesystemPath("C:\\Work\\Repo", "C:/Work/Repo/src")).toBe(true);
		expect(containsFilesystemPath("C:\\Work\\Repo", "D:\\Work\\Repo\\src")).toBe(false);
		expect(containsFilesystemPath("C:\\Work\\Repo", "C:\\work\\repo\\src")).toBe(false);
	});
});

test.each([
	["same", "src", "src", true],
	["child", "src", "src/lib", true],
	["mixed separators", "src", "src\\lib", true],
	["dot-dot name", "src", "src/..foo", true],
	["sibling", "src", "source", false],
	["escape", "src", "src/../outside", false],
	["case alias", "src", "Src/lib", false],
] as const)("logical containment: %s", (_name, root, target, expected) => {
	expect(containsLogicalPath(root, target)).toBe(expected);
});

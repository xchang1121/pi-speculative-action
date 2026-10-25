// @ts-check
import { createRequire } from "node:module";

/** @typedef {{ pin(path: string): object; identity(lease: object): Buffer; close(lease: object): void; liveCount(): number }} NativeFilesystem */
/** @type {NativeFilesystem | undefined} */
let native;

/** Load the package's N-API object lease provider; no pathname-handle fallback.
 * @returns {NativeFilesystem} */
export function windowsFilesystem() {
	if (process.platform !== "win32" || process.arch !== "x64") throw new Error("object leases require Windows x64 NTFS");
	return native ??= createRequire(import.meta.url)("../vendor/filesystem-win32-x64.node");
}

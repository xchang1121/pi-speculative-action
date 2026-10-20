import { readText } from "./filesystem.ts";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { onExit } from "./async.ts";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { linuxOverlayfsCapability, mountLinuxOverlayfs } from "../src/linux-overlayfs.ts";

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

describe("Linux OverlayFS workspace substrate", () => {
	it.for(["normal", "recovered"])("proves copy-on-write isolation and namespace visibility through %s unmount", async (mode, { skip }) => {
		const capability = await linuxOverlayfsCapability();
		if (!capability.available) return skip(capability.detail);
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-overlayfs-test-"));
		const lowerRoot = path.join(root, "lower");
		const privateRoot = path.join(root, "private");
		await Promise.all([mkdir(lowerRoot), mkdir(privateRoot)]);
		await writeFile(path.join(lowerRoot, "value.txt"), "lower\n", "utf8");
		const wrapper = path.join(root, "fusermount-transient-failure");
		const options = mode === "recovered" ? { fusermountBinary: wrapper } : {};
		if (mode === "recovered") {
			const probeUnmounted = path.join(root, "probe-unmounted");
			const failureInjected = path.join(root, "failure-injected");
			await writeFile(
				wrapper,
				`#!/bin/sh\nif [ ! -e ${shellQuote(probeUnmounted)} ]; then\n  : > ${shellQuote(probeUnmounted)}\n  exec ${shellQuote(capability.fusermountBinary)} "$@"\nfi\nif [ ! -e ${shellQuote(failureInjected)} ]; then\n  : > ${shellQuote(failureInjected)}\n  exit 42\nfi\nexec ${shellQuote(capability.fusermountBinary)} "$@"\n`,
				"utf8",
			);
			await chmod(wrapper, 0o755);
			const probed = await linuxOverlayfsCapability(options);
			expect(probed.available).toBe(true);
		}
		const mounted = await mountLinuxOverlayfs({ lowerRoot, privateRoot, options });
		await using _mounted = onExit(async () => { await mounted.close(); await rm(root, { recursive: true, force: true }); });
		await writeFile(path.join(mounted.root, "value.txt"), "copy-up\n", "utf8");
		expect(await readText(lowerRoot, "value.txt")).toBe("lower\n");
		expect(
			(await promisify(execFile)("unshare", [
				"--user",
				"--map-root-user",
				"--mount",
				"--pid",
				"--fork",
				"--mount-proc",
				"--",
				"cat",
				path.join(mounted.root, "value.txt"),
			], { encoding: "utf8" })).stdout,
		).toBe("copy-up\n");
		await mounted.close();
		if (mode === "recovered") {
			const demoted = await linuxOverlayfsCapability(options);
			expect(demoted.available).toBe(false);
			expect(demoted.detail).toMatch(/unmount/i);
			expect(await readText(lowerRoot, "value.txt")).toBe("lower\n");
		}
	});
});

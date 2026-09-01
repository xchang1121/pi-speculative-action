import { execFile } from "node:child_process";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { linuxOverlayfsCapability, mountLinuxOverlayfs } from "../src/linux-overlayfs.ts";

describe("Linux OverlayFS workspace substrate", () => {
	it("proves copy-on-write isolation and visibility in descendant namespaces", async () => {
		const capability = await linuxOverlayfsCapability();
		if (!capability.available) {
			expect(capability.detail.length).toBeGreaterThan(0);
			return;
		}
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-overlayfs-test-"));
		const lowerRoot = path.join(root, "lower");
		const privateRoot = path.join(root, "private");
		await Promise.all([mkdir(lowerRoot), mkdir(privateRoot)]);
		await writeFile(path.join(lowerRoot, "value.txt"), "lower\n", "utf8");
		const mounted = await mountLinuxOverlayfs({ lowerRoot, privateRoot });
		try {
			await writeFile(path.join(mounted.root, "value.txt"), "copy-up\n", "utf8");
			expect(await readFile(path.join(lowerRoot, "value.txt"), "utf8")).toBe("lower\n");
			expect(
				await output("unshare", [
					"--user",
					"--map-root-user",
					"--mount",
					"--pid",
					"--fork",
					"--mount-proc",
					"--",
					"cat",
					path.join(mounted.root, "value.txt"),
				]),
			).toBe("copy-up\n");
		} finally {
			await mounted.close();
			await rm(root, { recursive: true, force: true });
		}
	});

	it("demotes the driver after a recovered unmount failure", async () => {
		if (process.platform !== "linux") return;
		const host = await linuxOverlayfsCapability();
		if (!host.available) return;
		const root = await mkdtemp(path.join(os.tmpdir(), "pi-overlayfs-health-test-"));
		const lowerRoot = path.join(root, "lower");
		const privateRoot = path.join(root, "private");
		const probeUnmounted = path.join(root, "probe-unmounted");
		const failureInjected = path.join(root, "failure-injected");
		const wrapper = path.join(root, "fusermount-transient-failure");
		await Promise.all([mkdir(lowerRoot), mkdir(privateRoot)]);
		await writeFile(path.join(lowerRoot, "value.txt"), "lower\n", "utf8");
		await writeFile(
			wrapper,
			`#!/bin/sh\nif [ ! -e ${shellQuote(probeUnmounted)} ]; then\n  : > ${shellQuote(probeUnmounted)}\n  exec ${shellQuote(host.fusermountBinary)} "$@"\nfi\nif [ ! -e ${shellQuote(failureInjected)} ]; then\n  : > ${shellQuote(failureInjected)}\n  exit 42\nfi\nexec ${shellQuote(host.fusermountBinary)} "$@"\n`,
			"utf8",
		);
		await chmod(wrapper, 0o755);
		const options = { fusermountBinary: wrapper };
		const probed = await linuxOverlayfsCapability(options);
		expect(probed.available).toBe(true);
		const mounted = await mountLinuxOverlayfs({ lowerRoot, privateRoot, options });
		let safelyClosed = false;
		try {
			await writeFile(path.join(mounted.root, "value.txt"), "copy-up\n", "utf8");
			await mounted.close();
			safelyClosed = true;
			const demoted = await linuxOverlayfsCapability(options);
			expect(demoted.available).toBe(false);
			expect(demoted.detail).toMatch(/unmount/i);
			expect(await readFile(path.join(lowerRoot, "value.txt"), "utf8")).toBe("lower\n");
		} finally {
			if (safelyClosed) await rm(root, { recursive: true, force: true });
		}
	});
});

function shellQuote(value: string): string {
	return `'${value.replaceAll("'", `'\\''`)}'`;
}

function output(executable: string, args: readonly string[]): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(executable, args, { encoding: "utf8" }, (error, stdout, stderr) => {
			if (error) reject(new Error(`${executable} failed: ${stderr || error.message}`, { cause: error }));
			else resolve(stdout);
		});
	});
}

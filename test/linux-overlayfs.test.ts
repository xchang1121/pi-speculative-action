import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { disposable, onExit } from "./async.ts";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { LinuxOverlayfsCapabilityRegistry, linuxOverlayfsCapability, mountLinuxOverlayfs } from "../src/linux-overlayfs.ts";

const shellQuote = (value: string) => `'${value.replaceAll("'", `'\\''`)}'`;

describe("Linux OverlayFS workspace substrate", () => {
	it.each([
		["bounds request aliases and releases lifecycle-owned capability state", false],
		["fails the registry closed instead of evicting degraded driver identities", true],
	] as const)("%s", async (_name, degraded) => {
		await using registry = disposable(new LinuxOverlayfsCapabilityRegistry(degraded ? { degradedCapacity: 1 } : { requestCapacity: 4, resolvedCapacity: 2, negativeTtlMs: 60_000 }));
		if (degraded) {
			for (const [id, detail] of [["one", "first unsafe mount"], ["two", "second unsafe mount"]]) registry.markDegraded({
				available: true, binary: `/driver/${id}`, fusermountBinary: `/unmount/${id}`, fingerprint: id, detail: "ready",
			}, detail);
		} else await Promise.all(Array.from({ length: 128 }, (_, index) => registry.capability({ overlayfsBinary: path.join(os.tmpdir(), `missing-overlay-${index}`) })));
		expect(registry.inspect()).toMatchObject(degraded ? { degradedEntries: 0, disabled: true } : { requestEntries: 4, disabled: false, disposed: false });
		expect(registry.inspect().resolvedEntries).toBeLessThanOrEqual(2);
		if (degraded) await expect(registry.capability()).resolves.toMatchObject({ available: false, detail: expect.stringMatching(/registry disabled/i) });
		registry.dispose();
		expect(registry.inspect()).toEqual({ requestEntries: 0, resolvedEntries: 0, degradedEntries: 0, disabled: degraded, disposed: true });
		await expect(registry.capability()).rejects.toThrow("registry is disposed");
	});

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
		expect(await readFile(path.join(lowerRoot, "value.txt"), "utf8")).toBe("lower\n");
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
			expect(await readFile(path.join(lowerRoot, "value.txt"), "utf8")).toBe("lower\n");
		}
	});
});

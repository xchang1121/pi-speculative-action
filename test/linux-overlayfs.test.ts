import { readText } from "./filesystem.ts";
import { execHostText } from "../src/executable-path.ts";
import { onExit } from "./async.ts";
import { chmod, link, mkdir, mkdtemp, open, rename, rm, writeFile } from "node:fs/promises";
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
		await mkdir(path.join(lowerRoot, "aliases"));
		const aliases = ["alias.txt", "aliases/second.txt", "aliases/third.txt"];
		for (const alias of aliases) await link(path.join(lowerRoot, "value.txt"), path.join(lowerRoot, alias));
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
		const held = await open(path.join(mounted.root, "value.txt"));
		try {
			const identity = await held.stat();
			expect(identity.nlink).toBe(4);
			await chmod(path.join(mounted.root, aliases[2]!), 0o600);
			await rename(path.join(mounted.root, "value.txt"), path.join(mounted.root, aliases[0]!));
			await writeFile(path.join(mounted.root, "value.txt"), "copy-up\n", "utf8");
			expect(await readText(lowerRoot, "value.txt")).toBe("lower\n");
			expect(await execHostText("unshare", ["--user", "--map-root-user", "--mount", "--pid", "--fork", "--mount-proc", "--",
				"cat", path.join(mounted.root, "value.txt")])).toBe("copy-up\n");
			for (let index = 0; index < aliases.length; index++) {
				expect(await readText(mounted.root, aliases[index]!)).toBe("copy-up\n");
				await rm(path.join(mounted.root, aliases[index]!));
				expect(await held.stat()).toMatchObject({ dev: identity.dev, ino: identity.ino, mode: 0o100600, nlink: 3 - index });
			}
			await rm(path.join(mounted.root, "value.txt"));
			expect(await held.stat()).toMatchObject({ dev: identity.dev, ino: identity.ino, nlink: 0 });
			expect(await held.readFile("utf8")).toBe("copy-up\n");
		} finally { await held.close(); }
		await mounted.close();
		if (mode === "recovered") {
			const demoted = await linuxOverlayfsCapability(options);
			expect(demoted.available).toBe(false);
			expect(demoted.detail).toMatch(/unmount/i);
			expect(await readText(lowerRoot, "value.txt")).toBe("lower\n");
		}
	});
});

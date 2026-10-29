import { expect, test } from "vitest";
import fs, { lstat, lutimes, mkdir, mkdtemp, readFile, rename, rm, symlink, utimes, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import * as filesystemEvidence from "../src/filesystem-evidence.ts";
import { nativeProcessLibrary } from "../src/process-startup.mjs";
import { ResourceVersionManager, type ResourceVersionToken } from "../src/resource-version.ts";

const linux = test.skipIf(process.platform !== "linux" || !process.env.PI_SPEC_HELD_EXEC);
const metadata = async (paths: readonly string[]) => Promise.all(paths.map(async target => {
  const stat = await lstat(target, { bigint: true });
  return { target, fields: Object.fromEntries(filesystemEvidence.FILESYSTEM_METADATA_FIELDS.map(field => [field, String(stat[field])])) };
}));

linux("captures, consumes and revalidates resource inputs without changing source metadata", { timeout: 30_000 }, async () => {
  nativeProcessLibrary(process.env.PI_SPEC_HELD_EXEC!);
  const root = await mkdtemp(path.join(os.tmpdir(), "pi-readonly-inputs-")), directory = path.join(root, "directory");
  const file = path.join(directory, "input"), relative = path.join(root, "relative"), absolute = path.join(root, "absolute");
  const broken = path.join(root, "broken"), cycle = path.join(root, "cycle"), nonUtf8 = path.join(root, "non-utf8"), manager = new ResourceVersionManager(root, { watch: false });
  let token: ResourceVersionToken | undefined;
  try {
    await mkdir(directory); await writeFile(file, "source\n");
    await symlink("directory/input", relative); await symlink(file, absolute);
    await symlink("absent", broken); await symlink("cycle", cycle); await symlink(Buffer.from([0xff]), nonUtf8);
    for (const target of [root, directory, file]) await utimes(target, 915148800, 946684800);
    for (const target of [relative, absolute, broken, cycle, nonUtf8]) await lutimes(target, 915148800, 946684800);
    const paths = [root, directory, file, relative, absolute, broken, cycle, nonUtf8], before = await metadata(paths);
    token = await manager.capture(undefined, 64 * 1024);
    const inputs = token.view!;
    expect((await inputs.readFile(relative)).toString()).toBe("source\n");
    expect((await inputs.readFile(absolute)).toString()).toBe("source\n");
    expect(await inputs.readdir(directory)).toEqual(["input"]);
    expect(await inputs.exists(broken)).toBe(false);
    inputs.seal();
    expect((await manager.validate(token)).expired).toBe(false);
    expect(await metadata(paths)).toEqual(before);
    await using filesystem = await filesystemEvidence.openFilesystemReadView();
    await expect(filesystem.readlink(file)).rejects.toMatchObject({ code: "EINVAL" });
    await expect(filesystem.pin(cycle, true)).rejects.toMatchObject({ code: "ELOOP" });
    await expect(filesystem.pin(path.join(file, "child"), true)).rejects.toMatchObject({ code: "ENOTDIR" });
    await expect(filesystem.readlink(nonUtf8)).rejects.toThrow("unrepresentable_symlink_target");
    const image = await filesystemEvidence.readFilesystemDirectoryImage(directory, 4096, undefined, filesystem);
    expect(await filesystemEvidence.readFilesystemDirectoryImage(directory, image.length)).toEqual(image);
    const entries: string[] = []; let cookie = 0n;
    for (let offset = 0; offset < image.length;) {
      const end = offset + image.readUInt16LE(offset + 16), zero = image.indexOf(0, offset + 19), next = image.readBigInt64LE(offset + 8);
      expect(next > cookie).toBe(true); cookie = next;
      expect(image.subarray(zero + 1, end).every(byte => byte === 0)).toBe(true);
      const name = image.toString("utf8", offset + 19, zero);
      if (name === "input") expect(image.readBigUInt64LE(offset)).toBe((await lstat(file, { bigint: true })).ino);
      entries.push(name); offset = end;
    }
    expect(entries.sort()).toEqual([".", "..", "input"]);
    await expect(filesystemEvidence.readFilesystemDirectoryImage(directory, image.length - 1)).rejects.toThrow("capture budget");
    await expect(filesystemEvidence.readFilesystemDirectoryImage(directory, 4096, AbortSignal.abort(new Error("cancelled")))).rejects.toThrow("cancelled");
    expect(await metadata(paths)).toEqual(before);
    await rename(file, path.join(directory, "old")); await writeFile(file, "replacement\n");
    expect((await inputs.readFile(relative)).toString()).toBe("source\n");
    expect((await manager.validate(token)).expired).toBe(true);
    await token.release();
    await expect(inputs.readFile(relative)).rejects.toThrow();
  } finally { await token?.release(); manager.close(); await rm(root, { recursive: true, force: true }); }
});

linux("rejects mount crossings before reading an unqualified filesystem", { timeout: 30_000 }, async () => {
  const command = promisify(execFile), namespace = await fs.readlink("/proc/self/ns/mnt");
  if (!process.env.PI_TEST_MOUNT_NAMESPACE_PARENT) {
    const output = await mkdtemp(path.join(os.tmpdir(), "pi-mount-qualification-")), report = path.join(output, "tests.json");
    try {
      const cli = path.join(path.dirname(fileURLToPath(import.meta.resolve("vitest/package.json"))), "vitest.mjs");
      await command("unshare", ["--user", "--map-root-user", "--mount", "--propagation", "private", process.execPath,
        cli, "run", fileURLToPath(import.meta.url), "--testNamePattern", "^rejects mount crossings before reading an unqualified filesystem$",
        "--maxWorkers=1", "--no-file-parallelism", "--reporter=json", "--outputFile", report],
      { env: { ...process.env, PI_TEST_MOUNT_NAMESPACE_PARENT: namespace }, timeout: 25_000, maxBuffer: 1024 * 1024 });
      expect(JSON.parse(await readFile(report, "utf8"))).toMatchObject({ numPassedTests: 1, numFailedTests: 0 });
    } finally { await rm(output, { recursive: true, force: true }); }
    return;
  }
  expect(namespace).not.toBe(process.env.PI_TEST_MOUNT_NAMESPACE_PARENT);
  nativeProcessLibrary(process.env.PI_SPEC_HELD_EXEC!);
  for (const type of ["tmpfs", "bind"]) {
  const fixture = await mkdtemp(path.join(os.tmpdir(), "pi-readonly-mounts-")), source = path.join(fixture, "source");
  const input = path.join(source, "input"), route = path.join(source, "route");
  const lowerRoot = path.join(fixture, "lower"), privateRoot = path.join(fixture, "private"), mountRoot = path.join(privateRoot, "workspace");
  let mounted = false;
  const mount = async () => {
    await command("mount", type === "bind" ? ["--bind", lowerRoot, mountRoot] : ["-t", "tmpfs", "-o", "mode=700,nosuid,nodev", "tmpfs", mountRoot]);
    mounted = true;
    if (type === "tmpfs") await writeFile(path.join(mountRoot, "input"), "mounted\n");
  };
  const unmount = async () => { await command("umount", [mountRoot]); mounted = false; };
  // Change a real mount at the initial snapshot boundary; never synthesize mountinfo content.
  const openDuring = async (change: () => Promise<void>) => {
    const original = fs.readFile;
    let armed = true;
    fs.readFile = (async (...args: Parameters<typeof readFile>) => {
      const image = await original(...args);
      if (armed && args[0] === "/proc/self/mountinfo") { armed = false; await change(); }
      return image;
    }) as typeof readFile;
    try { return await filesystemEvidence.openFilesystemReadView(); }
    finally { fs.readFile = original; }
  };
  const capture = async (view: filesystemEvidence.FilesystemReadView) => {
    expect((await filesystemEvidence.captureStableFile(input, 1024, true, { view })).content?.toString()).toBe("source\n");
    await view.verify();
  };
  try {
    await mkdir(source); await mkdir(lowerRoot); await mkdir(mountRoot, { recursive: true });
    await writeFile(input, "source\n"); await writeFile(path.join(lowerRoot, "input"), "mounted\n");
    await writeFile(path.join(mountRoot, "input"), "underlying\n"); await symlink(mountRoot, route);
    for (const target of [source, input]) await utimes(target, 915148800, 946684800);
    await lutimes(route, 915148800, 946684800);
    const paths = [source, input, route], before = await metadata(paths);
    {
      await using view = await openDuring(mount);
      await capture(view);
      await expect(view.verify([fixture])).rejects.toThrow("mount topology changed");
      await view.verify();
      await expect(view.pin(path.join(route, "input"), true)).rejects.toMatchObject({ code: "EXDEV" });
      await expect(view.pin(path.join(mountRoot, "input"), true)).rejects.toMatchObject({ code: "EXDEV" });
      await expect(view.verify()).rejects.toThrow("mount topology changed");
    }
    {
      await using view = await openDuring(unmount);
      await capture(view);
      await expect(view.pin("/proc/self/status", true)).rejects.toMatchObject({ code: "EXDEV" });
      await view.verify();
      const handle = await view.pin(path.join(mountRoot, "input"), true); await handle.close();
      await expect(view.verify()).rejects.toThrow("mount topology changed");
    }
    {
      await using view = await filesystemEvidence.openFilesystemReadView();
      const handle = await view.pin(path.join(route, "input"), true); await handle.close();
      await view.verify(); await mount();
      await expect(view.verify()).rejects.toThrow("mount topology changed");
    }
    await unmount();
    {
      await using view = await filesystemEvidence.openFilesystemReadView();
      await expect(view.pin(path.join(mountRoot, "missing"), true)).rejects.toMatchObject({ code: "ENOENT" });
      await view.verify(); await mount();
      await expect(view.verify()).rejects.toThrow("mount topology changed");
    }
    expect(await metadata(paths)).toEqual(before);
  } finally { if (mounted) await unmount(); await rm(fixture, { recursive: true, force: true }); }
  }
});

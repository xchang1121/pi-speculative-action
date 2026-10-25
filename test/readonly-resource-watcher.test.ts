import { expect, test } from "vitest";
import { link, lstat, lutimes, mkdir, mkdtemp, rename, rm, symlink, utimes, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import * as filesystemEvidence from "../src/filesystem-evidence.ts";
import { nativeProcessLibrary } from "../src/process-startup.mjs";
import { ResourceVersionManager, type ResourceVersionToken } from "../src/resource-version.ts";

const linux = test.skipIf(process.platform !== "linux" || !process.env.PI_SPEC_HELD_EXEC);
const metadata = async (paths: readonly string[]) => Promise.all(paths.map(async target => {
  const stat = await lstat(target, { bigint: true });
  return { target, fields: Object.fromEntries(filesystemEvidence.FILESYSTEM_METADATA_FIELDS.map(field => [field, String(stat[field])])) };
}));

linux("starting recursive resource observation preserves inode and symlink metadata", { timeout: 30_000 }, async () => {
  nativeProcessLibrary(process.env.PI_SPEC_HELD_EXEC!);
  const fixture = await mkdtemp(path.join(os.tmpdir(), "pi-readonly-watcher-")), root = path.join(fixture, "root");
  const directory = path.join(root, "directory"), file = path.join(directory, "input"), outside = path.join(fixture, "outside");
  const relative = path.join(root, "relative"), absolute = path.join(root, "absolute"), external = path.join(root, "external"), chain = path.join(fixture, "chain");
  const manager = new ResourceVersionManager(root); let token: ResourceVersionToken | undefined;
  try {
    await mkdir(directory, { recursive: true }); await writeFile(file, "source\n"); await writeFile(outside, "outside\n");
    await symlink("directory/input", relative); await symlink(file, absolute);
    await symlink(outside, chain); await symlink(chain, external);
    const plain = [root, directory, file, outside], links = [relative, absolute, external, chain];
    for (const target of plain) await utimes(target, 915148800, 946684800);
    for (const target of links) await lutimes(target, 915148800, 946684800);
    const before = await metadata([...plain, ...links]);
    token = await manager.observeChanges();
    expect(token.watching).toBe(true);
    expect(await metadata([...plain, ...links])).toEqual(before);
  } finally { await token?.release(); manager.close(); await rm(fixture, { recursive: true, force: true }); }
});

linux("observes outside hardlink writes and follows created and renamed subtrees", { timeout: 30_000 }, async () => {
  nativeProcessLibrary(process.env.PI_SPEC_HELD_EXEC!);
  const fixture = await mkdtemp(path.join(os.tmpdir(), "pi-readonly-watch-events-")), root = path.join(fixture, "root");
  const file = path.join(root, "input"), alias = path.join(fixture, "alias"), created = path.join(root, "created"), moved = path.join(root, "moved");
  const manager = new ResourceVersionManager(root), tokens: ResourceVersionToken[] = [];
  const observe = async () => { const token = await manager.observeChanges(); tokens.push(token); expect(token.watching).toBe(true); return token; };
  const notified = async (token: ResourceVersionToken, target: string) => {
    await expect.poll(() => manager.changesSince(token).paths, { timeout: 3000 }).toContain(target);
    expect(manager.changesSince(token).uncertain).toBe(false);
  };
  try {
    await mkdir(root); await writeFile(file, "first\n"); await link(file, alias);
    const initial = await observe();
    await writeFile(alias, "through external alias\n"); await notified(initial, file);
    const beforeCreate = await observe();
    await mkdir(created); await writeFile(path.join(created, "child"), "created\n"); await notified(beforeCreate, created);
    const afterCreate = await observe();
    await writeFile(path.join(created, "child"), "changed\n"); await notified(afterCreate, path.join(created, "child"));
    const beforeMove = await observe();
    await rename(created, moved); await notified(beforeMove, moved);
    const afterMove = await observe();
    await writeFile(path.join(moved, "child"), "moved\n"); await notified(afterMove, path.join(moved, "child"));
  } finally { for (const token of tokens) await token.release(); manager.close(); await rm(fixture, { recursive: true, force: true }); }
});

linux("closing during watcher initialization rejects the pending observation", { timeout: 30_000 }, async () => {
  nativeProcessLibrary(process.env.PI_SPEC_HELD_EXEC!);
  const root = await mkdtemp(path.join(os.tmpdir(), "pi-readonly-watch-close-")), manager = new ResourceVersionManager(root);
  try {
    await writeFile(path.join(root, "input"), "source\n");
    const pending = manager.observeChanges(); manager.close();
    await expect(pending).rejects.toThrow("resource_version_manager_closed");
  } finally { manager.close(); await rm(root, { recursive: true, force: true }); }
});

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
    expect(await metadata(paths)).toEqual(before);
    await rename(file, path.join(directory, "old")); await writeFile(file, "replacement\n");
    expect((await inputs.readFile(relative)).toString()).toBe("source\n");
    expect((await manager.validate(token)).expired).toBe(true);
    await token.release();
    await expect(inputs.readFile(relative)).rejects.toThrow();
  } finally { await token?.release(); manager.close(); await rm(root, { recursive: true, force: true }); }
});

linux("rejects mount crossings before reading an unqualified filesystem", { timeout: 30_000 }, async () => {
  nativeProcessLibrary(process.env.PI_SPEC_HELD_EXEC!);
  await using filesystem = await filesystemEvidence.openFilesystemReadView();
  await expect(filesystem.pin("/proc/self/status", true)).rejects.toMatchObject({ code: "EXDEV" });
});

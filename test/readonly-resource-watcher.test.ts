import { expect, test } from "vitest";
import { link, lstat, lutimes, mkdir, mkdtemp, readFile, rename, rm, symlink, utimes, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import * as filesystemEvidence from "../src/filesystem-evidence.ts";
import { nativeProcessLibrary } from "../src/process-startup.mjs";
import { ResourceVersionManager, type ResourceVersionToken } from "../src/resource-version.ts";
import { WorkspaceSandboxService, type SandboxWorkspaceContext } from "../src/workspace-sandbox.ts";
import { captureAbsenceDependency, captureDirectoryDependency, captureFileDependency, captureMetadataDependency, captureSymlinkDependency, validateDynamicDependencyCertificate } from "../src/provenance-validation.ts";
import { ProcessReusePlanner, ProvenanceCertificateStore, sealProcessCertificate } from "../src/process-reuse.ts";
import { PROCESS_CERTIFICATE } from "./process-fixture.ts";

const linux = test.skipIf(process.platform !== "linux" || !process.env.PI_SPEC_HELD_EXEC);
const metadata = async (paths: readonly string[]) => Promise.all(paths.map(async target => {
  const stat = await lstat(target, { bigint: true });
  return { target, fields: Object.fromEntries(filesystemEvidence.FILESYSTEM_METADATA_FIELDS.map(field => [field, String(stat[field])])) };
}));

linux.for(["capture", "validate", "replay"])("preserves source metadata through dependency evidence and persisted replay (%s)", { timeout: 30_000 }, async mode => {
  nativeProcessLibrary(process.env.PI_SPEC_HELD_EXEC!);
  const fixture = await mkdtemp(path.join(os.tmpdir(), "pi-readonly-evidence-")), root = path.join(fixture, "real/source"), routeLink = path.join(fixture, "route"), route = path.join(routeLink, "source");
  const file = path.join(root, "input"), alias = path.join(root, "alias"), symbolic = path.join(root, "symbolic"), broken = path.join(root, "broken");
  const logical = (name: string) => path.join(route, name), aliases = [logical("input"), logical("alias")].sort();
  const capture = async () => ({ complete: true, taints: [], dependencies: [
    (await captureFileDependency(logical("input"), logical("input"), "input", { includeMetadata: true, aliases })).dependency,
    await captureDirectoryDependency(route, route, true, ["symbolic"], true),
    await captureDirectoryDependency(logical("empty"), logical("empty"), false, [], false),
    (await captureAbsenceDependency(logical("absent"), logical("absent"), true, ["symbolic"]))!,
    (await captureAbsenceDependency(logical("broken/child"), logical("broken/child"), false))!,
    await captureSymlinkDependency(logical("symbolic"), logical("symbolic")),
    await captureSymlinkDependency(logical("broken"), logical("broken")),
    await captureMetadataDependency(logical("symbolic"), logical("symbolic"), true, ["mode", "size"]),
    await captureMetadataDependency(logical("symbolic"), logical("symbolic"), false, ["mode", "size"]),
  ] });
  let store: ProvenanceCertificateStore | undefined;
  try {
    await mkdir(path.join(root, "empty"), { recursive: true }); await writeFile(file, "original\n"); await link(file, alias);
    await symlink("real", routeLink); await symlink("input", symbolic); await symlink("missing", broken);
    // Prepare validation fixtures before setting the source's deliberately old access times.
    const prepared = mode === "capture" ? undefined : await capture();
    for (const target of [root, file]) await utimes(target, 915148800, 946684800);
    for (const target of [routeLink, symbolic, broken]) await lutimes(target, 915148800, 946684800);
    const paths = [root, file, alias, routeLink, symbolic, broken], before = await metadata(paths);
    const evidence = prepared ?? await capture();
    expect(evidence.dependencies[0]).toMatchObject({ aliases });
    if (mode === "replay") {
      store = new ProvenanceCertificateStore(path.join(fixture, "store"));
      const data = await store.artifacts.put("reused output\n");
      const certificate = sealProcessCertificate({ ...PROCESS_CERTIFICATE, dependencyCertificate: evidence,
        result: { replayProfile: "buffered_noninteractive", journal: [{ sequence: 0, kind: "output", fd: 1, data }], exit: { kind: "code", code: 0 } } });
      expect(await new ProcessReusePlanner({ store }).publishCompleted(certificate)).toBe(true);
      await store.dispose(); store = new ProvenanceCertificateStore(path.join(fixture, "store"));
      const plan = await new ProcessReusePlanner({ store }).plan({ weakKey: certificate.weakKey, executablePath: certificate.prototype.executablePath,
        contract: { sink: "buffered", orderedJournal: true, transactionalEffects: true } });
      expect(plan.kind).toBe("completed_replay");
      if (plan.kind === "completed_replay") expect(plan.artifacts.read(data).toString()).toBe("reused output\n");
    } else if (mode === "validate") expect((await validateDynamicDependencyCertificate(evidence)).status).toBe("valid");
    expect(await metadata(paths)).toEqual(before);
    await expect(captureMetadataDependency(logical("broken"), logical("broken"), true)).rejects.toMatchObject({ code: "ENOENT" });
    const failed = await validateDynamicDependencyCertificate({ ...evidence, dependencies: [
      { ...evidence.dependencies.find(dependency => dependency.kind === "symlink")!, path: logical("input") },
    ] });
    expect(failed.status).toBe("indeterminate");
    expect(await metadata(paths)).toEqual(before);
    await writeFile(file, "changed\n");
    const changed = await metadata(paths);
    expect((await validateDynamicDependencyCertificate(evidence)).status).toBe("stale");
    expect(await metadata(paths)).toEqual(changed);
  } finally { await store?.dispose(); await rm(fixture, { recursive: true, force: true }); }
});

linux.for(["allocate", "adopt", "failure"])("preserves source metadata through workspace preparation and actual adoption (%s)", { timeout: 30_000 }, async mode => {
  const fixture = await mkdtemp(path.join(os.tmpdir(), "pi-readonly-workspace-")), parent = path.join(fixture, "real");
  const route = path.join(fixture, "route"), physical = path.join(parent, "source"), root = path.join(route, "source");
  const directory = path.join(physical, "directory"), file = path.join(directory, "input"), alias = path.join(physical, "alias"), broken = path.join(physical, "broken");
  const service = new WorkspaceSandboxService();
  try {
    await mkdir(directory, { recursive: true }); await writeFile(file, "original\n");
    await symlink("real", route); await symlink("directory/input", alias); await symlink("missing", broken);
    for (const target of [physical, directory, file]) await utimes(target, 915148800, 946684800);
    for (const target of [route, alias, broken]) await lutimes(target, 915148800, 946684800);
    const paths = [physical, directory, file, route, alias, broken], before = await metadata(paths);
    const execute = async (workspace: SandboxWorkspaceContext) => {
      expect((await readFile(path.join(workspace.sandboxRoot, "alias"))).toString()).toBe("original\n");
      if (mode !== "allocate") await writeFile(path.join(workspace.sandboxRoot, "directory/input"), "adopted\n");
      if (mode === "failure") throw new Error("discard private changes");
      return { result: { content: [], details: {} }, isError: false };
    };
    const snapshot = mode === "adopt" ? await service.capture({ cwd: root, driver: "git", execute }) : undefined;
    if (mode === "allocate") await service.withWorkspace(root, execute);
    if (mode === "failure") await expect(service.capture({ cwd: root, driver: "git", execute })).rejects.toThrow("discard private changes");
    const after = await metadata(paths), changed = after.flatMap((entry, index) => Object.entries(entry.fields).flatMap(([field, value]) =>
      value === before[index]!.fields[field] ? [] : [{ target: entry.target, field, before: before[index]!.fields[field], after: value }]));
    expect(changed).toEqual([]);
    if (snapshot) {
      await service.commitDelta(snapshot);
      expect((await readFile(file)).toString()).toBe("adopted\n");
      expect((await readFile(alias)).toString()).toBe("adopted\n");
    }
  } finally { await service.dispose(); await rm(fixture, { recursive: true, force: true }); }
});

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

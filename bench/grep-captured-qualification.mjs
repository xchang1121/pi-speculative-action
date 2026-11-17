import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";
import { createGrepTool } from "@earendil-works/pi-coding-agent";
import { createResourceSnapshotExecutionWorld } from "../dist/agent-execution-world.js";
import { PI_ACTION_SEMANTICS } from "../dist/action-semantics.js";
import { createClosedSearchProfile } from "../dist/pi-tool-invocation.js";
import { searchJourney } from "./search-journey.mjs";

const pathRules = Object.freeze({ homeDir: os.homedir() });
const selectedCases = new Set(
	process.argv
		.find((arg) => arg.startsWith("--case="))
		?.slice(7)
		.split(",") ?? [],
);
const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-grep-evidence-"));
const configuredAtStart = process.env.RIPGREP_CONFIG_PATH;
let profile;
let preparedProfiles = 0;
try {
	const configuration = path.join(root, "controlled-rg-config");
	await fs.writeFile(configuration, "--sort=path\n--no-ignore-global\n");
	process.env.RIPGREP_CONFIG_PATH = configuration;
	const report = await qualifyNamespace();
	console.log(
		JSON.stringify({ platform: process.platform, node: process.version, preparedProfiles, report }, null, 2),
	);
} finally {
	if (configuredAtStart === undefined) delete process.env.RIPGREP_CONFIG_PATH;
	else process.env.RIPGREP_CONFIG_PATH = configuredAtStart;
	await profile?.dispose();
	assert.equal(path.dirname(root), path.resolve(os.tmpdir()));
	await fs.rm(root, { recursive: true, force: true });
}

async function qualifyCaptured(
	cwd,
	profile,
	args,
	expected,
	changed,
	rejected = false,
	stableChange = false,
) {
	const reads = new Set();
	const enumerated = new Set();
	let executions = 0;
	const bound = profile.invocations.get("grep");
	assert.ok(bound, "existing rg is not qualified by the production profile");
	const observeInputs = (view) => ({
		...view,
		readFile: (target, ...options) => {
			reads.add(path.relative(cwd, target));
			return view.readFile(target, ...options);
		},
		readdir: (target) => {
			enumerated.add(path.relative(cwd, target).split(path.sep).join("/"));
			return view.readdir(target);
		},
		...(view.prepare
			? {
					prepare: (binding, key, build, ...options) =>
						view.prepare(
							binding,
							key,
							(borrowed, signal) => build(observeInputs(borrowed), signal),
							...options,
						),
				}
			: {}),
	});
	const invocation = {
		...bound,
		filesystem: (view, request) => {
			executions++;
			return bound.filesystem(observeInputs(view), request);
		},
	};
	const world = createResourceSnapshotExecutionWorld(PI_ACTION_SEMANTICS, {
		maxBytes: () => 8 * 1024 * 1024,
	});
	const probe = searchJourney({
		cwd,
		name: "grep",
		tools: [createGrepTool(cwd)],
		args,
		invocation,
		world,
		settings: { resourceCacheMaxEntries: 32, resourceCacheMaxBytes: 16 * 1024 * 1024 },
	});
	let callID = 0;
	const actor = async () => (await probe.actor(`Actor-${++callID}`)).output;
	try {
		await probe.start("probe");
		const completion = await probe.candidate;
		assert.equal(
			completion.observation === "unobserved" ? completion.cause.code : completion.status,
			rejected === "unkeyable" ? "action_not_keyable" : rejected ? "failed" : "succeeded",
			JSON.stringify(completion),
		);
		const materialized = reads.size;
		if (stableChange) {
			assert.deepEqual(await changed(), expected);
			changed = undefined;
		}
		if (rejected) {
			if (expected instanceof Error) await assert.rejects(actor, { message: expected.message });
			else assert.deepEqual(await actor(), expected);
			assert.equal(executions, rejected === "unkeyable" ? 0 : 1);
			assert.equal(probe.actorCalls(), 1);
			return {
				producerCalls: executions,
				actorCalls: probe.actorCalls(),
				reads: [...reads],
				enumerated: [...enumerated],
				rejected,
				actorError: expected instanceof Error,
			};
		}
		assert.ok(reads.size || enumerated.size, "captured input observation must reach prepared views");
		assert.deepEqual(await actor(), expected);
		assert.equal(executions, 1);
		assert.equal(reads.size, materialized);
		assert.equal(probe.actorCalls(), 0);
		if (changed) {
			const next = await changed();
			assert.deepEqual(await actor(), next);
			assert.equal(probe.actorCalls(), 0);
		}
		return {
			producerCalls: executions,
			inputFilesRead: materialized,
			actorCalls: probe.actorCalls(),
			reads: [...reads],
			enumerated: [...enumerated],
		};
	} finally {
		await probe.host.dispose();
	}
}

async function qualifyNamespace() {
	const cwd = path.join(root, "namespace");
	await fs.mkdir(cwd);
	for (const directory of [
		".git",
		".git/info",
		"search",
		"search/nested",
		"search/empty",
		"search/blocked",
		"search/linked-config",
		"search/file-only",
		"search/file-only/deep",
		"search/syntax",
		"search/syntax/literal [x]",
		"search/syntax/!bang#",
		"rules",
	])
		await fs.mkdir(path.join(cwd, directory));
	const fixtures = {
		".git/HEAD": "ref: refs/heads/main\n",
		".git/info/exclude": "excluded.txt\n",
		".gitignore": "ignored.*\nsearch/blocked/\n",
		".ignore": "*.tmp\n",
		"search/.gitignore": "*.log\n",
		"search/nested/.gitignore": "*.txt\n!keep.txt\n",
		"search/a.txt": "needle without final newline",
		"search/z.txt": "before\r\nneedle Z\r\nafter\r\n",
		"search/nested/keep.txt": "needle nested\n",
		"search/nested/drop.txt": "needle excluded by nested rule\n",
		"search/blocked/inside.txt": "needle explicit ignored directory\n",
		"search/skip.tmp": "needle excluded by dot ignore\n",
		"search/skip.log": "needle excluded by gitignore\n",
		"search/excluded.txt": "needle excluded by git info\n",
		"search/ignored.txt": "needle excluded by root\n",
		"search/中文 name.txt": "n.e and NEEDLE unicode\n",
		"search/é.txt": "needle composed\n",
		"search/e\u0301.txt": "needle decomposed\n",
		"search/encoded\u00a0name.txt": "needle encoded nonbreaking name\n",
		"search/\ue000.txt": "needle BMP order\n",
		"search/\u{10000}.txt": "needle supplementary order\n",
		"search/utf16.txt": Buffer.concat([
			Buffer.from([0xff, 0xfe]),
			Buffer.from("before\r\nneedle unicode\r\nafter\r\n", "utf16le"),
		]),
		"search/binary.txt": Buffer.from("before\0needle binary\0after"),
		"search/long.txt": "needle " + "x".repeat(4096),
		"rules/shared-ignore": "hidden.txt\n",
		"search/linked-config/hidden.txt": "needle filtered by linked config\n",
		"search/linked-config/visible.txt": "needle visible beside linked config\n",
		"search/file-only/.ignore": "*\n!*/\n",
		"search/file-only/deep/.ignore": "!value.txt\n",
		"search/file-only/deep/value.txt": "needle reintroduced below excluded files\n",
		"search/syntax/.ignore": "*\n",
		"search/syntax/literal [x]/value.txt": "needle literal directory\n",
		"search/syntax/!bang#/value.txt": "needle punctuation directory\n",
	};
	for (const [name, contents] of Object.entries(fixtures)) await fs.writeFile(path.join(cwd, name), contents);
	const outside = path.join(root, "outside-namespace");
	await fs.mkdir(outside);
	await fs.writeFile(path.join(outside, "external.txt"), "needle outside captured namespace\n");
	const directoryLink = process.platform === "win32" ? "junction" : "dir";
	const cwdAlias = path.join(root, "cwd-alias");
	await fs.symlink(cwd, cwdAlias, directoryLink);
	for (const [name, target] of [
		["internal-link", path.join(cwd, "search/nested")],
		["external-link", outside],
		["dangling-link", path.join(cwd, "missing")],
		["cycle-link", path.join(cwd, "search")],
	]) {
		const parent =
			process.platform === "win32" && ["external-link", "dangling-link"].includes(name)
				? path.join(cwd, `edge-${name}`)
				: path.join(cwd, "search");
		await fs.mkdir(parent, { recursive: true });
		await fs.symlink(target, path.join(parent, name), directoryLink);
	}
	if (process.platform !== "win32") {
		await fs.symlink(path.join(cwd, "search/a.txt"), path.join(cwd, "search/file-link"));
		await fs.symlink(path.join(cwd, "rules/shared-ignore"), path.join(cwd, "search/linked-config/.ignore"));
	} else await fs.writeFile(path.join(cwd, "search/linked-config/.ignore"), fixtures["rules/shared-ignore"]);
	if (process.platform === "linux")
		await promisify(execFile)("mkfifo", [path.join(cwd, "search/input.pipe")]);
	const large = await fs.open(path.join(cwd, "search/ignored.bin"), "wx");
	try {
		await large.truncate(16 * 1024 * 1024);
	} finally {
		await large.close();
	}
	const configured = path.join(root, "configured-parent/workspace");
	const configurationCases = [];
	await fs.mkdir(path.join(configured, "search"), { recursive: true });
	await fs.mkdir(path.join(configured, "../.git"));
	for (const [name, contents] of Object.entries({
		"../.gitignore": "/workspace/search/by-git.txt\n",
		"../.ignore": "/workspace/search/by-ignore.txt\n",
		"../.rgignore": "/workspace/search/by-rg.txt\n",
		"search/by-git.txt": "needle parent git\n",
		"search/by-ignore.txt": "needle parent ignore\n",
		"search/by-rg.txt": "needle parent rg\n",
		"search/visible.txt": "needle visible\n",
	}))
		await fs.writeFile(path.join(configured, name), contents);
	await fs.symlink(path.join(configured, "search"), path.join(configured, "alias"), directoryLink);
	for (const mode of ["absolute", "dot-relative", "bare-relative", "missing-common"]) {
		const workspace = path.join(root, `git-${mode}`);
		const gitdir = path.join(root, `metadata-${mode}`, "worktree");
		const common =
			mode === "bare-relative" ? path.join(workspace, "local-common") : path.join(gitdir, "../common");
		for (const directory of [path.join(workspace, "search"), gitdir, path.join(common, "info")])
			await fs.mkdir(directory, { recursive: true });
		await fs.writeFile(
			path.join(workspace, ".git"),
			"gitdir: " +
				(mode === "absolute" ? gitdir : path.relative(workspace, gitdir)) +
				"\r\nsecond line ignored\n",
		);
		if (mode !== "missing-common")
			await fs.writeFile(
				path.join(gitdir, "commondir"),
				(mode === "absolute" ? common : mode === "dot-relative" ? "../common" : "local-common") +
					"\r\nsecond line ignored\n",
			);
		await fs.writeFile(path.join(common, "info/exclude"), "/search/excluded.txt\n");
		for (const name of ["visible", "excluded"])
			await fs.writeFile(path.join(workspace, `search/${name}.txt`), `needle ${name}\n`);
		configurationCases.push([
			`git-${mode}`,
			{},
			false,
			workspace,
			mode === "missing-common"
				? [path.join(gitdir, "commondir"), common + "\n"]
				: [path.join(common, "info/exclude"), "/search/visible.txt\n"],
		]);
	}
	for (const mode of ["directory", "file"]) {
		const workspace = path.join(root, `jj-${mode}`);
		await fs.mkdir(path.join(workspace, "search"), { recursive: true });
		if (mode === "directory") await fs.mkdir(path.join(workspace, ".jj"));
		else await fs.writeFile(path.join(workspace, ".jj"), "repository marker\n");
		for (const name of ["visible", "excluded"])
			await fs.writeFile(path.join(workspace, `search/${name}.txt`), `needle ${name}\n`);
		await fs.writeFile(path.join(workspace, ".gitignore"), "/search/excluded.txt\n");
		configurationCases.push([`jj-${mode}`, {}, false, workspace, [".gitignore", "/search/visible.txt\n"]]);
	}
	const checks = [];
	for (const [label, overrides, rejected, workspace = cwd, configurationMutation] of [
		["directory-ignore", { limit: 1000 }],
		["path-at-prefix", { path: "@search" }],
		["path-absolute", { path: path.join(cwd, "search") }],
		["path-file-url", { path: pathToFileURL(path.join(cwd, "search")).href }],
		[
			"path-encoded-name",
			{ path: pathToFileURL(path.join(cwd, "search/encoded\u00a0name.txt")).href, context: 1 },
		],
		[
			"path-home",
			{
				path: "~/" + path.relative(pathRules.homeDir, path.join(cwd, "search")).split(path.sep).join("/"),
				glob: "search/a.txt",
			},
		],
		["path-external", { path: outside }, "unkeyable"],
		["nested-search", { path: "search/nested" }],
		["glob", { glob: "**/{a,z}.txt" }],
		["glob-override", { glob: "*.tmp" }],
		["glob-relative", { glob: "search/nested/keep.txt" }],
		["glob-anchored", { glob: "/search/a.txt" }],
		["glob-negative", { glob: "!*.txt" }],
		["glob-root-negative", { glob: "!search/" }],
		["glob-positive-directory", { glob: "**/blocked{,/**}" }],
		["glob-negative-directory", { glob: "!**/nested/" }],
		["glob-literal-directory", { path: "search/syntax", glob: "**/literal*{,/**}", context: 1 }],
		["glob-config-data", { path: "search/syntax", pattern: ".", glob: "**" }],
		["mixed-encoding-context", { pattern: "(?P<word>needle)", context: 1 }],
		["literal-case", { pattern: "n.e", literal: true, ignoreCase: true }],
		["negative-query", { pattern: "not-present-anywhere" }],
		["invalid-pattern", { pattern: "[" }, true],
		["empty-selection-invalid-pattern", { pattern: "[", glob: "*.absent" }, true],
		["limit", { limit: 1 }],
		["file", { path: "search/utf16.txt", context: 1 }],
		["explicit-ignored-directory", { path: "search/blocked" }],
		["ignored-subtree-change", {}],
		["explicit-directory-link", { path: "search/internal-link" }],
		[
			"explicit-external-link",
			{ path: process.platform === "win32" ? "edge-external-link/external-link" : "search/external-link" },
		],
		["link-glob", { path: "search/internal-link", glob: "*.txt" }],
		["link-glob-relative", { path: "search/internal-link", glob: "search/internal-link/keep.txt" }],
		["link-glob-physical-name", { path: "search/internal-link", glob: "search/nested/keep.txt" }],
		["link-glob-directory", { path: "search/cycle-link", glob: "**/blocked{,/**}" }],
		["link-glob-negative-directory", { path: "search/cycle-link", glob: "!**/nested/" }],
		["cwd-alias-glob", { glob: "search/a.txt" }, false, cwdAlias],
		[
			"explicit-dangling-link",
			{ path: process.platform === "win32" ? "edge-dangling-link/dangling-link" : "search/dangling-link" },
			true,
		],
		...(process.platform === "win32"
			? [
					["discovered-external-link", { path: "edge-external-link" }],
					["discovered-dangling-link", { path: "edge-dangling-link" }, true],
				]
			: []),
		...(process.platform === "win32"
			? []
			: [
					["explicit-file-link", { path: "search/file-link" }],
					["linked-ignore-file", { path: "search/linked-config" }],
				]),
		["parent-config-alias", { path: "alias" }, false, configured],
		["parent-config", {}, false, configured, ["../.ignore", "/workspace/search/visible.txt\n"]],
		...configurationCases,
		[
			"git-pointer-data",
			{ path: ".", pattern: "gitdir: ", glob: ".git" },
			false,
			path.join(root, "git-absolute"),
		],
		["explicit-git-directory", { path: ".git", pattern: "ref:" }],
	]) {
		if (selectedCases.size && !selectedCases.has(label)) continue;
		const args = { path: "search", pattern: "needle", ...overrides };
		const reference = () => createGrepTool(workspace).execute("reference", args);
		const expected = await reference().catch((error) => {
			if (!rejected) throw error;
			return error;
		});
		if (label === "limit") assert.equal(expected.details?.matchLimitReached, 1);
		if (label.startsWith("parent-config") || configurationCases.some(([name]) => name === label)) {
			assert.match(expected.content[0].text, /needle visible/);
			// Existing rg versions differ on .jj recognition; the full result must match that pinned native engine.
			if (!label.startsWith("jj-"))
				assert.equal(
					expected.content[0].text.includes("needle excluded"),
					label === "git-missing-common",
					JSON.stringify({ label, expected }),
				);
			assert.ok(
				!expected.content[0].text.includes("needle parent"),
				"native ancestor rules must actually filter fixture files",
			);
		}
		const mutation =
			configurationMutation ??
			{
				"directory-ignore": ["search/z.txt", "needle changed after sealing\n"],
				"nested-search": ["search/nested/.gitignore", "*.txt\n!drop.txt\n"],
				"negative-query": ["search/arrived.txt", "not-present-anywhere\n"],
				"linked-ignore-file": ["rules/shared-ignore", "visible.txt\n"],
				"ignored-subtree-change": ["search/blocked/arrived.txt", "needle arrived inside ignored tree\n"],
			}[label];
		const changed = mutation
			? async () => {
					await fs.writeFile(path.resolve(workspace, mutation[0]), mutation[1]);
					return reference();
				}
			: undefined;
		if (profile?.cwd !== workspace) {
			await profile?.dispose();
			profile = { cwd: workspace, ...(await createClosedSearchProfile(workspace)) };
			preparedProfiles++;
		}
		const captured = await qualifyCaptured(
			workspace,
			profile,
			args,
			expected,
			changed,
			rejected,
			label === "ignored-subtree-change",
		);
		assert.ok(
			!captured.reads.some((file) => file.endsWith("ignored.bin")),
			"ignored payload must not consume the input budget",
		);
		if (!["explicit-ignored-directory", "glob-positive-directory", "link-glob-directory"].includes(label))
			assert.ok(
				!captured.enumerated.includes("search/blocked"),
				"ignored directory must not consume the enumeration budget",
			);
		if (label.endsWith("glob-negative-directory"))
			assert.ok(
				!captured.enumerated.includes("search/nested"),
				"negative glob directory must not consume the enumeration budget",
			);
		if (label !== "explicit-git-directory")
			assert.ok(
				!captured.enumerated.includes(".git"),
				"repository metadata uses its named configuration, not a recursive walk",
			);
		checks.push({
			label,
			outputBytes: expected instanceof Error ? 0 : Buffer.byteLength(JSON.stringify(expected)),
			...captured,
		});
		console.log(JSON.stringify({ semanticCase: checks.at(-1) }));
	}
	for (const label of selectedCases)
		assert.ok(
			checks.some((check) => check.label === label),
			`unknown/unavailable semantic case: ${label}`,
		);
	return {
		mode: "small-semantic-fixture",
		checks,
		qualification:
			"Full host key/route/transaction/adoption; content, ignore rules and negative names each invalidate before one Actor fallback.",
	};
}

import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

if (process.platform !== "linux") throw new Error("Run this probe inside Linux or WSL 2");

const root = await mkdtemp(path.join(os.tmpdir(), "pi-overlay-view-"));
const lower = path.join(root, "lower");
const upper = path.join(root, "upper");
const work = path.join(root, "work");
const merged = path.join(root, "merged");
await Promise.all([mkdir(lower), mkdir(upper), mkdir(work), mkdir(merged)]);
await writeFile(path.join(lower, "value.txt"), "lower\n", "utf8");

const helper = spawn(
	"unshare",
	[
		"--user",
		"--map-root-user",
		"--mount",
		"--propagation",
		"private",
		"/bin/sh",
		"-eu",
		"-c",
		String.raw`
mount -t overlay overlay \
  -o "lowerdir=$1,upperdir=$2,workdir=$3,userxattr,index=off,metacopy=off,redirect_dir=nofollow" \
  "$4"
trap 'umount "$4"' EXIT HUP INT TERM
printf 'ready\n'
IFS= read -r _
`,
		"overlay-view",
		lower,
		upper,
		work,
		merged,
	],
	{ stdio: ["pipe", "pipe", "pipe"] },
);
helper.stdin.on("error", () => undefined);
const helperClosed = new Promise<void>((resolve) => {
	helper.once("close", () => resolve());
});

try {
	await readyLine(helper);
	const externalView = `/proc/${helper.pid}/root${merged}`;
	const initial = await readFile(path.join(externalView, "value.txt"), "utf8");
	await writeFile(path.join(externalView, "value.txt"), "changed\n", "utf8");
	const copiedUp = await readFile(path.join(upper, "value.txt"), "utf8");
	process.stdout.write(
		`${JSON.stringify({ helperPid: helper.pid, initial, copiedUp, externalViewAccessible: true }, null, 2)}\n`,
	);
} finally {
	helper.stdin.end("stop\n");
	let closed = await waitForClose(helperClosed);
	if (!closed) {
		helper.kill("SIGKILL");
		closed = await waitForClose(helperClosed);
	}
	if (closed) await rm(root, { recursive: true, force: true });
	else throw new Error(`overlay helper did not close; retained probe storage at ${root}`);
}

function waitForClose(closed: Promise<void>): Promise<boolean> {
	return Promise.race([
		closed.then(() => true),
		new Promise<false>((resolve) => setTimeout(() => resolve(false), 2_000)),
	]);
}

function readyLine(child: ChildProcessWithoutNullStreams): Promise<void> {
	return new Promise((resolve, reject) => {
		let stdout = "";
		let stderr = "";
		child.stdout.setEncoding("utf8");
		child.stderr.setEncoding("utf8");
		child.stdout.on("data", (chunk) => {
			stdout += chunk;
			if (stdout.includes("\n")) resolve();
		});
		child.stderr.on("data", (chunk) => {
			stderr += chunk;
		});
		child.once("error", reject);
		child.once("exit", (code) => reject(new Error(`overlay helper exited ${code}: ${stderr}`)));
	});
}

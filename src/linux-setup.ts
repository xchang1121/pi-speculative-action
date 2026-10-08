import path from "node:path";
import { StringDecoder } from "node:string_decoder";
import { fileURLToPath } from "node:url";
import { stripVTControlCharacters } from "node:util";
import { BorderedLoader, createLocalBashOperations, type ExtensionContext, truncateToVisualLines } from "@earendil-works/pi-coding-agent";
import { errorMessage } from "./error-utils.ts";
import { inspectLinuxEnvironment } from "./linux-environment.ts";

/** Show host prerequisites separately from the controller's authoritative runtime diagnostics. */
export async function checkLinuxEnvironment(ctx: ExtensionContext, refresh: () => Promise<void>): Promise<void> {
	if (process.platform !== "linux") {
		ctx.ui.notify("Linux environment checks require Pi running inside Linux or WSL 2. Open this menu there.", "warning");
		return;
	}
	if (!ctx.isIdle()) { ctx.ui.notify("Wait for the current task to finish before checking the Linux environment.", "warning"); return; }
	const report = await inspectLinuxEnvironment();
	ctx.ui.notify(report.text, report.warnings ? "warning" : "info");
	if (!ctx.isIdle()) { ctx.ui.notify("The task is now running. Refresh execution diagnostics after it finishes.", "warning"); return; }
	await refresh();
}

/** The installer and its native sources ship in src for both TypeScript and dist entry points. */
export async function installLinuxDependencies(ctx: ExtensionContext, refresh: () => Promise<void>): Promise<void> {
	if (process.platform !== "linux") {
		ctx.ui.notify("Linux dependencies must be installed from Pi running inside Linux or WSL 2. Open this menu there.", "warning");
		return;
	}
	if (!ctx.isIdle()) {
		ctx.ui.notify("Wait for the current task to finish before installing Linux dependencies.", "warning");
		return;
	}
	const report = await inspectLinuxEnvironment();
	if (!ctx.isIdle()) { ctx.ui.notify("The task is now running. Retry installation after it finishes.", "warning"); return; }
	if (!await ctx.ui.confirm("Install / update Linux dependencies?",
		`${report.summary}\n\nDownload and build the packaged Sandlock, process helpers and strace, plus optional fuse-overlayfs, in ~/.local/bin. ` +
		"Requires Git, C/Rust toolchains, make, tar and xz; FUSE also needs fusermount and access to /dev/fuse. " +
		"Missing prerequisites may leave individual components unavailable. System packages and permissions are not changed. Building may take several minutes.")) return;
	if (!ctx.isIdle()) { ctx.ui.notify("The task is now running. Retry installation after it finishes.", "warning"); return; }
	const script = fileURLToPath(new URL("../src/setup-linux-process-backend.mjs", import.meta.url));
	const quote = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`;
	const result = await ctx.ui.custom<{ output: string; failed: boolean; cancelled: boolean }>((tui, theme, _keys, done) => {
		const loader = new BorderedLoader(tui, theme, "Installing Linux dependencies…");
		const lifetime = new AbortController(), decoder = new StringDecoder("utf8");
		const signal = AbortSignal.any([loader.signal, lifetime.signal]);
		let output = "", failed = false, finished = false;
		const append = (text: string) => {
			output = (output + stripVTControlCharacters(text).replace(/\r/g, "\n").replace(/[\x00-\x08\x0b-\x1f\x7f]/g, "")).slice(-16_384);
			tui.requestRender();
		};
		loader.addChild({ render: width => truncateToVisualLines(output, 10, width).visualLines, invalidate() {} });
		void createLocalBashOperations({ shellPath: "/bin/sh" }).exec(
			`exec ${quote(process.execPath)} ${quote(script)}`, path.dirname(script),
			{ signal, onData: data => append(decoder.write(data)) },
		).then(({ exitCode }) => {
			failed = exitCode !== 0;
			if (failed) append(`\nInstaller exited with ${exitCode ?? "a signal"}.\n`);
		}, error => { failed = true; append(`\n${errorMessage(error)}\n`); }).finally(() => {
			append(decoder.end());
			finished = true;
			done({ output, failed, cancelled: signal.aborted });
		});
		return {
			render: width => loader.render(width),
			invalidate: () => loader.invalidate(),
			handleInput: data => loader.handleInput(data),
			dispose() { if (!finished) lifetime.abort(); loader.dispose(); },
		};
	});
	const partial = /\bunavailable\b/i.test(result.output);
	const status = result.cancelled ? "Installation cancelled; completed components remain installed."
		: result.failed ? "Linux dependency installation failed."
		: partial ? "Installation finished with unavailable components." : "Linux dependency installation finished.";
	const detail = result.output.trim().split("\n").slice(-14).join("\n");
	ctx.ui.notify(`${status}\n${detail}\nInstaller output describes installation checks; runtime qualification is reported separately. Restart Pi to load updated helpers and recheck workspace drivers.`,
		result.cancelled ? "warning" : result.failed ? "error" : partial ? "warning" : "info");
	await refresh();
}

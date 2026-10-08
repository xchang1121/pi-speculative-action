import path from "node:path";
import { fileURLToPath } from "node:url";
import type { BashOperations, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { deferred, nextTurn } from "./async.ts";
import { installLinuxDependencies } from "../src/linux-setup.ts";

type Component = {
	render(width: number): string[];
	invalidate(): void;
	handleInput?(data: string): void;
	dispose?(): void;
};

const mocks = vi.hoisted(() => ({
	exec: vi.fn<BashOperations["exec"]>(),
	createLocalBashOperations: vi.fn(),
	disposeLoader: vi.fn(),
	truncate: vi.fn((text: string, maxLines: number, _width: number) => ({ visualLines: text.split("\n").slice(-maxLines) })),
}));

vi.mock("@earendil-works/pi-coding-agent", () => ({
	createLocalBashOperations: mocks.createLocalBashOperations,
	truncateToVisualLines: mocks.truncate,
	BorderedLoader: class {
		private readonly controller = new AbortController();
		private readonly children: Component[] = [];
		private readonly message: string;
		onAbort?: () => void;
		constructor(_tui: unknown, _theme: unknown, message: string) { this.message = message; }
		get signal() { return this.controller.signal; }
		addChild(child: Component) { this.children.push(child); }
		render(width: number) { return [this.message, ...this.children.flatMap(child => child.render(width))]; }
		invalidate() {}
		handleInput(data: string) { if (data === "\u001b") { this.controller.abort(); this.onAbort?.(); } }
		dispose() { mocks.disposeLoader(); }
	},
}));

const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
const execPathDescriptor = Object.getOwnPropertyDescriptor(process, "execPath")!;
const script = fileURLToPath(new URL("../src/setup-linux-process-backend.mjs", import.meta.url));
const readyOutput = "Linux reuse setup complete: cached command replay ready; child handoff ready; running process capture ready; speculative producer ready; OverlayFS ready.";

beforeEach(() => {
	Object.defineProperty(process, "platform", { ...platformDescriptor, value: "linux" });
	vi.clearAllMocks();
	mocks.exec.mockReset().mockResolvedValue({ exitCode: 0 });
	mocks.createLocalBashOperations.mockReturnValue({ exec: mocks.exec });
});

afterEach(() => {
	Object.defineProperty(process, "platform", platformDescriptor);
	Object.defineProperty(process, "execPath", execPathDescriptor);
});

function fixture() {
	const shown = deferred<Component>();
	let component: Component | undefined;
	const tui = { requestRender: vi.fn() };
	const ui = {
		confirm: vi.fn(async () => true),
		notify: vi.fn<ExtensionContext["ui"]["notify"]>(),
		custom: vi.fn((factory: Parameters<ExtensionContext["ui"]["custom"]>[0]) => new Promise<unknown>((resolve, reject) => {
			const created = factory(tui as never, { fg: (_color: string, text: string) => text } as never, {} as never,
				result => { component?.dispose?.(); resolve(result); });
			Promise.resolve(created).then(value => { component = value; shown.resolve(value); }, reject);
		})),
	};
	const context = { cwd: "/some/unrelated/user-project", mode: "tui", hasUI: true, isIdle: vi.fn(() => true), ui } as unknown as ExtensionContext;
	const refresh = vi.fn(async () => undefined);
	return { context, ui, refresh, tui, shown: shown.promise };
}

describe("Linux dependency installation UI", () => {
	it.each(["win32", "darwin"])("does not start installation on %s", async platform => {
		Object.defineProperty(process, "platform", { ...platformDescriptor, value: platform });
		const { context, ui, refresh } = fixture();
		await installLinuxDependencies(context, refresh);
		expect(ui.notify).toHaveBeenCalledWith(expect.stringMatching(/Linux.*WSL/u), "warning");
		expect(ui.confirm).not.toHaveBeenCalled();
		expect(ui.custom).not.toHaveBeenCalled();
		expect(mocks.createLocalBashOperations).not.toHaveBeenCalled();
		expect(mocks.exec).not.toHaveBeenCalled();
		expect(refresh).not.toHaveBeenCalled();
	});

	it.each([false, true])("requires an idle session even if a task starts during confirmation (%s)", async startsDuringConfirmation => {
		const { context, ui, refresh } = fixture();
		vi.mocked(context.isIdle).mockReturnValue(false);
		if (startsDuringConfirmation) vi.mocked(context.isIdle).mockReturnValueOnce(true);
		await installLinuxDependencies(context, refresh);
		expect(ui.notify).toHaveBeenCalledWith(expect.stringMatching(/task/u), "warning");
		expect(ui.confirm).toHaveBeenCalledTimes(startsDuringConfirmation ? 1 : 0);
		expect(mocks.exec).not.toHaveBeenCalled();
		expect(refresh).not.toHaveBeenCalled();
	});

	it("waits for explicit confirmation and does nothing when declined", async () => {
		const { context, ui, refresh } = fixture(), confirmation = deferred<boolean>();
		ui.confirm.mockReturnValueOnce(confirmation.promise);
		const installing = installLinuxDependencies(context, refresh);
		expect(ui.confirm).toHaveBeenCalledWith(expect.stringMatching(/Install/u), expect.stringContaining("~/.local/bin"));
		expect(mocks.exec).not.toHaveBeenCalled();
		confirmation.resolve(false);
		await installing;
		expect(ui.custom).not.toHaveBeenCalled();
		expect(mocks.exec).not.toHaveBeenCalled();
		expect(refresh).not.toHaveBeenCalled();
	});

	it("runs the shipped installer with the current Node executable, safely quoted and outside the project", async () => {
		Object.defineProperty(process, "execPath", { ...execPathDescriptor, value: "/opt/node tools/node'$(touch /tmp/unwanted)" });
		const { context, ui, refresh } = fixture();
		mocks.exec.mockImplementationOnce(async (_command, _cwd, { onData }) => {
			onData(Buffer.from(`${readyOutput}\n`));
			return { exitCode: 0 };
		});
		await installLinuxDependencies(context, refresh);
		expect(mocks.createLocalBashOperations).toHaveBeenCalledExactlyOnceWith({ shellPath: "/bin/sh" });
		const [command, cwd, options] = mocks.exec.mock.calls[0]!;
		expect(command).toBe(`exec '/opt/node tools/node'\\''$(touch /tmp/unwanted)' '${script}'`);
		expect(path.isAbsolute(script)).toBe(true);
		expect(cwd).toBe(path.dirname(script));
		expect(cwd).not.toBe(context.cwd);
		expect(command).not.toContain(context.cwd);
		expect(options.signal).toBeInstanceOf(AbortSignal);
		expect(options.signal?.aborted).toBe(false);
		expect(ui.notify).toHaveBeenLastCalledWith(expect.stringContaining(readyOutput), "info");
		expect(refresh).toHaveBeenCalledOnce();
		expect(mocks.disposeLoader).toHaveBeenCalledOnce();
	});

	it("streams installer output while keeping refresh behind process settlement", async () => {
		const { context, ui, refresh, tui, shown } = fixture(), completion = deferred<{ exitCode: number }>();
		mocks.exec.mockReturnValueOnce(completion.promise);
		const installing = installLinuxDependencies(context, refresh), component = await shown;
		const options = mocks.exec.mock.calls[0]![2];
		options.onData(Buffer.from("Building Sandlock pinned revision…\n"));
		expect(component.render(100).join("\n")).toContain("Building Sandlock pinned revision…");
		expect(tui.requestRender).toHaveBeenCalled();
		expect(ui.notify).not.toHaveBeenCalled();
		expect(refresh).not.toHaveBeenCalled();
		options.onData(Buffer.from(readyOutput));
		completion.resolve({ exitCode: 0 });
		await installing;
		expect(ui.notify).toHaveBeenLastCalledWith(expect.stringContaining(readyOutput), "info");
		expect(refresh).toHaveBeenCalledOnce();
	});

	it("reports unavailable components as a warning even when the installer exits successfully", async () => {
		const { context, ui, refresh } = fixture();
		const partial = "Linux reuse setup complete: cached command replay ready; child handoff ready; running process capture unavailable; speculative producer unavailable; OverlayFS ready.";
		mocks.exec.mockImplementationOnce(async (_command, _cwd, { onData }) => {
			onData(Buffer.from("Rust stable is required to build Sandlock.\n"));
			onData(Buffer.from(partial));
			return { exitCode: 0 };
		});
		await installLinuxDependencies(context, refresh);
		expect(ui.notify).toHaveBeenLastCalledWith(expect.stringContaining(partial), "warning");
		expect(ui.notify.mock.calls.at(-1)?.[0]).toContain("Rust stable is required");
		expect(refresh).toHaveBeenCalledOnce();
	});

	it.each([2, null])("shows the actual output and failure when the installer exits with %s", async exitCode => {
		const { context, ui, refresh } = fixture();
		mocks.exec.mockImplementationOnce(async (_command, _cwd, { onData }) => {
			onData(Buffer.from("compiler: fatal error: sys/ptrace.h: No such file or directory\n"));
			return { exitCode };
		});
		await installLinuxDependencies(context, refresh);
		expect(ui.notify).toHaveBeenLastCalledWith(expect.stringContaining("compiler: fatal error: sys/ptrace.h"), "error");
		expect(ui.notify.mock.calls.at(-1)?.[0]).toContain(`Installer exited with ${exitCode ?? "a signal"}`);
		expect(refresh).toHaveBeenCalledOnce();
	});

	it("shows rejected process errors together with output already received", async () => {
		const { context, ui, refresh } = fixture();
		mocks.exec.mockImplementationOnce(async (_command, _cwd, { onData }) => {
			onData(Buffer.from("Preparing Linux helpers\n"));
			throw new Error("spawn /bin/sh EACCES");
		});
		await installLinuxDependencies(context, refresh);
		expect(ui.notify).toHaveBeenLastCalledWith(expect.stringContaining("spawn /bin/sh EACCES"), "error");
		expect(ui.notify.mock.calls.at(-1)?.[0]).toContain("Preparing Linux helpers");
		expect(refresh).toHaveBeenCalledOnce();
		expect(mocks.disposeLoader).toHaveBeenCalledOnce();
	});

	it.each(["escape", "dispose"])("aborts on %s and waits for child cleanup before refreshing", async cancel => {
		const { context, ui, refresh, shown } = fixture(), completion = deferred<{ exitCode: number | null }>();
		mocks.exec.mockReturnValueOnce(completion.promise);
		let finished = false;
		const installing = installLinuxDependencies(context, refresh).then(() => { finished = true; });
		const component = await shown, options = mocks.exec.mock.calls[0]![2];
		options.onData(Buffer.from("Held-exec Actor boundary ready: ~/.local/bin/pi-speculative-held-exec\n"));
		if (cancel === "escape") component.handleInput?.("\u001b");
		else component.dispose?.();
		expect(options.signal?.aborted).toBe(true);
		await nextTurn();
		expect(finished).toBe(false);
		expect(ui.notify).not.toHaveBeenCalled();
		expect(refresh).not.toHaveBeenCalled();
		completion.resolve({ exitCode: null });
		await installing;
		expect(ui.notify).toHaveBeenLastCalledWith(expect.stringMatching(/cancelled; completed components remain installed/u), "warning");
		expect(ui.notify.mock.calls.at(-1)?.[0]).toContain("Held-exec Actor boundary ready");
		expect(refresh).toHaveBeenCalledOnce();
	});

	it("bounds the live output tail and final report while retaining the final capability summary", async () => {
		const { context, ui, refresh, shown } = fixture(), completion = deferred<{ exitCode: number }>();
		mocks.exec.mockReturnValueOnce(completion.promise);
		const installing = installLinuxDependencies(context, refresh), component = await shown;
		const options = mocks.exec.mock.calls[0]![2];
		options.onData(Buffer.from(`old build output\n${"x".repeat(100_000)}\n`));
		for (let index = 0; index < 30; index++) options.onData(Buffer.from(`build step ${index}\n`));
		options.onData(Buffer.from(readyOutput));
		const rendered = component.render(100).join("\n");
		expect(rendered).toContain(readyOutput);
		expect(rendered).not.toContain("old build output");
		const retained = mocks.truncate.mock.calls.at(-1)![0];
		expect(retained.length).toBeLessThanOrEqual(16_384);
		expect(retained).not.toContain("old build output");
		completion.resolve({ exitCode: 0 });
		await installing;
		const report = ui.notify.mock.calls.at(-1)![0];
		expect(report).toContain(readyOutput);
		expect(report).not.toContain("build step 0\n");
		expect(report).toContain("Restart Pi");
		expect(report.split("\n").length).toBeLessThanOrEqual(16);
		expect(refresh).toHaveBeenCalledOnce();
	});

	it("decodes split UTF-8 output and removes terminal controls before displaying it", async () => {
		const { context, ui, refresh } = fixture(), bytes = Buffer.from("正在安装");
		mocks.exec.mockImplementationOnce(async (_command, _cwd, { onData }) => {
			onData(bytes.subarray(0, 2));
			onData(bytes.subarray(2));
			onData(Buffer.from("\r\u001b[31mcompiler warning\u001b[0m\u0007\n"));
			onData(Buffer.from(readyOutput));
			return { exitCode: 0 };
		});
		await installLinuxDependencies(context, refresh);
		const report = ui.notify.mock.calls.at(-1)![0];
		expect(report).toContain("正在安装\ncompiler warning\n");
		expect(report).not.toMatch(/[\u001b\u0007\ufffd]/u);
		expect(refresh).toHaveBeenCalledOnce();
	});
});

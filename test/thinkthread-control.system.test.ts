import { mkdtemp, rm } from "node:fs/promises";
import { getEventListeners } from "node:events";
import { createServer, type Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import { ClientConfig, MAX_CONTROL_FRAME_BYTES, SensitiveRequestFrame } from "@thinkthread/agent-posix";
import { describe, expect, it } from "vitest";
import { createThinkThreadClient } from "../src/thinkthread/control-transport.ts";

describe("ThinkThread control transport with a real socket peer", () => {
	it.each(["frame", "EOF", "oversize", "deadline", "abort"])("releases the control connection on %s", async (outcome) => {
		const directory = await mkdtemp(path.join(os.tmpdir(), "tt-transport-"));
		const endpoint = process.platform === "win32" ? `\\\\.\\pipe\\tt-${path.basename(directory)}` : path.join(directory, "control");
		const controller = new AbortController();
		const peers = new Set<Socket>();
		const server = createServer({ allowHalfOpen: true }, (socket) => {
			peers.add(socket);
			socket.on("error", () => undefined);
			socket.once("data", () => {
				if (outcome === "frame") socket.end("reply\n");
				if (outcome === "EOF") socket.end("partial");
				if (outcome === "oversize") socket.end(Buffer.alloc(MAX_CONTROL_FRAME_BYTES + 1, 65));
				if (outcome === "abort") controller.abort();
			});
			socket.once("end", () => { if (outcome !== "deadline") socket.end(); });
		});
		await new Promise<void>((resolve) => server.listen(endpoint, resolve));
		try {
			const client = createThinkThreadClient(new ClientConfig(endpoint, "test"), { timeoutMs: 100, signal: controller.signal });
			const pending = client.transport.roundTrip(new SensitiveRequestFrame(Buffer.from("request\n")));
			if (outcome === "frame") await expect(pending).resolves.toEqual(Buffer.from("reply"));
			else await expect(pending).rejects.toMatchObject({ delivery: "completion_unknown" });
			expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
		} finally {
			for (const peer of peers) peer.destroy();
			await new Promise<void>((resolve) => server.close(() => resolve()));
			await rm(directory, { recursive: true, force: true });
		}
	});
});

import { connect } from "node:net";
import { AgentPosixClient, ClientConfig, MAX_CONTROL_FRAME_BYTES, TransportError } from "@thinkthread/agent-posix";

/** The SDK owns authentication and schemas; this connection owns bounded byte delivery. */
export function createThinkThreadClient(
	config = ClientConfig.fromEnv(),
	options: { readonly timeoutMs?: number; readonly signal?: AbortSignal } = {},
): AgentPosixClient {
	return new AgentPosixClient(config, {
		roundTrip: (request) => new Promise((resolve, reject) => {
			if (options.signal?.aborted) return reject(new TransportError("Agent Control aborted", "not_sent"));
			const socket = connect(config.socketPath);
			const chunks: Buffer[] = [];
			let connected = false, settled = false, length = 0;
			const finish = (error?: Error, value?: Uint8Array) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				options.signal?.removeEventListener("abort", abort);
				socket.destroy();
				if (error) reject(new TransportError(error.message, connected ? "completion_unknown" : "not_sent"));
				else resolve(value!);
			};
			const abort = () => finish(new Error("Agent Control aborted"));
			const timer = setTimeout(() => finish(new Error("Agent Control deadline exceeded")), options.timeoutMs ?? 5_000);
			options.signal?.addEventListener("abort", abort, { once: true });
			socket.once("connect", () => {
				connected = true;
				socket.end(request.bytesForTransport(), (error?: Error | null) => { if (error) finish(error); });
			});
			socket.on("data", (chunk: Buffer) => {
				const newline = chunk.indexOf(10);
				const bytes = newline < 0 ? chunk : chunk.subarray(0, newline);
				length += bytes.length;
				if (length > MAX_CONTROL_FRAME_BYTES) return finish(new Error("Agent Control frame exceeds its size limit"));
				chunks.push(bytes);
				if (newline >= 0) finish(undefined, Buffer.concat(chunks, length));
			});
			socket.once("error", finish);
			socket.once("end", () => finish(new Error("Agent Control ended without a complete frame")));
			socket.once("close", () => finish(new Error("Agent Control connection closed")));
		}),
	});
}

import { connect } from "node:net";
import { createInterface } from "node:readline";

const socket = connect(process.env.PI_SUBCOMPUTE_SOCKET);
const lines = createInterface({ input: socket })[Symbol.asyncIterator]();
const receive = async () => JSON.parse((await lines.next()).value);
const send = (value) => socket.write(JSON.stringify(value) + "\n");
send({
	protocol: 1,
	op: "compute",
	context: process.env.PI_SUBCOMPUTE_CONTEXT,
	namespace: "cross-language.reverse",
	version: "1",
	key: process.argv[2] ?? "shared",
	inputResources: [{ path: "input.bin", scope: "content" }],
	envKeys: ["TRANSFORM_MODE"],
	environment: { TRANSFORM_MODE: process.env.TRANSFORM_MODE },
});
let reply = await receive();
const hit = reply.state === "hit";
if (reply.state === "miss") {
	const start = performance.now();
	const bytes = Buffer.from(reply.inputResources[0].content.value, "base64");
	const result = Buffer.from(bytes).reverse();
	send({
		op: "publish",
		value: { encoding: "base64", value: result.toString("base64") },
		durationMs: performance.now() - start,
	});
	reply = await receive();
}
socket.end();
if (reply.state === "error") throw new Error(reply.error);
process.stdout.write(JSON.stringify({ hit, value: reply.value }));

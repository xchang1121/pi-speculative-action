import { readFile } from "node:fs/promises";
import { connect } from "node:net";
import { createInterface } from "node:readline";

const socket = connect(process.env.PI_SUBCOMPUTE_SOCKET);
const lines = createInterface({ input: socket })[Symbol.asyncIterator]();
const receive = async () => JSON.parse((await lines.next()).value);
const send = (value) => socket.write(JSON.stringify(value) + "\n");
send({
	op: "compute",
	context: process.env.PI_SUBCOMPUTE_CONTEXT,
	namespace: "cross-language.reverse",
	key: process.argv[2] ?? "shared",
	inputResources: [{ path: "input.bin", scope: "content" }],
	includeInputContent: true,
	envKeys: ["TRANSFORM_MODE"],
	environment: { TRANSFORM_MODE: process.env.TRANSFORM_MODE },
});
let reply = await receive();
const hit = reply.state === "hit";
if (reply.state === "miss") {
	const start = performance.now();
	const bytes = Buffer.from(reply.inputResources[0].content.value, "base64");
	const token = reply.token;
	const include = process.argv[3] === "--include" && bytes.toString("utf8");
	let result = Buffer.from(bytes).reverse();
	if (include) {
		send({ op: "observe", token, reads: [{ path: include, scope: "content" }] });
		const observation = await receive();
		if (observation.state !== "observed" || observation.token !== token) throw new Error(JSON.stringify(observation));
		result = await readFile(include);
	}
	send({
		op: "publish", token,
		value: { encoding: "base64", value: result.toString("base64") },
		durationMs: performance.now() - start,
	});
	reply = await receive();
}
socket.end();
if (reply.state === "error") throw new Error(reply.error);
process.stdout.write(JSON.stringify({ hit, value: reply.value }));

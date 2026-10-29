import { randomUUID } from "node:crypto";
import {
	type AgentPosixClient,
	type DurableMethod,
	type OperationMap,
	type FsApplyParamsV1,
	type FsApplyV1,
	type FsRequestStatusV1,
	type FsRunKeyParamsV1,
	type FsRunKeyV1,
	type FsRunParamsV1,
	type FsRunV1,
	type FsSnapshotId,
	type FsSnapshotViewV1,
	FsWorkflows,
	parseRequestId,
	RejectedError,
	type RequestId,
	TransportError,
	WorkflowError,
} from "@thinkthread/agent-posix";
import { ThinkThreadDurableError, ThinkThreadRecoveryRequiredError } from "./errors.ts";
import { TimelineInterval } from "../task-timing.ts";

const MAX_INVOKE_ATTEMPTS = 3;
const STATUS_POLL_MS = 50;

export class DurableFsExecutor {
	private readonly client: AgentPosixClient;
	private readonly workflows: FsWorkflows;
	private readonly pendingCloses = new Set<RequestId>();

	constructor(client: AgentPosixClient) {
		this.client = client;
		this.workflows = new FsWorkflows(client);
	}

	requestID(): RequestId {
		return parseRequestId(`req-${randomUUID()}`);
	}

	async snapshotCreate(): Promise<FsSnapshotViewV1> {
		return this.execute("fs.snapshot.create", {});
	}

	runKeyWithInput(params: FsRunKeyParamsV1, input: Uint8Array): Promise<FsRunKeyV1> {
		return this.workflows.runKeyWithStdinBytes(params, input);
	}

	async runWithInput(
		params: Omit<FsRunParamsV1, "requestId">,
		input: Uint8Array,
		signal?: AbortSignal,
	): Promise<FsRunV1> {
		signal?.throwIfAborted();
		const requestID = this.requestID();
		const payload = await this.workflows.uploadBytes(input, requestID);
		if (signal?.aborted) {
			await this.client.fs.payloadRemove({ payloadId: payload.payloadId });
			signal.throwIfAborted();
		}
		const request = {
			...params,
			invocation: { ...params.invocation, stdinPayloadId: payload.payloadId },
		};
		try {
			return await this.execute("fs.run", request, signal, (params.limits?.timeoutMs ?? 120_000) + 5_000, requestID);
		} catch (error) {
			// Only a proven non-admission leaves the uploaded payload locally owned.
			if (error instanceof ThinkThreadDurableError && error.code === "not_sent") {
				await this.client.fs.payloadRemove({ payloadId: payload.payloadId });
				signal?.throwIfAborted();
			}
			throw error;
		}
	}

	async apply(params: Omit<FsApplyParamsV1, "requestId">): Promise<FsApplyV1> {
		return this.execute("fs.apply", params);
	}

	async snapshotRemove(snapshotID: FsSnapshotId): Promise<void> {
		await this.execute("fs.snapshot.remove", { snapshotId: snapshotID });
	}

	async drainCleanup(rounds = 3): Promise<number> {
		for (let round = 0; round < rounds && this.pendingCloses.size > 0; round++) {
			await Promise.all([...this.pendingCloses].map((requestID) => this.close(requestID)));
		}
		return this.pendingCloses.size;
	}

	private async execute<Method extends DurableMethod>(
		method: Method,
		params: Omit<OperationMap[Method]["params"], "requestId">,
		signal?: AbortSignal,
		timeoutMs = 5_000,
		requestID = this.requestID(),
	): Promise<OperationMap[Method]["result"]> {
		type Result = OperationMap[Method]["result"];
		const request = { ...params, requestId: requestID } as OperationMap[Method]["params"];
		const deadline = performance.now() + timeoutMs;
		let status: FsRequestStatusV1 | undefined, lastError: unknown, attempts = 0, terminal = false;
		const cancel = () => {
			if (method === "fs.run") void this.client.fs.requestCancel({ requestId: requestID }).catch(() => undefined);
		};
		signal?.addEventListener("abort", cancel, { once: true });
		try {
			for (;;) {
				if (!status) {
					if (attempts++ === MAX_INVOKE_ATTEMPTS) throw new ThinkThreadDurableError(method, requestID, "not_sent",
						`${method} could not be delivered after ${MAX_INVOKE_ATTEMPTS} attempts`, lastError);
					if (signal?.aborted) throw new ThinkThreadDurableError(method, requestID, "not_sent", "Cancelled before admission", signal.reason);
					try {
						const result = await this.client.invoke(method, request); terminal = true; return result;
					} catch (error) {
						lastError = error;
						const underlying = error instanceof WorkflowError ? error.cause : error;
						const transport = underlying instanceof TransportError ? underlying : undefined;
						if (transport?.delivery === "not_sent") continue;
						if (transport?.delivery !== "completion_unknown" && !(underlying instanceof RejectedError)) throw error;
						status = await this.status(method, requestID);
						if (!status && !transport) throw error;
					}
					continue;
				}
				if (status.method !== method) throw new ThinkThreadDurableError(method, requestID, "method_mismatch", `Request belongs to ${status.method}, not ${method}`);
				switch (status.state) {
					case "succeeded":
					case "cancelled":
						// A cancelled fs.run can still own a TARGET; its caller must receive and release it.
						terminal = true; return status.result as Result;
					case "failed":
						terminal = true; throw new ThinkThreadDurableError(method, requestID, status.error.code, status.error.message);
					case "needs_recovery":
						throw new ThinkThreadRecoveryRequiredError(method, requestID, status.error?.message ?? `${method} requires Runtime recovery`, status.error);
					case "closing":
						throw new ThinkThreadDurableError(method, requestID, "response_closing", "Response is closing without a locally retained result");
					case "accepted":
					case "running":
						if (performance.now() >= deadline) throw new ThinkThreadRecoveryRequiredError(method, requestID, "Request did not settle before its recovery deadline");
						if (signal?.aborted && method === "fs.run") await this.client.fs.requestCancel({ requestId: requestID }).catch(() => undefined);
						await new Promise((resolve) => setTimeout(resolve, STATUS_POLL_MS));
						status = (await this.status(method, requestID)) ?? missingStatus(method, requestID);
				}
			}
		} finally {
			if (terminal) await this.close(requestID);
			signal?.removeEventListener("abort", cancel);
		}
	}

	private async status(method: DurableMethod, requestID: RequestId): Promise<FsRequestStatusV1 | undefined> {
		try {
			return await this.client.fs.requestStatus({ requestId: requestID });
		} catch (error) {
			if (error instanceof RejectedError && error.response.error.code === "RequestNotFound") return undefined;
			throw new ThinkThreadRecoveryRequiredError(method, requestID, "Cannot reconcile admitted request", error);
		}
	}

	private async close(requestID: RequestId): Promise<void> {
		try {
			await TimelineInterval.outside(() => this.client.fs.requestClose({ requestId: requestID }));
			this.pendingCloses.delete(requestID);
		} catch (error) {
			if (error instanceof RejectedError && error.response.error.code === "RequestNotFound") {
				this.pendingCloses.delete(requestID);
				return;
			}
			this.pendingCloses.add(requestID);
		}
	}
}

function missingStatus(method: DurableMethod, requestID: RequestId): never {
	throw new ThinkThreadDurableError(
		method,
		requestID,
		"request_disappeared",
		`Durable request ${requestID} disappeared before reaching a terminal state`,
	);
}

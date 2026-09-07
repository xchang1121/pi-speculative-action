import { randomUUID } from "node:crypto";
import {
	type AgentPosixClient,
	type DurableMethod,
	type EmptyResultV1,
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
		const requestID = this.requestID();
		return this.execute("fs.snapshot.create", requestID, () =>
			this.client.fs.snapshotCreate({ requestId: requestID }),
		);
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
			...params, requestId: requestID,
			invocation: { ...params.invocation, stdinPayloadId: payload.payloadId },
		};
		try {
			return await this.execute("fs.run", requestID, () => this.client.fs.run(request), signal,
				(params.limits?.timeoutMs ?? 120_000) + 5_000);
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
		const requestID = this.requestID();
		return this.execute("fs.apply", requestID, () => this.client.fs.apply({ ...params, requestId: requestID }));
	}

	async snapshotRemove(snapshotID: FsSnapshotId): Promise<void> {
		const requestID = this.requestID();
		await this.execute<EmptyResultV1>("fs.snapshot.remove", requestID, () =>
			this.client.fs.snapshotRemove({ snapshotId: snapshotID, requestId: requestID }),
		);
	}

	cleanupBacklog(): number {
		return this.pendingCloses.size;
	}

	async drainCleanup(rounds = 3): Promise<number> {
		for (let round = 0; round < rounds && this.pendingCloses.size > 0; round++) {
			await Promise.all([...this.pendingCloses].map((requestID) => this.close(requestID)));
		}
		return this.pendingCloses.size;
	}

	private async execute<Result>(
		method: DurableMethod,
		requestID: RequestId,
		invoke: () => Promise<Result>,
		signal?: AbortSignal,
		timeoutMs = 5_000,
	): Promise<Result> {
		const deadline = performance.now() + timeoutMs;
		const cancel = () => {
			if (method === "fs.run") void this.client.fs.requestCancel({ requestId: requestID }).catch(() => undefined);
		};
		signal?.addEventListener("abort", cancel, { once: true });
		try {
			let lastError: unknown;
			for (let attempt = 0; attempt < MAX_INVOKE_ATTEMPTS; attempt++) {
				if (signal?.aborted) throw new ThinkThreadDurableError(method, requestID, "not_sent", "Cancelled before admission", signal.reason);
				try {
					const result = await invoke();
					await this.close(requestID);
					return result;
				} catch (error) {
					lastError = error;
					const underlying = error instanceof WorkflowError ? error.cause : error;
					const transport = underlying instanceof TransportError ? underlying : undefined;
					if (transport?.delivery === "not_sent") continue;
					if (transport?.delivery === "completion_unknown" || underlying instanceof RejectedError) {
						const status = await this.status(method, requestID);
						if (status === undefined) {
							if (transport) continue;
							throw error;
						}
						return await this.settleStatus<Result>(method, requestID, status, deadline, signal);
					}
					throw error;
				}
			}
			throw new ThinkThreadDurableError(
				method,
				requestID,
				"not_sent",
				`${method} could not be delivered after ${MAX_INVOKE_ATTEMPTS} attempts`,
				lastError,
			);
		} finally {
			signal?.removeEventListener("abort", cancel);
		}
	}

	private async settleStatus<Result>(
		method: DurableMethod,
		requestID: RequestId,
		initial: FsRequestStatusV1,
		deadline: number,
		signal?: AbortSignal,
	): Promise<Result> {
		let status = initial;
		for (;;) {
			if (status.method !== method) {
				throw new ThinkThreadDurableError(method, requestID, "method_mismatch", `Request belongs to ${status.method}, not ${method}`);
			}
			switch (status.state) {
				case "succeeded":
				case "cancelled": {
					// A cancelled fs.run can still own a TARGET; its caller must receive and release it.
					const result = status.result as Result;
					await this.close(requestID);
					return result;
				}
				case "failed": {
					await this.close(requestID);
					throw new ThinkThreadDurableError(method, requestID, status.error.code, status.error.message);
				}
				case "needs_recovery":
					throw new ThinkThreadRecoveryRequiredError(method, requestID, status.error?.message ?? `${method} requires Runtime recovery`, status.error);
				case "closing":
					throw new ThinkThreadDurableError(method, requestID, "response_closing", "Response is closing without a locally retained result");
				case "accepted":
				case "running":
					if (performance.now() >= deadline) {
						throw new ThinkThreadRecoveryRequiredError(method, requestID, "Request did not settle before its recovery deadline");
					}
					if (signal?.aborted && method === "fs.run") {
						await this.client.fs.requestCancel({ requestId: requestID }).catch(() => undefined);
					}
					await new Promise((resolve) => setTimeout(resolve, STATUS_POLL_MS));
					status = (await this.status(method, requestID)) ?? missingStatus(method, requestID);
					break;
			}
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
			await this.client.fs.requestClose({ requestId: requestID });
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

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
		const requestID = this.requestID();
		const request = { ...params, requestId: requestID };
		return this.execute("fs.run", requestID, () => this.workflows.runWithStdinBytes(request, input), signal);
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
	): Promise<Result> {
		if (signal?.aborted) throw abortError();
		const cancel = () => {
			if (method === "fs.run") void this.client.fs.requestCancel({ requestId: requestID }).catch(() => undefined);
		};
		signal?.addEventListener("abort", cancel, { once: true });
		try {
			let lastError: unknown;
			for (let attempt = 0; attempt < MAX_INVOKE_ATTEMPTS; attempt++) {
				try {
					const result = await invoke();
					await this.close(requestID);
					return result;
				} catch (error) {
					lastError = error;
					const transport = transportError(error);
					if (transport?.delivery === "not_sent") continue;
					if (transport?.delivery === "completion_unknown" || rejection(error)) {
						const status = await this.status(requestID);
						if (status === undefined) {
							if (transport) continue;
							throw error;
						}
						return await this.settleStatus<Result>(method, requestID, status, signal);
					}
					throw error;
				}
			}
			throw new ThinkThreadDurableError(
				method,
				requestID,
				"delivery_unresolved",
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
		signal?: AbortSignal,
	): Promise<Result> {
		let status = initial;
		for (;;) {
			if (status.method !== method) {
				throw new ThinkThreadDurableError(
					method,
					requestID,
					"method_mismatch",
					`Durable request ${requestID} belongs to ${status.method}, not ${method}`,
				);
			}
			switch (status.state) {
				case "succeeded": {
					const result = status.result as Result;
					await this.close(requestID);
					return result;
				}
				case "failed": {
					await this.close(requestID);
					throw new ThinkThreadDurableError(method, requestID, status.error.code, status.error.message);
				}
				case "cancelled":
					await this.close(requestID);
					throw abortError();
				case "needs_recovery":
					throw new ThinkThreadRecoveryRequiredError(
						method,
						requestID,
						status.error?.message ?? `${method} requires Runtime recovery`,
						status.error,
					);
				case "closing":
					throw new ThinkThreadDurableError(
						method,
						requestID,
						"response_closing",
						`${method} response is already closing without a locally retained result`,
					);
				case "accepted":
				case "running":
					if (signal?.aborted && method === "fs.run") {
						await this.client.fs.requestCancel({ requestId: requestID }).catch(() => undefined);
					}
					await delay(STATUS_POLL_MS);
					status = (await this.status(requestID)) ?? missingStatus(method, requestID);
					break;
			}
		}
	}

	private async status(requestID: RequestId): Promise<FsRequestStatusV1 | undefined> {
		try {
			return await this.client.fs.requestStatus({ requestId: requestID });
		} catch (error) {
			if (error instanceof RejectedError && error.response.error.code === "RequestNotFound") return undefined;
			throw error;
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

function transportError(error: unknown): TransportError | undefined {
	if (error instanceof TransportError) return error;
	if (error instanceof WorkflowError && error.cause instanceof TransportError) return error.cause;
	return undefined;
}

function rejection(error: unknown): RejectedError | undefined {
	if (error instanceof RejectedError) return error;
	if (error instanceof WorkflowError && error.cause instanceof RejectedError) return error.cause;
	return undefined;
}

function missingStatus(method: DurableMethod, requestID: RequestId): never {
	throw new ThinkThreadDurableError(
		method,
		requestID,
		"request_disappeared",
		`Durable request ${requestID} disappeared before reaching a terminal state`,
	);
}

function abortError(): Error {
	const error = new Error("ThinkThread fs operation aborted");
	error.name = "AbortError";
	return error;
}

function delay(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

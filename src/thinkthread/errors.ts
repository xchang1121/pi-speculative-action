import type { DurableMethod, RequestId } from "@thinkthread/agent-posix";

export class ThinkThreadDurableError extends Error {
	readonly method: DurableMethod;
	readonly requestID: RequestId;
	readonly code: string;

	constructor(method: DurableMethod, requestID: RequestId, code: string, message: string, cause?: unknown) {
		super(message, cause === undefined ? undefined : { cause });
		this.name = "ThinkThreadDurableError";
		this.method = method;
		this.requestID = requestID;
		this.code = code;
	}
}

export class ThinkThreadRecoveryRequiredError extends ThinkThreadDurableError {
	constructor(method: DurableMethod, requestID: RequestId, message: string, cause?: unknown) {
		super(method, requestID, "needs_recovery", message, cause);
		this.name = "ThinkThreadRecoveryRequiredError";
	}
}

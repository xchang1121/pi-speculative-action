import { createExecPrototype, sealProcessCertificate, sha256Digest, type ExecPrototype, type ProcessPrototypeInput, type ProcessProducerProof } from "../src/provenance-certificate.ts";

export const SPECULATIVE_PRODUCER: ProcessProducerProof = {
	observer: { provider: "test", fingerprint: "sha256:4f3ffc55e1940cebb5c92e16fe7d6e6091f561562fb8b0e044910473356abc2c" },
	execution: { authority: "speculative", confinement: { provider: "test", fingerprint: "sha256:9f1019cefff9d81735701d03105338cf8ef2f7ff0b21b2abedce983d883a8786" } },
};

export function processPrototype(overrides: Partial<ProcessPrototypeInput> = {}) {
	return createExecPrototype({
		executablePath: "/bin/tool",
		executableDigest: sha256Digest("tool"),
		argv: ["tool"],
		logicalCwd: "/workspace",
		environment: { MODE: "test" },
		umask: 0o22,
		processContextDigest: sha256Digest("process-context"),
		stdin: { type: "closed", eof: true },
		fileDescriptorTableComplete: true,
		inheritedFDs: [],
		platformFingerprint: "linux",
		...overrides,
	});
}

export function processCertificate(
	prototype: ExecPrototype,
	overrides: Partial<Omit<Parameters<typeof sealProcessCertificate>[0], "prototype">> = {},
) {
	return sealProcessCertificate({
		prototype,
		producer: SPECULATIVE_PRODUCER,
		dependencyCertificate: { complete: true, dependencies: [], taints: [] },
		result: { replayProfile: "buffered_noninteractive", journal: [], exit: { kind: "code", code: 0 } },
		...overrides,
	});
}

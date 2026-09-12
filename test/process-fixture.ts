import { createExecPrototype, sha256Digest, type ProcessPrototypeInput, type ProcessProducerProof } from "../src/provenance-certificate.ts";

export const SPECULATIVE_PRODUCER: ProcessProducerProof = {
	observer: { provider: "test", fingerprint: sha256Digest("observer-v1") },
	execution: { authority: "speculative", confinement: { provider: "test", fingerprint: sha256Digest("confinement-v1") } },
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

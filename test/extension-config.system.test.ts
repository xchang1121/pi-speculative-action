import path from "node:path";
import { writeFile } from "node:fs/promises";
import { fauxProvider } from "@earendil-works/pi-ai";
import {
	createAgentSession,
	DefaultResourceLoader,
	ModelRuntime,
	SessionManager,
	SettingsManager,
} from "@earendil-works/pi-coding-agent";
import { afterEach, expect, test, vi } from "vitest";
import { createSpeculativeActionExtension } from "../src/extension.ts";
import { createSpeculativeActionHost } from "../src/agent-integration.ts";
import { SpeculativeActionSettingsStore, type SpeculativeConfiguration } from "../src/config-schema.ts";
import { ProvenanceCertificateStore } from "../src/reuse-store.ts";
import { ProcessReusePlanner, sealProcessCertificate } from "../src/process-reuse.ts";
import { PROCESS_CERTIFICATE } from "./process-fixture.ts";
import { temporaryDirectories } from "./filesystem.ts";

const directories = temporaryDirectories("pi-extension-config-");
afterEach(async () => {
	vi.unstubAllEnvs();
	await directories.dispose();
});

test("extension storage GC retains accepted clock certificates and setSettings refreshes existing owners", async () => {
	const cwd = await directories.create();
	const agentDir = await directories.create();
	const storeRoot = path.join(agentDir, "cache");
	vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
	const initial = {
		enabled: false,
		drafterEnabled: false,
		config: {
			storage: { root: storeRoot, gcIntervalMs: 0 },
			processReuse: { replayAcceptedTaints: ["clock"] },
		},
	};
	await writeFile(path.join(agentDir, "speculative-action.json"), JSON.stringify(initial));
	const settings = new SpeculativeActionSettingsStore(cwd, agentDir);
	let configuration!: SpeculativeConfiguration;
	let store!: ProvenanceCertificateStore;
	const extension = createSpeculativeActionExtension({
		createSettingsStore: () => settings,
		createHost: (id, options) => {
			configuration = options.configuration!;
			store = configuration.run(() => new ProvenanceCertificateStore(storeRoot, { reservedRoot: true }));
			return createSpeculativeActionHost(id, options);
		},
	});
	const settingsManager = SettingsManager.inMemory({}, { projectTrusted: true });
	const loader = new DefaultResourceLoader({
		cwd,
		agentDir,
		settingsManager,
		noExtensions: true,
		extensionFactories: [extension],
	});
	await loader.reload();
	const actor = fauxProvider({ provider: "config-test", models: [{ id: "actor" }] });
	const modelRuntime = await ModelRuntime.create({
		authPath: path.join(agentDir, "auth.json"),
		modelsPath: null,
		refreshOnCreate: false,
	});
	modelRuntime.registerNativeProvider(actor.provider);
	const { session } = await createAgentSession({
		cwd,
		agentDir,
		model: actor.getModel(),
		modelRuntime,
		settingsManager,
		resourceLoader: loader,
		sessionManager: SessionManager.inMemory(cwd),
		tools: ["read"],
	});
	try {
		await session.bindExtensions({
			mode: "print",
			onError: (error) => {
				throw error;
			},
		});
		const certificate = sealProcessCertificate({
			...PROCESS_CERTIFICATE,
			dependencyCertificate: { ...PROCESS_CERTIFICATE.dependencyCertificate, taints: ["clock"] },
		});
		await store.put(certificate);
		const command = session.extensionRunner.getCommand("speculative-action")!;
		const context = session.extensionRunner.createCommandContext();
		const interact = async (choices: string[], values: string[] = [], discard = false) => {
			const notifications: string[] = [];
			await command.handler("", {
				...context,
				mode: "tui",
				ui: {
					...context.ui,
					select: async (_title, options) => {
						const prefix = choices.shift();
						if (!prefix) return undefined;
						const selected = options.find((option) => option.startsWith(prefix));
						expect(selected).toBeDefined();
						return selected;
					},
					input: async () => values.shift(),
					confirm: async () => discard,
					notify: (message) => {
						notifications.push(message);
					},
				},
			});
			expect(choices).toHaveLength(0);
			expect(values).toHaveLength(0);
			return notifications;
		};
		const collect = async () => {
			const gc = vi.spyOn(ProvenanceCertificateStore.prototype, "gc");
			try {
				expect(
					await interact(["Advanced settings", "Scheduling and storage", "Reclaim reusable command history"]),
				).toEqual([expect.stringMatching(/^Reusable command history reclaimed:/)]);
				expect(gc).toHaveBeenCalledTimes(1);
			} finally {
				gc.mockRestore();
			}
		};
		const tuning = ["Prediction sources", "Model Drafter", "Candidate requests"];
		expect(await interact([...tuning, "Back", "Back", "Apply changes", "Close"], ["3"])).toEqual([
			"Speculative-action settings applied.",
		]);
		expect(settings.effective()?.candidateLimit).toBe(3);
		expect(await interact([...tuning, "Back", "Back", "Close"], ["0"])).toEqual([
			"Invalid Candidate requests per Actor decision.",
		]);
		expect(settings.effective()?.candidateLimit).toBe(3);
		const applied = settings.effective();
		await interact(["Speculative production", "Close"], [], true);
		expect(settings.effective()).toEqual(applied);
		await collect();
		expect(await store.get(certificate.id)).toBeDefined();
		const plan = await new ProcessReusePlanner({ store }).plan({
			weakKey: certificate.weakKey,
			executablePath: certificate.prototype.executablePath,
			contract: { sink: "buffered", orderedJournal: true, transactionalEffects: true },
			validation: { acceptedTaints: ["clock"] },
		});
		expect(plan.kind).toBe("completed_replay");
		if (plan.kind === "completed_replay") await plan.artifacts.dispose();
		settings.setEffective({
			...initial,
			config: {
				...initial.config,
				processReuse: { replayAcceptedTaints: [] },
				subcomputation: { maxStepResultBytes: 64 },
			},
		});
		await command.handler("on", context);
		expect(configuration.current.processReuse.replayAcceptedTaints).toEqual([]);
		expect(configuration.current.subcomputation.maxStepResultBytes).toBe(64);
		await collect();
		expect(await store.get(certificate.id)).toBeUndefined();
	} finally {
		await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
		session.dispose();
		await store?.dispose();
	}
}, 30000);

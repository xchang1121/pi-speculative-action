import { describe, expect, it } from "vitest";
import { cpuAffinity, idleCpuCount } from "../src/system-resources.ts";

const cpu = (busy: number, idle: number) => ({ times: { user: busy, nice: 0, sys: 0, irq: 0, idle } });

describe("host resource observations", () => {
	it("uses allowed CPU IDs instead of the first available host CPUs", () => {
		const allowed = cpuAffinity("Name:\tnode\nCpus_allowed_list:\t2,4-5\nMems_allowed_list:\t0\n");
		expect(allowed).toEqual([2, 4, 5]);
		const before = Array.from({ length: 6 }, () => cpu(0, 0));
		const after = [cpu(0, 100), cpu(0, 100), cpu(100, 0), cpu(0, 100), cpu(75, 25), cpu(25, 75)];
		expect(idleCpuCount(before, after, 3, allowed)).toBe(1);
		expect(idleCpuCount(before, after, 1, [2])).toBe(0);
	});

	it("does not infer idle capacity from incomplete or reset counters", () => {
		expect(cpuAffinity("Cpus_allowed_list:\t5-2\n")).toBeUndefined();
		expect(cpuAffinity("Cpus_allowed_list:\t1-2-3\n")).toBeUndefined();
		expect(cpuAffinity("Cpus_allowed_list:\t1,,2\n")).toBeUndefined();
		expect(cpuAffinity("Cpus_allowed_list:\t0-999999999\n")).toBeUndefined();
		expect(idleCpuCount([cpu(10, 10)], [cpu(10, 10)], 1, [0])).toBeUndefined();
		expect(idleCpuCount([cpu(10, 10)], [cpu(0, 20)], 1, [0])).toBeUndefined();
		expect(idleCpuCount([cpu(0, 0)], [cpu(0, 10)], 1, [7])).toBeUndefined();
	});

	it("uses the busiest possible restricted subset when affinity IDs are unavailable", () => {
		const before = [cpu(0, 0), cpu(0, 0), cpu(0, 0)];
		const after = [cpu(0, 100), cpu(100, 0), cpu(50, 50)];
		expect(idleCpuCount(before, after, 1)).toBe(0);
		expect(idleCpuCount(before, after, 2)).toBe(0.5);
		expect(idleCpuCount(before, after, 3)).toBe(1.5);
	});
});

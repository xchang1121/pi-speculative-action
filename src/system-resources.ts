import os from "node:os";
import { readFile } from "node:fs/promises";

/** Scheduling hints only. They grant no execution or reuse authority. */
export interface ExecutionResourceSnapshot {
	/** CPUs this process can use, including its affinity/cpuset restriction. */
	readonly cpuCount: number;
	/** Idle CPU equivalents over the preceding sample; missing until a comparable sample exists. */
	readonly idleCpuCount?: number;
}

export interface ExecutionResourceMonitor {
	readonly initial: ExecutionResourceSnapshot;
	/** Called off the Actor path. A slow or failed sample must never hold up a tool call. */
	readonly sample: () => Promise<ExecutionResourceSnapshot>;
}

type CpuTimes = Pick<os.CpuInfo, "times">;

/** Linux lists CPU IDs, not a count: affinity to CPU 7 must not sample CPU 0. */
export function cpuAffinity(status: string): readonly number[] | undefined {
	const list = /^Cpus_allowed_list:\s*([\d,-]+)\s*$/m.exec(status)?.[1];
	if (!list) return;
	const cpus = new Set<number>();
	for (const part of list.split(",")) {
		if (!/^\d+(?:-\d+)?$/.test(part)) return;
		const [first, last = first] = part.split("-").map(Number);
		if (first === undefined || last === undefined || !Number.isSafeInteger(first) || !Number.isSafeInteger(last) ||
			first < 0 || last < first || last > 1_048_575 || cpus.size + last - first + 1 > 16_384) return;
		for (let cpu = first; cpu <= last; cpu++) cpus.add(cpu);
	}
	return [...cpus].sort((a, b) => a - b);
}

export function idleCpuCount(previous: readonly CpuTimes[], current: readonly CpuTimes[], cpuCount: number,
	allowed?: readonly number[]): number | undefined {
	const fractions: number[] = [];
	for (const index of allowed ?? current.map((_, index) => index)) {
		const before = previous[index]?.times, after = current[index]?.times;
		if (!before || !after) return;
		const deltas = (Object.keys(after) as (keyof typeof after)[]).map(key => after[key] - before[key]);
		const elapsed = deltas.reduce((sum, value) => sum + value, 0), idle = after.idle - before.idle;
		if (!Number.isFinite(elapsed) || elapsed <= 0 || deltas.some(value => value < 0)) return;
		fractions.push(Math.max(0, Math.min(1, idle / elapsed)));
	}
	if (fractions.length < cpuCount) return;
	// Where affinity IDs are unavailable, use the busiest possible subset instead of assuming idle host CPUs are usable.
	return fractions.sort((a, b) => a - b).slice(0, cpuCount).reduce((sum, value) => sum + value, 0);
}

export function createSystemResourceMonitor(): ExecutionResourceMonitor {
	let previous: readonly CpuTimes[] | undefined, previousAffinity: string | undefined;
	return {
		initial: { cpuCount: Math.max(1, os.availableParallelism()) },
		sample: async () => {
			const allowed = process.platform === "linux"
				? cpuAffinity(await readFile("/proc/self/status", "utf8").catch(() => "")) : undefined;
			const cpuCount = Math.max(1, Math.min(os.availableParallelism(), allowed?.length ?? Infinity));
			const current = os.cpus(), affinity = JSON.stringify([cpuCount, allowed]);
			const idle = previous && affinity === previousAffinity ? idleCpuCount(previous, current, cpuCount, allowed) : undefined;
			previous = current; previousAffinity = affinity;
			return { cpuCount, ...(idle === undefined ? {} : { idleCpuCount: idle }) };
		},
	};
}

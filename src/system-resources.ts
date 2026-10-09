import os from "node:os";
import { readFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

/** CPU/GPU equivalents, bytes for memory, and fractional I/O/network capacity. */
export const RESOURCE_DIMENSIONS = ["cpu", "memory", "io", "gpu", "gpuMemory", "network"] as const;
export type HardwareResources = Partial<Record<typeof RESOURCE_DIMENSIONS[number], number>>;

/** Scheduling hints only. They grant no execution or reuse authority. */
export interface ExecutionResourceSnapshot {
	/** CPUs this process can use, including its affinity/cpuset restriction. */
	readonly cpuCount: number;
	/** Idle CPU equivalents over the preceding sample; missing until a comparable sample exists. */
	readonly idleCpuCount?: number;
	readonly capacity?: HardwareResources;
	readonly available?: HardwareResources;
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
	let sampleSequence = 0, gpu: Pick<ExecutionResourceSnapshot, "capacity" | "available"> = {}, gpuUnavailable = false;
	const memory = () => ({ capacity: { memory: Math.min(os.totalmem(), process.constrainedMemory() || Infinity), io: 1 },
		available: { memory: Math.min(os.freemem(), process.availableMemory()) } });
	return {
		initial: { cpuCount: Math.max(1, os.availableParallelism()), ...memory() },
		sample: async () => {
			const allowed = process.platform === "linux"
				? cpuAffinity(await readFile("/proc/self/status", "utf8").catch(() => "")) : undefined;
			const cpuCount = Math.max(1, Math.min(os.availableParallelism(), allowed?.length ?? Infinity));
			const current = os.cpus(), affinity = JSON.stringify([cpuCount, allowed]);
			const idle = previous && affinity === previousAffinity ? idleCpuCount(previous, current, cpuCount, allowed) : undefined;
			previous = current; previousAffinity = affinity;
			const io = process.platform === "linux" ? ioAvailability(await readFile("/proc/pressure/io", "utf8").catch(() => "")) : undefined;
			// GPU telemetry is optional; never put a driver subprocess on the Actor path.
			if (!gpuUnavailable && sampleSequence++ % 4 === 0) {
				try { gpu = gpuAvailability((await promisify(execFile)("nvidia-smi", ["--query-gpu=memory.total,memory.free,utilization.gpu", "--format=csv,noheader,nounits"],
					{ timeout: 1000, windowsHide: true, maxBuffer: 64 * 1024 })).stdout) ?? {}; }
				catch (error) { gpu = {}; gpuUnavailable = (error as NodeJS.ErrnoException).code === "ENOENT"; }
			}
			const ram = memory();
			return { cpuCount, ...(idle === undefined ? {} : { idleCpuCount: idle }),
				capacity: { ...ram.capacity, ...gpu.capacity }, available: { ...ram.available, ...gpu.available, ...(io === undefined ? {} : { io }) } };
		},
	};
}

export function ioAvailability(pressure: string): number | undefined {
	const value = /^some\s+avg10=([\d.]+)/m.exec(pressure)?.[1];
	return value === undefined || !Number.isFinite(Number(value)) ? undefined : Math.max(0, 1 - Number(value) / 100);
}

export function gpuAvailability(csv: string): Pick<ExecutionResourceSnapshot, "capacity" | "available"> | undefined {
	const rows = csv.trim().split(/\r?\n/).map(line => line.split(",").map(value => Number(value.trim())));
	if (!rows.length || rows.some(row => row.length !== 3 || row.some(value => !Number.isFinite(value) || value < 0))) return;
	return { capacity: { gpu: rows.length, gpuMemory: rows.reduce((sum, row) => sum + row[0]!, 0) * 1024 * 1024 },
		available: { gpu: rows.reduce((sum, row) => sum + Math.max(0, 1 - row[2]! / 100), 0),
			gpuMemory: rows.reduce((sum, row) => sum + Math.min(row[0]!, row[1]!), 0) * 1024 * 1024 } };
}

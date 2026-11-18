import { statfsSync, type BigIntStats } from "node:fs";
import { expect, test, vi } from "vitest";
import { cachedCapture, fileIdentity, rememberCapture, settledFilesystemStat, settledIdentity } from "../src/filesystem-evidence.ts";

vi.mock("node:fs", async (original) => ({
	...(await original<typeof import("node:fs")>()),
	statfsSync: vi.fn(),
}));

function identity(dev: bigint): BigIntStats {
	return {
		dev,
		ino: 1n,
		size: 4n,
		mtimeNs: 1000000000n,
		ctimeNs: 1000000000n,
		isFile: () => true,
	} as BigIntStats;
}

test.for([0xef53n, 0x58465342n, 0x9123683en, 0xf2f52010n])(
	"local Linux filesystem %s uses a 100 ms quiet window",
	(type) => {
		const stat = identity(type);
		expect(settledIdentity(stat, 1099, type)).toBeUndefined();
		expect(settledIdentity(stat, 1100, type)).toBe(fileIdentity(stat));
		expect(settledIdentity(stat, 1100, 0x6969n)).toBeUndefined();
		expect(settledIdentity(stat, 3000, 0x6969n)).toBe(fileIdentity(stat));
	},
);

test("digest hits query the filesystem type once per device", () => {
	const query = vi.mocked(statfsSync);
	query.mockReturnValue({ type: 0xef53n } as ReturnType<typeof statfsSync>);
	for (const dev of [123401n, 123402n]) {
		const stat = identity(dev);
		rememberCapture({ identity: fileIdentity(stat), hash: "digest" }, 1100);
		for (let read = 0; read < 5; read++)
			expect(cachedCapture(stat, `/device-${dev}/input`)).toMatchObject({ hash: "digest", shared: true });
	}
	query.mockReturnValue({ type: 0x6969n } as ReturnType<typeof statfsSync>);
	const remote = identity(123403n);
	rememberCapture({ identity: fileIdentity(remote), hash: "remote" }, 1100);
	for (let read = 0; read < 3; read++) expect(cachedCapture(remote, "/remote/input")).toBeUndefined();
	expect(query).toHaveBeenCalledTimes(3);
});

test("directory entries settle only after both timestamps pass the device quiet window", () => {
	vi.mocked(statfsSync).mockReturnValue({ type: 0xef53n } as ReturnType<typeof statfsSync>);
	const dir = Object.assign(identity(123404n), { isFile: () => false, isDirectory: () => true });
	for (const field of ["mtimeNs", "ctimeNs"] as const) {
		const stat = Object.assign(Object.create(dir) as BigIntStats, { [field]: 1_000_000_001n });
		expect(settledFilesystemStat(stat, 1100, "/dir")).toBe(false);
		expect(settledFilesystemStat(stat, 1101, "/dir")).toBe(true);
	}
});

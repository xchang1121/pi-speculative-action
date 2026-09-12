import { execFile } from "node:child_process";

export function shellQuote(value: string): string {
	return `'${value.replaceAll("'", `'\\''`)}'`;
}

export function runProgram(executable: string, args: readonly string[], cwd?: string): Promise<string> {
	return new Promise((resolve, reject) => {
		execFile(executable, args, { encoding: "utf8", cwd }, (error, stdout, stderr) => {
			if (error) reject(new Error(`${executable} failed: ${stderr || error.message}`, { cause: error }));
			else resolve(stdout);
		});
	});
}

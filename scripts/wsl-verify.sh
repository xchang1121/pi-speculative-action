#!/usr/bin/env bash
# Run the Linux test lane inside WSL 2 on this checkout's HEAD plus uncommitted edits, on the Linux filesystem.
# Native helpers build into an isolated HOME, so the user's own Pi installation is untouched.
# From Windows: wsl.exe -e bash -lc 'bash "$(wslpath "C:/path/to/pi-speculative-action")/scripts/wsl-verify.sh" [vitest args]'
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
work=${PI_SPEC_WSL_WORK:-$HOME/.cache/pi-speculative-action/wsl}
clone=${PI_SPEC_WSL_CHECKOUT:-$work/checkout} home=${PI_SPEC_WSL_HOME:-$work/home}
mkdir -p "$work" "$home"
[ -d "$clone/.git" ] || git clone -q "$repo" "$clone"
git -C "$clone" fetch -q "$repo" HEAD && git -C "$clone" reset -q --hard FETCH_HEAD && git -C "$clone" clean -qfd -e node_modules
edits=$(mktemp) && git -C "$repo" diff HEAD --binary > "$edits"
[ -s "$edits" ] && git -C "$clone" apply --whitespace=nowarn "$edits"
rm -f "$edits" && cd "$clone"
[ -d node_modules ] && [ package-lock.json -ot node_modules ] || npm ci --no-audit --no-fund
CARGO_HOME=${CARGO_HOME:-$HOME/.cargo} RUSTUP_HOME=${RUSTUP_HOME:-$HOME/.rustup} PATH=$HOME/.cargo/bin:$PATH HOME=$home node ./src/setup-linux-process-backend.mjs
bin=$home/.local/bin
PI_SPEC_SANDLOCK=$bin/pi-speculative-sandlock PI_SPEC_HELD_EXEC=$bin/pi-speculative-held-exec PI_SPEC_STRACE=$bin/pi-speculative-strace \
	npx vitest --run --maxWorkers=1 --no-file-parallelism "$@"

#!/usr/bin/env bash
# Run the Linux test lane inside WSL 2 on this checkout's HEAD plus uncommitted edits, on the Linux filesystem.
# Native helpers build into an isolated HOME, so the user's own Pi installation is untouched.
# From Windows: wsl.exe -e bash -lc 'bash "$(wslpath "C:/path/to/pi-speculative-action")/scripts/wsl-verify.sh" [vitest args]'
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd -P)
work=${PI_SPEC_WSL_WORK:-$HOME/.cache/pi-speculative-action/wsl}
clone=$(realpath -m "${PI_SPEC_WSL_CHECKOUT:-$work/checkout}")
helper_home=${PI_SPEC_WSL_HOME:-$work/home}
case "$clone/" in "$repo/"|"$repo/"*) echo 'Verification checkout must be outside the source repository' >&2; exit 1;; esac
case "$repo/" in "$clone/"*) echo 'Verification checkout must not contain the source repository' >&2; exit 1;; esac
if [ -e "$clone" ] && { [ ! -d "$clone/.git" ] || [ "$(git -C "$clone" remote get-url origin 2>/dev/null)" != "$repo" ]; }; then
    echo 'Verification checkout must be a dedicated clone of this source repository' >&2; exit 1
fi
# Capture before any mirror mutation, including new files with arbitrary Git-valid names.
snapshot=$(mktemp -d)
trap 'rm -rf -- "$snapshot"' EXIT
git -C "$repo" diff HEAD --binary > "$snapshot/edits"
git -C "$repo" ls-files --others --exclude-standard -z | tar -C "$repo" --null -T - -cf "$snapshot/new.tar"
mkdir -p "$work" "$helper_home"
[ -d "$clone/.git" ] || git clone -q "$repo" "$clone"
git -C "$clone" fetch -q "$repo" HEAD && git -C "$clone" reset -q --hard FETCH_HEAD && git -C "$clone" clean -qfd -e node_modules
[ ! -s "$snapshot/edits" ] || git -C "$clone" apply --whitespace=nowarn "$snapshot/edits"
tar -C "$clone" -xf "$snapshot/new.tar"
cd "$clone"
[ -d node_modules ] && [ package-lock.json -ot node_modules ] || npm ci --no-audit --no-fund
CARGO_HOME=${CARGO_HOME:-$HOME/.cargo} RUSTUP_HOME=${RUSTUP_HOME:-$HOME/.rustup} PATH=$HOME/.cargo/bin:$PATH HOME=$helper_home node ./src/setup-linux-process-backend.mjs
bin=$helper_home/.local/bin
PI_SPEC_SANDLOCK=$bin/pi-speculative-sandlock PI_SPEC_HELD_EXEC=$bin/pi-speculative-held-exec PI_SPEC_STRACE=$bin/pi-speculative-strace \
	npx vitest --run --maxWorkers=1 --no-file-parallelism "$@"

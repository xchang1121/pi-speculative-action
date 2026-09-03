#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
    cat <<'EOF'
Install the self-contained ThinkThread Pi speculative-action Profile.

Usage:
  scripts/install-thinkthread-profile.sh \
    [--agent-posix-package FILE] \
    [--speculative-action-package FILE] [--model PROVIDER/MODEL ...] [--tt TT]

The installer writes only:
  ~/.local/share/pi-speculative-action
  ~/.config/thinkthread/profiles/pi-speculative-action.toml

After installation, launch from a target workspace with:
  tt pi-speculative-action

Options:
  --agent-posix-package FILE         Offline @thinkthread/agent-posix .tgz override
  --speculative-action-package FILE  Prebuilt speculative-action .tgz; defaults to this source tree
  --model PROVIDER/MODEL             Delegate one exact Child model; repeatable
  --tt TT                            ThinkThread host CLI; prefers /usr/bin/tt
  -h, --help                         Show this help
EOF
}

die() {
    printf 'install-thinkthread-profile: %s\n' "$*" >&2
    exit 1
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
package_root="$(cd -- "$script_dir/.." && pwd -P)"
install_root="$HOME/.local/share/pi-speculative-action"
profile_root="${XDG_CONFIG_HOME:-$HOME/.config}/thinkthread/profiles"
profile_path="$profile_root/pi-speculative-action.toml"
agent_posix_git_url="https://gitcode.com/aideveloper/capsule_public.git"
agent_posix_git_tag="v0.1.0"
agent_posix_git_commit="e7287acc187b4b17a9d2a0c8cad2f75f64ed538f"
agent_posix_version="${agent_posix_git_tag#v}"
sdk_package=""
sdk_source_kind="tarball"
speculative_package=""
tt_bin="${PI_SPECULATIVE_ACTION_TT:-}"
requested_models=()

while (($# > 0)); do
    case "$1" in
        --agent-posix-package)
            (($# >= 2)) || die "$1 requires a file"
            sdk_package="$2"
            shift 2
            ;;
        --speculative-action-package)
            (($# >= 2)) || die "$1 requires a file"
            speculative_package="$2"
            shift 2
            ;;
        --model)
            (($# >= 2)) || die "$1 requires provider/model"
            [[ "$2" == */* && "$2" != */ && "$2" != /* ]] || die "invalid model: $2"
            requested_models+=("$2")
            shift 2
            ;;
        --tt)
            (($# >= 2)) || die "$1 requires a command"
            tt_bin="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
done

command -v node >/dev/null 2>&1 || die "Node.js 22.19+ is required"
command -v npm >/dev/null 2>&1 || die "npm is required"
command -v tar >/dev/null 2>&1 || die "tar is required"
if [[ -z "$sdk_package" ]]; then
    command -v git >/dev/null 2>&1 || die "git is required to fetch the Agent POSIX SDK"
fi
node_version_ok="$(node -p 'const [major, minor] = process.versions.node.split(".").map(Number); major > 22 || (major === 22 && minor >= 19) ? "yes" : "no"')"
[[ "$node_version_ok" == yes ]] || die "Node.js 22.19+ is required; found $(node --version)"

if [[ -z "$tt_bin" ]]; then
    if [[ -x /usr/bin/tt ]]; then
        tt_bin=/usr/bin/tt
    else
        tt_bin="$(command -v tt || true)"
    fi
fi
[[ -n "$tt_bin" && -x "$tt_bin" ]] || die "the current ThinkThread tt binary is unavailable"

mkdir -p -- "$(dirname -- "$install_root")" "$profile_root"
transaction_root="$(mktemp -d "$(dirname -- "$install_root")/.pi-speculative-action-install.XXXXXX")"
payload_root="$transaction_root/payload"
previous_install="$transaction_root/previous-install"
previous_profile="$transaction_root/previous-profile.toml"
installed_new=false
profile_replaced=false
success=false

rollback_and_cleanup() {
    status=$?
    if [[ "$success" != true ]]; then
        if [[ "$profile_replaced" == true ]]; then
            rm -f -- "$profile_path"
            if [[ -f "$previous_profile" ]]; then
                mv -- "$previous_profile" "$profile_path"
            fi
        fi
        if [[ "$installed_new" == true ]]; then
            rm -rf -- "$install_root"
            if [[ -d "$previous_install" ]]; then
                mv -- "$previous_install" "$install_root"
            fi
        fi
    fi
    rm -rf -- "$transaction_root"
    exit "$status"
}
trap rollback_and_cleanup EXIT

package_output="$transaction_root/packages"
mkdir -p -- "$package_output" "$payload_root/runtime/packages" "$payload_root/config"

if [[ -z "$sdk_package" ]]; then
    sdk_release="$transaction_root/thinkthread-release"
    git init --quiet "$sdk_release"
    git -C "$sdk_release" remote add origin "$agent_posix_git_url"
    git -C "$sdk_release" fetch --quiet --depth 1 origin \
        "refs/tags/$agent_posix_git_tag:refs/tags/$agent_posix_git_tag"
    git -C "$sdk_release" -c advice.detachedHead=false checkout --quiet --detach \
        "$agent_posix_git_tag^{commit}"
    sdk_commit="$(git -C "$sdk_release" rev-parse HEAD)"
    [[ "$sdk_commit" == "$agent_posix_git_commit" ]] || \
        die "Agent POSIX release commit mismatch: expected $agent_posix_git_commit, found $sdk_commit"
    sdk_source="$sdk_release/sdk/agent-posix/ts"
    [[ -f "$sdk_source/package.json" && -f "$sdk_source/package-lock.json" ]] || \
        die "Agent POSIX release does not contain sdk/agent-posix/ts"
    (
        cd -- "$sdk_source"
        npm ci --ignore-scripts --no-audit --no-fund --loglevel=error
        npm run build
        npm pack --ignore-scripts --pack-destination "$package_output" --loglevel=error >/dev/null
    )
    sdk_package="$(find "$package_output" -maxdepth 1 -type f -name 'thinkthread-agent-posix-*.tgz' -print -quit)"
    sdk_source_kind="git"
fi
[[ -f "$sdk_package" ]] || die "Agent POSIX SDK package is unavailable: $sdk_package"

if [[ -z "$speculative_package" ]]; then
    speculative_source="$transaction_root/speculative-source"
    mkdir -p -- "$speculative_source"
    tar -C "$package_root" \
        --exclude=.git \
        --exclude=node_modules \
        --exclude=dist \
        -cf - . | tar -C "$speculative_source" -xf -
    (
        cd -- "$speculative_source"
        npm ci --ignore-scripts --no-audit --no-fund --loglevel=error
        npm install --ignore-scripts --no-save --package-lock=false --no-audit --no-fund --loglevel=error \
            "$sdk_package"
        npm run build
        npm pack --ignore-scripts --pack-destination "$package_output" --loglevel=error >/dev/null
    )
    speculative_package="$(find "$package_output" -maxdepth 1 -type f -name 'earendil-works-pi-speculative-action-*.tgz' -print -quit)"
fi
[[ -f "$speculative_package" ]] || die "speculative-action package is unavailable: $speculative_package"

sdk_archive="$payload_root/runtime/packages/agent-posix.tgz"
speculative_archive="$payload_root/runtime/packages/speculative-action.tgz"
install -m 0644 "$sdk_package" "$sdk_archive"
install -m 0644 "$speculative_package" "$speculative_archive"

speculative_version="$(tar -xOf "$speculative_archive" package/package.json | node -e 'let text=""; process.stdin.setEncoding("utf8"); process.stdin.on("data", chunk => text += chunk); process.stdin.on("end", () => process.stdout.write(JSON.parse(text).version));')"
[[ -n "$speculative_version" ]] || die "speculative-action package version is unavailable"

(
    cd -- "$payload_root/runtime"
    npm init --yes >/dev/null
    npm install --ignore-scripts --omit=dev --save-exact --no-audit --no-fund --loglevel=error \
        ./packages/agent-posix.tgz \
        ./packages/speculative-action.tgz \
        "@earendil-works/pi-agent-core@$speculative_version" \
        "@earendil-works/pi-ai@$speculative_version" \
        "@earendil-works/pi-coding-agent@$speculative_version"
)

install -m 0644 "$package_root/.thinkthread/speculative-action.json" \
    "$payload_root/config/speculative-action.json"

sdk_entry="$payload_root/runtime/node_modules/@thinkthread/agent-posix/dist/index.js"
runner_entry="$payload_root/runtime/node_modules/@earendil-works/pi-speculative-action/dist/thinkthread/tool-runner.js"
extension_entry="$payload_root/runtime/node_modules/@earendil-works/pi-speculative-action/dist/thinkthread/profile-extension.js"
[[ -f "$sdk_entry" ]] || die "installed Agent POSIX SDK has no dist/index.js"
[[ -f "$runner_entry" ]] || die "installed speculative-action package has no ThinkThread runner"
[[ -f "$extension_entry" ]] || die "installed speculative-action package has no ThinkThread extension"

node --input-type=module - \
    "$sdk_entry" \
    "$runner_entry" \
    "$sdk_archive" \
    "$payload_root/manifest.json" \
    "$speculative_version" \
    "$agent_posix_version" \
    "$sdk_source_kind" \
    "$agent_posix_git_url" \
    "$agent_posix_git_tag" \
    "$agent_posix_git_commit" <<'NODE'
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const expectedFingerprint = "fcc80b665cd990f9d1e3681a9d384cb99994f2b739cd4fbddc97bdda01391131";
const [
    sdkEntry,
    runnerEntry,
    sdkArchive,
    manifestPath,
    speculativeVersion,
    expectedSdkVersion,
    sdkSourceKind,
    sdkSourceUrl,
    sdkSourceTag,
    sdkSourceCommit,
] = process.argv.slice(2);
const sdk = await import(pathToFileURL(sdkEntry).href);
if (sdk.CONTROL_PROTOCOL_VERSION !== 2 || sdk.CONTRACT_FINGERPRINT !== expectedFingerprint) {
    throw new Error(`unsupported Agent POSIX SDK: protocol=${sdk.CONTROL_PROTOCOL_VERSION}, fingerprint=${sdk.CONTRACT_FINGERPRINT}`);
}
const sdkPackage = JSON.parse(await readFile(new URL("../package.json", pathToFileURL(sdkEntry)), "utf8"));
if (sdkPackage.version !== expectedSdkVersion) {
    throw new Error(`unsupported Agent POSIX SDK version: expected ${expectedSdkVersion}, found ${sdkPackage.version}`);
}
const runnerSha256 = createHash("sha256").update(await readFile(runnerEntry)).digest("hex");
const agentPosixTarballSha256 = createHash("sha256").update(await readFile(sdkArchive)).digest("hex");
const agentPosixSource = sdkSourceKind === "git"
    ? { kind: "git", url: sdkSourceUrl, tag: sdkSourceTag, commit: sdkSourceCommit }
    : { kind: "tarball" };
await writeFile(manifestPath, `${JSON.stringify({
    installedAt: new Date().toISOString(),
    speculativeActionVersion: speculativeVersion,
    agentPosixVersion: sdkPackage.version,
    agentPosixSource,
    agentPosixTarballSha256,
    contractFingerprint: sdk.CONTRACT_FINGERPRINT,
    executionBackendEpoch: "linux-execution-v10",
    runnerSha256,
}, null, 2)}\n`);
NODE

if [[ -d "$install_root" ]]; then
    mv -- "$install_root" "$previous_install"
fi
mv -- "$payload_root" "$install_root"
installed_new=true

if [[ -f "$profile_path" ]]; then
    mv -- "$profile_path" "$previous_profile"
fi
install -m 0600 "$package_root/.thinkthread/pi-speculative-action.toml" "$profile_path"
profile_replaced=true

"$tt_bin" profile show pi-speculative-action >/dev/null
for model in "${requested_models[@]}"; do
    "$tt_bin" model allow "$model" --profile pi-speculative-action
done

success=true
printf 'Installed speculative-action runtime: %s\n' "$install_root"
printf 'Installed ThinkThread Profile: %s\n' "$profile_path"
printf 'Launch from a target workspace with: tt pi-speculative-action\n'

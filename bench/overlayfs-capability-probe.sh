#!/bin/sh
set -eu

probe="$(mktemp -d "${TMPDIR:-/tmp}/pi-overlay-probe.XXXXXX")"
cleanup() {
	case "$probe" in
		"${TMPDIR:-/tmp}"/pi-overlay-probe.*)
			chmod -R u+rwX -- "$probe" 2>/dev/null || true
			rm -rf -- "$probe"
			;;
		*) printf '%s\n' "refusing to remove unexpected probe path: $probe" >&2 ;;
	esac
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$probe/lower" "$probe/upper" "$probe/work" "$probe/merged"
printf '%s\n' base > "$probe/lower/changed.txt"
printf '%s\n' deleted > "$probe/lower/deleted.txt"

unshare --user --map-root-user --mount --propagation private /bin/sh -eu -c '
	root="$1"
	mount -t overlay overlay \
		-o "lowerdir=$root/lower,upperdir=$root/upper,workdir=$root/work,userxattr,index=off,metacopy=off,redirect_dir=nofollow" \
		"$root/merged"
	trap '\''umount "$root/merged"'\'' EXIT HUP INT TERM
	printf "%s\n" changed > "$root/merged/changed.txt"
	rm -- "$root/merged/deleted.txt"
	mkdir "$root/merged/created"
	printf "%s\n" created > "$root/merged/created/value.txt"
	printf "merged-changed="
	cat "$root/merged/changed.txt"
	printf "upper-tree:\n"
	find "$root/upper" -mindepth 1 -printf "%y %P %s\n" | LC_ALL=C sort
' probe "$probe"

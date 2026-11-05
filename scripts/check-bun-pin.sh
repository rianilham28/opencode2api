#!/bin/sh
# Assert the vendored BoringSSL still IS the TLS identity the client ships.
#
# The ClientHello is the credential, so the TLS tree is a build input pinned to
# Bun's own. `scripts/fetch-boringssl.sh` names two facts — the Bun release the
# OpenCode client bundles (`bun_ref`) and the BoringSSL commit that release
# pins (`commit`) — and this script checks all three things a pin can drift
# against, because a stale tree is a silent 429, not a build error:
#
#   1. OFFLINE  the checkout in vendor/boringssl is at `commit`;
#   2. ONLINE   the `bun_ref` release tag still pins that same commit;
#   3. CLIENT   the installed client embeds a Bun whose release IS `bun_ref`.
#
# A failure here does NOT mean "bump the pin". It means: capture the new
# client's ClientHello, re-run the shape gate, and only then move `commit` —
# the full-shape test in crates/opencode2api-transport is what decides whether
# the new tree is still the same hello.
#
# Offline only: PIN_CHECK_OFFLINE=1. Different client binary: OPENCODE_BIN=…
set -eu

root=$(unset CDPATH; cd -- "$(dirname -- "$0")/.." && pwd)
fetch_script="$root/scripts/fetch-boringssl.sh"
bun_ref=$(sed -n 's/^bun_ref=//p' "$fetch_script" | head -1)
commit=$(sed -n 's/^commit=//p' "$fetch_script" | head -1)
if [ -z "${bun_ref:-}" ] || [ -z "${commit:-}" ]; then
    echo "check-bun-pin: scripts/fetch-boringssl.sh must name bun_ref= and commit=" >&2
    exit 1
fi

if [ ! -d "$root/vendor/boringssl/.git" ]; then
    echo "check-bun-pin: no checkout at vendor/boringssl — run scripts/fetch-boringssl.sh" >&2
    exit 1
fi
have=$(git -C "$root/vendor/boringssl" rev-parse HEAD 2>/dev/null || true)
if [ "$have" != "$commit" ]; then
    echo "check-bun-pin: checkout is $have, fetch script pins $commit" >&2
    echo "  run scripts/fetch-boringssl.sh to move the tree, then cargo test -p opencode2api-transport client_hello" >&2
    exit 1
fi
echo "checkout matches the fetch pin: $commit"

# 3. The installed client's runtime. The OpenCode binary is a Bun-compiled
# executable and carries its own version banner; a client that moved to a new
# Bun is the earliest signal that the pin needs a fresh capture.
client="${OPENCODE_BIN:-$HOME/.opencode/bin/opencode}"
if [ -r "$client" ] && command -v strings >/dev/null 2>&1; then
    bun_version=$(strings -a "$client" 2>/dev/null \
        | sed -n 's/^Bun v\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\).*/\1/p' \
        | head -1)
    if [ -n "${bun_version:-}" ] && [ "bun-v$bun_version" != "$bun_ref" ]; then
        echo "check-bun-pin: the client at $client embeds Bun v$bun_version," >&2
        echo "  but this tree pins $bun_ref. Capture the client's hello, re-gate it," >&2
        echo "  then update bun_ref= and commit= in scripts/fetch-boringssl.sh." >&2
        exit 1
    fi
    [ -n "${bun_version:-}" ] && echo "installed client embeds Bun v$bun_version ($bun_ref)"
fi

if [ "${PIN_CHECK_OFFLINE:-0}" = "1" ]; then
    echo "offline: skipped the upstream tag check"
    exit 0
fi

# 2. Upstream: does that release still pin this commit? Fetched by tag, not by
# branch, because `main` moves ahead of what any shipped client contains.
url="https://raw.githubusercontent.com/oven-sh/bun/${bun_ref}/scripts/build/deps/boringssl.ts"
if ! upstream=$(curl -fsSL --max-time 30 "$url" 2>/dev/null); then
    echo "check-bun-pin: could not read $url (network?)" >&2
    echo "  set PIN_CHECK_OFFLINE=1 to check only the local checkout" >&2
    exit 2
fi
tagged=$(printf '%s\n' "$upstream" \
    | sed -n 's/^const BORINGSSL_COMMIT = "\([0-9a-f]*\)";/\1/p' \
    | head -1)
if [ -z "${tagged:-}" ]; then
    echo "check-bun-pin: $bun_ref has no BORINGSSL_COMMIT line (format moved?)" >&2
    exit 2
fi
if [ "$tagged" != "$commit" ]; then
    echo "check-bun-pin: $bun_ref pins $tagged, this tree pins $commit" >&2
    echo "  re-capture the client's hello before moving the pin" >&2
    exit 1
fi
echo "$bun_ref still pins the same BoringSSL: $commit"

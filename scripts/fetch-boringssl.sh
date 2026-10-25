#!/bin/sh
# Fetch the BoringSSL commit Bun itself pins, for the vendor leg's ClientHello.
#
# Why a checkout instead of `boring-sys`'s bundled tree: the handshake is the
# vendor identity (a foreign TLS stack gets 429 while byte-identical requests
# over Bun's stack get 200), so the TLS library must be the one the client
# ships. `.cargo/config.toml` points `BORING_BSSL_SOURCE_PATH` at this
# directory; `crates/opencode2api-transport`'s JA3 gate then fails the build
# if the tree stops producing Bun's measured fingerprint.
set -eu

# scripts/build/deps/boringssl.ts:27 in oven-sh/bun (bun-v1.4.4).
commit=41bf9b59c2ebf277a7aa427e1ecad5cc80dd4d4f
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
dest="$root/vendor/boringssl"

if [ -d "$dest/.git" ]; then
    have=$(git -C "$dest" rev-parse HEAD 2>/dev/null || true)
    if [ "$have" = "$commit" ]; then
        echo "boringssl: $commit already checked out at $dest"
        exit 0
    fi
fi

mkdir -p "$dest"
if [ ! -d "$dest/.git" ]; then
    git -C "$dest" init -q
    git -C "$dest" remote add origin https://github.com/oven-sh/boringssl
fi
# A specific commit, not a branch: fetching by SHA is why this is a `git
# fetch` rather than a `--branch` clone.
git -C "$dest" fetch -q --depth 1 origin "$commit"
git -C "$dest" checkout -q --detach FETCH_HEAD
echo "boringssl: checked out $commit at $dest"

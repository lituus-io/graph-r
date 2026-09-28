#!/usr/bin/env bash
# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# Run graph-r's CI gates locally, in CI order: fmt, clippy, feature matrix,
# docs, every test suite (incl. the 0.2.1 golden fixtures), the Python wheel
# and its pytest suite, the mirror crate (memory, local directory and the GCS
# emulator when Docker is available), benches in test mode, short fuzz runs,
# supply-chain checks and an API semver check against the previous tag.
# Expects the ../link-r and ../objstore-r siblings. Never touches the cloud.
set -euo pipefail
cd "$(dirname "$0")/.."
export RUSTFLAGS="${RUSTFLAGS:--D warnings}"
FUZZ_SECS="${FUZZ_SECS:-20}"
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
warn() { printf '\033[33mskip: %s\033[0m\n' "$*"; }
have() { cargo "$1" --version >/dev/null 2>&1; }

step fmt; cargo fmt --check; (cd mirror && cargo fmt --check)
step clippy; cargo clippy -q --all-features --all-targets
step "feature matrix"
for f in "" bridge llm bridge,llm; do
  if [ -z "$f" ]; then cargo check -q --no-default-features --all-targets; else cargo check -q --no-default-features --features "$f" --all-targets; fi
done
step docs; RUSTDOCFLAGS="-D warnings" cargo doc -q --all-features --no-deps
step "tests"; PROPTEST_CASES="${PROPTEST_CASES:-256}" cargo test -q --all-features
step "benches (test mode)"; cargo bench -q --all-features -- --test >/dev/null

step "python wheel + pytest"
if command -v python3 >/dev/null; then
  (cd bindings/python && { [ -d .venv ] || python3 -m venv .venv; } && . .venv/bin/activate \
    && pip install -q maturin pytest && cargo fmt --check && cargo clippy -q --all-targets \
    && maturin develop -q --release && pytest -q tests/)
else warn "python3 not found"; fi

started=""
if [ -z "${OBJSTORE_FAKE_GCS:-}" ] && command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker rm -f graphr-ci-fakegcs >/dev/null 2>&1 || true
  docker run -d --name graphr-ci-fakegcs -p 4445:4445 fsouza/fake-gcs-server \
    -scheme http -port 4445 -public-host 127.0.0.1:4445 -external-url http://127.0.0.1:4445 >/dev/null
  started=1
  for _ in $(seq 1 30); do curl -sf "http://127.0.0.1:4445/storage/v1/b?project=test" >/dev/null && break; sleep 1; done
  export OBJSTORE_FAKE_GCS=http://127.0.0.1:4445
fi
trap '[ -n "$started" ] && docker rm -f graphr-ci-fakegcs >/dev/null 2>&1 || true' EXIT
step "mirror"
[ -n "${OBJSTORE_FAKE_GCS:-}" ] || warn "no Docker: the GCS emulator test is skipped"
(cd mirror && cargo clippy -q --all-features --all-targets && PROPTEST_CASES="${PROPTEST_CASES:-256}" cargo test -q --all-features \
  && cargo bench -q --all-features -- --test >/dev/null)

step "fuzz (${FUZZ_SECS}s per target)"
if have fuzz; then
  for t in fuzz_base_parse fuzz_wal_replay fuzz_op_decode fuzz_postings_decode fuzz_wal_committed_len; do
    (cd fuzz && cargo fuzz run "$t" -- -max_total_time="$FUZZ_SECS" 2>&1 | tail -1)
  done
  (cd mirror/fuzz && cargo fuzz run fuzz_manifest -- -max_total_time="$FUZZ_SECS" 2>&1 | tail -1)
else warn "cargo-fuzz not installed"; fi

step "supply chain"
if have deny; then cargo deny check; (cd mirror && cargo deny --manifest-path Cargo.toml check --config ../deny.toml); else warn "cargo-deny not installed"; fi
if have audit; then cargo audit; else warn "cargo-audit not installed"; fi

step "semver vs previous tag"
if have semver-checks; then
  cur="v$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys;print(json.load(sys.stdin)["packages"][0]["version"])')"
  prev=$(git tag --sort=-v:refname --merged HEAD | grep -v "^$cur$" | head -1)
  base="$(cd .. && pwd)/.graph-r-semver-baseline"
  git worktree remove --force "$base" >/dev/null 2>&1 || rm -rf "$base"
  git worktree add -q --detach "$base" "$prev"
  cargo semver-checks --baseline-root "$base" --all-features --release-type minor
  git worktree remove --force "$base"
else warn "cargo-semver-checks not installed"; fi

step "all gates passed"

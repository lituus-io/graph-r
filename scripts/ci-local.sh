#!/usr/bin/env bash
# Author: support@lituus.io
# Copyright: lituus-io, all rights reserved
#
# Run graph-r's CI gates locally, in CI order: fmt, clippy, feature matrix,
# docs, every test suite, the Python wheel
# and its pytest suite, benches in test mode, short fuzz runs,
# supply-chain checks and an API semver check against the previous tag.
# Expects the ../link-r and ../objstore-r siblings. Never touches the cloud.
set -euo pipefail
cd "$(dirname "$0")/.."
export RUSTFLAGS="${RUSTFLAGS:--D warnings}"
FUZZ_SECS="${FUZZ_SECS:-20}"
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
warn() { printf '\033[33mskip: %s\033[0m\n' "$*"; }
have() { cargo "$1" --version >/dev/null 2>&1; }

step fmt; cargo fmt --check
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

step "fuzz (${FUZZ_SECS}s per target)"
if have fuzz; then
  for t in fuzz_base_parse fuzz_wal_replay fuzz_op_decode fuzz_postings_decode fuzz_wal_committed_len fuzz_open_image; do
    (cd fuzz && cargo fuzz run "$t" -- -max_total_time="$FUZZ_SECS" 2>&1 | tail -1)
  done
else warn "cargo-fuzz not installed"; fi

step "supply chain"
if have deny; then cargo deny check; else warn "cargo-deny not installed"; fi
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

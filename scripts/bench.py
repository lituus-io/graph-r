#!/usr/bin/env python3
# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
"""Record and check criterion benchmark baselines.

Criterion already measures well; what it does not do is fail a build. This turns
its output into a gate:

    cargo bench --all-features            # produces target/criterion/**/new/
    python3 scripts/bench.py --save       # write benches/baselines/store.json
    python3 scripts/bench.py --check      # compare, exit 1 on a regression

A baseline is committed so a regression is reviewable in a diff rather than
inferred from a run that happens to be slower. Absolute timings differ across
machines, so `--check` is only meaningful against the baseline's own hardware --
in CI that means comparing a run to the committed numbers on the same runner
class, and treating a failure as "look at this", not "the build is broken".

Tolerance is deliberately loose. Benchmark noise on a shared CI runner routinely
reaches double digits; a gate that fires on noise gets disabled within a week.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
CRITERION = REPO / "target" / "criterion"
BASELINE = REPO / "benches" / "baselines" / "store.json"
# A regression has to clear this much to fail, in percent.
TOLERANCE_PCT = 15.0


def collect() -> dict[str, float]:
    """Median nanoseconds per benchmark id from the most recent criterion run."""
    if not CRITERION.is_dir():
        sys.exit("no target/criterion -- run `cargo bench --all-features` first")
    out: dict[str, float] = {}
    for estimates in sorted(CRITERION.glob("**/new/estimates.json")):
        # .../criterion/<group>/<bench>/<param>/new/estimates.json
        bench_id = "/".join(estimates.relative_to(CRITERION).parts[:-2])
        try:
            data = json.loads(estimates.read_text())
            out[bench_id] = float(data["median"]["point_estimate"])
        except (ValueError, KeyError) as exc:  # malformed or partial run
            print(f"skipping {bench_id}: {exc}", file=sys.stderr)
    if not out:
        sys.exit("criterion produced no estimates -- did the benches actually run?")
    return out


def fmt(ns: float) -> str:
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("us", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.3f} {unit}"
    return f"{ns:.1f} ns"


def save() -> None:
    current = collect()
    BASELINE.parent.mkdir(parents=True, exist_ok=True)
    BASELINE.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n")
    print(f"wrote {len(current)} baselines to {BASELINE.relative_to(REPO)}")
    for name, ns in sorted(current.items()):
        print(f"  {name:<44} {fmt(ns)}")


def check() -> int:
    if not BASELINE.is_file():
        sys.exit(f"no baseline at {BASELINE.relative_to(REPO)} -- run --save first")
    baseline = json.loads(BASELINE.read_text())
    current = collect()

    regressions, missing = [], []
    print(f"{'benchmark':<44} {'baseline':>12} {'current':>12} {'delta':>9}")
    for name, base_ns in sorted(baseline.items()):
        if name not in current:
            missing.append(name)
            continue
        now_ns = current[name]
        delta = (now_ns - base_ns) / base_ns * 100.0
        flag = ""
        if delta > TOLERANCE_PCT:
            regressions.append((name, base_ns, now_ns, delta))
            flag = "  REGRESSION"
        print(f"{name:<44} {fmt(base_ns):>12} {fmt(now_ns):>12} {delta:>+8.1f}%{flag}")

    for name in sorted(set(current) - set(baseline)):
        print(f"{name:<44} {'(new)':>12} {fmt(current[name]):>12}")
    for name in missing:
        print(f"{name:<44} {'(gone)':>12}", file=sys.stderr)

    if regressions:
        print(
            f"\n{len(regressions)} benchmark(s) regressed by more than "
            f"{TOLERANCE_PCT:.0f}%. If the change is intentional, re-run with "
            f"--save and commit the new baseline.",
            file=sys.stderr,
        )
        return 1
    print(f"\nno regression beyond {TOLERANCE_PCT:.0f}%")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--save", action="store_true", help="write the committed baseline")
    mode.add_argument("--check", action="store_true", help="compare against the baseline")
    args = ap.parse_args()
    if args.save:
        save()
        return 0
    return check()


if __name__ == "__main__":
    sys.exit(main())

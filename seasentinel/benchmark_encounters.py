"""Benchmark séquentiel vs parallèle (bandes + halo) du détecteur de rencontres.

    python -m seasentinel.benchmark_encounters --input data/dma_clean.csv

Le chargement du CSV est EXCLU des mesures (il est mesuré à part) : on compare
l'étape « moteur d'analyse » du pipeline, partitionnement et envoi aux workers compris.
"""
from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path

from .detectors.encounter import (DEFAULT_BUCKET_S, DEFAULT_MAX_DIST_KM, DEFAULT_MAX_SOG,
                                  DEFAULT_MIN_DURATION_S, slow_points)
from .parallel_encounters import EncResult, run_encounters_parallel, run_encounters_sequential
from .positions_io import read_positions_csv


def median_run(fn, repeat: int) -> EncResult:
    runs = sorted((fn() for _ in range(repeat)), key=lambda r: r.wall_s)
    return runs[len(runs) // 2]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", required=True)
    ap.add_argument("--workers", type=int, nargs="+", default=None)
    ap.add_argument("--strategy", nargs="+", default=["quantile", "uniform"], choices=["quantile", "uniform"])
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--max-sog", type=float, default=DEFAULT_MAX_SOG)
    ap.add_argument("--max-dist-km", type=float, default=DEFAULT_MAX_DIST_KM)
    ap.add_argument("--bucket", type=float, default=DEFAULT_BUCKET_S)
    ap.add_argument("--min-duration", type=float, default=DEFAULT_MIN_DURATION_S)
    ap.add_argument("--max-positions", type=int, default=None)
    ap.add_argument("--output", default="data/benchmark_encounters.csv")
    a = ap.parse_args()

    cores = os.cpu_count() or 1
    workers = a.workers or sorted({w for w in (2, 4, 8, cores) if 2 <= w <= cores} or {2})

    t0 = time.perf_counter()
    positions = []
    for p in read_positions_csv(a.input):
        positions.append(p)
        if a.max_positions and len(positions) >= a.max_positions:
            break
    pts = slow_points(positions, a.max_sog, a.bucket)
    print(f"Chargement : {len(positions):,} positions, {len(pts):,} lentes (sog <= {a.max_sog}) "
          f"en {time.perf_counter() - t0:.1f} s")
    del positions
    print(f"Cœurs détectés : {cores}   répétitions : {a.repeat} (médiane)\n")
    print(f"{'mode':<20}{'N':>3}{'temps s':>10}{'accél.':>9}{'effic.':>8}{'déséq.':>8}{'pts/worker max':>16}{'rencontres':>12}   résultat")

    kw = dict(max_dist_km=a.max_dist_km, bucket_s=a.bucket, min_duration_s=a.min_duration)
    base = median_run(lambda: run_encounters_sequential(pts, **kw), a.repeat)
    ref = base.keys()
    rows = []

    def record(mode: str, w: int, res: EncResult) -> None:
        mx = max((s.positions for s in res.stats), default=0)
        r = {"mode": mode, "workers": w, "time_s": round(res.wall_s, 3),
             "speedup": round(base.wall_s / res.wall_s, 2),
             "efficiency": round(base.wall_s / res.wall_s / w, 2),
             "imbalance": round(res.imbalance, 2), "max_points_worker": mx,
             "encounters": len(res.alerts), "identical": res.keys() == ref}
        rows.append(r)
        print(f"{mode:<20}{w:>3}{r['time_s']:>10.2f}{r['speedup']:>9.2f}{r['efficiency']:>8.2f}"
              f"{r['imbalance']:>8.2f}{mx:>16,}{r['encounters']:>12}   {'OK' if r['identical'] else 'DIFFERENT !'}")

    record("sequentiel", 1, base)
    for w in workers:
        for strat in a.strategy:
            res = median_run(lambda: run_encounters_parallel(pts, w, strat, **kw), a.repeat)
            record(f"bandes-{strat}", w, res)

    Path(a.output).parent.mkdir(parents=True, exist_ok=True)
    with open(a.output, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=list(rows[0]))
        wr.writeheader()
        wr.writerows(rows)
    print(f"\nRésultats -> {a.output}")
    if not all(r["identical"] for r in rows):
        raise SystemExit("ERREUR : un résultat parallèle diffère du séquentiel")


if __name__ == "__main__":
    main()

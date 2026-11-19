"""Benchmark séquentiel vs parallèle du détecteur de gap.

    python -m seasentinel.benchmark --input data/dma_clean.csv --bbox 55 58 8 12.5

Produit le tableau du dossier : temps, accélération, efficacité, équilibre des
workers, et vérifie que le résultat parallèle est IDENTIQUE au séquentiel.
"""
from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

from .detectors.gap import DEFAULT_MIN_GAP_S
from .parallel import RunResult, run_parallel_inmemory, run_parallel_ranges, run_sequential_inmemory
from .partition import imbalance, partition_positions
from .positions_io import read_positions_csv


def median_run(fn, repeat: int) -> RunResult:
    """Exécute `repeat` fois, retourne la run de temps médian (limite le bruit)."""
    runs = sorted((fn() for _ in range(repeat)), key=lambda r: r.wall_s)
    return runs[len(runs) // 2]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", required=True, help="CSV propre trié (sortie de preprocess)")
    ap.add_argument("--workers", type=int, nargs="+", default=None, help="défaut : 2 4 ... jusqu'au nb de cœurs")
    ap.add_argument("--granularity", type=int, nargs="+", default=[1, 4],
                    help="plages par worker : 1 = statique, 4 = dynamique (défaut : 1 4)")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--min-gap", type=float, default=DEFAULT_MIN_GAP_S)
    ap.add_argument("--bbox", type=float, nargs=4, metavar=("LAT_MIN", "LAT_MAX", "LON_MIN", "LON_MAX"))
    ap.add_argument("--inmemory", action="store_true",
                    help="compare aussi l'approche naïve (positions envoyées par pickle). Gourmand en RAM.")
    ap.add_argument("--max-positions", type=int, default=None, help="limite pour --inmemory")
    ap.add_argument("--output", default="data/benchmark.csv")
    a = ap.parse_args()

    cores = os.cpu_count() or 1
    workers = a.workers or sorted({w for w in (2, 4, 8, cores) if 2 <= w <= cores} or {2})
    bbox = tuple(a.bbox) if a.bbox else None
    rows: list[dict] = []

    def record(mode, w, gran, res: RunResult, base: RunResult, ref_keys) -> None:
        rows.append({
            "mode": mode, "workers": w, "granularity": gran, "time_s": round(res.wall_s, 3),
            "speedup": round(base.wall_s / res.wall_s, 2),
            "efficiency": round(base.wall_s / res.wall_s / w, 2),
            "imbalance": round(res.imbalance, 2), "alerts": len(res.alerts),
            "identical": res.keys() == ref_keys,
        })
        r = rows[-1]
        print(f"{mode:<22}{w:>3}{gran:>5}{r['time_s']:>9.2f}{r['speedup']:>9.2f}{r['efficiency']:>8.2f}"
              f"{r['imbalance']:>8.2f}{r['alerts']:>8}   {'OK' if r['identical'] else 'DIFFERENT !'}")

    print(f"Cœurs détectés : {cores}   répétitions : {a.repeat} (médiane)\n")
    print(f"{'mode':<22}{'N':>3}{'gran':>5}{'temps s':>9}{'accél.':>9}{'effic.':>8}{'déséq.':>8}{'alertes':>8}   résultat")

    # --- approche 1 : plages d'octets, le worker lit sa plage
    base = median_run(lambda: run_parallel_ranges(a.input, 1, 1, a.min_gap, bbox), a.repeat)
    ref = base.keys()
    record("sequentiel", 1, 1, base, base, ref)
    for w in workers:
        for g in a.granularity:
            res = median_run(lambda: run_parallel_ranges(a.input, w, g, a.min_gap, bbox), a.repeat)
            record("parallele-plages", w, g, res, base, ref)

    # --- approche 2 : positions en mémoire envoyées par pickle
    if a.inmemory:
        print("\nChargement en mémoire pour l'approche naïve...")
        positions = []
        for p in read_positions_csv(a.input):
            positions.append(p)
            if a.max_positions and len(positions) >= a.max_positions:
                break
        mem_base = median_run(lambda: run_sequential_inmemory(positions, a.min_gap, bbox), a.repeat)
        mem_ref = mem_base.keys()
        record("seq-memoire", 1, 1, mem_base, mem_base, mem_ref)
        for w in workers:
            for strat in ("hash", "balanced"):
                loads = [len(p) for p in partition_positions(positions, w, strat)]
                res = median_run(lambda: run_parallel_inmemory(positions, w, strat, a.min_gap, bbox), a.repeat)
                record(f"pickle-{strat}", w, 1, res, mem_base, mem_ref)
                print(f"   (déséquilibre des charges, positions par worker : {imbalance(loads):.2f})")

    Path(a.output).parent.mkdir(parents=True, exist_ok=True)
    with open(a.output, "w", newline="") as f:
        w_ = csv.DictWriter(f, fieldnames=list(rows[0]))
        w_.writeheader()
        w_.writerows(rows)
    print(f"\nRésultats -> {a.output}")
    if not all(r["identical"] for r in rows):
        raise SystemExit("ERREUR : un résultat parallèle diffère du séquentiel")


if __name__ == "__main__":
    main()

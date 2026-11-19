"""Prétraitement : validation, doublons, positions impossibles, tri.

Entrée : n'importe quel itérable de Position (simulateur, DMA, GFW).
Sortie : liste triée par (MMSI, horodatage) + statistiques de qualité.
"""
from __future__ import annotations

import argparse
import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .dma_reader import BBox, ReadStats
from .geo import KNOT_KMH, haversine_km
from .ingestion import iter_batches
from .models import CSV_FIELDS, Position

MAX_SPEED_KN = 60.0     # au-delà, un saut de position est physiquement impossible
MAX_CONSECUTIVE_JUMPS = 3  # après 3 rejets d'affilée, on suppose que l'ancre était fausse


@dataclass
class PreprocessStats:
    total_in: int = 0
    invalid: int = 0      # plage invalide (lat, lon, sog, cog, mmsi...)
    duplicates: int = 0   # même (mmsi, timestamp)
    jumps: int = 0        # saut de position impossible
    total_out: int = 0

    @property
    def dropped(self) -> int:
        return self.invalid + self.duplicates + self.jumps

    @property
    def invalid_rate(self) -> float:
        """Taux de données écartées (indicateur du dossier)."""
        return self.dropped / self.total_in if self.total_in else 0.0


def preprocess(positions: Iterable[Position], max_speed_kn: float = MAX_SPEED_KN,
               stats: PreprocessStats | None = None) -> tuple[list[Position], PreprocessStats]:
    st = stats if stats is not None else PreprocessStats()
    seen: set[tuple[int, float]] = set()
    kept: list[Position] = []

    # 1) validation + dédoublonnage
    for p in positions:
        st.total_in += 1
        if not p.is_valid():
            st.invalid += 1
            continue
        key = (p.mmsi, p.timestamp)
        if key in seen:
            st.duplicates += 1
            continue
        seen.add(key)
        kept.append(p)
    seen.clear()

    # 2) tri par navire puis par temps : les détecteurs en dépendent
    kept.sort(key=lambda p: (p.mmsi, p.timestamp))

    # 3) suppression des sauts impossibles, navire par navire
    out: list[Position] = []
    anchor: Position | None = None
    streak = 0
    for p in kept:
        if anchor is None or p.mmsi != anchor.mmsi:
            anchor, streak = p, 0
            out.append(p)
            continue
        hours = (p.timestamp - anchor.timestamp) / 3600.0
        speed_kn = haversine_km(anchor.lat, anchor.lon, p.lat, p.lon) / KNOT_KMH / hours
        if speed_kn > max_speed_kn and streak < MAX_CONSECUTIVE_JUMPS:
            st.jumps += 1
            streak += 1
            continue
        anchor, streak = p, 0
        out.append(p)

    st.total_out = len(out)
    return out, st


def write_clean_csv(positions: Iterable[Position], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(CSV_FIELDS)
        for p in positions:
            w.writerow(p.to_row())


def main() -> None:
    ap = argparse.ArgumentParser(description="Lit un CSV/ZIP DMA, nettoie, écrit un CSV propre.")
    ap.add_argument("--input", required=True, help="aisdk-AAAA-MM-JJ.csv ou .zip")
    ap.add_argument("--output", default="data/dma_clean.csv")
    ap.add_argument("--max-rows", type=int, default=None, help="limite de lignes lues (tests)")
    ap.add_argument("--bbox", type=float, nargs=4, metavar=("LAT_MIN", "LAT_MAX", "LON_MIN", "LON_MAX"),
                    help="ne garder qu'une zone")
    a = ap.parse_args()

    rs = ReadStats()
    t0 = time.perf_counter()
    bbox: BBox | None = tuple(a.bbox) if a.bbox else None
    stream = (p for batch in iter_batches(a.input, stats=rs, bbox=bbox, max_rows=a.max_rows) for p in batch)
    clean, ps = preprocess(stream)
    write_clean_csv(clean, Path(a.output))
    dt = time.perf_counter() - t0

    print(f"Lignes lues            : {rs.rows_read:,}")
    for reason, n in rs.rejected.most_common():
        print(f"  rejet lecture {reason:<24}: {n:,}")
    print(f"Positions de navires   : {ps.total_in:,}")
    print(f"  invalides (plage)    : {ps.invalid:,}")
    print(f"  doublons             : {ps.duplicates:,}")
    print(f"  sauts impossibles    : {ps.jumps:,}")
    print(f"Positions propres      : {ps.total_out:,}  ({ps.invalid_rate:.2%} écartées au prétraitement)")
    print(f"Navires distincts      : {len({p.mmsi for p in clean}):,}")
    print(f"Temps total            : {dt:.1f} s  ->  {a.output}")


if __name__ == "__main__":
    main()

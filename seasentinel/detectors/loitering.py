"""Détecteur de stationnement prolongé (« loitering »), SÉQUENTIEL, par navire.

Règle : un navire reste lent (sog <= max_sog) ET dans un rayon `radius_km` autour de
la première position de la séquence, pendant au moins `min_duration_s`, sans trou de
plus de `max_gap_s` entre deux positions.

Précondition : flux trié par (mmsi, timestamp). Mémoire constante (une séquence en cours).

Limite connue : sans liste de ports, un navire à quai est « en stationnement ». Sur
des données réelles, ce détecteur signale donc aussi les navires amarrés ; une zone
d'exclusion des ports (ex. événements « port visit » de GFW) est une extension prévue.
"""
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from typing import Iterable, Iterator

from ..geo import haversine_km
from ..models import Position
from ..positions_io import load_truth, read_positions_csv

DEFAULT_MAX_SOG = 1.0
DEFAULT_RADIUS_KM = 0.5
DEFAULT_MIN_DURATION_S = 3600.0
DEFAULT_MAX_GAP_S = 1800.0


@dataclass(frozen=True)
class LoiterAlert:
    mmsi: int
    start: float
    end: float
    lat: float            # position d'ancrage (première position de la séquence)
    lon: float
    mean_sog: float
    max_dist_km: float    # écart maximal à l'ancrage

    @property
    def duration_s(self) -> float:
        return self.end - self.start

    @property
    def mmsis(self) -> list[int]:
        return [self.mmsi]


def detect_loitering(positions: Iterable[Position], max_sog: float = DEFAULT_MAX_SOG,
                     radius_km: float = DEFAULT_RADIUS_KM,
                     min_duration_s: float = DEFAULT_MIN_DURATION_S,
                     max_gap_s: float = DEFAULT_MAX_GAP_S) -> Iterator[LoiterAlert]:
    cur: int | None = None
    anchor: Position | None = None
    last: Position | None = None
    n = 0
    sum_sog = 0.0
    max_d = 0.0

    def close() -> LoiterAlert | None:
        if anchor is not None and last.timestamp - anchor.timestamp >= min_duration_s:
            return LoiterAlert(anchor.mmsi, anchor.timestamp, last.timestamp,
                               anchor.lat, anchor.lon, sum_sog / n, max_d)
        return None

    for p in positions:
        if p.mmsi != cur:
            if (a := close()) is not None:
                yield a
            cur, anchor, last = p.mmsi, None, None
        if anchor is not None:
            dt = p.timestamp - last.timestamp
            if dt < 0:
                raise ValueError("flux non trié par (mmsi, timestamp) : lancer preprocess() avant")
            d = haversine_km(anchor.lat, anchor.lon, p.lat, p.lon)
            if p.sog <= max_sog and d <= radius_km and dt <= max_gap_s:
                last, n, sum_sog, max_d = p, n + 1, sum_sog + p.sog, max(max_d, d)
                continue
            if (a := close()) is not None:
                yield a
            anchor = None
        if p.sog <= max_sog:
            anchor, last, n, sum_sog, max_d = p, p, 1, p.sog, 0.0
    if (a := close()) is not None:
        yield a


def main() -> None:
    from ..evaluation import evaluate
    ap = argparse.ArgumentParser(description="Détection de stationnement prolongé (séquentiel).")
    ap.add_argument("--input", required=True)
    ap.add_argument("--max-sog", type=float, default=DEFAULT_MAX_SOG)
    ap.add_argument("--radius-km", type=float, default=DEFAULT_RADIUS_KM)
    ap.add_argument("--min-duration", type=float, default=DEFAULT_MIN_DURATION_S)
    ap.add_argument("--truth")
    ap.add_argument("--sort", action="store_true", help="trier en mémoire d'abord (sim_positions.csv)")
    a = ap.parse_args()
    t0 = time.perf_counter()
    src: Iterable[Position] = read_positions_csv(a.input)
    if a.sort:
        from ..preprocess import preprocess
        src, _ = preprocess(src)
    alerts = list(detect_loitering(src, a.max_sog, a.radius_km, a.min_duration))
    print(f"Stationnements détectés : {len(alerts)} sur {len({x.mmsi for x in alerts})} navires "
          f"({time.perf_counter() - t0:.1f} s)")
    for x in sorted(alerts, key=lambda x: -x.duration_s)[:10]:
        print(f"  MMSI {x.mmsi}  {x.duration_s / 3600:6.2f} h   ({x.lat:.4f}, {x.lon:.4f})")
    if a.truth:
        print("Évaluation :", evaluate(alerts, load_truth(a.truth), "loitering", explained_by=("encounter",)))


if __name__ == "__main__":
    main()

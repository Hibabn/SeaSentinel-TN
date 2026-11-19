"""Détecteur de rencontre : deux navires LENTS et PROCHES pendant une certaine durée.

Algorithme
  1. Ne garder que les positions lentes (sog <= max_sog), rangées par « tranche de
     temps » (bucket_s secondes : deux navires ne sont comparés que dans la même tranche,
     car ils n'émettent jamais à la même seconde).
  2. Dans chaque tranche, une GRILLE de cellules (côté >= max_dist) évite de comparer
     tous les navires deux à deux : on ne compare que une cellule avec ses voisines.
  3. Un « contact » = (paire de navires, tranche). Des contacts consécutifs (trous
     tolérés <= max_gap_s) forment une rencontre ; on garde celles >= min_duration_s.

Version parallèle : voir parallel_encounters.py (bandes géographiques + halo). Le
paramètre `core` de find_contacts y sert à ne compter chaque contact qu'une fois.

Limite connue : sans liste de ports, deux navires amarrés côte à côte sont « en
rencontre ». À filtrer par une zone d'exclusion des ports (extension).
"""
from __future__ import annotations

import argparse
import math
import time
from dataclasses import dataclass
from typing import Iterable

from ..geo import haversine_km
from ..models import Position
from ..positions_io import load_truth, read_positions_csv

DEFAULT_MAX_DIST_KM = 0.5
DEFAULT_MAX_SOG = 2.0
DEFAULT_BUCKET_S = 300
DEFAULT_MIN_DURATION_S = 3600.0
DEFAULT_MAX_GAP_S = 900.0
KM_PER_DEG = 110.0   # borne basse volontaire : les cellules sont donc toujours >= max_dist

Point = tuple[int, int, float, float]   # (tranche, mmsi, lat, lon)
Contacts = dict[tuple[int, int], tuple[list, list, list]]   # paire -> (tranches, lats, lons)


@dataclass(frozen=True)
class EncounterAlert:
    mmsi_a: int
    mmsi_b: int
    start: float
    end: float
    lat: float
    lon: float
    n_contacts: int

    @property
    def duration_s(self) -> float:
        return self.end - self.start

    @property
    def mmsis(self) -> list[int]:
        return [self.mmsi_a, self.mmsi_b]


def slow_points(positions: Iterable[Position], max_sog: float = DEFAULT_MAX_SOG,
                bucket_s: float = DEFAULT_BUCKET_S) -> list[Point]:
    """Positions lentes sous forme de tuples compacts (rapides à envoyer entre processus)."""
    return [(int(p.timestamp // bucket_s), p.mmsi, p.lat, p.lon) for p in positions if p.sog <= max_sog]


def find_contacts(points: Iterable[Point], max_dist_km: float = DEFAULT_MAX_DIST_KM,
                  core: tuple[float, float] | None = None) -> Contacts:
    """Tous les contacts (paire, tranche) à moins de max_dist_km.

    core=(lon_min, lon_max) : ne retient que les contacts dont le navire de plus petit
    MMSI est dans [lon_min, lon_max[. Les bandes se partagent ainsi les contacts sans
    doublon, même quand `points` contient aussi un halo venu des bandes voisines.
    """
    by_slot: dict[int, dict[int, tuple[float, float]]] = {}
    max_abs_lat = 0.0
    for slot, mmsi, lat, lon in points:
        by_slot.setdefault(slot, {})[mmsi] = (lat, lon)
        if abs(lat) > max_abs_lat:
            max_abs_lat = abs(lat)
    contacts: Contacts = {}
    if not by_slot:
        return contacts
    cell_lat = max_dist_km / KM_PER_DEG
    cell_lon = cell_lat / math.cos(math.radians(min(max_abs_lat, 85.0)))
    lo, hi = core if core else (-math.inf, math.inf)

    for slot, vessels in by_slot.items():
        if len(vessels) < 2:
            continue
        grid: dict[tuple[int, int], list] = {}
        for mmsi, (lat, lon) in vessels.items():
            grid.setdefault((int(lat // cell_lat), int(lon // cell_lon)), []).append((mmsi, lat, lon))

        def check(a, b) -> None:
            if a[0] > b[0]:
                a, b = b, a                     # a = plus petit MMSI = « propriétaire » du contact
            if not (lo <= a[2] < hi):
                return
            if abs(a[1] - b[1]) > cell_lat * 1.001:   # pré-filtre peu coûteux
                return
            if haversine_km(a[1], a[2], b[1], b[2]) > max_dist_km:
                return
            entry = contacts.get((a[0], b[0]))
            if entry is None:
                entry = contacts[(a[0], b[0])] = ([], [], [])
            entry[0].append(slot)
            entry[1].append(a[1])
            entry[2].append(a[2])

        for (ci, cj), items in grid.items():
            n = len(items)
            for x in range(n):
                for y in range(x + 1, n):
                    check(items[x], items[y])
            for di, dj in ((0, 1), (1, -1), (1, 0), (1, 1)):
                other = grid.get((ci + di, cj + dj))
                if other:
                    for a in items:
                        for b in other:
                            check(a, b)
    return contacts


def merge_contacts(parts: Iterable[Contacts]) -> Contacts:
    out: Contacts = {}
    for part in parts:
        for pair, (s, la, lo) in part.items():
            if pair in out:
                out[pair][0].extend(s)
                out[pair][1].extend(la)
                out[pair][2].extend(lo)
            else:
                out[pair] = (list(s), list(la), list(lo))
    return out


def build_encounters(contacts: Contacts, bucket_s: float = DEFAULT_BUCKET_S,
                     min_duration_s: float = DEFAULT_MIN_DURATION_S,
                     max_gap_s: float = DEFAULT_MAX_GAP_S) -> list[EncounterAlert]:
    """Regroupe les contacts successifs d'une paire en rencontres."""
    gap_slots = max(1, int(max_gap_s // bucket_s))
    alerts: list[EncounterAlert] = []
    for (a, b), (slots, lats, lons) in contacts.items():
        order = sorted(range(len(slots)), key=slots.__getitem__)
        first_i: int | None = None     # 1er contact de la rencontre en cours
        prev = 0                       # dernière tranche vue
        n = 0

        def emit() -> None:
            first = slots[first_i]
            if (prev - first + 1) * bucket_s >= min_duration_s:
                alerts.append(EncounterAlert(a, b, first * bucket_s, (prev + 1) * bucket_s,
                                             lats[first_i], lons[first_i], n))

        for i in order:
            s = slots[i]
            if first_i is not None and s == prev:
                continue                                  # doublon de tranche
            if first_i is not None and s - prev > gap_slots:
                emit()                                    # trop long sans contact : on clôt
                first_i, n = None, 0
            if first_i is None:
                first_i = i
            n += 1
            prev = s
        if first_i is not None:
            emit()
    alerts.sort(key=lambda x: (x.start, x.mmsi_a, x.mmsi_b))
    return alerts


def detect_encounters(positions: Iterable[Position], max_dist_km: float = DEFAULT_MAX_DIST_KM,
                      max_sog: float = DEFAULT_MAX_SOG, bucket_s: float = DEFAULT_BUCKET_S,
                      min_duration_s: float = DEFAULT_MIN_DURATION_S,
                      max_gap_s: float = DEFAULT_MAX_GAP_S) -> list[EncounterAlert]:
    """Version séquentielle complète (référence du benchmark)."""
    pts = slow_points(positions, max_sog, bucket_s)
    return build_encounters(find_contacts(pts, max_dist_km), bucket_s, min_duration_s, max_gap_s)


def main() -> None:
    from ..evaluation import evaluate
    ap = argparse.ArgumentParser(description="Détection de rencontres (séquentiel).")
    ap.add_argument("--input", required=True)
    ap.add_argument("--max-dist-km", type=float, default=DEFAULT_MAX_DIST_KM)
    ap.add_argument("--max-sog", type=float, default=DEFAULT_MAX_SOG)
    ap.add_argument("--min-duration", type=float, default=DEFAULT_MIN_DURATION_S)
    ap.add_argument("--truth")
    a = ap.parse_args()
    t0 = time.perf_counter()
    alerts = detect_encounters(read_positions_csv(a.input), a.max_dist_km, a.max_sog,
                               min_duration_s=a.min_duration)
    print(f"Rencontres détectées : {len(alerts)}  ({time.perf_counter() - t0:.1f} s)")
    for x in sorted(alerts, key=lambda x: -x.duration_s)[:10]:
        print(f"  MMSI {x.mmsi_a} & {x.mmsi_b}  {x.duration_s / 3600:6.2f} h   ({x.lat:.4f}, {x.lon:.4f})")
    if a.truth:
        print("Évaluation :", evaluate(alerts, load_truth(a.truth), "encounter", exact_mmsis=True))


if __name__ == "__main__":
    main()

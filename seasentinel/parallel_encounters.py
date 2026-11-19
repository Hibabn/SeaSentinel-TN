"""Moteur parallèle des rencontres : partition GÉOGRAPHIQUE en bandes + halo.

Les rencontres ne se partitionnent pas par navire (il faut comparer des navires
DIFFÉRENTS proches). On découpe donc l'espace en N bandes de longitude :

  - bandes « quantile » : chacune contient autant de positions (équilibre de charge,
    car le trafic est très concentré : ports, détroits) ;
  - bandes « uniform »  : largeur égale (simple, mais déséquilibrée).

HALO : deux navires de part et d'autre d'une frontière de bande doivent quand même se
voir. Chaque worker reçoit donc sa bande PLUS une marge de `halo` degrés de chaque côté
(>= max_dist). Pour qu'un contact ne soit pas compté par deux workers, il appartient
à la bande qui contient le navire de plus petit MMSI (voir `core` dans find_contacts).

Les workers renvoient des CONTACTS ; le parent les fusionne puis forme les rencontres
(une rencontre peut traverser une frontière sans être coupée en deux).
"""
from __future__ import annotations

import math
import multiprocessing as mp
import os
import time
from bisect import bisect_right
from dataclasses import dataclass, field

from .detectors.encounter import (DEFAULT_BUCKET_S, DEFAULT_MAX_DIST_KM, DEFAULT_MAX_GAP_S,
                                  DEFAULT_MIN_DURATION_S, KM_PER_DEG, EncounterAlert, Point,
                                  build_encounters, find_contacts, merge_contacts)
from .parallel import WorkerStat
from .partition import imbalance


@dataclass
class EncResult:
    alerts: list[EncounterAlert]
    wall_s: float
    workers: int
    positions: int                  # points reçus par les workers (halo compris)
    stats: list[WorkerStat] = field(default_factory=list)

    @property
    def imbalance(self) -> float:
        return imbalance([s.seconds for s in self.stats])

    def keys(self) -> list[tuple]:
        return sorted((a.mmsi_a, a.mmsi_b, a.start, a.end) for a in self.alerts)


def band_bounds(points: list[Point], n: int, strategy: str = "quantile",
                sample: int = 200_000) -> list[float]:
    """Frontières internes (n-1 valeurs croissantes) entre les bandes de longitude."""
    if n <= 1 or not points:
        return []
    if strategy == "uniform":
        lons = [p[3] for p in points]
        lo, hi = min(lons), max(lons)
        raw = [lo + (hi - lo) * i / n for i in range(1, n)]
    elif strategy == "quantile":
        step = max(1, len(points) // sample)
        lons = sorted(p[3] for p in points[::step])
        raw = [lons[len(lons) * i // n] for i in range(1, n)]
    else:
        raise ValueError("strategy doit être 'quantile' ou 'uniform'")
    out: list[float] = []
    for b in raw:
        if not out or b > out[-1]:
            out.append(b)
    return out


def halo_degrees(points: list[Point], max_dist_km: float) -> float:
    """Marge en degrés de longitude couvrant max_dist_km à la latitude la plus défavorable."""
    max_abs_lat = max((abs(p[2]) for p in points), default=0.0)
    return max_dist_km / (KM_PER_DEG * math.cos(math.radians(min(max_abs_lat, 85.0))))


def partition_bands(points: list[Point], bounds: list[float], halo: float) -> list[list[Point]]:
    """Données de chaque bande : son cœur + le halo des bandes voisines."""
    k = len(bounds) + 1
    parts: list[list[Point]] = [[] for _ in range(k)]
    for pt in points:
        lon = pt[3]
        c = bisect_right(bounds, lon)               # bande « propriétaire »
        parts[c].append(pt)
        if c > 0 and lon < bounds[c - 1] + halo:    # proche de la frontière basse -> voisin du dessous
            parts[c - 1].append(pt)
        if c < k - 1 and lon >= bounds[c] - halo:   # proche de la frontière haute -> voisin du dessus
            parts[c + 1].append(pt)
    return parts


def _task(args: tuple) -> tuple:
    pts, max_dist_km, core = args
    t0 = time.perf_counter()
    contacts = find_contacts(pts, max_dist_km, core)
    return os.getpid(), len(pts), time.perf_counter() - t0, contacts


def run_encounters_sequential(points: list[Point], max_dist_km: float = DEFAULT_MAX_DIST_KM,
                              bucket_s: float = DEFAULT_BUCKET_S,
                              min_duration_s: float = DEFAULT_MIN_DURATION_S,
                              max_gap_s: float = DEFAULT_MAX_GAP_S) -> EncResult:
    t0 = time.perf_counter()
    pid, n, secs, contacts = _task((points, max_dist_km, None))
    alerts = build_encounters(contacts, bucket_s, min_duration_s, max_gap_s)
    return EncResult(alerts, time.perf_counter() - t0, 1, n, [WorkerStat(pid, n, secs, 1)])


def run_encounters_parallel(points: list[Point], workers: int, strategy: str = "quantile",
                            max_dist_km: float = DEFAULT_MAX_DIST_KM,
                            bucket_s: float = DEFAULT_BUCKET_S,
                            min_duration_s: float = DEFAULT_MIN_DURATION_S,
                            max_gap_s: float = DEFAULT_MAX_GAP_S,
                            halo: float | None = None) -> EncResult:
    """`halo` : à ne fixer que pour démontrer son utilité (halo=0 => rencontres ratées)."""
    t0 = time.perf_counter()
    bounds = band_bounds(points, workers, strategy)
    h = halo_degrees(points, max_dist_km) if halo is None else halo
    parts = partition_bands(points, bounds, h)
    edges = [-math.inf] + bounds + [math.inf]
    tasks = [(parts[i], max_dist_km, (edges[i], edges[i + 1])) for i in range(len(parts))]
    with mp.Pool(len(tasks)) as pool:
        results = pool.map(_task, tasks, chunksize=1)
    contacts = merge_contacts(r[3] for r in results)
    alerts = build_encounters(contacts, bucket_s, min_duration_s, max_gap_s)
    by_pid: dict[int, WorkerStat] = {}
    for pid, n, secs, _ in results:
        st = by_pid.setdefault(pid, WorkerStat(pid))
        st.positions += n
        st.seconds += secs
        st.tasks += 1
    return EncResult(alerts, time.perf_counter() - t0, len(tasks),
                     sum(r[1] for r in results), sorted(by_pid.values(), key=lambda s: s.pid))

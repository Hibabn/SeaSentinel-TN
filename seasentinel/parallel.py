"""Moteur parallèle du détecteur de gap (multiprocessing).

Deux approches, volontairement comparées dans le benchmark :

1. run_parallel_ranges : on découpe le CSV TRIÉ en plages d'octets alignées sur un
   changement de MMSI. Chaque worker lit SA plage, la parse et lance le détecteur.
   Aucune position ne circule entre processus (seules les alertes reviennent) :
   « on amène le calcul aux données ». Partition par navire (un navire = une plage).

2. run_parallel_inmemory : les positions sont déjà en mémoire dans le processus
   parent ; on les partitionne par navire (hash ou équilibré) et on les ENVOIE aux
   workers (pickle). Approche naïve : le coût de sérialisation peut dépasser le
   gain de calcul quand le détecteur est léger.

Compatible Windows (démarrage « spawn ») : fonctions de worker au niveau module,
et tout lancement doit être protégé par `if __name__ == "__main__"`.
"""
from __future__ import annotations

import multiprocessing as mp
import os
import time
from dataclasses import dataclass, field
from typing import Iterator, Sequence

from .detectors.gap import DEFAULT_MIN_GAP_S, BBox, GapAlert, detect_gaps
from .models import Position
from .partition import imbalance, partition_positions


@dataclass
class WorkerStat:
    pid: int
    positions: int = 0
    seconds: float = 0.0   # temps de calcul réellement occupé
    tasks: int = 0


@dataclass
class RunResult:
    alerts: list[GapAlert]
    wall_s: float                    # temps total vu de l'extérieur (pool compris)
    workers: int
    positions: int
    stats: list[WorkerStat] = field(default_factory=list)

    @property
    def imbalance(self) -> float:
        """max/moyenne du temps occupé par worker (1.0 = parfaitement équilibré)."""
        return imbalance([s.seconds for s in self.stats])

    def keys(self) -> list[tuple]:
        return sorted((a.mmsi, a.start, a.end) for a in self.alerts)


def _aggregate(results: Sequence[tuple], wall: float, workers: int) -> RunResult:
    by_pid: dict[int, WorkerStat] = {}
    alerts: list[GapAlert] = []
    total = 0
    for pid, n, secs, al in results:
        st = by_pid.setdefault(pid, WorkerStat(pid))
        st.positions += n
        st.seconds += secs
        st.tasks += 1
        total += n
        alerts.extend(al)
    alerts.sort(key=lambda a: (a.mmsi, a.start))
    return RunResult(alerts, wall, workers, total, sorted(by_pid.values(), key=lambda s: s.pid))


# ----------------------------------------------------------------- approche 1
def split_ranges(path, parts: int) -> list[tuple[int, int]]:
    """Découpe un CSV trié par MMSI en <= `parts` plages [début, fin) d'octets.
    Chaque frontière tombe sur un changement de MMSI : aucun navire n'est coupé."""
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        f.readline()                       # en-tête
        first = f.tell()
        if first >= size:
            return []
        bounds = [first]
        for i in range(1, parts):
            target = first + (size - first) * i // parts
            if target <= bounds[-1]:
                continue
            f.seek(target)
            f.readline()                   # termine la ligne coupée
            line = f.readline()
            if not line:
                break
            mmsi = line.split(b",", 1)[0]
            boundary = size
            while True:
                pos = f.tell()
                line = f.readline()
                if not line:
                    break
                if line.split(b",", 1)[0] != mmsi:
                    boundary = pos
                    break
            if boundary >= size:
                break
            if boundary > bounds[-1]:
                bounds.append(boundary)
        bounds.append(size)
    return list(zip(bounds[:-1], bounds[1:]))


def _iter_range(path, start: int, end: int, counter: list[int]) -> Iterator[Position]:
    pos = start
    with open(path, "rb") as f:
        f.seek(start)
        for line in f:
            if pos >= end:
                break
            pos += len(line)
            m, t, la, lo, s, c = line.decode().split(",")
            counter[0] += 1
            yield Position(int(m), float(t), float(la), float(lo), float(s), float(c))


def process_range(path, start: int, end: int, bbox: BBox | None = None,
                  min_gap_s: float = DEFAULT_MIN_GAP_S) -> tuple:
    """Tâche d'un worker : lit sa plage, détecte. Retourne (pid, n, secondes, alertes)."""
    t0 = time.perf_counter()
    counter = [0]
    alerts = list(detect_gaps(_iter_range(path, start, end, counter), min_gap_s, bbox))
    return os.getpid(), counter[0], time.perf_counter() - t0, alerts


def run_parallel_ranges(path, workers: int, granularity: int = 1,
                        min_gap_s: float = DEFAULT_MIN_GAP_S, bbox: BBox | None = None) -> RunResult:
    """workers=1 : exécution séquentielle (référence, même code, sans Pool).
    granularity : plages par worker. 1 = répartition statique ; >1 = dynamique
    (un worker qui finit tôt reprend une autre plage => meilleur équilibre)."""
    t0 = time.perf_counter()
    tasks = [(path, s, e, bbox, min_gap_s) for s, e in split_ranges(path, workers * granularity)]
    if workers == 1:
        results = [process_range(*t) for t in tasks]
    else:
        with mp.Pool(workers) as pool:
            results = pool.starmap(process_range, tasks, chunksize=1)
    return _aggregate(results, time.perf_counter() - t0, workers)


# ----------------------------------------------------------------- approche 2
def _detect_partition(args: tuple) -> tuple:
    positions, min_gap_s, bbox = args
    t0 = time.perf_counter()
    alerts = list(detect_gaps(positions, min_gap_s, bbox))
    return os.getpid(), len(positions), time.perf_counter() - t0, alerts


def run_sequential_inmemory(positions: list[Position], min_gap_s: float = DEFAULT_MIN_GAP_S,
                            bbox: BBox | None = None) -> RunResult:
    t0 = time.perf_counter()
    res = _detect_partition((positions, min_gap_s, bbox))
    return _aggregate([res], time.perf_counter() - t0, 1)


def run_parallel_inmemory(positions: list[Position], workers: int, strategy: str = "hash",
                          min_gap_s: float = DEFAULT_MIN_GAP_S, bbox: BBox | None = None) -> RunResult:
    """Partition par navire dans le parent, envoi par pickle aux workers."""
    t0 = time.perf_counter()
    parts = partition_positions(positions, workers, strategy)
    with mp.Pool(workers) as pool:
        results = pool.map(_detect_partition, [(p, min_gap_s, bbox) for p in parts], chunksize=1)
    return _aggregate(results, time.perf_counter() - t0, workers)

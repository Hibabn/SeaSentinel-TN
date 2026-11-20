"""Rejeu « temps réel » d'un fichier de positions (thread producteur).

On n'a pas de flux AIS en direct : on rejoue un fichier en respectant les écarts de
temps entre les messages, accélérés d'un facteur `speed` (speed=600 : 1 s réelle =
10 min de données ; 12 h de simulation défilent en 72 s). speed=0 : sans pause.

Le thread découpe le flux en « tops » de `tick_s` secondes de données et dépose
chaque top dans une Queue BORNÉE : si le moteur prend du retard, put() bloque et le
lecteur ralentit tout seul (contre-pression, même principe qu'en phase 2).
"""
from __future__ import annotations

import queue
import threading
import time
from typing import Sequence

from ..dma_reader import BBox
from ..models import Position
from ..positions_io import read_positions_csv

END = None   # sentinelle de fin de flux


def load_time_sorted(path, bbox: BBox | None = None, limit: int | None = None
                     ) -> list[Position]:
    """Charge un CSV commun et le trie par temps s'il ne l'est pas déjà.

    sim_positions.csv est déjà trié par temps. dma_clean.csv est trié par navire :
    il faut le charger en entier pour le trier, donc utiliser --bbox/--limit sur les
    gros fichiers (5 M positions ~ plusieurs Go de mémoire en objets Python)."""
    out: list[Position] = []
    for p in read_positions_csv(path):
        if bbox and not (bbox[0] <= p.lat <= bbox[1] and bbox[2] <= p.lon <= bbox[3]):
            continue
        out.append(p)
        if limit and len(out) >= limit:
            break
    if any(out[i].timestamp > out[i + 1].timestamp for i in range(len(out) - 1)):
        out.sort(key=lambda p: (p.timestamp, p.mmsi))
    return out


class Replayer(threading.Thread):
    def __init__(self, positions: Sequence[Position], out: queue.Queue, speed: float = 600.0,
                 tick_s: float = 60.0, stop: threading.Event | None = None):
        super().__init__(name="replay", daemon=True)
        self.positions = positions
        self.out = out
        self.speed = speed
        self.tick_s = tick_s
        self.stop_event = stop or threading.Event()
        self.sent = 0                     # positions déposées dans la file
        self.lag_s = 0.0                  # retard maximal sur l'horaire prévu
        self.error: BaseException | None = None

    @property
    def sim_start(self) -> float | None:
        return self.positions[0].timestamp if self.positions else None

    @property
    def sim_end(self) -> float | None:
        return self.positions[-1].timestamp if self.positions else None

    def _put(self, item) -> bool:
        while not self.stop_event.is_set():
            try:
                self.out.put(item, timeout=0.2)
                return True
            except queue.Full:
                continue
        return False

    def _wait_until(self, deadline: float) -> None:
        while not self.stop_event.is_set():
            left = deadline - time.perf_counter()
            if left <= 0:
                self.lag_s = max(self.lag_s, -left)
                return
            self.stop_event.wait(min(left, 0.2))

    def run(self) -> None:
        try:
            if not self.positions:
                return
            t0 = time.perf_counter()
            start = self.sim_start
            batch: list[Position] = []
            cur = int(self.positions[0].timestamp // self.tick_s)
            for p in self.positions:
                tick = int(p.timestamp // self.tick_s)
                if tick != cur and batch:
                    if self.speed > 0:
                        self._wait_until(t0 + (batch[0].timestamp - start) / self.speed)
                    if not self._put(batch):
                        return
                    self.sent += len(batch)
                    batch, cur = [], tick
                batch.append(p)
            if batch and (self.speed <= 0 or not self.stop_event.is_set()):
                if self.speed > 0:
                    self._wait_until(t0 + (batch[0].timestamp - start) / self.speed)
                if self._put(batch):
                    self.sent += len(batch)
        except BaseException as exc:      # transmis au thread principal
            self.error = exc
        finally:
            self._put(END)

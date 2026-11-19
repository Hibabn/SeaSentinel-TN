"""Lecture/écriture du CSV commun (mmsi,timestamp,lat,lon,sog,cog) en streaming."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterator

from .models import Position
from .simulator import Anomaly


def read_positions_csv(path) -> Iterator[Position]:
    """Génère les Position d'un CSV propre, une par une (pas de liste en mémoire)."""
    with open(path, "r", newline="") as f:
        r = csv.reader(f)
        next(r)  # en-tête
        for m, t, la, lo, s, c in r:
            yield Position(int(m), float(t), float(la), float(lo), float(s), float(c))


def load_truth(path) -> list[Anomaly]:
    with open(Path(path)) as f:
        return [Anomaly(**d) for d in json.load(f)]

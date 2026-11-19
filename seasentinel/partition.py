"""Stratégies de partitionnement PAR NAVIRE.

Règle d'or : toutes les positions d'un navire vont au même worker, dans l'ordre,
sinon le détecteur de gap verrait de faux silences.
"""
from __future__ import annotations

from collections import Counter
from typing import Iterable, Mapping, Sequence

from .models import Position


def hash_worker(mmsi: int, n: int) -> int:
    """Hachage simple. (hash() d'un int Python est l'int lui-même.) Équilibre
    correct en moyenne, mais ignore le fait que certains navires émettent 100x plus."""
    return mmsi % n


def balanced_assignment(counts: Mapping[int, int], n: int) -> dict[int, int]:
    """Glouton « plus gros d'abord » (LPT) : chaque navire va au worker le moins chargé.
    Nécessite de connaître le nombre de positions par navire (un premier passage)."""
    loads = [0] * n
    out: dict[int, int] = {}
    for mmsi, c in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        w = min(range(n), key=lambda i: (loads[i], i))
        out[mmsi] = w
        loads[w] += c
    return out


def imbalance(loads: Sequence[float]) -> float:
    """max / moyenne : 1.0 = équilibre parfait ; 1.5 = le plus chargé fait 50 % de plus."""
    if not loads:
        return 1.0
    mean = sum(loads) / len(loads)
    return max(loads) / mean if mean else 1.0


def partition_positions(positions: Iterable[Position], n: int,
                        strategy: str = "hash") -> list[list[Position]]:
    """Répartit des positions (triées) en n listes, l'ordre interne étant conservé."""
    if strategy not in ("hash", "balanced"):
        raise ValueError("strategy doit être 'hash' ou 'balanced'")
    positions = positions if isinstance(positions, list) else list(positions)
    parts: list[list[Position]] = [[] for _ in range(n)]
    if strategy == "hash":
        for p in positions:
            parts[hash_worker(p.mmsi, n)].append(p)
    else:
        assign = balanced_assignment(Counter(p.mmsi for p in positions), n)
        for p in positions:
            parts[assign[p.mmsi]].append(p)
    return parts

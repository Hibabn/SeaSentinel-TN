"""Ingestion concurrente : un thread lit le fichier, le consommateur traite.

Lire un CSV = attente disque + parsing : le thread producteur lit pendant que
le consommateur (prétraitement) travaille sur le lot précédent. La Queue bornée
(maxsize) crée de la contre-pression : le lecteur se met en pause si le
consommateur est en retard, donc la mémoire reste bornée.
"""
from __future__ import annotations

import queue
import threading
from typing import Iterator

from .dma_reader import ReadStats, read_dma_csv
from .models import Position

_DONE = object()  # sentinelle de fin de flux


def iter_batches(path, batch_size: int = 50_000, queue_size: int = 8,
                 stats: ReadStats | None = None, **read_kw) -> Iterator[list[Position]]:
    """Lit `path` dans un thread producteur et génère des lots de Position."""
    q: queue.Queue = queue.Queue(maxsize=queue_size)
    stats = stats if stats is not None else ReadStats()

    def producer() -> None:
        try:
            batch: list[Position] = []
            for p in read_dma_csv(path, stats=stats, **read_kw):
                batch.append(p)
                if len(batch) >= batch_size:
                    q.put(batch)
                    batch = []
            if batch:
                q.put(batch)
            q.put(_DONE)
        except Exception as exc:  # on transmet l'erreur au consommateur
            q.put(exc)

    t = threading.Thread(target=producer, name="csv-reader", daemon=True)
    t.start()
    while True:
        item = q.get()
        if item is _DONE:
            break
        if isinstance(item, Exception):
            raise item
        yield item
    t.join()

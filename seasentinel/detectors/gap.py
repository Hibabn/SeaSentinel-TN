"""Détecteur d'interruption AIS (« gap »), version SÉQUENTIELLE.

Règle : pour un même navire, si l'écart entre deux positions consécutives est
>= `min_gap_s`, on lève une alerte (début = dernière position avant le silence,
fin = première position après).

Précondition : flux trié par (mmsi, timestamp) -- c'est la sortie du prétraitement.
Le détecteur ne garde que la position précédente : mémoire constante, il peut
lire des millions de lignes en streaming. Cette même fonction servira de brique
dans chaque worker du moteur parallèle (partition par hash(MMSI) en phase 4).

Une alerte est un signal à vérifier, pas une preuve : une perte de couverture
radio ou une sortie de zone produit le même motif.
"""
from __future__ import annotations

import argparse
import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from ..geo import KNOT_KMH, haversine_km
from ..models import Position
from ..positions_io import load_truth, read_positions_csv

DEFAULT_MIN_GAP_S = 3600.0   # 1 h de silence
EDGE_MARGIN_DEG = 0.1        # ~11 km : zone « bord de carte »

BBox = tuple[float, float, float, float]  # lat_min, lat_max, lon_min, lon_max


@dataclass(frozen=True)
class GapAlert:
    mmsi: int
    start: float          # dernière position avant le silence (epoch)
    end: float            # première position après le silence
    last_lat: float
    last_lon: float
    resume_lat: float
    resume_lon: float
    jump_km: float        # distance entre les deux positions
    implied_speed_kn: float  # vitesse moyenne nécessaire pour la parcourir

    @property
    def duration_s(self) -> float:
        return self.end - self.start

    @property
    def mmsis(self) -> list[int]:
        return [self.mmsi]


def _near_edge(lat: float, lon: float, bbox: BBox, margin: float) -> bool:
    return (lat - bbox[0] < margin or bbox[1] - lat < margin
            or lon - bbox[2] < margin or bbox[3] - lon < margin)


def detect_gaps(positions: Iterable[Position], min_gap_s: float = DEFAULT_MIN_GAP_S,
                bbox: BBox | None = None, edge_margin_deg: float = EDGE_MARGIN_DEG
                ) -> Iterator[GapAlert]:
    """Génère les GapAlert d'un flux trié par (mmsi, timestamp).

    `bbox` : si la zone d'étude est une fenêtre (cas DMA), un navire qui sort de
    la fenêtre puis y revient ressemble à un gap. On ignore les silences qui
    commencent ou finissent près du bord de la fenêtre.
    """
    prev: Position | None = None
    for p in positions:
        if prev is not None and p.mmsi == prev.mmsi:
            dt = p.timestamp - prev.timestamp
            if dt < 0:
                raise ValueError("flux non trié par (mmsi, timestamp) : lancer preprocess() avant")
            if dt >= min_gap_s and not (
                bbox and (_near_edge(prev.lat, prev.lon, bbox, edge_margin_deg)
                          or _near_edge(p.lat, p.lon, bbox, edge_margin_deg))):
                d = haversine_km(prev.lat, prev.lon, p.lat, p.lon)
                yield GapAlert(p.mmsi, prev.timestamp, p.timestamp, prev.lat, prev.lon,
                               p.lat, p.lon, d, d / KNOT_KMH / (dt / 3600.0))
        prev = p


def evaluate(alerts: Iterable, truth: Iterable, anomaly_type: str) -> dict:
    """Compare alertes et vérité terrain : une alerte est juste si elle touche le
    même navire et que son intervalle chevauche celui de l'anomalie injectée."""
    alerts = list(alerts)
    truths = [a for a in truth if a.type == anomaly_type]

    def hit(al, tr) -> bool:
        return bool(set(al.mmsis) & set(tr.mmsis)) and al.start < tr.end and tr.start < al.end

    tp_alerts = sum(any(hit(al, tr) for tr in truths) for al in alerts)
    found = sum(any(hit(al, tr) for al in alerts) for tr in truths)
    return {
        "alertes": len(alerts), "verite": len(truths),
        "vrais_positifs": tp_alerts, "faux_positifs": len(alerts) - tp_alerts,
        "faux_negatifs": len(truths) - found,
        "precision": tp_alerts / len(alerts) if alerts else 1.0,
        "rappel": found / len(truths) if truths else 1.0,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Détection de gaps AIS (séquentiel).")
    ap.add_argument("--input", required=True, help="CSV propre (mmsi,timestamp,lat,lon,sog,cog)")
    ap.add_argument("--min-gap", type=float, default=DEFAULT_MIN_GAP_S, help="secondes (défaut 3600)")
    ap.add_argument("--bbox", type=float, nargs=4, metavar=("LAT_MIN", "LAT_MAX", "LON_MIN", "LON_MAX"))
    ap.add_argument("--truth", help="sim_truth.json : affiche précision et rappel")
    ap.add_argument("--output", default=None, help="CSV des alertes")
    ap.add_argument("--sort", action="store_true",
                    help="trier/nettoyer en mémoire d'abord (pour sim_positions.csv, trié par temps)")
    a = ap.parse_args()

    t0 = time.perf_counter()
    n = 0
    alerts: list[GapAlert] = []

    def counted() -> Iterator[Position]:
        nonlocal n
        for p in read_positions_csv(a.input):
            n += 1
            yield p

    source: Iterable[Position] = counted()
    if a.sort:
        from ..preprocess import preprocess
        source, _ = preprocess(counted())
    alerts = list(detect_gaps(source, a.min_gap, tuple(a.bbox) if a.bbox else None))
    dt = time.perf_counter() - t0
    print(f"Positions analysées : {n:,}  en {dt:.1f} s  ({n / dt:,.0f} pos/s)")
    print(f"Gaps détectés       : {len(alerts):,}  sur {len({g.mmsi for g in alerts}):,} navires")
    for g in sorted(alerts, key=lambda g: -g.duration_s)[:10]:
        print(f"  MMSI {g.mmsi}  {g.duration_s / 3600:6.2f} h   saut {g.jump_km:7.1f} km")
    if a.truth:
        print("Évaluation          :", evaluate(alerts, load_truth(a.truth), "gap"))
    if a.output:
        Path(a.output).parent.mkdir(parents=True, exist_ok=True)
        with open(a.output, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["mmsi", "start", "end", "duration_s", "last_lat", "last_lon",
                        "resume_lat", "resume_lon", "jump_km", "implied_speed_kn"])
            for g in alerts:
                w.writerow([g.mmsi, g.start, g.end, g.duration_s, g.last_lat, g.last_lon,
                            g.resume_lat, g.resume_lon, round(g.jump_km, 3), round(g.implied_speed_kn, 2)])


if __name__ == "__main__":
    main()

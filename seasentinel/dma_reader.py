"""Lecteur du format DMA (Danish Maritime Authority) -> flux de Position.

Le fichier DMA fait plusieurs Go : on le lit EN STREAMING (ligne par ligne),
jamais en entier en mémoire. Les colonnes sont retrouvées PAR NOM (et non par
position) pour résister à un changement d'ordre. L'en-tête réel commence par
« # Timestamp » : le « # » est retiré.

Format des champs utilisés :
    Timestamp  : JJ/MM/AAAA HH:MM:SS (UTC)
    Latitude, Longitude, SOG, COG : décimaux (point ou virgule acceptés)
"""
from __future__ import annotations

import csv
import io
import zipfile
from calendar import timegm
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

from .models import Position

VESSEL_CLASSES = ("Class A", "Class B")
REQUIRED_COLUMNS = ("Timestamp", "Type of mobile", "MMSI", "Latitude", "Longitude", "SOG", "COG")

# (lat_min, lat_max, lon_min, lon_max)
BBox = tuple[float, float, float, float]


@dataclass
class ReadStats:
    """Compteurs de lecture : alimente l'indicateur « taux de données invalides »."""
    rows_read: int = 0
    kept: int = 0
    rejected: Counter = field(default_factory=Counter)

    @property
    def rejected_total(self) -> int:
        return sum(self.rejected.values())


def parse_timestamp(s: str) -> float | None:
    """'16/11/2026 00:00:00' -> secondes epoch UTC. Découpage manuel (strptime est ~10x plus lent)."""
    try:
        d, t = s.split(" ")
        dd, mm, yy = d.split("/")
        hh, mi, ss = t.split(":")
        return float(timegm((int(yy), int(mm), int(dd), int(hh), int(mi), int(ss), 0, 0, 0)))
    except (ValueError, AttributeError):
        return None


def parse_float(s: str) -> float | None:
    s = s.strip()
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


@contextmanager
def open_text(path: Path):
    """Ouvre un .csv ou un .zip (lit le 1er fichier du zip sans l'extraire)."""
    path = Path(path)
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as z:
            name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
            with z.open(name) as raw, io.TextIOWrapper(raw, encoding="utf-8", newline="") as f:
                yield f
    else:
        with open(path, "r", encoding="utf-8", newline="") as f:
            yield f


def _column_index(header: list[str]) -> dict[str, int]:
    names = [h.lstrip("# ").strip() for h in header]
    idx = {n: i for i, n in enumerate(names)}
    missing = [c for c in REQUIRED_COLUMNS if c not in idx]
    if missing:
        raise ValueError(f"Colonnes manquantes dans l'en-tête DMA : {missing}")
    return idx


def read_dma_csv(path, stats: ReadStats | None = None, bbox: BBox | None = None,
                 max_rows: int | None = None) -> Iterator[Position]:
    """Génère des Position depuis un CSV/ZIP DMA.

    Rejette (et compte dans `stats.rejected`) : cibles qui ne sont pas des navires
    (stations de base, balises...), MMSI/horodatage/nombres illisibles, champs
    vides, et positions hors `bbox`. Les contrôles de plage (lat, sog...) sont
    faits ensuite par le prétraitement (Position.is_valid).
    """
    stats = stats if stats is not None else ReadStats()
    with open_text(path) as f:
        reader = csv.reader(f)
        ix = _column_index(next(reader))
        i_ts, i_ty, i_mm = ix["Timestamp"], ix["Type of mobile"], ix["MMSI"]
        i_la, i_lo, i_sog, i_cog = ix["Latitude"], ix["Longitude"], ix["SOG"], ix["COG"]
        need = max(i_ts, i_ty, i_mm, i_la, i_lo, i_sog, i_cog)
        for row in reader:
            if max_rows is not None and stats.rows_read >= max_rows:
                return
            stats.rows_read += 1
            if len(row) <= need:
                stats.rejected["ligne_incomplete"] += 1
                continue
            if row[i_ty] not in VESSEL_CLASSES:
                stats.rejected["pas_un_navire"] += 1
                continue
            try:
                mmsi = int(row[i_mm])
            except ValueError:
                stats.rejected["mmsi_illisible"] += 1
                continue
            ts = parse_timestamp(row[i_ts])
            if ts is None:
                stats.rejected["horodatage_illisible"] += 1
                continue
            lat, lon = parse_float(row[i_la]), parse_float(row[i_lo])
            sog, cog = parse_float(row[i_sog]), parse_float(row[i_cog])
            if None in (lat, lon, sog, cog):
                stats.rejected["champ_vide_ou_illisible"] += 1
                continue
            if bbox and not (bbox[0] <= lat <= bbox[1] and bbox[2] <= lon <= bbox[3]):
                stats.rejected["hors_zone"] += 1
                continue
            stats.kept += 1
            yield Position(mmsi, ts, lat, lon, sog, cog)

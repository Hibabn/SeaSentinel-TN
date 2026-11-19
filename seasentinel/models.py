"""Modèle de données commun à toutes les sources (simulateur, DMA, GFW).

Le reste du pipeline ne manipule QUE des Position : il ignore d'où viennent
les données. Une Position est immuable et légère, donc facile à envoyer entre
processus (pickle) dans le moteur parallèle.
"""
from __future__ import annotations

from dataclasses import dataclass

CSV_FIELDS = ("mmsi", "timestamp", "lat", "lon", "sog", "cog")


@dataclass(frozen=True, slots=True)
class Position:
    mmsi: int          # identifiant du navire (9 chiffres)
    timestamp: float   # secondes depuis epoch, UTC
    lat: float         # degrés décimaux
    lon: float         # degrés décimaux
    sog: float         # vitesse sur le fond, en nœuds
    cog: float         # cap sur le fond, en degrés (0-360)

    def is_valid(self) -> bool:
        """Contrôles de base utilisés par le prétraitement (phase 2)."""
        return (
            200_000_000 <= self.mmsi <= 775_999_999   # codes pays (MID) des navires
            and -90.0 <= self.lat <= 90.0
            and -180.0 <= self.lon <= 180.0
            and not (self.lat == 0.0 and self.lon == 0.0)
            and 0.0 <= self.sog <= 102.2      # 102.3 = « non disponible » en AIS
            and 0.0 <= self.cog <= 360.0
            and self.timestamp > 0
        )

    def to_row(self) -> tuple:
        return (self.mmsi, self.timestamp, self.lat, self.lon, self.sog, self.cog)

    @classmethod
    def from_row(cls, row: dict) -> "Position":
        """Construit une Position depuis un dict de chaînes (ligne csv.DictReader)."""
        return cls(
            mmsi=int(row["mmsi"]),
            timestamp=float(row["timestamp"]),
            lat=float(row["lat"]),
            lon=float(row["lon"]),
            sog=float(row["sog"]),
            cog=float(row["cog"]),
        )

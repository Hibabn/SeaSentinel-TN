"""Événement d'alerte : format unique échangé entre le moteur, la base et le réseau.

Une même anomalie produit plusieurs événements au cours de sa vie :
    ouverte  -> la règle est déjà satisfaite, l'anomalie continue
    fermee   -> l'anomalie est terminée (valeurs définitives)
    annulee  -> une alerte ouverte s'est révélée non pertinente
Tous portent le même `id` : la base fait un « upsert » et garde le dernier état.

Un événement est un dict JSON-compatible : il passe tel quel dans une mp.Queue
(pickle) et sur le socket TCP (une ligne JSON par message).
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

OPEN, CLOSED, CANCELLED = "ouverte", "fermee", "annulee"
KINDS = ("gap", "loitering", "encounter")
LABELS = {"gap": "Gap AIS", "loitering": "Stationnement", "encounter": "Rencontre"}


@dataclass
class AlertEvent:
    kind: str                   # "gap" | "loitering" | "encounter"
    status: str                 # OPEN | CLOSED | CANCELLED
    mmsis: list[int]
    start: float                # epoch (temps des données)
    end: float | None           # None tant que l'anomalie est ouverte
    lat: float
    lon: float
    severity: str               # "faible" | "moyenne" | "elevee"
    detected_at: float          # horloge des données au moment de l'événement
    details: dict = field(default_factory=dict)

    @property
    def id(self) -> str:
        """Identifiant stable : même anomalie => même id, quel que soit son état."""
        return f"{self.kind}:{'-'.join(map(str, self.mmsis))}:{int(self.start)}"

    @property
    def duration_s(self) -> float:
        return (self.end if self.end is not None else self.detected_at) - self.start

    def to_dict(self) -> dict:
        d = asdict(self)
        d["id"] = self.id
        d["duration_s"] = self.duration_s
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "AlertEvent":
        keys = cls.__dataclass_fields__
        return cls(**{k: v for k, v in d.items() if k in keys})

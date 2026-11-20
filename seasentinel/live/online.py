"""Moteur de détection EN LIGNE : positions reçues une par une, dans l'ordre du temps.

Les détecteurs des phases 3 et 5 lisent un fichier complet trié par (MMSI, temps).
En temps réel, les positions arrivent mélangées entre navires, dans l'ordre du
temps. Chaque détecteur garde donc un petit ÉTAT PAR NAVIRE (ou par paire) et
applique exactement la même règle, au fil de l'eau.

Garantie (testée dans tests/test_online.py) : sur un même jeu de données, les
alertes FERMÉES du moteur en ligne sont identiques à celles des détecteurs batch.

Ce que le temps réel apporte en plus :
  - une alerte OUVERTE dès que la règle est satisfaite, sans attendre la fin ;
  - une veille « silence en cours » : un navire muet depuis min_gap_s déclenche une
    alerte gap ouverte avant même de réapparaître.

Hypothèse : flux trié par horodatage (le module replay le garantit). Une position
plus ancienne que la précédente du même navire est rejetée (« hors_ordre »).
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field, replace
from typing import Iterable

from ..detectors.encounter import (DEFAULT_BUCKET_S, DEFAULT_MAX_DIST_KM, DEFAULT_MAX_GAP_S as
                                   ENC_MAX_GAP_S, DEFAULT_MAX_SOG as ENC_MAX_SOG,
                                   DEFAULT_MIN_DURATION_S as ENC_MIN_DURATION_S, EncounterAlert,
                                   find_contacts)
from ..detectors.gap import DEFAULT_MIN_GAP_S, EDGE_MARGIN_DEG, BBox, GapAlert, _near_edge
from ..detectors.loitering import (DEFAULT_MAX_GAP_S as LOI_MAX_GAP_S,
                                   DEFAULT_MAX_SOG as LOI_MAX_SOG,
                                   DEFAULT_MIN_DURATION_S as LOI_MIN_DURATION_S,
                                   DEFAULT_RADIUS_KM, LoiterAlert)
from ..geo import KNOT_KMH, haversine_km
from ..models import Position
from ..preprocess import MAX_CONSECUTIVE_JUMPS, MAX_SPEED_KN, PreprocessStats
from .events import CANCELLED, CLOSED, KINDS, OPEN, AlertEvent

# Gravité des rencontres et stationnements (le gap a sa propre règle, cf. GapAlert).
# Une rencontre lente en mer est le motif d'un transbordement : priorité haute.
LOITER_SEVERITY = "moyenne"
ENCOUNTER_SEVERITY = "elevee"


@dataclass
class EngineConfig:
    kinds: tuple[str, ...] = KINDS
    # gap
    min_gap_s: float = DEFAULT_MIN_GAP_S
    bbox: BBox | None = None
    edge_margin_deg: float = EDGE_MARGIN_DEG
    watch_every_s: float = 60.0           # période de la veille « silence en cours »
    # stationnement
    loiter_max_sog: float = LOI_MAX_SOG
    loiter_radius_km: float = DEFAULT_RADIUS_KM
    loiter_min_duration_s: float = LOI_MIN_DURATION_S
    loiter_max_gap_s: float = LOI_MAX_GAP_S
    # rencontre
    enc_max_dist_km: float = DEFAULT_MAX_DIST_KM
    enc_max_sog: float = ENC_MAX_SOG
    enc_bucket_s: float = DEFAULT_BUCKET_S
    enc_min_duration_s: float = ENC_MIN_DURATION_S
    enc_max_gap_s: float = ENC_MAX_GAP_S


# ------------------------------------------------------------------ nettoyage
class OnlineCleaner:
    """Même règles que preprocess() (validation, doublons, sauts impossibles), par navire."""

    def __init__(self, max_speed_kn: float = MAX_SPEED_KN):
        self.max_speed_kn = max_speed_kn
        self.stats = PreprocessStats()
        self.out_of_order = 0
        self._last_seen: dict[int, float] = {}       # dernier horodatage valide vu
        self._anchor: dict[int, Position] = {}       # dernière position conservée
        self._streak: dict[int, int] = {}

    def accept(self, p: Position) -> bool:
        st = self.stats
        st.total_in += 1
        if not p.is_valid():
            st.invalid += 1
            return False
        last = self._last_seen.get(p.mmsi)
        if last is not None:
            if p.timestamp == last:
                st.duplicates += 1
                return False
            if p.timestamp < last:
                self.out_of_order += 1
                return False
        self._last_seen[p.mmsi] = p.timestamp
        anchor = self._anchor.get(p.mmsi)
        if anchor is not None:
            hours = (p.timestamp - anchor.timestamp) / 3600.0
            speed = haversine_km(anchor.lat, anchor.lon, p.lat, p.lon) / KNOT_KMH / hours
            streak = self._streak.get(p.mmsi, 0)
            if speed > self.max_speed_kn and streak < MAX_CONSECUTIVE_JUMPS:
                st.jumps += 1
                self._streak[p.mmsi] = streak + 1
                return False
        self._anchor[p.mmsi] = p
        self._streak[p.mmsi] = 0
        st.total_out += 1
        return True


# ------------------------------------------------------------------------ gap
def gap_event(g: GapAlert, now: float) -> AlertEvent:
    return AlertEvent("gap", CLOSED, [g.mmsi], g.start, g.end, g.last_lat, g.last_lon,
                      g.severity, now, {"resume_lat": g.resume_lat, "resume_lon": g.resume_lon,
                                        "jump_km": round(g.jump_km, 3),
                                        "implied_speed_kn": round(g.implied_speed_kn, 2)})


class OnlineGap:
    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg
        self.last: dict[int, Position] = {}
        self.open: dict[int, AlertEvent] = {}      # alertes « silence en cours »

    def _edge(self, p: Position) -> bool:
        return bool(self.cfg.bbox) and _near_edge(p.lat, p.lon, self.cfg.bbox, self.cfg.edge_margin_deg)

    def update(self, p: Position) -> list[AlertEvent]:
        prev = self.last.get(p.mmsi)
        self.last[p.mmsi] = p
        if prev is None:
            return []
        opened = self.open.pop(p.mmsi, None)
        dt = p.timestamp - prev.timestamp
        if dt >= self.cfg.min_gap_s and not (self._edge(prev) or self._edge(p)):
            d = haversine_km(prev.lat, prev.lon, p.lat, p.lon)
            g = GapAlert(p.mmsi, prev.timestamp, p.timestamp, prev.lat, prev.lon,
                         p.lat, p.lon, d, d / KNOT_KMH / (dt / 3600.0))
            return [gap_event(g, p.timestamp)]
        if opened is not None:                       # réapparition en bord de zone
            # nouvel objet : l'ancien a peut-être déjà été mis dans une file
            return [replace(opened, status=CANCELLED, end=p.timestamp, detected_at=p.timestamp)]
        return []

    def watch(self, now: float) -> list[AlertEvent]:
        """Navires muets depuis >= min_gap_s : alerte ouverte sans attendre leur retour."""
        out = []
        for mmsi, prev in self.last.items():
            if (mmsi not in self.open and now - prev.timestamp >= self.cfg.min_gap_s
                    and not self._edge(prev)):
                ev = AlertEvent("gap", OPEN, [mmsi], prev.timestamp, None, prev.lat, prev.lon,
                                "moyenne", now, {"silence_en_cours": True})
                self.open[mmsi] = ev
                out.append(ev)
        return out


# --------------------------------------------------------------- stationnement
@dataclass
class _LoiterState:
    anchor: Position
    last: Position
    n: int = 1
    sum_sog: float = 0.0
    max_d: float = 0.0
    opened: bool = False


def loiter_event(a: LoiterAlert, status: str, now: float) -> AlertEvent:
    return AlertEvent("loitering", status, [a.mmsi], a.start, a.end if status == CLOSED else None,
                      a.lat, a.lon, LOITER_SEVERITY, now,
                      {"mean_sog": round(a.mean_sog, 2), "max_dist_km": round(a.max_dist_km, 3)})


class OnlineLoitering:
    """Même machine à états que detect_loitering, une instance par navire."""

    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg
        self.state: dict[int, _LoiterState] = {}

    def _alert(self, s: _LoiterState) -> LoiterAlert:
        return LoiterAlert(s.anchor.mmsi, s.anchor.timestamp, s.last.timestamp,
                           s.anchor.lat, s.anchor.lon, s.sum_sog / s.n, s.max_d)

    def _close(self, s: _LoiterState, now: float) -> list[AlertEvent]:
        if s.last.timestamp - s.anchor.timestamp >= self.cfg.loiter_min_duration_s:
            return [loiter_event(self._alert(s), CLOSED, now)]
        return []

    def update(self, p: Position) -> list[AlertEvent]:
        c = self.cfg
        out: list[AlertEvent] = []
        s = self.state.get(p.mmsi)
        if s is not None:
            dt = p.timestamp - s.last.timestamp
            d = haversine_km(s.anchor.lat, s.anchor.lon, p.lat, p.lon)
            if p.sog <= c.loiter_max_sog and d <= c.loiter_radius_km and dt <= c.loiter_max_gap_s:
                s.last, s.n, s.sum_sog, s.max_d = p, s.n + 1, s.sum_sog + p.sog, max(s.max_d, d)
                if not s.opened and s.last.timestamp - s.anchor.timestamp >= c.loiter_min_duration_s:
                    s.opened = True
                    out.append(loiter_event(self._alert(s), OPEN, p.timestamp))
                return out
            out += self._close(s, p.timestamp)
        if p.sog <= c.loiter_max_sog:
            self.state[p.mmsi] = _LoiterState(p, p, 1, p.sog, 0.0)
        else:
            self.state.pop(p.mmsi, None)
        return out

    def flush(self, now: float) -> list[AlertEvent]:
        out = [e for s in self.state.values() for e in self._close(s, now)]
        self.state.clear()
        return out


# ------------------------------------------------------------------- rencontre
@dataclass
class _PairState:
    first: int          # première tranche de la rencontre
    prev: int           # dernière tranche avec contact
    lat: float
    lon: float
    n: int = 1
    opened: bool = False


def encounter_event(a: EncounterAlert, status: str, now: float) -> AlertEvent:
    return AlertEvent("encounter", status, [a.mmsi_a, a.mmsi_b], a.start,
                      a.end if status == CLOSED else None, a.lat, a.lon, ENCOUNTER_SEVERITY, now,
                      {"n_contacts": a.n_contacts})


class OnlineEncounter:
    """On accumule les positions lentes de la tranche de temps courante. Quand le temps
    passe à la tranche suivante, la tranche terminée est analysée avec la même grille
    que la phase 5 (find_contacts), puis l'état de chaque paire est mis à jour."""

    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg
        self.bucket = cfg.enc_bucket_s
        self.gap_slots = max(1, int(cfg.enc_max_gap_s // cfg.enc_bucket_s))
        self.cur_slot: int | None = None
        self.buf: dict[int, tuple[float, float]] = {}
        self.pairs: dict[tuple[int, int], _PairState] = {}

    def _alert(self, pair: tuple[int, int], s: _PairState) -> EncounterAlert:
        return EncounterAlert(pair[0], pair[1], s.first * self.bucket, (s.prev + 1) * self.bucket,
                              s.lat, s.lon, s.n)

    def _long_enough(self, s: _PairState) -> bool:
        return (s.prev - s.first + 1) * self.bucket >= self.cfg.enc_min_duration_s

    def _close(self, pair, s: _PairState, now: float) -> list[AlertEvent]:
        return [encounter_event(self._alert(pair, s), CLOSED, now)] if self._long_enough(s) else []

    def _process_slot(self, now: float) -> list[AlertEvent]:
        slot = self.cur_slot
        out: list[AlertEvent] = []
        if len(self.buf) >= 2:
            pts = [(slot, m, la, lo) for m, (la, lo) in self.buf.items()]
            for pair, (_, lats, lons) in find_contacts(pts, self.cfg.enc_max_dist_km).items():
                s = self.pairs.get(pair)
                if s is not None and slot - s.prev > self.gap_slots:
                    out += self._close(pair, s, now)
                    s = None
                if s is None:
                    s = self.pairs[pair] = _PairState(slot, slot, lats[0], lons[0])
                else:
                    s.prev, s.n = slot, s.n + 1
                if not s.opened and self._long_enough(s):
                    s.opened = True
                    out.append(encounter_event(self._alert(pair, s), OPEN, now))
        self.buf = {}
        for pair, s in list(self.pairs.items()):     # paires sans contact depuis trop longtemps
            if slot - s.prev > self.gap_slots:
                out += self._close(pair, s, now)
                del self.pairs[pair]
        return out

    def update(self, p: Position) -> list[AlertEvent]:
        slot = int(p.timestamp // self.bucket)
        out: list[AlertEvent] = []
        if self.cur_slot is None:
            self.cur_slot = slot
        elif slot > self.cur_slot:
            out = self._process_slot(p.timestamp)
            self.cur_slot = slot
        if p.sog <= self.cfg.enc_max_sog:
            self.buf[p.mmsi] = (p.lat, p.lon)        # dernière position de la tranche
        return out

    def flush(self, now: float) -> list[AlertEvent]:
        out = self._process_slot(now) if self.cur_slot is not None else []
        out += [e for pair, s in self.pairs.items() for e in self._close(pair, s, now)]
        self.pairs.clear()
        return out


# ---------------------------------------------------------------------- moteur
@dataclass
class EngineStats:
    positions_in: int = 0
    positions_ok: int = 0
    events: Counter = field(default_factory=Counter)   # (kind, status) -> nombre


class OnlineEngine:
    """Nettoyage + 3 détecteurs. feed(p) renvoie les événements produits par p."""

    def __init__(self, cfg: EngineConfig | None = None):
        self.cfg = cfg or EngineConfig()
        k = self.cfg.kinds
        self.cleaner = OnlineCleaner()
        self.gap = OnlineGap(self.cfg) if "gap" in k else None
        self.loiter = OnlineLoitering(self.cfg) if "loitering" in k else None
        self.enc = OnlineEncounter(self.cfg) if "encounter" in k else None
        self.clock: float | None = None            # horloge des données
        self.vessels: dict[int, Position] = {}     # dernière position de chaque navire
        self.stats = EngineStats()
        self._next_watch: float | None = None

    def _count(self, events: list[AlertEvent]) -> list[AlertEvent]:
        for e in events:
            self.stats.events[(e.kind, e.status)] += 1
        return events

    def feed(self, p: Position) -> list[AlertEvent]:
        self.stats.positions_in += 1
        if not self.cleaner.accept(p):
            return []
        self.stats.positions_ok += 1
        if self.clock is None or p.timestamp > self.clock:
            self.clock = p.timestamp
        self.vessels[p.mmsi] = p
        out: list[AlertEvent] = []
        if self.gap:
            out += self.gap.update(p)
        if self.loiter:
            out += self.loiter.update(p)
        if self.enc:
            out += self.enc.update(p)
        if self.gap:
            if self._next_watch is None:
                self._next_watch = self.clock + self.cfg.watch_every_s
            elif self.clock >= self._next_watch:
                out += self.gap.watch(self.clock)
                self._next_watch = self.clock + self.cfg.watch_every_s
        return self._count(out)

    def feed_many(self, positions: Iterable[Position]) -> list[AlertEvent]:
        out: list[AlertEvent] = []
        for p in positions:
            out += self.feed(p)
        return out

    def flush(self) -> list[AlertEvent]:
        """Fin du flux : ferme les stationnements et rencontres encore en cours.
        Les gaps ouverts restent ouverts (le navire n'est jamais réapparu)."""
        now = self.clock or 0.0
        out: list[AlertEvent] = []
        if self.loiter:
            out += self.loiter.flush(now)
        if self.enc:
            out += self.enc.flush(now)
        return self._count(out)

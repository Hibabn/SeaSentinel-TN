"""Simulateur de trafic AIS avec anomalies injectées (vérité terrain).

Zone : golfe de Gabès. Chaque navire avance pas à pas (cap et vitesse avec un
peu de bruit). On injecte trois types d'anomalies, et on enregistre pour
chacune sa « vérité terrain » : c'est ce qui permettra de mesurer les faux
positifs / faux négatifs des détecteurs.

  - gap        : le navire arrête d'émettre pendant 1 à 2 h, puis réapparaît
  - loitering  : le navire reste quasi immobile pendant 1h30 à 3h
  - encounter  : deux navires convergent, restent à < 500 m l'un de l'autre,
                 lents, pendant 1 à 2 h, puis repartent

Usage :
    python -m seasentinel.simulator --vessels 40 --hours 12 --seed 42
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from .geo import KNOT_KMH, bearing_deg, haversine_km, move_point
from .models import CSV_FIELDS, Position

# (lat_min, lat_max, lon_min, lon_max) : golfe de Gabès, approximatif
BBOX = (33.0, 35.0, 10.0, 12.0)
CENTER = ((BBOX[0] + BBOX[1]) / 2, (BBOX[2] + BBOX[3]) / 2)
MIDS = (672, 255, 247, 215, 218)  # préfixes MMSI : TUN, PRT, ITA, MLT, DEU
DEFAULT_START = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()


@dataclass
class Anomaly:
    type: str            # "gap" | "loitering" | "encounter"
    mmsis: list[int]
    start: float         # timestamp epoch
    end: float


@dataclass
class Scenario:
    positions: list[Position]
    anomalies: list[Anomaly]


class Vessel:
    """Navire simulé : machine à états cruise / goto / hold."""

    def __init__(self, mmsi: int, lat: float, lon: float, sog: float, cog: float):
        self.mmsi, self.lat, self.lon = mmsi, lat, lon
        self.sog, self.cog = sog, cog
        self.cruise_sog = sog
        self.mode = "cruise"
        self.target: tuple[float, float] | None = None
        self.arrived_at: float | None = None

    def start_goto(self, target: tuple[float, float], sog: float) -> None:
        self.mode, self.target, self.sog = "goto", target, sog

    def hold(self) -> None:
        self.mode = "hold"

    def resume(self, rng: random.Random) -> None:
        self.mode = "cruise"
        self.sog = self.cruise_sog
        self.cog = rng.uniform(0, 360)

    def step(self, t: float, dt: float, rng: random.Random) -> None:
        if self.mode == "goto":
            dist = haversine_km(self.lat, self.lon, *self.target)
            step_km = self.sog * KNOT_KMH * dt / 3600
            if dist <= max(step_km, 0.05):
                self.lat, self.lon = self.target
                self.mode, self.arrived_at, self.sog = "hold", t, 0.2
            else:
                self.cog = bearing_deg(self.lat, self.lon, *self.target)
        elif self.mode == "hold":
            self.sog = rng.uniform(0.1, 0.4)
            self.cog = rng.uniform(0, 360)
        else:  # cruise
            self.cog = (self.cog + rng.gauss(0, 3)) % 360
            self.sog = min(max(self.sog + rng.gauss(0, 0.15), 0.6 * self.cruise_sog),
                           1.4 * self.cruise_sog)
            nlat, nlon = move_point(self.lat, self.lon, self.cog,
                                    self.sog * KNOT_KMH * dt / 3600)
            if not (BBOX[0] < nlat < BBOX[1] and BBOX[2] < nlon < BBOX[3]):
                self.cog = bearing_deg(self.lat, self.lon, *CENTER)  # demi-tour vers le large
        self.lat, self.lon = move_point(self.lat, self.lon, self.cog,
                                        self.sog * KNOT_KMH * dt / 3600)


def _random_point(rng: random.Random, margin: float = 0.3) -> tuple[float, float]:
    return (rng.uniform(BBOX[0] + margin, BBOX[1] - margin),
            rng.uniform(BBOX[2] + margin, BBOX[3] - margin))


def generate(n_vessels: int = 40, hours: float = 12.0, step_s: int = 60,
             seed: int = 42, n_gaps: int = 3, n_loiter: int = 3,
             n_encounters: int = 2, start: float = DEFAULT_START) -> Scenario:
    """Génère un scénario complet. Même seed => mêmes données (reproductible)."""
    if hours < 6:
        raise ValueError("hours doit être >= 6 pour placer les anomalies")
    if n_vessels < n_gaps + n_loiter:
        raise ValueError("pas assez de navires pour les gaps et stationnements demandés")

    rng = random.Random(seed)
    duration = hours * 3600
    vessels: dict[int, Vessel] = {}

    def new_mmsi(i: int) -> int:
        return MIDS[i % len(MIDS)] * 1_000_000 + 1000 + i

    # --- navires « normaux » ---
    for i in range(n_vessels):
        lat, lon = _random_point(rng)
        sog = rng.uniform(6, 14)
        v = Vessel(new_mmsi(i), lat, lon, sog, rng.uniform(0, 360))
        vessels[v.mmsi] = v
    normal_ids = list(vessels)

    anomalies: list[Anomaly] = []
    events: list[tuple[float, int, str]] = []   # (t, mmsi, "hold" | "resume")
    silent: dict[int, tuple[float, float]] = {}  # mmsi -> (début, fin) de silence

    # --- gaps AIS ---
    chosen = rng.sample(normal_ids, n_gaps + n_loiter)
    for mmsi in chosen[:n_gaps]:
        gs = rng.uniform(3600, duration - 3 * 3600)
        ge = gs + rng.uniform(3600, 7200)
        silent[mmsi] = (gs, ge)
        anomalies.append(Anomaly("gap", [mmsi], start + gs, start + ge))

    # --- stationnements ---
    for mmsi in chosen[n_gaps:]:
        ls = rng.uniform(3600, duration - 4 * 3600)
        le = ls + rng.uniform(5400, 10800)
        events += [(ls, mmsi, "hold"), (le, mmsi, "resume")]
        anomalies.append(Anomaly("loitering", [mmsi], start + ls, start + le))

    # --- rencontres : 2 navires dédiés par rencontre ---
    encounter_pairs: list[tuple[Vessel, Vessel, float]] = []
    for k in range(n_encounters):
        meet = _random_point(rng, margin=0.5)
        t_meet = rng.uniform(3600, 4 * 3600)
        hold_end = t_meet + rng.uniform(3600, 7200)
        approach_sog = 9.0
        dist_km = approach_sog * KNOT_KMH * t_meet / 3600
        side = rng.uniform(0, 360)
        pair = []
        for j, bearing in enumerate((side, side + 180)):
            mmsi = new_mmsi(n_vessels + 2 * k + j)
            slat, slon = move_point(*meet, bearing, dist_km)
            v = Vessel(mmsi, slat, slon, approach_sog, 0.0)
            # B se place ~200 m de A : les deux navires sont « à couple »
            target = meet if j == 0 else move_point(*meet, rng.uniform(0, 360), 0.2)
            v.start_goto(target, approach_sog)
            vessels[mmsi] = v
            events.append((hold_end, mmsi, "resume"))
            pair.append(v)
        encounter_pairs.append((pair[0], pair[1], hold_end))

    events.sort()
    positions: list[Position] = []
    n_steps = int(duration // step_s)
    ei = 0
    for s in range(n_steps):
        t = s * step_s
        while ei < len(events) and events[ei][0] <= t:
            _, mmsi, action = events[ei]
            if action == "hold":
                vessels[mmsi].hold()
            else:
                vessels[mmsi].resume(rng)
            ei += 1
        for mmsi, v in vessels.items():
            v.step(t, step_s, rng)
            gap = silent.get(mmsi)
            if gap and gap[0] <= t < gap[1]:
                continue  # navire silencieux : rien n'est émis
            positions.append(Position(
                mmsi=mmsi,
                timestamp=start + t,
                lat=round(v.lat + rng.gauss(0, 0.00005), 5),   # bruit GPS ~5 m
                lon=round(v.lon + rng.gauss(0, 0.00005), 5),
                sog=round(v.sog, 1),
                cog=round(v.cog % 360, 1),
            ))

    for a, b, hold_end in encounter_pairs:
        if a.arrived_at is not None and b.arrived_at is not None:
            anomalies.append(Anomaly("encounter", [a.mmsi, b.mmsi],
                                     start + max(a.arrived_at, b.arrived_at),
                                     start + hold_end))

    positions.sort(key=lambda p: (p.timestamp, p.mmsi))
    anomalies.sort(key=lambda a: a.start)
    return Scenario(positions, anomalies)


def write_csv(positions: list[Position], path: Path) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(CSV_FIELDS)
        for p in positions:
            w.writerow(p.to_row())


def write_truth(anomalies: list[Anomaly], path: Path) -> None:
    with open(path, "w") as f:
        json.dump([asdict(a) for a in anomalies], f, indent=2)


def main() -> None:
    ap = argparse.ArgumentParser(description="Simulateur AIS SeaSentinel")
    ap.add_argument("--vessels", type=int, default=40)
    ap.add_argument("--hours", type=float, default=12)
    ap.add_argument("--step", type=int, default=60, help="période d'émission (s)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--gaps", type=int, default=3)
    ap.add_argument("--loiterings", type=int, default=3)
    ap.add_argument("--encounters", type=int, default=2)
    ap.add_argument("--out-dir", default="data")
    ap.add_argument("--name", default="sim")
    a = ap.parse_args()

    sc = generate(a.vessels, a.hours, a.step, a.seed, a.gaps, a.loiterings, a.encounters)
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    write_csv(sc.positions, out / f"{a.name}_positions.csv")
    write_truth(sc.anomalies, out / f"{a.name}_truth.json")
    print(f"{len(sc.positions)} positions, {len({p.mmsi for p in sc.positions})} navires, "
          f"{len(sc.anomalies)} anomalies -> {out}/")


if __name__ == "__main__":
    main()

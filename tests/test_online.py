"""Le moteur en ligne doit retrouver EXACTEMENT les alertes des détecteurs batch."""
import random
from dataclasses import replace

import pytest
from seasentinel.detectors.encounter import detect_encounters
from seasentinel.detectors.gap import detect_gaps
from seasentinel.detectors.loitering import detect_loitering
from seasentinel.live.events import CANCELLED, CLOSED, OPEN, AlertEvent
from seasentinel.live.online import EngineConfig, OnlineCleaner, OnlineEngine
from seasentinel.models import Position
from seasentinel.preprocess import preprocess
from seasentinel.simulator import generate


def noisy(positions, seed=0):
    """Ajoute doublons, positions invalides et sauts impossibles, en gardant l'ordre du temps."""
    rng = random.Random(seed)
    out = []
    for p in positions:
        out.append(p)
        r = rng.random()
        if r < 0.01:
            out.append(p)                                   # doublon exact
        elif r < 0.02:
            out.append(replace(p, timestamp=p.timestamp + 1, lat=95.0))   # invalide
        elif r < 0.025:
            out.append(replace(p, timestamp=p.timestamp + 2, lat=p.lat + 1.5))  # saut
    out.sort(key=lambda p: (p.timestamp, p.mmsi))
    return out


def run_online(positions, cfg=None):
    eng = OnlineEngine(cfg)
    events = eng.feed_many(positions) + eng.flush()
    return eng, events


def closed(events, kind):
    return sorted((tuple(e.mmsis), e.start, e.end) for e in events
                  if e.kind == kind and e.status == CLOSED)


SCENARIOS = [dict(seed=42), dict(seed=7, n_vessels=60, n_gaps=5, n_loiter=4, n_encounters=3),
             dict(seed=3, hours=8)]


@pytest.mark.parametrize("kw", SCENARIOS)
@pytest.mark.parametrize("with_noise", [False, True])
def test_online_equals_batch(kw, with_noise):
    stream = generate(**kw).positions
    if with_noise:
        stream = noisy(stream, kw["seed"])
    cfg = EngineConfig(enc_min_duration_s=1800)
    eng, events = run_online(stream, cfg)

    clean, st = preprocess(stream)
    assert eng.cleaner.stats == st                       # même nettoyage
    by_time = sorted(clean, key=lambda p: (p.timestamp, p.mmsi))

    gaps = sorted(((g.mmsi,), g.start, g.end) for g in detect_gaps(clean, cfg.min_gap_s))
    loi = sorted(((a.mmsi,), a.start, a.end) for a in detect_loitering(clean))
    enc = sorted(((a.mmsi_a, a.mmsi_b), a.start, a.end)
                 for a in detect_encounters(by_time, min_duration_s=1800))
    assert gaps and loi and enc                           # le test n'est pas vide
    assert closed(events, "gap") == gaps
    assert closed(events, "loitering") == loi
    assert closed(events, "encounter") == enc


def test_open_alert_comes_before_close_with_same_id():
    _, events = run_online(generate(seed=42).positions, EngineConfig(enc_min_duration_s=1800))
    first_status = {}
    for e in events:
        first_status.setdefault(e.id, e.status)
    for e in events:
        if e.status == CLOSED and e.kind in ("loitering", "encounter"):
            assert first_status[e.id] == OPEN       # signalé pendant l'anomalie, pas après


def test_gap_watchdog_fires_before_vessel_reappears():
    sc = generate(seed=42)
    truth = [a for a in sc.anomalies if a.type == "gap"]
    _, events = run_online(sc.positions)
    for t in truth:
        opened = [e for e in events if e.kind == "gap" and e.status == OPEN and e.mmsis == t.mmsis]
        assert opened, "silence non signalé en cours"
        assert opened[0].detected_at < t.end           # alerte avant la réapparition
        assert opened[0].detected_at - opened[0].start >= 3600


def test_gap_open_then_cancelled_near_bbox_edge():
    bbox = (33.0, 35.0, 10.0, 12.0)
    pts = [Position(247000001, 1000.0 + 60 * i, 34.0, 11.0, 10.0, 90.0) for i in range(3)]
    pts.append(Position(247000002, 1000.0, 34.5, 11.5, 10.0, 0.0))
    # le navire 2 continue d'émettre (fait avancer l'horloge) ; le 1 se tait 2 h
    pts += [Position(247000002, 1000.0 + 60 * i, 34.5, 11.5, 10.0, 0.0) for i in range(1, 130)]
    pts.append(Position(247000001, 1000.0 + 60 * 125, 34.0, 11.95, 10.0, 90.0))  # bord de zone
    pts.sort(key=lambda p: (p.timestamp, p.mmsi))
    _, events = run_online(pts, EngineConfig(bbox=bbox, kinds=("gap",)))
    statuses = [e.status for e in events if e.mmsis == [247000001]]
    assert statuses == [OPEN, CANCELLED]


def test_cleaner_rejects_out_of_order():
    c = OnlineCleaner()
    assert c.accept(Position(247000001, 100.0, 34.0, 11.0, 5.0, 0.0))
    assert not c.accept(Position(247000001, 50.0, 34.0, 11.0, 5.0, 0.0))
    assert c.out_of_order == 1


def test_event_roundtrip_and_stable_id():
    e = AlertEvent("encounter", OPEN, [215001043, 247001042], 1767230000.0, None, 33.5, 10.9,
                   "elevee", 1767233600.0, {"n_contacts": 4})
    d = e.to_dict()
    assert d["id"] == "encounter:215001043-247001042:1767230000"
    assert d["duration_s"] == 3600.0
    assert AlertEvent.from_dict(d) == e

import math
import random

import pytest
from seasentinel.detectors.encounter import (build_encounters, detect_encounters, find_contacts,
                                             merge_contacts, slow_points)
from seasentinel.evaluation import evaluate
from seasentinel.models import Position
from seasentinel.parallel_encounters import (band_bounds, halo_degrees, partition_bands,
                                             run_encounters_parallel, run_encounters_sequential)
from seasentinel.preprocess import preprocess
from seasentinel.simulator import generate

DLON_200M = 0.2 / (111.19 * math.cos(math.radians(34.5)))


def pair(a, b, t0, n, lat=34.5, lon=10.5, sog=0.2, dlon=DLON_200M, step=60):
    out = []
    for i in range(n):
        t = t0 + step * i
        out += [Position(a, t, lat, lon, sog, 0.0), Position(b, t, lat, lon + dlon, sog, 0.0)]
    return out


def test_close_slow_pair_for_long_enough_is_an_encounter():
    (e,) = detect_encounters(pair(672000001, 672000002, 0 + 300, 40), min_duration_s=1800)
    assert (e.mmsi_a, e.mmsi_b) == (672000001, 672000002) and e.duration_s >= 1800
    assert e.lat == pytest.approx(34.5) and e.n_contacts >= 6


def test_too_short_far_or_fast_pairs_are_ignored():
    assert detect_encounters(pair(1, 2, 300, 8), min_duration_s=1800) == []                 # trop court
    assert detect_encounters(pair(1, 2, 300, 40, dlon=0.01), min_duration_s=1800) == []     # ~0,9 km
    assert detect_encounters(pair(1, 2, 300, 40, sog=6.0), min_duration_s=1800) == []       # trop rapides


def test_pair_is_ordered_and_vessels_cannot_meet_themselves():
    (e,) = detect_encounters(pair(9, 5, 300, 40), min_duration_s=1800)
    assert (e.mmsi_a, e.mmsi_b) == (5, 9)


def test_small_gap_is_tolerated_large_gap_splits():
    first = pair(1, 2, 300, 30)                       # 30 min
    near = pair(1, 2, 300 + 60 * 30 + 600, 30)        # reprise après 10 min de trou : même rencontre
    far = pair(1, 2, 300 + 60 * 30 + 7200, 30)        # reprise 2 h après : autre épisode
    assert len(detect_encounters(first + near, min_duration_s=1800, max_gap_s=900)) == 1
    assert len(detect_encounters(first + far, min_duration_s=1800, max_gap_s=900)) == 2


def test_pair_across_grid_cell_edge_is_found():
    # une cellule fait ~0,5 km : on essaie plusieurs positions pour tomber à cheval sur une arête
    cell_lon = 0.5 / 110.0 / math.cos(math.radians(34.5))
    for k in range(1, 8):
        lon = k * cell_lon - DLON_200M / 2
        assert len(detect_encounters(pair(1, 2, 300, 40, lon=lon), min_duration_s=1800)) == 1


def test_contacts_merge_does_not_lose_anything():
    pts = slow_points(pair(1, 2, 300, 20) + pair(3, 4, 300, 20, lon=11.0))
    whole = find_contacts(pts)
    parts = merge_contacts([find_contacts(pts, core=(-math.inf, 10.7)), find_contacts(pts, core=(10.7, math.inf))])
    assert {k: sorted(v[0]) for k, v in whole.items()} == {k: sorted(v[0]) for k, v in parts.items()}


@pytest.mark.parametrize("a_first", [True, False])
def test_halo_is_what_makes_boundary_pairs_visible(a_first):
    a, b = (1, 2) if a_first else (2, 1)
    pts = slow_points(pair(a, b, 300, 20, lon=11.0 - DLON_200M / 2))   # un navire de chaque côté de 11.0
    bounds = [11.0]
    n_contacts = {}
    for halo in (0.0, halo_degrees(pts, 0.5)):
        parts = partition_bands(pts, bounds, halo)
        cores = [(-math.inf, 11.0), (11.0, math.inf)]
        n_contacts[halo] = sum(len(find_contacts(p, core=c)) for p, c in zip(parts, cores))
    assert n_contacts[0.0] == 0          # sans halo : rencontre ratée
    assert sorted(n_contacts.values()) == [0, 1]   # avec halo : trouvée UNE seule fois


def scene(seed=5, n_pairs=60, n_loners=300):
    rng = random.Random(seed)
    pos = []
    for k in range(n_pairs):
        pos += pair(10_000 + 2 * k, 10_001 + 2 * k, 300 + rng.randrange(0, 20) * 60, rng.randrange(8, 40),
                    lat=rng.uniform(34, 35), lon=rng.uniform(10, 12))
    for k in range(n_loners):                       # navires lents isolés (bruit)
        t0 = rng.randrange(0, 3000)
        pos += [Position(50_000 + k, t0 + 60 * i, rng.uniform(34, 35), rng.uniform(10, 12), 0.3, 0.0)
                for i in range(20)]
    base = slow_points(pos)
    mid = band_bounds(base, 2)[0]                   # paires à cheval sur la frontière
    for k in range(40):
        pos += pair(70_000 + 2 * k, 70_001 + 2 * k, 300, 30, lat=34.5, lon=mid + rng.uniform(-0.01, 0.01))
    return slow_points(pos)


@pytest.mark.parametrize("workers,strategy", [(2, "quantile"), (3, "quantile"), (3, "uniform"), (4, "quantile")])
def test_parallel_equals_sequential(workers, strategy):
    pts = scene()
    kw = dict(min_duration_s=1800)
    seq = run_encounters_sequential(pts, **kw)
    par = run_encounters_parallel(pts, workers, strategy, **kw)
    assert len(seq.alerts) > 30
    assert par.keys() == seq.keys()


def test_without_halo_parallel_misses_encounters():
    pts = scene()
    seq = run_encounters_sequential(pts, min_duration_s=1800)
    par = run_encounters_parallel(pts, 2, "quantile", min_duration_s=1800, halo=0.0)
    assert len(par.alerts) < len(seq.alerts)


def test_quantile_bands_are_balanced_and_uniform_may_not_be():
    rng = random.Random(1)
    pts = [(0, i, 34.0, rng.gauss(10.0, 0.05)) for i in range(5000)] + \
          [(0, 9000 + i, 34.0, rng.uniform(10.0, 14.0)) for i in range(50)]
    def loads(strategy):
        return [len(p) for p in partition_bands(pts, band_bounds(pts, 4, strategy), 0.0)]
    q, u = loads("quantile"), loads("uniform")
    assert max(q) / (sum(q) / 4) < 1.2
    assert max(u) / (sum(u) / 4) > 2.0


@pytest.mark.parametrize("seed", [42, 1, 7])
def test_simulator_ground_truth(seed):
    sc = generate(n_vessels=40, hours=12.0, seed=seed)
    clean, _ = preprocess(sc.positions)
    alerts = detect_encounters(clean, min_duration_s=1800)
    res = evaluate(alerts, sc.anomalies, "encounter", exact_mmsis=True)
    assert res["verite"] == 2 and res["rappel"] == 1.0 and res["faux_positifs"] == 0
    par = run_encounters_parallel(slow_points(clean), 2, "quantile", min_duration_s=1800)
    assert par.keys() == sorted((a.mmsi_a, a.mmsi_b, a.start, a.end) for a in alerts)

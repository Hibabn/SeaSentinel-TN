import pickle

import pytest
from seasentinel.detectors.gap import detect_gaps
from seasentinel.models import Position
from seasentinel.parallel import (process_range, run_parallel_inmemory, run_parallel_ranges,
                                  run_sequential_inmemory, split_ranges)
from seasentinel.partition import balanced_assignment, hash_worker, imbalance, partition_positions
from seasentinel.positions_io import read_positions_csv
from seasentinel.preprocess import preprocess, write_clean_csv
from seasentinel.simulator import generate


@pytest.fixture(scope="module")
def sim_csv(tmp_path_factory):
    sc = generate(n_vessels=60, hours=12.0, seed=42)
    clean, _ = preprocess(sc.positions)
    path = tmp_path_factory.mktemp("d") / "clean.csv"
    write_clean_csv(clean, path)
    return path, clean


def vessel_sets(path, ranges):
    out = []
    for s, e in ranges:
        c = [0]
        from seasentinel.parallel import _iter_range
        out.append({p.mmsi for p in _iter_range(path, s, e, c)})
    return out


@pytest.mark.parametrize("parts", [1, 2, 3, 7, 16, 500])
def test_ranges_cover_file_and_never_split_a_vessel(sim_csv, parts):
    path, clean = sim_csv
    ranges = split_ranges(path, parts)
    assert 1 <= len(ranges) <= parts
    assert all(b > a for a, b in ranges)
    assert all(ranges[i][1] == ranges[i + 1][0] for i in range(len(ranges) - 1))
    sets = vessel_sets(path, ranges)
    for i in range(len(sets)):
        for j in range(i + 1, len(sets)):
            assert not (sets[i] & sets[j])                 # un navire = une seule plage
    assert set().union(*sets) == {p.mmsi for p in clean}   # aucun navire perdu
    assert sum(process_range(path, s, e)[1] for s, e in ranges) == len(clean)


def test_empty_file_has_no_ranges(tmp_path):
    p = tmp_path / "e.csv"
    p.write_text("mmsi,timestamp,lat,lon,sog,cog\n")
    assert split_ranges(p, 4) == []


@pytest.mark.parametrize("workers,gran", [(1, 1), (2, 1), (2, 3), (3, 2)])
def test_parallel_ranges_equals_sequential(sim_csv, workers, gran):
    path, clean = sim_csv
    expected = sorted((a.mmsi, a.start, a.end) for a in detect_gaps(clean, 1800))
    res = run_parallel_ranges(path, workers, gran, min_gap_s=1800)
    assert res.keys() == expected and len(expected) >= 3
    assert res.positions == len(clean)
    assert len(res.stats) <= workers


@pytest.mark.parametrize("strategy", ["hash", "balanced"])
def test_inmemory_parallel_equals_sequential(sim_csv, strategy):
    _, clean = sim_csv
    seq = run_sequential_inmemory(clean, 1800)
    par = run_parallel_inmemory(clean, 2, strategy, 1800)
    assert par.keys() == seq.keys() and par.positions == len(clean)


def test_partition_keeps_vessels_together_and_ordered(sim_csv):
    _, clean = sim_csv
    for strategy in ("hash", "balanced"):
        parts = partition_positions(clean, 4, strategy)
        assert sum(map(len, parts)) == len(clean)
        owner = {}
        for i, part in enumerate(parts):
            for p in part:
                assert owner.setdefault(p.mmsi, i) == i
            keys = [(p.mmsi, p.timestamp) for p in part]
            assert keys == sorted(keys)


def test_balanced_beats_hash_on_skewed_load():
    counts = {4: 1000, 8: 900, 12: 800, 1: 10, 2: 10, 3: 10}     # tous les gros navires => même worker avec mmsi % 4
    hash_loads = [0] * 4
    for m, c in counts.items():
        hash_loads[hash_worker(m, 4)] += c
    bal = balanced_assignment(counts, 4)
    bal_loads = [0] * 4
    for m, c in counts.items():
        bal_loads[bal[m]] += c
    assert imbalance(bal_loads) < imbalance(hash_loads)
    assert set(bal) == set(counts)


def test_imbalance_metric():
    assert imbalance([5, 5, 5, 5]) == 1.0
    assert imbalance([10, 0, 0, 0]) == 4.0
    assert imbalance([]) == 1.0


def test_position_survives_pickle():
    p = Position(672000001, 1e9, 34.0, 10.5, 8.0, 90.0)
    assert pickle.loads(pickle.dumps(p)) == p

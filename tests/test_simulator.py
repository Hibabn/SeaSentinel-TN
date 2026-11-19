import pytest
from seasentinel.geo import haversine_km
from seasentinel.simulator import generate


@pytest.fixture(scope="module")
def sc():
    return generate(n_vessels=30, hours=12, seed=7)


def by_mmsi(positions, mmsi):
    return [p for p in positions if p.mmsi == mmsi]


def test_deterministic_with_same_seed():
    a = generate(n_vessels=10, hours=6, seed=1)
    b = generate(n_vessels=10, hours=6, seed=1)
    assert a.positions == b.positions and a.anomalies == b.anomalies


def test_different_seed_gives_different_data():
    assert generate(n_vessels=10, hours=6, seed=1).positions != \
           generate(n_vessels=10, hours=6, seed=2).positions


def test_stream_is_sorted_and_valid(sc):
    ts = [(p.timestamp, p.mmsi) for p in sc.positions]
    assert ts == sorted(ts)
    assert all(p.is_valid() for p in sc.positions)


def test_expected_anomaly_counts(sc):
    kinds = [a.type for a in sc.anomalies]
    assert kinds.count("gap") == 3
    assert kinds.count("loitering") == 3
    assert kinds.count("encounter") == 2


def test_gap_means_silence(sc):
    for a in (x for x in sc.anomalies if x.type == "gap"):
        inside = [p for p in by_mmsi(sc.positions, a.mmsis[0]) if a.start <= p.timestamp < a.end]
        assert inside == []
        assert by_mmsi(sc.positions, a.mmsis[0])  # le navire a bien émis avant/après


def test_loitering_is_slow(sc):
    for a in (x for x in sc.anomalies if x.type == "loitering"):
        inside = [p for p in by_mmsi(sc.positions, a.mmsis[0]) if a.start + 120 <= p.timestamp < a.end]
        assert inside and all(p.sog < 1.0 for p in inside)


def test_encounter_vessels_are_close_and_slow(sc):
    for a in (x for x in sc.anomalies if x.type == "encounter"):
        mid = (a.start + a.end) / 2
        pa = min(by_mmsi(sc.positions, a.mmsis[0]), key=lambda p: abs(p.timestamp - mid))
        pb = min(by_mmsi(sc.positions, a.mmsis[1]), key=lambda p: abs(p.timestamp - mid))
        assert haversine_km(pa.lat, pa.lon, pb.lat, pb.lon) < 0.5
        assert pa.sog < 1.0 and pb.sog < 1.0


def test_too_short_duration_rejected():
    with pytest.raises(ValueError):
        generate(hours=2)

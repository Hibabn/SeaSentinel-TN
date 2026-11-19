from seasentinel.models import Position
from seasentinel.preprocess import preprocess


def P(mmsi=672000001, t=1000.0, lat=34.0, lon=10.5, sog=8.0, cog=90.0):
    return Position(mmsi, t, lat, lon, sog, cog)


def test_removes_invalid():
    out, st = preprocess([P(), P(t=1060, lat=95.0), P(t=1120, sog=102.3), P(t=1180, mmsi=42)])
    assert len(out) == 1 and st.invalid == 3 and st.total_in == 4


def test_removes_duplicates_keeps_first():
    out, st = preprocess([P(sog=5.0), P(sog=9.0)])
    assert len(out) == 1 and out[0].sog == 5.0 and st.duplicates == 1


def test_sorted_by_mmsi_then_time():
    a, b = 672000001, 672000002
    out, _ = preprocess([P(b, 20), P(a, 30), P(b, 10), P(a, 5)])
    assert [(p.mmsi, p.timestamp) for p in out] == [(a, 5), (a, 30), (b, 10), (b, 20)]


def test_removes_teleport_but_keeps_neighbours():
    pts = [P(t=1000), P(t=1060, lat=34.001), P(t=1120, lat=40.0), P(t=1180, lat=34.002)]
    out, st = preprocess(pts)
    assert st.jumps == 1 and [p.lat for p in out] == [34.0, 34.001, 34.002]


def test_bad_first_point_does_not_wipe_the_vessel():
    pts = [P(t=1000, lat=50.0)] + [P(t=1000 + 60 * i, lat=34.0 + 0.001 * i) for i in range(1, 8)]
    out, st = preprocess(pts)
    assert st.jumps == 3 and len(out) == 5      # reprend après 3 rejets d'affilée


def test_invalid_rate():
    _, st = preprocess([P(), P(), P(t=2000, lat=99.0), P(t=3000)])
    assert st.invalid_rate == 0.5


def test_works_on_simulator_output():
    from seasentinel.simulator import generate
    sc = generate(n_vessels=20, hours=6.0, seed=1)
    out, st = preprocess(sc.positions)
    assert st.total_out == len(sc.positions) and st.invalid_rate == 0.0

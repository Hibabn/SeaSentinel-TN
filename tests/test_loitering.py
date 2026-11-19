import pytest
from seasentinel.detectors.loitering import detect_loitering
from seasentinel.evaluation import evaluate
from seasentinel.models import Position
from seasentinel.preprocess import preprocess
from seasentinel.simulator import generate

A, B = 672000001, 672000002


def P(mmsi=A, t=1000.0, lat=34.0, lon=10.5, sog=0.2):
    return Position(mmsi, t, lat, lon, sog, 90.0)


def run(n, step=60, **kw):
    return [P(t=1000 + step * i, **kw) for i in range(n)]


def test_detects_long_slow_run():
    (a,) = detect_loitering(run(100), min_duration_s=3600)        # 99 min
    assert a.mmsi == A and a.start == 1000 and a.end == 1000 + 99 * 60
    assert a.duration_s == 99 * 60 and a.max_dist_km == 0.0 and a.mean_sog == pytest.approx(0.2)


def test_too_short_is_ignored():
    assert list(detect_loitering(run(30), min_duration_s=3600)) == []


def test_fast_vessel_is_ignored():
    assert list(detect_loitering(run(200, sog=8.0))) == []


def test_drifting_vessel_is_cut_by_radius():
    pts = [P(t=1000 + 60 * i, lat=34.0 + 0.0005 * i) for i in range(200)]   # ~55 m / pas
    alerts = list(detect_loitering(pts, radius_km=0.5, min_duration_s=3600))
    assert alerts == []       # chaque séquence tient < 10 pas : trop courte


def test_gap_inside_run_splits_it():
    pts = run(50) + [P(t=1000 + 60 * 50 + 7200 + 60 * i) for i in range(50)]
    assert list(detect_loitering(pts, min_duration_s=3000, max_gap_s=1800)) == []
    assert len(list(detect_loitering(pts, min_duration_s=3000, max_gap_s=9000))) == 1


def test_run_ends_when_vessel_speeds_up_and_is_flushed_at_change_of_vessel():
    pts = run(80) + [P(t=1000 + 60 * 80, sog=9.0)] + run(80, mmsi=B)
    alerts = list(detect_loitering(pts, min_duration_s=3600))
    assert [a.mmsi for a in alerts] == [A, B]


def test_unsorted_is_rejected():
    with pytest.raises(ValueError):
        list(detect_loitering([P(t=2000), P(t=1000)]))


@pytest.mark.parametrize("seed", [42, 1, 7, 123])
def test_simulator_ground_truth(seed):
    sc = generate(n_vessels=40, hours=12.0, seed=seed)
    clean, _ = preprocess(sc.positions)
    res = evaluate(detect_loitering(clean), sc.anomalies, "loitering", explained_by=("encounter",))
    assert res["verite"] == 3 and res["rappel"] == 1.0
    # les seuls faux positifs sont les navires des rencontres (eux aussi à l'arrêt)
    assert res["faux_positifs"] == res["faux_positifs_expliques"]

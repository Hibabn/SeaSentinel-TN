import pytest
from seasentinel.detectors.gap import GapAlert, detect_gaps, evaluate
from seasentinel.models import Position
from seasentinel.preprocess import preprocess
from seasentinel.simulator import Anomaly, generate

A, B = 672000001, 672000002


def P(mmsi=A, t=1000.0, lat=34.0, lon=10.5):
    return Position(mmsi, t, lat, lon, 8.0, 90.0)


def test_regular_stream_has_no_gap():
    assert list(detect_gaps([P(t=1000 + 60 * i) for i in range(100)])) == []


def test_detects_gap_with_correct_bounds():
    pts = [P(t=1000), P(t=1060), P(t=1060 + 7200, lat=34.1), P(t=1060 + 7260, lat=34.1)]
    (g,) = detect_gaps(pts)
    assert (g.mmsi, g.start, g.end, g.duration_s) == (A, 1060, 8260, 7200)
    assert g.jump_km == pytest.approx(11.12, abs=0.1)
    assert g.implied_speed_kn == pytest.approx(g.jump_km / 1.852 / 2, rel=1e-6)


def test_threshold_is_inclusive():
    pts = [P(t=1000), P(t=1000 + 3600)]
    assert len(list(detect_gaps(pts, min_gap_s=3600))) == 1
    assert len(list(detect_gaps(pts, min_gap_s=3601))) == 0


def test_vessels_are_not_mixed():
    pts = [P(A, 1000), P(B, 50_000)]      # écart énorme, mais navires différents
    assert list(detect_gaps(pts)) == []


def test_unsorted_input_is_rejected():
    with pytest.raises(ValueError):
        list(detect_gaps([P(t=2000), P(t=1000)]))


def test_gap_near_window_edge_is_ignored():
    bbox = (33.0, 35.0, 10.0, 12.0)
    edge = [P(t=1000, lat=33.02), P(t=9000, lat=34.0)]     # part du bord sud
    inside = [P(t=1000, lat=34.0), P(t=9000, lat=34.0)]
    assert list(detect_gaps(edge, bbox=bbox)) == []
    assert len(list(detect_gaps(inside, bbox=bbox))) == 1


def test_works_lazily_on_generator():
    gen = (P(t=1000 + 7200 * i) for i in range(3))
    assert len(list(detect_gaps(gen))) == 2


def test_simulator_ground_truth_is_found():
    sc = generate(n_vessels=40, hours=12.0, seed=42)
    clean, _ = preprocess(sc.positions)
    alerts = list(detect_gaps(clean, min_gap_s=1800))
    res = evaluate(alerts, sc.anomalies, "gap")
    assert res["verite"] == 3
    assert res["rappel"] == 1.0 and res["precision"] == 1.0


@pytest.mark.parametrize("seed", [1, 7, 123])
def test_ground_truth_other_seeds(seed):
    sc = generate(n_vessels=30, hours=10.0, seed=seed)
    clean, _ = preprocess(sc.positions)
    res = evaluate(detect_gaps(clean, min_gap_s=1800), sc.anomalies, "gap")
    assert res["rappel"] == 1.0 and res["faux_positifs"] == 0


def test_evaluate_counts_false_positives_and_misses():
    truth = [Anomaly("gap", [A], 1000, 5000), Anomaly("gap", [B], 1000, 5000)]
    alerts = [GapAlert(A, 1500, 4000, 0, 0, 0, 0, 0, 0),      # juste
              GapAlert(A, 9000, 12000, 0, 0, 0, 0, 0, 0)]     # faux
    r = evaluate(alerts, truth, "gap")
    assert (r["vrais_positifs"], r["faux_positifs"], r["faux_negatifs"]) == (1, 1, 1)

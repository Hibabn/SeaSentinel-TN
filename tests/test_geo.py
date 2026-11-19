import pytest
from seasentinel.geo import bearing_deg, haversine_km, move_point


def test_one_degree_latitude_is_about_111_km():
    assert haversine_km(34.0, 10.0, 35.0, 10.0) == pytest.approx(111.19, abs=0.1)


def test_distance_is_zero_for_same_point():
    assert haversine_km(34.0, 10.0, 34.0, 10.0) == 0.0


def test_bearing_north_and_east():
    assert bearing_deg(34.0, 10.0, 35.0, 10.0) == pytest.approx(0.0, abs=0.01)
    assert bearing_deg(0.0, 10.0, 0.0, 11.0) == pytest.approx(90.0, abs=0.01)


def test_move_point_roundtrip():
    lat, lon = move_point(34.0, 10.5, 45.0, 25.0)
    assert haversine_km(34.0, 10.5, lat, lon) == pytest.approx(25.0, abs=0.01)

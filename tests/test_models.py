import dataclasses

import pytest
from seasentinel.models import CSV_FIELDS, Position


def make(**kw):
    base = dict(mmsi=672000123, timestamp=1.7e9, lat=34.0, lon=10.5, sog=8.0, cog=90.0)
    base.update(kw)
    return Position(**base)


def test_valid_position():
    assert make().is_valid()


def test_invalid_positions():
    assert not make(mmsi=123).is_valid()
    assert not make(lat=95.0).is_valid()
    assert not make(lat=0.0, lon=0.0).is_valid()      # « Null Island »
    assert not make(sog=102.3).is_valid()             # valeur AIS « non disponible »
    assert not make(cog=400.0).is_valid()


def test_row_roundtrip():
    p = make()
    row = dict(zip(CSV_FIELDS, map(str, p.to_row())))
    assert Position.from_row(row) == p


def test_position_is_immutable():
    with pytest.raises(dataclasses.FrozenInstanceError):
        make().lat = 1.0

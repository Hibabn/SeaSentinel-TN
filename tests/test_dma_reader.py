import zipfile

from seasentinel.dma_reader import ReadStats, parse_float, parse_timestamp, read_dma_csv
from seasentinel.ingestion import iter_batches

HEADER = ("# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,"
          "Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,"
          "Draught,Destination,ETA,Data source type,A,B,C,D")


def line(ts="16/11/2026 00:00:00", ty="Class A", mmsi="352004150", lat="57.633667", lon="8.999492",
         sog="9.7", cog="232.1"):
    return f"{ts},{ty},{mmsi},{lat},{lon},Under way using engine,0.0,{sog},{cog},233,Unknown,Unknown,,Undefined,,,,Undefined,,Unknown,,AIS,,,,"


def write(tmp_path, *lines):
    p = tmp_path / "aisdk.csv"
    p.write_text("\n".join([HEADER, *lines]) + "\n", encoding="utf-8")
    return p


def test_parse_timestamp():
    assert parse_timestamp("01/01/1970 00:01:00") == 60.0
    assert parse_timestamp("16/11/2026 00:00:00") == 1794787200.0
    assert parse_timestamp("n'importe quoi") is None


def test_parse_float_comma_and_empty():
    assert parse_float("57,8794") == 57.8794
    assert parse_float("") is None
    assert parse_float("abc") is None


def test_reads_vessel_and_skips_base_station(tmp_path):
    base = line(ty="Base Station", mmsi="2194005", sog="", cog="")
    st = ReadStats()
    out = list(read_dma_csv(write(tmp_path, line(), base), stats=st))
    assert len(out) == 1 and out[0].mmsi == 352004150
    assert out[0].lat == 57.633667 and out[0].sog == 9.7
    assert st.rejected["pas_un_navire"] == 1 and st.rows_read == 2


def test_rejects_bad_rows(tmp_path):
    st = ReadStats()
    rows = [line(mmsi="abc"), line(ts="oups"), line(sog=""), line(lat="x")]
    assert list(read_dma_csv(write(tmp_path, *rows), stats=st)) == []
    assert st.rejected_total == 4


def test_bbox_and_max_rows(tmp_path):
    p = write(tmp_path, line(lat="57.0", lon="9.0"), line(lat="30.0", lon="9.0"), line())
    st = ReadStats()
    assert len(list(read_dma_csv(p, stats=st, bbox=(50, 60, 0, 20)))) == 2
    assert st.rejected["hors_zone"] == 1
    assert len(list(read_dma_csv(p, max_rows=1))) == 1


def test_reads_zip(tmp_path):
    csv_path = write(tmp_path, line())
    z = tmp_path / "aisdk.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.write(csv_path, "aisdk.csv")
    assert len(list(read_dma_csv(z))) == 1


def test_threaded_batches_match_direct_read(tmp_path):
    rows = [line(ts=f"16/11/2026 00:{i // 60:02d}:{i % 60:02d}") for i in range(250)]
    p = write(tmp_path, *rows)
    batches = list(iter_batches(p, batch_size=100, queue_size=2))
    assert [len(b) for b in batches] == [100, 100, 50]
    assert [x for b in batches for x in b] == list(read_dma_csv(p))


def test_threaded_reader_propagates_errors(tmp_path):
    import pytest
    p = tmp_path / "bad.csv"
    p.write_text("a,b,c\n1,2,3\n")
    with pytest.raises(ValueError):
        list(iter_batches(p))

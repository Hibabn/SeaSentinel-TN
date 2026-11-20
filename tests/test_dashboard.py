"""Le tableau de bord s'exécute sans erreur sur une base réelle (sans navigateur)."""
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
pytest.importorskip("streamlit_folium")
from streamlit.testing.v1 import AppTest  # noqa: E402

from seasentinel.live.online import EngineConfig  # noqa: E402
from seasentinel.live.run import run_pipeline  # noqa: E402
from seasentinel.simulator import generate  # noqa: E402

APP = str(Path(__file__).resolve().parents[1] / "app" / "dashboard.py")


def test_dashboard_without_database(tmp_path):
    at = AppTest.from_file(APP, default_timeout=60)
    at.run()   # base absente : message d'aide, pas d'exception
    assert not at.exception


def test_dashboard_shows_pipeline_results(tmp_path, monkeypatch):
    db = tmp_path / "alerts.db"
    sc = generate(n_vessels=20, hours=8, seed=42)
    run_pipeline(sc.positions, db, port=0, speed=0, cfg=EngineConfig(enc_min_duration_s=1800),
                 linger_s=0, verbose=False)
    monkeypatch.setattr("sys.argv", ["dashboard.py", "--db", str(db), "--data-dir", str(tmp_path)])
    at = AppTest.from_file(APP, default_timeout=60)
    at.run()
    assert not at.exception
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["Navires suivis"] == str(len({p.mmsi for p in sc.positions}))
    assert int(metrics["Alertes terminées"]) > 0

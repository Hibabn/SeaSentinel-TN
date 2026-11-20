"""Bout en bout : rejeu -> moteur -> (SQLite via processus écrivain, TCP) -> client."""
import queue
import socket
import threading
import time

from seasentinel.detectors.encounter import detect_encounters
from seasentinel.detectors.gap import detect_gaps
from seasentinel.detectors.loitering import detect_loitering
from seasentinel.live.client import messages
from seasentinel.live.events import CLOSED
from seasentinel.live.online import EngineConfig
from seasentinel.live.replay import END, Replayer, load_time_sorted
from seasentinel.live.run import run_pipeline
from seasentinel.live.store import AlertStore
from seasentinel.preprocess import preprocess
from seasentinel.simulator import generate, write_csv


def test_replayer_paces_and_keeps_everything():
    pos = generate(n_vessels=10, hours=6, seed=1).positions
    q = queue.Queue(maxsize=4)
    r = Replayer(pos, q, speed=6 * 3600 / 0.6, tick_s=300)   # 6 h en ~0,6 s
    t0 = time.perf_counter()
    r.start()
    got = []
    while (b := q.get()) is not END:
        got += b
    assert got == list(pos)
    assert 0.4 < time.perf_counter() - t0 < 3


def test_load_time_sorted_sorts_vessel_sorted_file(tmp_path):
    pos = generate(n_vessels=8, hours=6, seed=2).positions
    by_vessel = sorted(pos, key=lambda p: (p.mmsi, p.timestamp))
    path = tmp_path / "v.csv"
    write_csv(by_vessel, path)
    out = load_time_sorted(path)
    assert [p.timestamp for p in out] == sorted(p.timestamp for p in pos)
    assert len(load_time_sorted(path, limit=10)) == 10


def test_full_pipeline_matches_batch_and_reaches_tcp_client(tmp_path):
    sc = generate(n_vessels=30, hours=8, seed=42)
    db = tmp_path / "alerts.db"
    received, done = [], threading.Event()

    def client(server):
        def run():
            sock = socket.create_connection(("127.0.0.1", server.port))
            for m in messages(sock):
                received.append(m)
                if m["type"] == "end":
                    break
            sock.close()
            done.set()
        threading.Thread(target=run, daemon=True).start()

    cfg = EngineConfig(enc_min_duration_s=1800)
    s = run_pipeline(sc.positions, db, port=0, speed=0, cfg=cfg, linger_s=0,
                     wait_clients=1, verbose=False, on_server=client)
    assert done.wait(10)
    assert s["writer_exitcode"] == 0
    assert s["positions"] == len(sc.positions)

    # 1) la base contient exactement les alertes des détecteurs batch
    clean, _ = preprocess(sc.positions)
    expected = (
        {("gap", (g.mmsi,), g.start, g.end) for g in detect_gaps(clean)}
        | {("loitering", (a.mmsi,), a.start, a.end) for a in detect_loitering(clean)}
        | {("encounter", (a.mmsi_a, a.mmsi_b), a.start, a.end) for a in
           detect_encounters(sorted(clean, key=lambda p: (p.timestamp, p.mmsi)),
                             min_duration_s=1800)})
    store = AlertStore(db, readonly=True)
    in_db = {(a["kind"], tuple(a["mmsis"]), a["start_ts"], a["end_ts"])
             for a in store.alerts(statuses=[CLOSED])}
    assert in_db == expected and expected
    # 2) le client TCP a reçu chaque événement, dans l'ordre
    alerts = [m for m in received if m["type"] == "alert"]
    assert len(alerts) == s["evenements"]
    assert [m["seq"] for m in alerts] == list(range(1, len(alerts) + 1))
    # 3) carte et indicateurs disponibles pour le tableau de bord
    assert len(store.vessels()) == len({p.mmsi for p in sc.positions})   # 30 + 4 en rencontre
    meta = store.meta()
    assert meta["finished"] is True and meta["positions_in"] == len(sc.positions)
    assert meta["writer_pid"] != meta["main_pid"]


def test_pipeline_stops_cleanly_from_outside(tmp_path):
    pos = generate(n_vessels=10, hours=12, seed=3).positions
    stop = threading.Event()
    started = {}

    def arm(server):                               # chrono lancé une fois tout démarré
        started["t"] = time.perf_counter()
        threading.Timer(1.0, stop.set).start()

    s = run_pipeline(pos, tmp_path / "a.db", port=0, speed=60, verbose=False, stop=stop,
                     on_server=arm)
    assert time.perf_counter() - started["t"] < 6  # 12 h à x60 = 12 min : interrompu bien avant
    assert s["writer_exitcode"] == 0
    assert 0 < s["positions"] < len(pos)
    assert AlertStore(tmp_path / "a.db", readonly=True).meta()["finished"] is False

"""Gestionnaire d'alertes : processus écrivain unique + lectures concurrentes (WAL)."""
import threading
import time

from seasentinel.live.events import CLOSED, OPEN, AlertEvent
from seasentinel.live.store import AlertStore, AlertWriter


def ev(status, end=None, mmsis=(247001002,), start=1000.0, kind="loitering"):
    return AlertEvent(kind, status, list(mmsis), start, end, 33.0, 10.5, "moyenne",
                      end or start + 3600, {"mean_sog": 0.2})


def test_writer_process_persists_and_upserts(tmp_path):
    db = tmp_path / "alerts.db"
    w = AlertWriter(db).start()
    assert w.process.pid is not None and w.process.is_alive()
    w.alerts([ev(OPEN)])
    w.alerts([ev(CLOSED, end=9000.0)])                    # même id -> mise à jour
    w.alerts([ev(OPEN, mmsis=(1, 2), kind="encounter")])
    w.vessels([(247001002, 9000.0, 33.0, 10.5, 0.2, 90.0)])
    w.tracks([(247001002, 1000.0, 33.0, 10.5), (247001002, 1300.0, 33.01, 10.5)])
    w.meta(sim_time=9000.0)
    assert w.stop() == 0

    s = AlertStore(db, readonly=True)
    alerts = {a["id"]: a for a in s.alerts()}
    a = alerts["loitering:247001002:1000"]
    assert a["status"] == CLOSED and a["end_ts"] == 9000.0 and a["duration_s"] == 8000.0
    assert a["first_seen"] == 4600.0                      # première détection conservée
    assert a["details"] == {"mean_sog": 0.2}
    assert alerts["encounter:1-2:1000"]["mmsis"] == [1, 2]
    assert s.counts() == {("loitering", CLOSED): 1, ("encounter", OPEN): 1}
    assert s.vessels()[0]["mmsi"] == 247001002
    assert len(s.tracks([247001002])[247001002]) == 2
    meta = s.meta()
    assert meta["sim_time"] == 9000.0
    assert meta["writer_pid"] == w.process.pid           # écrit par un AUTRE processus
    s.close()


def test_closed_alert_is_never_reopened(tmp_path):
    db = tmp_path / "a.db"
    with AlertWriter(db) as w:
        w.alerts([ev(CLOSED, end=9000.0)])
        w.alerts([ev(OPEN)])                              # message tardif : ignoré
    s = AlertStore(db, readonly=True)
    assert s.alerts()[0]["status"] == CLOSED


def test_fresh_start_empties_previous_run(tmp_path):
    db = tmp_path / "a.db"
    with AlertWriter(db) as w:
        w.alerts([ev(OPEN)])
    with AlertWriter(db, fresh=True):
        pass
    assert AlertStore(db, readonly=True).alerts() == []


def test_readers_never_blocked_while_writer_writes(tmp_path):
    """Pendant que le processus écrivain insère 5 000 alertes, 4 threads lecteurs
    interrogent la base sans erreur « database is locked »."""
    db = tmp_path / "a.db"
    errors, reads = [], [0]
    stop = threading.Event()

    def reader():
        s = AlertStore(db, readonly=True)
        while not stop.is_set():
            try:
                s.counts()
                reads[0] += 1
            except Exception as exc:        # noqa: BLE001 - on veut tout voir
                errors.append(exc)
        s.close()

    with AlertWriter(db) as w:
        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for i in range(50):
            w.alerts([ev(OPEN, mmsis=(i, j), kind="encounter") for j in range(100)])
            time.sleep(0.002)
    stop.set()
    for t in threads:
        t.join()
    assert not errors
    assert reads[0] > 0
    assert sum(AlertStore(db, readonly=True).counts().values()) == 5000

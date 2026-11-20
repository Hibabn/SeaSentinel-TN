"""Gestionnaire d'alertes : base SQLite alimentée par UN SEUL processus écrivain.

Pourquoi un seul écrivain ?
  SQLite verrouille tout le fichier pendant une écriture. Avec plusieurs processus
  écrivains, ils s'attendent les uns les autres (« database is locked »). Ici, tout
  le monde ENVOIE des messages dans une multiprocessing.Queue, et un processus dédié
  est le seul à écrire : il regroupe les messages en lots (une transaction par lot,
  beaucoup plus rapide qu'un commit par alerte).

Lecture concurrente : la base est en mode WAL (write-ahead log). Les lecteurs (tableau
de bord Streamlit, tests) lisent pendant que l'écrivain écrit, sans se bloquer.

Messages acceptés par la file (tuples, donc picklables) :
    ("alerts",  [dict d'AlertEvent, ...])
    ("vessels", [(mmsi, ts, lat, lon, sog, cog), ...])   dernière position de chaque navire
    ("tracks",  [(mmsi, ts, lat, lon), ...])             trace échantillonnée
    ("meta",    {clé: valeur})                            indicateurs du système
    None                                                  fin : vider la file et quitter
"""
from __future__ import annotations

import json
import multiprocessing as mp
import os
import queue
import signal
import sqlite3
import time
from pathlib import Path
from typing import Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    id          TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    status      TEXT NOT NULL,
    mmsis       TEXT NOT NULL,          -- "215001043-247001042"
    start_ts    REAL NOT NULL,
    end_ts      REAL,
    duration_s  REAL,
    lat         REAL, lon REAL,
    severity    TEXT,
    details     TEXT,                   -- JSON
    first_seen  REAL,                   -- horloge des données à la 1re détection
    updated     REAL
);
CREATE INDEX IF NOT EXISTS idx_alerts_kind ON alerts(kind, status);
CREATE TABLE IF NOT EXISTS vessels (
    mmsi INTEGER PRIMARY KEY, ts REAL, lat REAL, lon REAL, sog REAL, cog REAL
);
CREATE TABLE IF NOT EXISTS tracks (
    mmsi INTEGER, ts REAL, lat REAL, lon REAL, PRIMARY KEY (mmsi, ts)
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""

# Une alerte fermée ne redevient jamais ouverte (garde-fou si des messages se croisaient).
UPSERT_ALERT = """
INSERT INTO alerts (id, kind, status, mmsis, start_ts, end_ts, duration_s, lat, lon,
                    severity, details, first_seen, updated)
VALUES (:id, :kind, :status, :mmsis, :start, :end, :duration_s, :lat, :lon,
        :severity, :details, :detected_at, :detected_at)
ON CONFLICT(id) DO UPDATE SET
    status = excluded.status, end_ts = excluded.end_ts, duration_s = excluded.duration_s,
    lat = excluded.lat, lon = excluded.lon, severity = excluded.severity,
    details = excluded.details, updated = excluded.updated
WHERE alerts.status = 'ouverte' OR excluded.status = 'fermee'
"""


def connect(path, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        conn = sqlite3.connect(f"file:{Path(path).as_posix()}?mode=ro", uri=True,
                               timeout=5, check_same_thread=False)
    else:
        conn = sqlite3.connect(path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


class AlertStore:
    """Accès à la base. En écriture : à n'utiliser QUE dans le processus écrivain."""

    def __init__(self, path, readonly: bool = False, fresh: bool = False):
        self.path = Path(path)
        if not readonly:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = connect(self.path, readonly)
        if not readonly:
            self.conn.executescript(SCHEMA)
            if fresh:
                # vider plutôt que supprimer le fichier : sous Windows, un tableau de bord
                # ouvert garde le fichier verrouillé et la suppression échouerait.
                self.conn.executescript("DELETE FROM alerts; DELETE FROM vessels;"
                                        "DELETE FROM tracks; DELETE FROM meta;")
            self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------ écriture
    def apply(self, msg: tuple) -> int:
        """Applique un message de la file. Renvoie le nombre de lignes concernées."""
        kind, payload = msg
        c = self.conn
        if kind == "alerts":
            rows = [dict(d, mmsis="-".join(map(str, d["mmsis"])),
                         details=json.dumps(d.get("details", {}))) for d in payload]
            c.executemany(UPSERT_ALERT, rows)
        elif kind == "vessels":
            c.executemany("INSERT OR REPLACE INTO vessels VALUES (?,?,?,?,?,?)", payload)
        elif kind == "tracks":
            c.executemany("INSERT OR IGNORE INTO tracks VALUES (?,?,?,?)", payload)
        elif kind == "meta":
            c.executemany("INSERT OR REPLACE INTO meta VALUES (?,?)",
                          [(k, json.dumps(v)) for k, v in payload.items()])
        else:
            raise ValueError(f"message inconnu : {kind!r}")
        return len(payload)

    # ------------------------------------------------------------- lecture
    def alerts(self, kinds: Iterable[str] | None = None, statuses: Iterable[str] | None = None,
               limit: int | None = None) -> list[dict]:
        sql, args = "SELECT * FROM alerts WHERE 1=1", []
        if kinds is not None:
            kinds = list(kinds)
            sql += f" AND kind IN ({','.join('?' * len(kinds))})"
            args += kinds
        if statuses is not None:
            statuses = list(statuses)
            sql += f" AND status IN ({','.join('?' * len(statuses))})"
            args += statuses
        sql += " ORDER BY updated DESC, id"
        if limit:
            sql += f" LIMIT {int(limit)}"
        out = []
        for r in self.conn.execute(sql, args):
            d = dict(r)
            d["mmsis"] = [int(m) for m in d["mmsis"].split("-")]
            d["details"] = json.loads(d["details"] or "{}")
            out.append(d)
        return out

    def counts(self) -> dict[tuple[str, str], int]:
        return {(r[0], r[1]): r[2] for r in
                self.conn.execute("SELECT kind, status, COUNT(*) FROM alerts GROUP BY kind, status")}

    def vessels(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute("SELECT * FROM vessels")]

    def tracks(self, mmsis: Iterable[int]) -> dict[int, list[tuple[float, float]]]:
        mmsis = list(mmsis)
        out: dict[int, list] = {m: [] for m in mmsis}
        if not mmsis:
            return out
        sql = (f"SELECT mmsi, lat, lon FROM tracks WHERE mmsi IN ({','.join('?' * len(mmsis))})"
               " ORDER BY mmsi, ts")
        for m, la, lo in self.conn.execute(sql, mmsis):
            out[m].append((la, lo))
        return out

    def meta(self) -> dict:
        return {k: json.loads(v) for k, v in self.conn.execute("SELECT key, value FROM meta")}


# ------------------------------------------------------- processus écrivain
def writer_main(db_path: str, q, ready, fresh: bool = True, max_batch: int = 2000) -> None:
    """Boucle du processus écrivain : bloque sur la file, puis vide ce qui est déjà
    arrivé (jusqu'à max_batch messages) et écrit le tout en UNE transaction."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)   # Ctrl+C : c'est le parent qui décide de l'arrêt
    store = AlertStore(db_path, fresh=fresh)
    pid = os.getpid()
    batches = rows = 0
    store.apply(("meta", {"writer_pid": pid, "writer_started": time.time()}))
    store.conn.commit()
    ready.set()
    stop = False
    while not stop:
        batch = [q.get()]
        while len(batch) < max_batch:
            try:
                batch.append(q.get_nowait())
            except queue.Empty:
                break
        with store.conn:                            # une transaction par lot
            for msg in batch:
                if msg is None:
                    stop = True
                    continue
                rows += store.apply(msg)
            batches += 1
            store.apply(("meta", {"writer_batches": batches, "writer_rows": rows}))
    store.close()


class AlertWriter:
    """Côté parent : démarre le processus écrivain et lui envoie des messages.

        with AlertWriter("data/alerts.db") as w:
            w.alerts(events)
    """

    def __init__(self, db_path, fresh: bool = True, maxsize: int = 10_000):
        ctx = mp.get_context("spawn")           # même comportement sous Windows et Linux
        self.db_path = str(db_path)
        self.queue = ctx.Queue(maxsize=maxsize)  # bornée : contre-pression si l'écrivain traîne
        self._ready = ctx.Event()
        self.process = ctx.Process(target=writer_main, name="alert-writer",
                                   args=(self.db_path, self.queue, self._ready, fresh), daemon=True)
        self.sent = 0

    def start(self, timeout: float = 30.0) -> "AlertWriter":
        self.process.start()
        if not self._ready.wait(timeout):
            raise RuntimeError("le processus écrivain SQLite n'a pas démarré")
        return self

    def send(self, msg: tuple) -> None:
        self.queue.put(msg)
        self.sent += 1

    def alerts(self, events) -> None:
        if events:
            self.send(("alerts", [e.to_dict() if hasattr(e, "to_dict") else e for e in events]))

    def vessels(self, rows) -> None:
        if rows:
            self.send(("vessels", list(rows)))

    def tracks(self, rows) -> None:
        if rows:
            self.send(("tracks", list(rows)))

    def meta(self, **kv) -> None:
        self.send(("meta", kv))

    def stop(self, timeout: float = 30.0) -> int | None:
        """Envoie la sentinelle, attend que tout soit écrit. Renvoie le code de sortie."""
        if self.process.is_alive():
            self.queue.put(None)
            self.process.join(timeout)
            if self.process.is_alive():
                self.process.terminate()
                self.process.join()
        return self.process.exitcode

    def __enter__(self) -> "AlertWriter":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

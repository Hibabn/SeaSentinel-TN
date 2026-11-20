"""Pipeline de surveillance en temps réel (phase 6).

    python -m seasentinel.live.run --input data/sim_positions.csv --speed 600

Architecture (un processus principal + un processus écrivain) :

    [thread replay] --Queue bornée--> [thread principal : moteur en ligne]
                                          |  événements d'alerte
                    +---------------------+----------------------+
                    v                                            v
     mp.Queue -> [PROCESSUS écrivain SQLite]        [serveur TCP : 1 thread accept
                    |  (seul à écrire, WAL)           + 2 threads par client]
                    v                                            |
             data/alerts.db  <-- lecture --  Streamlit           v
                                                    clients console / asyncio

Arrêt propre : Ctrl+C -> on arrête le rejeu, on vide la file de l'écrivain (aucune
alerte perdue), on prévient les clients, puis on ferme le serveur.
"""
from __future__ import annotations

import argparse
import os
import queue
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Sequence

from ..models import Position
from .client import format_alert
from .events import KINDS
from .online import EngineConfig, OnlineEngine
from .replay import END, Replayer, load_time_sorted
from .server import PRINT_LOCK, AlertServer
from .store import AlertWriter

STATUS_EVERY_S = 1.0       # période (temps réel) des messages d'état


def run_pipeline(positions: Sequence[Position], db_path, host: str = "127.0.0.1",
                 port: int = 5050, speed: float = 600.0, tick_s: float = 60.0,
                 cfg: EngineConfig | None = None, linger_s: float = 10.0,
                 wait_clients: int = 0, track_every_s: float = 300.0, verbose: bool = True,
                 stop: threading.Event | None = None, on_server=None,
                 color: bool = True) -> dict:
    """Lance tout, rejoue `positions`, renvoie un résumé. `stop` permet d'arrêter de
    l'extérieur ; `on_server(server)` est appelé quand le serveur écoute (tests)."""
    stop = stop or threading.Event()

    def say(*args, **kw) -> None:
        if verbose:
            with PRINT_LOCK:
                print(*args, **kw, flush=True)

    engine = OnlineEngine(cfg)
    writer = AlertWriter(db_path).start()
    server = AlertServer(host, port, verbose=verbose).start()
    if on_server:
        on_server(server)
    say(f"Processus principal PID {os.getpid()}  |  écrivain SQLite PID {writer.process.pid}"
        f"  ->  {db_path}")
    say(f"Clients : python -m seasentinel.live.client --port {server.port}")

    q: queue.Queue = queue.Queue(maxsize=32)
    replayer = Replayer(positions, q, speed, tick_s, stop)
    sim_start, sim_end = replayer.sim_start, replayer.sim_end
    writer.meta(main_pid=os.getpid(), speed=speed, sim_start=sim_start, sim_end=sim_end,
                positions_total=len(positions), running=True, finished=False,
                started_at=time.time(), tcp_port=server.port)

    last_track: dict[int, float] = {}
    alert_count: Counter = Counter()
    sent_events = 0
    t_real0 = time.perf_counter()
    next_status = 0.0

    def publish(events) -> None:
        nonlocal sent_events
        if not events:
            return
        writer.alerts(events)
        for e in events:
            d = e.to_dict()
            server.broadcast_alert(d)
            alert_count[(e.kind, e.status)] += 1
            sent_events += 1
            say("  " + format_alert(d, color=color))

    def status(final: bool = False) -> dict:
        st = {"type": "status", "sim_time": engine.clock, "sim_start": sim_start,
              "sim_end": sim_end, "positions_in": engine.stats.positions_in,
              "positions_ok": engine.stats.positions_ok, "vessels": len(engine.vessels),
              "clients": server.n_clients, "events": sent_events, "queue": q.qsize(),
              "real_elapsed_s": round(time.perf_counter() - t_real0, 2),
              "replay_lag_s": round(replayer.lag_s, 2), "running": not final}
        server.broadcast(st)
        writer.meta(**{k: v for k, v in st.items() if k != "type"},
                    tcp_clients=server.clients_info(),
                    alert_counts={f"{k}|{s}": n for (k, s), n in alert_count.items()})
        return st

    completed = False
    final = None
    try:
        if wait_clients:
            say(f"En attente de {wait_clients} client(s) TCP avant de démarrer le rejeu...")
            while server.n_clients < wait_clients and not stop.is_set():
                time.sleep(0.1)
        replayer.start()
        while not stop.is_set():
            try:
                batch = q.get(timeout=0.5)
            except queue.Empty:
                continue
            if batch is END:
                break
            publish(engine.feed_many(batch))
            # dernières positions + trace échantillonnée -> base (pour la carte)
            seen = {p.mmsi: p for p in batch}
            writer.vessels([engine.vessels[m].to_row() for m in seen if m in engine.vessels])
            track = []
            for m, p in seen.items():
                if p.timestamp - last_track.get(m, -1e18) >= track_every_s:
                    last_track[m] = p.timestamp
                    track.append((m, p.timestamp, p.lat, p.lon))
            writer.tracks(track)
            if time.perf_counter() >= next_status:
                status()
                next_status = time.perf_counter() + STATUS_EVERY_S
        if replayer.error:
            raise replayer.error
        publish(engine.flush())                     # stationnements/rencontres encore en cours
        completed = not stop.is_set()
        final = status(final=True)
        server.broadcast({"type": "end"})
        writer.meta(running=False, finished=completed, finished_at=time.time())
        if completed and linger_s:
            say("Flux terminé. Serveur encore ouvert "
                f"{f'{linger_s:.0f} s' if linger_s > 0 else 'indéfiniment'} (Ctrl+C pour quitter)...")
            end_at = time.monotonic() + linger_s
            # attente par tranches : sous Windows, Event.wait() sans délai ignore Ctrl+C
            while not stop.wait(STATUS_EVERY_S) and (linger_s < 0 or time.monotonic() < end_at):
                final = status(final=True)          # nb de clients à jour dans le tableau de bord
    except KeyboardInterrupt:
        say("\nInterruption : arrêt propre...")
    finally:
        stop.set()
        replayer.join(timeout=2)
        if final is None:                           # interruption : état final quand même enregistré
            final = status(final=True)
            server.broadcast({"type": "end"})
            writer.meta(running=False, finished=False, finished_at=time.time())
        n_clients = server.n_clients
        server.stop()
        code = writer.stop()

    real = time.perf_counter() - t_real0
    summary = {
        "positions": engine.stats.positions_in, "positions_ok": engine.stats.positions_ok,
        "rejets": engine.stats.positions_in - engine.stats.positions_ok,
        "evenements": sent_events,
        "alertes": dict(alert_count),
        "duree_reelle_s": round(real, 2),
        "duree_simulee_h": round(((engine.clock or 0) - (sim_start or 0)) / 3600, 2),
        "clients_a_la_fin": n_clients, "writer_exitcode": code,
        "retard_max_rejeu_s": round(replayer.lag_s, 2), "final_status": final,
    }
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="SeaSentinel : surveillance en temps réel.")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--input", help="CSV commun (mmsi,timestamp,lat,lon,sog,cog)")
    src.add_argument("--simulate", action="store_true", help="générer un scénario à la volée")
    ap.add_argument("--vessels", type=int, default=40, help="(--simulate)")
    ap.add_argument("--hours", type=float, default=12, help="(--simulate)")
    ap.add_argument("--seed", type=int, default=42, help="(--simulate)")
    ap.add_argument("--db", default="data/alerts.db")
    ap.add_argument("--host", default="127.0.0.1", help="0.0.0.0 pour accepter le réseau local")
    ap.add_argument("--port", type=int, default=5050)
    ap.add_argument("--speed", type=float, default=600, help="accélération (0 = sans pause)")
    ap.add_argument("--tick", type=float, default=60, help="secondes de données par lot")
    ap.add_argument("--bbox", type=float, nargs=4, metavar=("LAT_MIN", "LAT_MAX", "LON_MIN", "LON_MAX"))
    ap.add_argument("--limit", type=int, help="nombre max de positions chargées")
    ap.add_argument("--kinds", nargs="*", choices=KINDS, default=list(KINDS))
    ap.add_argument("--min-gap", type=float, default=3600)
    ap.add_argument("--enc-min-duration", type=float, default=1800)
    ap.add_argument("--wait-clients", type=int, default=0,
                    help="attendre N clients TCP avant de démarrer (démo)")
    ap.add_argument("--linger", type=float, default=10,
                    help="secondes de maintien du serveur après la fin (-1 = jusqu'à Ctrl+C)")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--no-color", action="store_true")
    a = ap.parse_args()
    if os.name == "nt" and not a.no_color:
        os.system("")                    # active les couleurs ANSI dans la console Windows

    t0 = time.perf_counter()
    if a.simulate:
        from ..simulator import generate
        positions = generate(a.vessels, a.hours, seed=a.seed).positions
        origin = f"simulation ({a.vessels} navires, {a.hours} h, seed {a.seed})"
    else:
        positions = load_time_sorted(a.input, tuple(a.bbox) if a.bbox else None, a.limit)
        origin = a.input
    if not positions:
        raise SystemExit("aucune position à rejouer")
    print(f"{len(positions):,} positions chargées depuis {origin} en {time.perf_counter() - t0:.1f} s")
    span_h = (positions[-1].timestamp - positions[0].timestamp) / 3600
    if a.speed > 0:
        print(f"{span_h:.1f} h de données, accélération x{a.speed:g} : ~{span_h * 3600 / a.speed:.0f} s")

    cfg = EngineConfig(kinds=tuple(a.kinds), min_gap_s=a.min_gap,
                       bbox=tuple(a.bbox) if a.bbox else None,
                       enc_min_duration_s=a.enc_min_duration)
    Path(a.db).parent.mkdir(parents=True, exist_ok=True)
    s = run_pipeline(positions, a.db, a.host, a.port, a.speed, a.tick, cfg,
                     linger_s=a.linger, wait_clients=a.wait_clients, verbose=not a.quiet,
                     color=not a.no_color)
    print("\nRésumé")
    for k, v in s.items():
        if k != "final_status":
            print(f"  {k:<20}: {v}")


if __name__ == "__main__":
    main()

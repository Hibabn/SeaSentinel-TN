"""Serveur TCP : multi-clients, ordre, filtres, client lent, clients asyncio."""
import asyncio
import json
import socket
import threading
import time

import pytest
from seasentinel.live.client import format_alert, messages, send
from seasentinel.live.events import OPEN, AlertEvent
from seasentinel.live.server import AlertServer
from seasentinel.live.stress import run_clients, summarize


def alert(i, kind="gap"):
    return AlertEvent(kind, OPEN, [247000000 + i], 1000.0 + i, None, 33.0, 10.5, "moyenne",
                      5000.0 + i, {}).to_dict()


def wait_for(cond, timeout=5.0):
    t = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > t:
            raise AssertionError("condition jamais atteinte")
        time.sleep(0.01)


@pytest.fixture
def server():
    s = AlertServer(port=0).start()          # port libre choisi par l'OS
    yield s
    s.stop()


def open_client(server):
    sock = socket.create_connection(("127.0.0.1", server.port), timeout=5)
    return sock, messages(sock)


def test_hello_then_alerts_in_order_for_every_client(server):
    clients = [open_client(server) for _ in range(5)]
    wait_for(lambda: server.n_clients == 5)
    for _, it in clients:
        assert next(it)["type"] == "hello"
    for i in range(30):
        assert server.broadcast_alert(alert(i)) == 5
    server.broadcast({"type": "end"})
    for sock, it in clients:
        seqs = []
        for m in it:
            if m["type"] == "end":
                break
            seqs.append(m["seq"])
        assert seqs == list(range(1, 31))
        sock.close()


def test_late_client_gets_history(server):
    for i in range(3):
        server.broadcast_alert(alert(i))
    sock, it = open_client(server)
    hello = next(it)
    assert [a["mmsis"][0] for a in hello["history"]] == [247000000, 247000001, 247000002]
    sock.close()


def test_subscription_filter(server):
    sock, it = open_client(server)
    next(it)
    send(sock, {"cmd": "subscribe", "kinds": ["encounter"]})
    assert next(it) == {"type": "ok", "subscribed": ["encounter"]}
    server.broadcast_alert(alert(1, "gap"))
    server.broadcast_alert(alert(2, "encounter"))
    server.broadcast({"type": "end"})
    got = []
    for m in it:
        got.append(m)
        if m["type"] == "end":
            break
    assert [m["alert"]["kind"] for m in got if m["type"] == "alert"] == ["encounter"]
    sock.close()


def test_commands_and_errors(server):
    sock, it = open_client(server)
    next(it)
    send(sock, {"cmd": "ping"})
    assert next(it)["type"] == "pong"
    send(sock, {"cmd": "subscribe", "kinds": ["sous-marin"]})
    assert next(it)["type"] == "error"
    sock.sendall(b"pas du json\n")
    assert next(it)["type"] == "error"
    send(sock, {"cmd": "stats"})
    st = next(it)
    assert st["type"] == "stats" and len(st["clients"]) == 1
    sock.close()


def test_disconnected_client_does_not_break_server(server):
    a, _ = open_client(server)
    b, it_b = open_client(server)
    wait_for(lambda: server.n_clients == 2)
    a.close()
    wait_for(lambda: server.n_clients == 1)
    next(it_b)
    server.broadcast_alert(alert(1))
    assert next(it_b)["type"] == "alert"
    b.close()


class _StuckSocket:
    """Socket dont sendall() reste bloqué, comme un client qui ne lit plus jamais.
    (Ne pas dépendre des tampons TCP : leur taille diffère entre Windows et Linux.)"""

    def __init__(self, sock):
        self._sock = sock
        self._released = threading.Event()

    def sendall(self, data):
        self._released.wait()                 # bloqué jusqu'à la fermeture
        raise OSError("socket fermé")

    def shutdown(self, how):
        self._released.set()
        self._sock.shutdown(how)

    def __getattr__(self, name):              # makefile, close... : vrai socket
        return getattr(self._sock, name)


def test_slow_client_is_dropped_without_blocking_others():
    srv = AlertServer(port=0, client_queue=200).start()
    try:
        slow = socket.create_connection(("127.0.0.1", srv.port))   # ne lit jamais
        wait_for(lambda: srv.n_clients == 1)
        c = next(iter(srv._clients.values()))
        wait_for(lambda: c.out.unfinished_tasks == 0)              # « hello » parti
        c.sock = _StuckSocket(c.sock)                              # client désormais bloqué
        fast, it = open_client(srv)
        wait_for(lambda: srv.n_clients == 2)
        received = []

        def read_all():
            try:
                for m in it:
                    received.append(m)
            except OSError:
                pass

        threading.Thread(target=read_all, daemon=True).start()
        worst = 0.0
        for i in range(2000):
            t0 = time.perf_counter()
            srv.broadcast_alert(alert(i))
            worst = max(worst, time.perf_counter() - t0)
            if i % 50 == 49:
                time.sleep(0.01)                     # rafales réalistes : le client rapide suit
        assert worst < 0.5                           # broadcast jamais bloqué par le client lent
        wait_for(lambda: srv.stats["trop_lent"] == 1)
        wait_for(lambda: len([m for m in received if m["type"] == "alert"]) == 2000, 10)
        assert srv.n_clients == 1                    # seul le client lent a été déconnecté
        slow.close()
        fast.close()
    finally:
        srv.stop()


def test_many_asyncio_clients(server):
    """50 clients asyncio simultanés reçoivent tous les 200 alertes, dans l'ordre."""
    out = {}
    t = threading.Thread(target=lambda: out.update(
        r=asyncio.run(run_clients(50, port=server.port, timeout=10))))
    t.start()
    wait_for(lambda: server.n_clients == 50)
    for i in range(200):
        server.broadcast_alert(alert(i))
    server.broadcast({"type": "end"})
    t.join(15)
    s = summarize(out["r"])
    assert s["sans_erreur"] == 50
    assert s["alertes_min"] == s["alertes_max"] == 200
    assert s["ordre_respecte"]


def test_format_alert_is_readable():
    line = format_alert(alert(1), color=False)
    assert "Gap AIS" in line and "247000001" in line and "OUVERTE" in line
    json.dumps(alert(1))        # sérialisable
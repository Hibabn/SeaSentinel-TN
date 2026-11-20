"""Serveur TCP multi-clients : diffusion des alertes en temps réel (socket + threads).

Protocole : une ligne JSON par message (UTF-8, terminée par "\\n").

  serveur -> client
    {"type": "hello",  "clients": n, "history": [alerte, ...]}   rattrapage des dernières alertes
    {"type": "alert",  "seq": 12, "sent_at": <epoch>, "alert": {...}}
    {"type": "status", ...}       état du système (horloge simulée, positions traitées...)
    {"type": "end"}               fin du flux rejoué
    {"type": "ok" | "pong" | "stats" | "error", ...}   réponses aux commandes

  client -> serveur
    {"cmd": "subscribe", "kinds": ["gap", "encounter"]}   filtre (liste vide/absente = tout)
    {"cmd": "ping"}      {"cmd": "stats"}

Threads :
  - 1 thread « accept » : attend les connexions ;
  - par client, 1 thread LECTEUR (commandes) et 1 thread ÉCRIVAIN qui vide la file du
    client. broadcast() ne fait que déposer le message dans chaque file : il ne touche
    jamais au réseau, donc un client lent ne ralentit ni le moteur ni les autres.
  - Si la file d'un client est pleine (client trop lent ou bloqué), il est déconnecté.

Un verrou (Lock) protège la liste des clients, le numéro de séquence et l'historique :
broadcast() et l'arrivée d'un client sont donc atomiques l'un par rapport à l'autre,
et chaque client reçoit les alertes dans l'ordre de `seq`, sans trou.
"""
from __future__ import annotations

import itertools
import json
import queue
import socket
import sys
import threading
import time
from collections import Counter, deque

from .events import KINDS

PROTOCOL_VERSION = 1
PRINT_LOCK = threading.Lock()      # partagé avec run.py : affichage console thread-safe


def encode(msg: dict) -> bytes:
    return (json.dumps(msg, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


class _Client:
    _ids = itertools.count(1)

    def __init__(self, sock: socket.socket, addr, maxq: int):
        self.id = next(self._ids)
        self.sock = sock
        self.addr = addr
        self.kinds: set[str] | None = None          # None = toutes les alertes
        self.out: queue.Queue = queue.Queue(maxsize=maxq)
        self.alive = True
        self.connected_at = time.time()

    def wants(self, kind: str) -> bool:
        return self.kinds is None or kind in self.kinds

    def __repr__(self) -> str:
        return f"client#{self.id}{self.addr}"


class AlertServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 5050, history: int = 50,
                 client_queue: int = 2000, verbose: bool = False):
        self.host, self.port = host, port
        self.client_queue = client_queue
        self.verbose = verbose
        self._lock = threading.Lock()
        self._clients: dict[int, _Client] = {}
        self._history: deque = deque(maxlen=history)
        self._stop = threading.Event()
        self._sock: socket.socket | None = None
        self._threads: list[threading.Thread] = []
        self.seq = 0
        self.stats = Counter()                       # connexions, déconnexions, trop_lents...
        self.status: dict = {}                       # dernier état diffusé

    # ------------------------------------------------------------ cycle de vie
    def start(self) -> "AlertServer":
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sys.platform != "win32":                  # sous Windows, SO_REUSEADDR est dangereux
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(64)
        s.settimeout(0.5)                            # pour vérifier régulièrement _stop
        self._sock = s
        self.port = s.getsockname()[1]               # utile avec port=0 (port libre choisi par l'OS)
        self._spawn(self._accept_loop, "tcp-accept")
        self._log(f"serveur TCP en écoute sur {self.host}:{self.port}")
        return self

    def stop(self, drain_s: float = 3.0) -> None:
        """Arrêt propre : plus de nouvelles connexions, on laisse chaque thread écrivain
        envoyer ce qui reste dans sa file (au plus drain_s secondes), puis on ferme."""
        self._stop.set()
        if self._sock:
            self._sock.close()
        with self._lock:
            clients = list(self._clients.values())
        deadline = time.monotonic() + drain_s
        for c in clients:
            while c.alive and c.out.unfinished_tasks and time.monotonic() < deadline:
                time.sleep(0.01)
        for c in clients:
            self._disconnect(c, "arret")
        for t in self._threads:
            t.join(timeout=2)

    def __enter__(self) -> "AlertServer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    @property
    def n_clients(self) -> int:
        with self._lock:
            return len(self._clients)

    def clients_info(self) -> list[dict]:
        with self._lock:
            return [{"id": c.id, "addr": f"{c.addr[0]}:{c.addr[1]}",
                     "kinds": ", ".join(sorted(c.kinds)) if c.kinds else "toutes",
                     "file": c.out.qsize()} for c in self._clients.values()]

    # ------------------------------------------------------------- diffusion
    def broadcast_alert(self, alert: dict) -> int:
        """Diffuse une alerte (dict d'AlertEvent). Renvoie le nombre de destinataires."""
        with self._lock:
            self.seq += 1
            msg = {"type": "alert", "seq": self.seq, "sent_at": time.time(), "alert": alert}
            self._history.append(msg)
            data = encode(msg)
            targets = [c for c in self._clients.values() if c.wants(alert["kind"])]
            slow = [c for c in targets if not self._offer(c, data)]
        for c in slow:
            self._disconnect(c, "trop_lent")
        self.stats["alertes_diffusees"] += 1
        return len(targets) - len(slow)

    def broadcast(self, msg: dict) -> None:
        """Message à tous les clients (status, end...)."""
        if msg.get("type") == "status":
            self.status = msg
        data = encode(msg)
        with self._lock:
            slow = [c for c in self._clients.values() if not self._offer(c, data)]
        for c in slow:
            self._disconnect(c, "trop_lent")

    @staticmethod
    def _offer(c: _Client, data: bytes) -> bool:
        try:
            c.out.put_nowait(data)
            return True
        except queue.Full:
            return False

    # -------------------------------------------------------------- threads
    def _spawn(self, target, name: str, *args) -> None:
        t = threading.Thread(target=target, name=name, args=args, daemon=True)
        t.start()
        self._threads = [x for x in self._threads if x.is_alive()] + [t]

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:                          # socket fermé par stop()
                break
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)   # pas d'attente Nagle
            c = _Client(conn, addr, self.client_queue)
            with self._lock:                         # atomique vis-à-vis de broadcast_alert
                self._clients[c.id] = c
                hello = {"type": "hello", "server": "SeaSentinel", "version": PROTOCOL_VERSION,
                         "client_id": c.id, "clients": len(self._clients), "kinds": list(KINDS),
                         "history": [m["alert"] for m in self._history], "status": self.status}
                c.out.put_nowait(encode(hello))
            self.stats["connexions"] += 1
            self._log(f"+ {c} ({self.n_clients} connecté(s))")
            self._spawn(self._writer, f"tcp-send-{c.id}", c)
            self._spawn(self._reader, f"tcp-recv-{c.id}", c)

    def _writer(self, c: _Client) -> None:
        while c.alive:
            data = c.out.get()
            try:
                if data is None:                     # sentinelle posée par _disconnect
                    break
                c.sock.sendall(data)
            except OSError:
                self._disconnect(c, "erreur_envoi")
                break
            finally:
                c.out.task_done()                    # permet à stop() de savoir que tout est parti

    def _reader(self, c: _Client) -> None:
        f = c.sock.makefile("r", encoding="utf-8", newline="\n")
        try:
            for line in f:
                if line.strip():
                    self._handle(c, line)
        except (OSError, ValueError):
            pass
        finally:
            f.close()
            self._disconnect(c, "deconnexion")

    def _handle(self, c: _Client, line: str) -> None:
        try:
            cmd = json.loads(line)
            name = cmd.get("cmd")
        except (json.JSONDecodeError, AttributeError):
            self._offer(c, encode({"type": "error", "message": "JSON invalide"}))
            return
        if name == "subscribe":
            kinds = set(cmd.get("kinds") or [])
            unknown = kinds - set(KINDS)
            if unknown:
                reply = {"type": "error", "message": f"types inconnus : {sorted(unknown)}"}
            else:
                with self._lock:
                    c.kinds = kinds or None
                reply = {"type": "ok", "subscribed": sorted(kinds) or list(KINDS)}
        elif name == "ping":
            reply = {"type": "pong", "time": time.time()}
        elif name == "stats":
            reply = {"type": "stats", "clients": self.clients_info(), "seq": self.seq,
                     "counters": dict(self.stats), "status": self.status}
        else:
            reply = {"type": "error", "message": f"commande inconnue : {name!r}"}
        self._offer(c, encode(reply))

    def _disconnect(self, c: _Client, reason: str) -> None:
        with self._lock:
            if self._clients.pop(c.id, None) is None:
                return                               # déjà fait par un autre thread
            c.alive = False
        self.stats[reason] += 1
        try:
            c.sock.shutdown(socket.SHUT_RDWR)        # débloque recv()/sendall() des 2 threads
        except OSError:
            pass
        c.sock.close()
        try:
            c.out.put_nowait(None)
        except queue.Full:                           # file pleine : on fait de la place
            try:
                c.out.get_nowait()
                c.out.put_nowait(None)
            except (queue.Empty, queue.Full):
                pass
        self._log(f"- {c} ({reason})")

    def _log(self, text: str) -> None:
        if self.verbose:
            with PRINT_LOCK:                         # sinon les lignes de 2 threads se mélangent
                print(f"[serveur] {text}", flush=True)

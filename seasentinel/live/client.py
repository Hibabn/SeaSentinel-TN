"""Client console : se connecte au serveur et affiche les alertes en temps réel.

    python -m seasentinel.live.client                       # toutes les alertes
    python -m seasentinel.live.client --kinds encounter gap # filtre côté serveur

On peut en lancer plusieurs en même temps (un terminal chacun) : c'est la démo
« multi-clients » du serveur.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Iterator

from .events import CANCELLED, CLOSED, KINDS, LABELS, OPEN

COLORS = {"elevee": "\033[91m", "moyenne": "\033[93m", "faible": "\033[92m"}
STATUS_TXT = {OPEN: "OUVERTE", CLOSED: "FERMÉE ", CANCELLED: "ANNULÉE"}
RESET, DIM = "\033[0m", "\033[2m"


def connect(host: str, port: int, wait_s: float = 30.0) -> socket.socket:
    """Se connecte, en réessayant tant que le serveur n'est pas encore démarré."""
    deadline = time.monotonic() + wait_s
    while True:
        try:
            return socket.create_connection((host, port), timeout=5)
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)


def send(sock: socket.socket, msg: dict) -> None:
    sock.sendall((json.dumps(msg) + "\n").encode("utf-8"))


def messages(sock: socket.socket) -> Iterator[dict]:
    sock.settimeout(None)
    with sock.makefile("r", encoding="utf-8", newline="\n") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def utc(ts: float | None) -> str:
    if ts is None:
        return "--"
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%d/%m %H:%M")


def _paint(text: str, width: int, code: str) -> str:
    """Colore le texte puis complète avec des espaces HORS des codes ANSI
    (certaines consoles Windows perdent les espaces placés avant un code de reset)."""
    return f"{code}{text}{RESET if code else ''}" + " " * max(0, width - len(text))


def format_alert(a: dict, color: bool = True) -> str:
    mmsis = " & ".join(map(str, a["mmsis"]))
    sev = a["severity"]
    code = COLORS.get(sev, "") if color else ""
    extra = ""
    d = a.get("details", {})
    if a["kind"] == "gap" and "jump_km" in d:
        extra = f" | saut {d['jump_km']:.1f} km"
    elif a["kind"] == "gap" and d.get("silence_en_cours"):
        extra = " | silence en cours"
    elif a["kind"] == "encounter":
        extra = f" | {d.get('n_contacts', '?')} contacts"
    status = STATUS_TXT.get(a["status"], a["status"]).strip()
    return (f"[{utc(a['detected_at'])}] {_paint(status, 8, code)}"
            f"{_paint(LABELS[a['kind']], 14, code)}{_paint(sev, 8, code)}"
            f"MMSI {mmsis:<22} {a['duration_s'] / 3600:5.2f} h  "
            f"({a['lat']:.4f}, {a['lon']:.4f}){extra}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Client console SeaSentinel (alertes en direct).")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5050)
    ap.add_argument("--kinds", nargs="*", choices=KINDS, help="types d'alertes à recevoir")
    ap.add_argument("--no-color", action="store_true")
    ap.add_argument("--stay", action="store_true", help="rester connecté après la fin du flux")
    a = ap.parse_args()
    color = not a.no_color
    if color and os.name == "nt":
        os.system("")                    # active les couleurs ANSI dans la console Windows

    sock = connect(a.host, a.port)
    if a.kinds:
        send(sock, {"cmd": "subscribe", "kinds": a.kinds})
    n = 0
    try:
        for msg in messages(sock):
            t = msg["type"]
            if t == "hello":
                print(f"Connecté à {a.host}:{a.port} (client n°{msg['client_id']}, "
                      f"{msg['clients']} connecté(s))")
                hist = [h for h in msg["history"] if not a.kinds or h["kind"] in a.kinds]
                if hist:
                    print(f"{DIM if color else ''}-- {len(hist)} alerte(s) récente(s) --"
                          f"{RESET if color else ''}")
                    for h in hist:
                        print("  " + format_alert(h, color))
            elif t == "alert":
                n += 1
                lat_ms = (time.time() - msg["sent_at"]) * 1000
                print(f"{format_alert(msg['alert'], color)} | n°{msg['seq']} | {lat_ms:.1f} ms")
            elif t == "ok":
                print(f"Abonnement : {', '.join(msg['subscribed'])}")
            elif t == "error":
                print(f"Erreur serveur : {msg['message']}", file=sys.stderr)
            elif t == "end":
                print(f"Fin du flux ({n} alertes reçues).")
                if not a.stay:
                    break
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()


if __name__ == "__main__":
    main()

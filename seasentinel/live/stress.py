"""Test de charge du serveur : N clients simultanés dans UN SEUL thread (asyncio).

Avec des sockets bloquants, 200 clients demanderaient 200 threads. Avec asyncio,
une boucle d'événements unique gère toutes les connexions : chaque client est une
coroutine qui « attend » (await) sa prochaine ligne sans bloquer les autres.

    python -m seasentinel.live.stress --clients 100

Lancer d'abord le pipeline (python -m seasentinel.live.run ...). Chaque client compte
les alertes reçues, vérifie qu'elles arrivent dans l'ordre (seq croissant, sans trou)
et mesure la latence serveur -> client.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from dataclasses import dataclass, field


@dataclass
class ClientResult:
    alerts: int = 0
    in_order: bool = True
    latencies_ms: list[float] = field(default_factory=list)
    error: str | None = None


async def one_client(host: str, port: int, timeout: float, kinds=None) -> ClientResult:
    res = ClientResult()
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
    except (OSError, asyncio.TimeoutError) as exc:
        res.error = f"connexion : {exc!r}"
        return res
    if kinds:
        writer.write((json.dumps({"cmd": "subscribe", "kinds": kinds}) + "\n").encode())
        await writer.drain()
    last_seq = None
    try:
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout)
            if not line:
                break
            msg = json.loads(line)
            if msg["type"] == "alert":
                res.alerts += 1
                res.latencies_ms.append((time.time() - msg["sent_at"]) * 1000)
                if kinds is None and last_seq is not None and msg["seq"] != last_seq + 1:
                    res.in_order = False
                last_seq = msg["seq"]
            elif msg["type"] == "end":
                break
    except asyncio.TimeoutError:
        res.error = "délai dépassé"
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
    return res


async def run_clients(n: int, host: str = "127.0.0.1", port: int = 5050,
                      timeout: float = 120.0, kinds=None) -> list[ClientResult]:
    return list(await asyncio.gather(*(one_client(host, port, timeout, kinds) for _ in range(n))))


def summarize(results: list[ClientResult]) -> dict:
    ok = [r for r in results if r.error is None]
    lat = sorted(x for r in ok for x in r.latencies_ms)
    counts = [r.alerts for r in ok]
    return {
        "clients": len(results), "sans_erreur": len(ok),
        "alertes_min": min(counts, default=0), "alertes_max": max(counts, default=0),
        "tous_identiques": len(set(counts)) <= 1,
        "ordre_respecte": all(r.in_order for r in ok),
        "latence_moy_ms": round(statistics.fmean(lat), 2) if lat else None,
        "latence_p95_ms": round(lat[int(0.95 * (len(lat) - 1))], 2) if lat else None,
        "latence_max_ms": round(lat[-1], 2) if lat else None,
        "erreurs": sorted({r.error for r in results if r.error}),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="N clients asyncio simultanés sur le serveur d'alertes.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5050)
    ap.add_argument("--clients", type=int, default=50)
    ap.add_argument("--timeout", type=float, default=120.0, help="silence max par client (s)")
    a = ap.parse_args()
    print(f"{a.clients} clients asyncio -> {a.host}:{a.port} ... (attente de la fin du flux)")
    t0 = time.perf_counter()
    s = summarize(asyncio.run(run_clients(a.clients, a.host, a.port, a.timeout)))
    print(f"Terminé en {time.perf_counter() - t0:.1f} s")
    for k, v in s.items():
        print(f"  {k:<16}: {v}")


if __name__ == "__main__":
    main()

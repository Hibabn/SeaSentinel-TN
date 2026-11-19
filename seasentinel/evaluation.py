"""Comparaison alertes / vérité terrain du simulateur (précision, rappel)."""
from __future__ import annotations

from typing import Iterable


def evaluate(alerts: Iterable, truth: Iterable, anomaly_type: str,
             exact_mmsis: bool = False, explained_by: tuple[str, ...] = ()) -> dict:
    """Une alerte est juste si son intervalle chevauche celui d'une anomalie injectée
    du bon type et qu'elle concerne le même navire (exact_mmsis=True : la même PAIRE).

    `explained_by` : types d'anomalies dont une alerte « fausse » peut être la
    conséquence normale. Ex. : deux navires en rencontre sont aussi à l'arrêt, donc
    le détecteur de stationnement les signale légitimement.
    """
    alerts = list(alerts)
    truth = list(truth)
    wanted = [t for t in truth if t.type == anomaly_type]
    others = [t for t in truth if t.type in explained_by]

    def overlap(al, tr) -> bool:
        return al.start < tr.end and tr.start < al.end

    def hit(al, tr) -> bool:
        a, b = set(al.mmsis), set(tr.mmsis)
        return (a == b if exact_mmsis else bool(a & b)) and overlap(al, tr)

    ok = [al for al in alerts if any(hit(al, t) for t in wanted)]
    fp = [al for al in alerts if al not in ok]
    explained = sum(any(set(al.mmsis) & set(t.mmsis) and overlap(al, t) for t in others) for al in fp)
    found = sum(any(hit(al, t) for al in alerts) for t in wanted)
    return {
        "alertes": len(alerts), "verite": len(wanted),
        "vrais_positifs": len(ok), "faux_positifs": len(fp),
        "faux_positifs_expliques": explained,
        "faux_negatifs": len(wanted) - found,
        "precision": len(ok) / len(alerts) if alerts else 1.0,
        "rappel": found / len(wanted) if wanted else 1.0,
    }

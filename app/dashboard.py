"""Tableau de bord SeaSentinel (Streamlit + Folium).

    streamlit run app/dashboard.py                      # lit data/alerts.db
    streamlit run app/dashboard.py -- --db autre.db

Le tableau de bord ne fait que LIRE la base (connexion en lecture seule, mode WAL) :
il peut tourner pendant que le processus écrivain du pipeline écrit. Il se rafraîchit
tout seul (st.fragment). On peut l'ouvrir avant, pendant ou après le rejeu.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))                 # pour importer le paquet seasentinel

import altair as alt                          # noqa: E402
import folium                                 # noqa: E402
import pandas as pd                           # noqa: E402
import streamlit as st                        # noqa: E402
from streamlit_folium import st_folium        # noqa: E402

from seasentinel.live.events import CANCELLED, CLOSED, KINDS, LABELS, OPEN   # noqa: E402
from seasentinel.live.store import AlertStore   # noqa: E402

# Couleur = type d'alerte (identité). 3 premières teintes de la palette de référence :
# seules validées « toutes paires » (daltonisme compris), ce qu'exige une carte.
KIND_COLOR = {"gap": "#2a78d6", "encounter": "#eb6834", "loitering": "#1baf7a"}
VESSEL_COLOR = "#8a8984"
STATUS_LABEL = {OPEN: "en cours", CLOSED: "terminée", CANCELLED: "annulée"}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/alerts.db")
    ap.add_argument("--data-dir", default="data")
    a, _ = ap.parse_known_args()
    return a


def resolve(p: str) -> Path:
    path = Path(p)
    return path if path.is_absolute() or path.exists() else ROOT / path


def utc(ts) -> str:
    if ts is None or pd.isna(ts):
        return "--"
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%d/%m/%Y %H:%M UTC")


def speed_txt(speed) -> str:
    if speed is None:
        return "?"
    return "maximale (sans pause)" if speed <= 0 else f"x{speed:g}"


def hours(s) -> str:
    return "--" if s is None or pd.isna(s) else f"{s / 3600:.2f} h"


# ------------------------------------------------------------------- lecture
def read_snapshot(db: Path) -> dict | None:
    if not db.exists():
        return None
    try:
        store = AlertStore(db, readonly=True)
        snap = {"alerts": store.alerts(), "vessels": store.vessels(), "meta": store.meta()}
        mmsis = {m for a in snap["alerts"] for m in a["mmsis"]}
        snap["tracks"] = store.tracks(mmsis)
        store.close()
        return snap
    except sqlite3.Error as exc:               # base en cours de création
        st.warning(f"Base indisponible pour l'instant : {exc}")
        return None


def alerts_frame(alerts: list[dict], sim_now) -> pd.DataFrame:
    rows = []
    for a in alerts:
        dur = a["duration_s"] if a["status"] != OPEN else (sim_now or a["start_ts"]) - a["start_ts"]
        d = a["details"]
        info = ""
        if a["kind"] == "gap":
            info = (f"saut {d['jump_km']:.1f} km, {d['implied_speed_kn']:.1f} nd"
                    if "jump_km" in d else "silence en cours")
        elif a["kind"] == "loitering":
            info = f"vitesse moy. {d.get('mean_sog', 0):.2f} nd"
        elif a["kind"] == "encounter":
            info = f"{d.get('n_contacts', '?')} contacts"
        rows.append({"Type": LABELS[a["kind"]], "État": STATUS_LABEL[a["status"]],
                     "Gravité": a["severity"], "MMSI": " & ".join(map(str, a["mmsis"])),
                     "Début": utc(a["start_ts"]), "Fin": utc(a["end_ts"]), "Durée": hours(dur),
                     "Lat": round(a["lat"], 4), "Lon": round(a["lon"], 4), "Détails": info,
                     "_kind": a["kind"], "_status": a["status"]})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------- carte
def build_map(snap: dict, alerts: list[dict]) -> folium.Map:
    vessels = snap["vessels"]
    pts = [(v["lat"], v["lon"]) for v in vessels] + [(a["lat"], a["lon"]) for a in alerts]
    center = (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts)) if pts else (34, 11)
    m = folium.Map(location=center, zoom_start=8, tiles="OpenStreetMap", control_scale=True)
    if pts:
        m.fit_bounds([[min(p[0] for p in pts), min(p[1] for p in pts)],
                      [max(p[0] for p in pts), max(p[1] for p in pts)]], padding=(20, 20))

    in_alert = {m_: a["kind"] for a in alerts if a["status"] == OPEN for m_ in a["mmsis"]}
    fg_v = folium.FeatureGroup("Navires")
    for v in vessels:
        kind = in_alert.get(v["mmsi"])
        folium.CircleMarker(
            (v["lat"], v["lon"]), radius=6 if kind else 3, weight=2 if kind else 0,
            color="#ffffff", fill=True, fill_opacity=0.95,
            fill_color=KIND_COLOR[kind] if kind else VESSEL_COLOR,
            tooltip=f"MMSI {v['mmsi']} | {v['sog']:.1f} nd | {utc(v['ts'])}"
                    + (f" | {LABELS[kind]} en cours" if kind else "")).add_to(fg_v)

    fg_t = folium.FeatureGroup("Traces des navires en alerte")
    drawn = set()
    for a in alerts:
        for mm in a["mmsis"]:
            tr = snap["tracks"].get(mm, [])
            if mm not in drawn and len(tr) >= 2:
                drawn.add(mm)
                folium.PolyLine(tr, color=KIND_COLOR[a["kind"]], weight=2, opacity=0.55,
                                tooltip=f"trace MMSI {mm}").add_to(fg_t)

    fg_a = folium.FeatureGroup("Alertes")
    for a in alerts:
        c = KIND_COLOR[a["kind"]]
        mmsis = " & ".join(map(str, a["mmsis"]))
        popup = (f"<b>{LABELS[a['kind']]}</b> ({STATUS_LABEL[a['status']]})<br>MMSI {mmsis}<br>"
                 f"gravité : {a['severity']}<br>début : {utc(a['start_ts'])}<br>"
                 f"fin : {utc(a['end_ts'])}")
        if a["kind"] == "gap" and "resume_lat" in a["details"]:
            d = a["details"]
            folium.PolyLine([(a["lat"], a["lon"]), (d["resume_lat"], d["resume_lon"])], color=c,
                            weight=2, dash_array="6 6",
                            tooltip=f"silence : saut de {d['jump_km']:.1f} km").add_to(fg_a)
            popup += f"<br>saut : {d['jump_km']:.1f} km"
        if a["kind"] == "loitering":
            folium.Circle((a["lat"], a["lon"]), radius=500, color=c, weight=2,
                          fill=True, fill_opacity=0.12).add_to(fg_a)
        folium.CircleMarker((a["lat"], a["lon"]), radius=9, color="#ffffff", weight=2,
                            fill=True, fill_color=c,
                            fill_opacity=0.95 if a["status"] == OPEN else 0.55,
                            tooltip=f"{LABELS[a['kind']]} - MMSI {mmsis}",
                            popup=folium.Popup(popup, max_width=260)).add_to(fg_a)
    for fg in (fg_t, fg_v, fg_a):
        fg.add_to(m)
    folium.LayerControl(collapsed=True).add_to(m)
    return m


def legend() -> None:
    items = "".join(
        f"<span style='display:inline-flex;align-items:center;margin-right:18px'>"
        f"<span style='width:12px;height:12px;border-radius:50%;background:{KIND_COLOR[k]};"
        f"display:inline-block;margin-right:6px'></span>{LABELS[k]}</span>" for k in KINDS)
    items += (f"<span style='display:inline-flex;align-items:center'><span style='width:8px;"
              f"height:8px;border-radius:50%;background:{VESSEL_COLOR};display:inline-block;"
              f"margin-right:6px'></span>Navire sans alerte</span>")
    st.markdown(f"<div style='font-size:0.9rem;margin:4px 0 8px'>{items}</div>",
                unsafe_allow_html=True)


# ----------------------------------------------------------------- vue live
def live_view(db: Path, kinds: list[str], statuses: list[str]) -> None:
    snap = read_snapshot(db)
    if snap is None:
        st.info(f"Aucune base trouvée ({db}). Lancez le pipeline :\n\n"
                "`python -m seasentinel.live.run --input data/sim_positions.csv --speed 600`")
        return
    meta = snap["meta"]
    sim_now = meta.get("sim_time")
    alerts = [a for a in snap["alerts"] if a["kind"] in kinds and a["status"] in statuses]

    if meta.get("running"):
        state = "🟢 rejeu en cours"
    elif meta.get("finished"):
        state = "⚪ rejeu terminé"
    else:
        state = "🟠 rejeu interrompu"
    st.caption(f"{state}  ·  horloge des données : **{utc(sim_now)}**  ·  accélération "
               f"{speed_txt(meta.get('speed'))}  ·  PID principal {meta.get('main_pid', '?')}  ·  "
               f"PID écrivain SQLite {meta.get('writer_pid', '?')}  ·  port TCP "
               f"{meta.get('tcp_port', '?')}")
    s0, s1 = meta.get("sim_start"), meta.get("sim_end")
    if s0 and s1 and sim_now and s1 > s0:
        st.progress(min(1.0, (sim_now - s0) / (s1 - s0)))

    all_alerts = snap["alerts"]
    c = st.columns(6)
    c[0].metric("Positions traitées", f"{meta.get('positions_in', 0):,}".replace(",", " "))
    c[1].metric("Navires suivis", len(snap["vessels"]))
    c[2].metric("Alertes en cours", sum(a["status"] == OPEN for a in all_alerts))
    c[3].metric("Alertes terminées", sum(a["status"] == CLOSED for a in all_alerts))
    c[4].metric("Clients TCP", meta.get("clients", 0))
    c[5].metric("Lots écrits (SQLite)", meta.get("writer_batches", 0))

    left, right = st.columns([3, 2])
    with left:
        legend()
        st_folium(build_map(snap, alerts), height=520, use_container_width=True,
                  returned_objects=[], key="carte")
    with right:
        st.markdown("**Par type**")
        counts = pd.DataFrame(
            [{"Type": LABELS[k], "En cours": sum(a["kind"] == k and a["status"] == OPEN
                                                  for a in all_alerts),
              "Terminées": sum(a["kind"] == k and a["status"] == CLOSED for a in all_alerts)}
             for k in KINDS])
        st.dataframe(counts, hide_index=True, width="stretch")
        st.markdown("**Clients TCP connectés**")
        clients = meta.get("tcp_clients") or []
        if clients:
            cl = pd.DataFrame(clients).rename(columns={"id": "n°", "addr": "adresse",
                                                       "kinds": "abonnement", "file": "en attente"})
            cl["abonnement"] = cl["abonnement"].map(
                lambda k: ", ".join(k) if isinstance(k, list) else str(k))
            st.dataframe(cl, hide_index=True, width="stretch")
        else:
            st.caption("aucun (python -m seasentinel.live.client)")

    st.markdown(f"**Alertes ({len(alerts)})**")
    df = alerts_frame(alerts, sim_now)
    if df.empty:
        st.caption("Aucune alerte pour ces filtres.")
    else:
        st.dataframe(df.drop(columns=["_kind", "_status"]), hide_index=True,
                     width="stretch", height=min(420, 38 + 35 * len(df)))


# -------------------------------------------------------------- benchmarks
def speedup_chart(df: pd.DataFrame, series_col: str, title: str) -> alt.Chart:
    """Accélération selon le nombre de workers ; diagonale grise = accélération idéale."""
    order = list(dict.fromkeys(df[series_col]))
    palette = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"][:len(order)]
    maxw = int(df["workers"].max())
    ideal = pd.DataFrame({"workers": [1, maxw], "speedup": [1, maxw]})
    base = alt.Chart(df).encode(
        x=alt.X("workers:Q", title="Nombre de workers (processus)",
                scale=alt.Scale(domain=[1, maxw]), axis=alt.Axis(tickMinStep=1, grid=False)),
        y=alt.Y("speedup:Q", title="Accélération (x)", scale=alt.Scale(zero=True)),
        color=alt.Color(f"{series_col}:N", title=None, sort=order,
                        scale=alt.Scale(domain=order, range=palette),
                        legend=alt.Legend(orient="top", labelLimit=0)),
        tooltip=[alt.Tooltip(f"{series_col}:N", title="mode"), "workers:Q",
                 alt.Tooltip("speedup:Q", title="accélération"),
                 alt.Tooltip("efficiency:Q", title="efficacité"),
                 alt.Tooltip("time_s:Q", title="temps (s)")])
    lines = base.mark_line(strokeWidth=2) + base.mark_point(size=70, filled=True,
                                                             stroke="white", strokeWidth=2)
    ref = alt.Chart(ideal).mark_line(strokeDash=[4, 4], color="#b5b4ae", strokeWidth=1.5).encode(
        x="workers:Q", y="speedup:Q")
    one = alt.Chart(pd.DataFrame({"y": [1]})).mark_rule(color="#8a8984", strokeWidth=1).encode(y="y:Q")
    return (ref + one + lines).properties(title=title, height=300)


def benchmarks_view(data_dir: Path) -> None:
    st.markdown("Résultats mesurés sur la journée DMA du 16/11/2026 (12 cœurs). "
                "Ligne pointillée : accélération idéale (N workers = N fois plus vite) ; "
                "ligne pleine grise : temps séquentiel (x1).")
    found = False
    gap = data_dir / "benchmark.csv"
    if gap.exists():
        found = True
        df = pd.read_csv(gap)
        plages = df[df["mode"].isin(["sequentiel", "parallele-plages"])].copy()
        plages["série"] = plages.apply(
            lambda r: "séquentiel" if r["mode"] == "sequentiel"
            else ("1 plage par worker (statique)" if r["granularity"] == 1
                  else f"{r['granularity']} plages par worker (dynamique)"), axis=1)
        seq = plages[plages["mode"] == "sequentiel"]
        rows = [plages[plages["série"] == s] for s in plages["série"].unique() if s != "séquentiel"]
        chart_df = pd.concat([pd.concat([seq.assign(série=r["série"].iloc[0]), r]) for r in rows])
        st.altair_chart(speedup_chart(chart_df, "série", "Phase 4 - détecteur de gap (5,3 M positions)"),
                        width="stretch")
        pk = df[df["mode"].str.startswith("pickle")]
        if not pk.empty:
            best = pk["time_s"].min()
            seq_mem = df.loc[df["mode"] == "seq-memoire", "time_s"]
            if not seq_mem.empty:
                fr = lambda x, n: f"{x:.{n}f}".replace(".", ",")   # décimales à la française
                st.caption(f"Approche « envoyer les données » (pickle) : au mieux {fr(best, 1)} s "
                           f"contre {fr(seq_mem.iloc[0], 2)} s en séquentiel en mémoire. La "
                           "sérialisation coûte bien plus que le calcul : il vaut mieux amener le "
                           "calcul aux données.")
        with st.expander("Tableau phase 4"):
            st.dataframe(df, hide_index=True, width="stretch")
    for fname, title in (("benchmark_encounters.csv", "Phase 5 - rencontres, tranches de 5 min"),
                         ("bench_enc_60.csv", "Phase 5 - rencontres, tranches de 1 min (plus de calcul)")):
        p = data_dir / fname
        if not p.exists():
            continue
        found = True
        df = pd.read_csv(p)
        seq = df[df["mode"] == "sequentiel"]
        parts = [pd.concat([seq.assign(mode=m), df[df["mode"] == m]])
                 for m in df["mode"].unique() if m != "sequentiel"]
        st.altair_chart(speedup_chart(pd.concat(parts), "mode", title), width="stretch")
        with st.expander(f"Tableau {fname}"):
            st.dataframe(df, hide_index=True, width="stretch")
    if not found:
        st.info(f"Aucun fichier de benchmark dans {data_dir}.")


ARCHITECTURE = """
```
 sim_positions.csv / dma_clean.csv
          │
   [thread replay] ── Queue bornée (contre-pression) ──▶ [thread principal : moteur en ligne]
                                                             nettoyage + gap + stationnement + rencontre
                                                                     │ événements d'alerte
                       ┌─────────────────────────────────────────────┴───────────────────┐
                       ▼                                                                 ▼
      multiprocessing.Queue ──▶ [PROCESSUS écrivain SQLite]               [serveur TCP : thread accept
                                   seul écrivain, lots, WAL                 + 2 threads par client, Lock]
                                          │                                              │ JSON lignes
                                          ▼                                              ▼
                                   data/alerts.db ◀── lecture seule ── Streamlit   clients console / asyncio
```

| Technique du cours | Où | Pourquoi |
|---|---|---|
| Threads + Queue bornée | `ingestion.py`, `live/replay.py` | lecture pendant le calcul, mémoire bornée |
| Multiprocessing (Pool) | `parallel.py`, `parallel_encounters.py` | contourner le GIL pour le calcul (phases 4-5) |
| Processus dédié + mp.Queue | `live/store.py` | un seul écrivain SQLite, pas de verrouillage |
| Sockets TCP + threads + Lock | `live/server.py` | diffusion multi-clients en temps réel |
| asyncio | `live/stress.py` | 100+ clients dans un seul thread |
| Event / sentinelles | `live/run.py` | arrêt propre sans perte d'alerte |
"""


# --------------------------------------------------------------------- page
def main() -> None:
    args = parse_args()
    st.set_page_config(page_title="SeaSentinel TN", page_icon="⚓", layout="wide")
    st.title("SeaSentinel TN - surveillance maritime en temps réel")
    db, data_dir = resolve(args.db), resolve(args.data_dir)

    with st.sidebar:
        st.header("Filtres")
        kinds = st.multiselect("Types d'alerte", KINDS, default=list(KINDS),
                               format_func=LABELS.get)
        statuses = st.multiselect("États", [OPEN, CLOSED, CANCELLED], default=[OPEN, CLOSED],
                                  format_func=STATUS_LABEL.get)
        st.header("Rafraîchissement")
        auto = st.toggle("Automatique", value=True)
        every = st.slider("Période (s)", 1, 10, 2, disabled=not auto)
        st.caption(f"Base : `{db}`")

    tab_live, tab_bench, tab_arch = st.tabs(["Surveillance", "Benchmarks", "Architecture"])
    with tab_live:
        if auto:
            st.fragment(run_every=every)(live_view)(db, kinds, statuses)
        else:
            live_view(db, kinds, statuses)
    with tab_bench:
        benchmarks_view(data_dir)
    with tab_arch:
        st.markdown(ARCHITECTURE)


main()

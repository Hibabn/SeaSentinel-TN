# SeaSentinel TN

Surveillance maritime distribuée : détection de comportements anormaux (gap AIS,
rencontre, stationnement) à partir de flux AIS, avec traitement parallèle, puis
diffusion des alertes en temps réel (base SQLite, serveur TCP multi-clients,
tableau de bord web).

## Installation
```bash
python -m venv .venv
source .venv/bin/activate        # Windows : .venv\Scripts\activate
pip install -r requirements.txt
pytest                           # 118 tests
```

## Techniques de programmation concurrente utilisées

| Technique | Fichier | Rôle dans le projet |
|---|---|---|
| Thread producteur + `queue.Queue` bornée | `ingestion.py`, `live/replay.py` | lire le fichier pendant que le calcul avance ; la file bornée limite la mémoire (contre-pression) |
| `multiprocessing.Pool` (partition par navire) | `parallel.py` | détecteur de gap sur 5,3 M positions : plages d'octets lues par chaque worker |
| `multiprocessing.Pool` (partition géographique + halo) | `parallel_encounters.py` | rencontres : bandes de longitude, sans doublon ni rencontre perdue aux frontières |
| Processus dédié + `multiprocessing.Queue` | `live/store.py` | **un seul écrivain** SQLite : pas de « database is locked », écritures groupées en lots |
| Sockets TCP + 1 thread accept + 2 threads par client + `Lock` | `live/server.py` | diffusion des alertes à N clients ; un client lent n'en bloque aucun autre |
| `asyncio` | `live/stress.py` | 50 à 100+ clients simultanés dans un seul thread (test de charge) |
| `threading.Event`, sentinelles, `task_done` | `live/run.py`, `live/server.py` | arrêt propre (Ctrl+C) sans perdre d'alerte |
| Lecture concurrente (SQLite WAL) | `live/store.py`, `app/dashboard.py` | le tableau de bord lit pendant que l'écrivain écrit |

## Phase 1 : simulateur
```bash
python -m seasentinel.simulator --vessels 40 --hours 12 --seed 42
# -> data/sim_positions.csv  (flux de positions)
# -> data/sim_truth.json     (vérité terrain des anomalies injectées)
```

## Phase 2 : lecteur DMA + prétraitement
```bash
# test rapide sur 2 millions de lignes, zone Skagerrak/Kattegat
python -m seasentinel.preprocess --input data/aisdk-2026-11-16.csv --max-rows 2000000 --output data/dma_clean.csv
```

## Phase 3 : détecteur de gap AIS (séquentiel)
```bash
python -m seasentinel.detectors.gap --input data/sim_positions.csv --sort --min-gap 1800 --truth data/sim_truth.json
python -m seasentinel.detectors.gap --input data/dma_clean.csv --bbox 55 58 8 12.5 --output data/dma_gaps.csv
```

## Phase 4 : moteur parallèle + benchmark
```bash
python -m seasentinel.benchmark --input data/dma_clean.csv --bbox 55 58 8 12.5
python -m seasentinel.benchmark --input data/dma_clean.csv --bbox 55 58 8 12.5 --inmemory --max-positions 1500000
```

## Phase 5 : stationnement + rencontres (3 détecteurs du MVP)
```bash
python -m seasentinel.detectors.loitering --input data/sim_positions.csv --sort --truth data/sim_truth.json
python -m seasentinel.detectors.encounter --input data/sim_positions.csv --truth data/sim_truth.json --min-duration 1800
python -m seasentinel.benchmark_encounters --input data/dma_clean.csv
python -m seasentinel.benchmark_encounters --input data/dma_clean.csv --bucket 60 --repeat 1 --output data/bench_enc_60.csv
```

## Phase 6 : surveillance en temps réel

```
 sim_positions.csv
        │
 [thread replay] ── Queue bornée ──▶ [thread principal : moteur en ligne]
                                      nettoyage + gap + stationnement + rencontre
                                                  │ événements d'alerte
                  ┌───────────────────────────────┴──────────────────────────┐
                  ▼                                                          ▼
 mp.Queue ──▶ [PROCESSUS écrivain SQLite]                [serveur TCP : thread accept
               seul écrivain, lots, mode WAL               + 2 threads par client, Lock]
                  │                                                          │ 1 ligne JSON / message
                  ▼                                                          ▼
           data/alerts.db ◀── lecture seule ── Streamlit         clients console / asyncio
```

**Moteur en ligne** (`live/online.py`) : les positions arrivent une par une, dans
l'ordre du temps et mélangées entre navires. Chaque détecteur garde un petit état par
navire (ou par paire) et applique la même règle que la phase 3/5. Les alertes fermées
sont **identiques** à celles des détecteurs batch (vérifié par les tests, y compris sur
des données bruitées). En plus, le temps réel donne :
- une alerte **ouverte** dès que la règle est satisfaite (rencontre signalée après 30 min,
  pas à la fin) ;
- une **veille « silence en cours »** : un navire muet depuis 1 h est signalé avant même
  de réapparaître.

Chaque anomalie a un identifiant stable (`type:mmsi:début`) : la base fait un *upsert*
`ouverte -> fermee` (ou `annulee`).

### Démo (4 terminaux)
```bash
# 1. tableau de bord (laisser ouvert) -> http://localhost:8501
streamlit run app/dashboard.py

# 2. pipeline : attend 2 clients puis rejoue 12 h en ~72 s (x600)
python -m seasentinel.live.run --input data/sim_positions.csv --speed 600 --wait-clients 2 --linger -1

# 3. et 4. deux clients console (le 2e ne reçoit que les rencontres)
python -m seasentinel.live.client
python -m seasentinel.live.client --kinds encounter
```

Test de charge pendant un rejeu : `python -m seasentinel.live.stress --clients 100`
(chaque client vérifie qu'il reçoit toutes les alertes, dans l'ordre, et mesure la latence).

Autres options utiles :
```bash
python -m seasentinel.live.run --simulate --vessels 150 --hours 24 --speed 1200   # scénario généré à la volée
python -m seasentinel.live.run --input data/sim_positions.csv --speed 0            # sans pause (le plus vite possible)
python -m seasentinel.live.run --input data/dma_clean.csv --bbox 57.5 57.8 10.3 10.7 --limit 300000 --speed 3000
python -m seasentinel.live.run ... --host 0.0.0.0     # clients depuis d'autres PC du réseau local
```
`dma_clean.csv` est trié par navire : le rejeu doit le charger puis le trier par temps,
donc limiter la zone (`--bbox`) ou le volume (`--limit`).

### Protocole TCP (une ligne JSON par message)
| Sens | Message |
|---|---|
| serveur -> client | `hello` (avec les 50 dernières alertes), `alert` (`seq`, `sent_at`, `alert`), `status`, `end` |
| client -> serveur | `{"cmd": "subscribe", "kinds": ["gap"]}`, `{"cmd": "ping"}`, `{"cmd": "stats"}` |

### Choix de conception
- **Un seul écrivain SQLite.** SQLite verrouille tout le fichier pendant une écriture. Tout
  le monde envoie ses écritures au processus écrivain, qui les regroupe en lots (une
  transaction pour des centaines de lignes). Mode WAL : le tableau de bord lit en même temps.
- **Une file par client TCP.** `broadcast()` ne touche jamais le réseau : il dépose le
  message dans la file de chaque client. Un client trop lent (file pleine) est déconnecté,
  au lieu de ralentir le moteur et tous les autres.
- **Ordre garanti.** Le numéro de séquence, l'historique et la liste des clients sont
  protégés par le même verrou : chaque client reçoit les alertes dans l'ordre, sans trou.
- **Arrêt propre.** Ctrl+C : le rejeu s'arrête, l'état final est enregistré, les files des
  clients sont vidées avant fermeture, l'écrivain écrit tout ce qui reste puis quitte.
- **Windows.** Processus en mode `spawn`, attentes par tranches de 0,5 s (Ctrl+C reste actif),
  pas de `SO_REUSEADDR`, couleurs ANSI activées dans la console.

## Résultats mesurés (journée DMA du 16/11/2026, 12 cœurs)

| Expérience | Séquentiel | Meilleur parallèle | Accélération |
|---|---|---|---|
| Gap, plages d'octets (phase 4) | 11,1 s | 2,96 s (8 workers) | x3,77 |
| Gap, envoi par pickle (phase 4) | 0,13 s (en mémoire) | 6,1 s | x0,02 |
| Rencontres, tranches de 5 min (phase 5) | 2,79 s | 3,68 s (12 workers) | x0,76 |
| Rencontres, tranches de 1 min (phase 5) | 36,6 s | 13,3 s (8 workers) | x2,75 |

Résultat identique au séquentiel dans les 32 exécutions parallèles. Leçons :
1. **Amener le calcul aux données** (chaque worker lit sa plage) plutôt qu'envoyer les
   données (pickle), sinon la sérialisation coûte plus que le calcul.
2. **Le parallélisme ne paie que si le calcul domine** : avec des tranches de 5 min, le
   coût de démarrage des processus et de transfert dépasse le gain ; avec des tranches de
   1 min (bien plus de comparaisons), on gagne x2,75.
3. Au-delà de 8 workers, le gain plafonne : lecture disque, fusion des résultats dans le
   parent et déséquilibre (jusqu'à x3,18 en bandes uniformes) limitent l'accélération (loi
   d'Amdahl).

Ces courbes sont aussi affichées dans l'onglet **Benchmarks** du tableau de bord.

## Structure
```
seasentinel/
  models.py, geo.py          # Position commune, géographie
  dma_reader.py, ingestion.py, preprocess.py, simulator.py   # phases 1-2
  detectors/gap.py, loitering.py, encounter.py               # détecteurs batch (3, 5)
  parallel.py, partition.py, parallel_encounters.py          # moteurs parallèles (4, 5)
  benchmark.py, benchmark_encounters.py, evaluation.py
  live/                      # phase 6
    events.py                #   AlertEvent : format commun moteur / base / réseau
    online.py                #   moteur en ligne (états par navire / par paire)
    replay.py                #   rejeu temps réel (thread + Queue bornée)
    store.py                 #   base SQLite + processus écrivain unique
    server.py                #   serveur TCP multi-clients (threads)
    client.py                #   client console
    stress.py                #   test de charge asyncio
    run.py                   #   orchestrateur
app/dashboard.py             # Streamlit + Folium
tests/                       # 118 tests
data/                        # CSV et base générés (ignorés par git)
```

# SeaSentinel TN

Surveillance maritime distribuée : détection de comportements anormaux (gap AIS,
rencontre, stationnement) à partir de flux AIS, avec traitement parallèle.

## Installation
```bash
python -m venv .venv
source .venv/bin/activate        # Windows : .venv\Scripts\activate
pip install -r requirements.txt
```

## Phase 1 : simulateur
```bash
python -m seasentinel.simulator --vessels 40 --hours 12 --seed 42
# -> data/sim_positions.csv  (flux de positions)
# -> data/sim_truth.json     (vérité terrain des anomalies injectées)
pytest
```

## Phase 2 : lecteur DMA + prétraitement
```bash
# test rapide sur 2 millions de lignes, zone Skagerrak/Kattegat
python -m seasentinel.preprocess --input data/aisdk-2026-11-16.csv --max-rows 2000000 --output data/dma_clean.csv
pytest
```

## Structure
```
seasentinel/
  models.py      # Position : format commun à toutes les sources
  geo.py         # haversine, cap, déplacement
  dma_reader.py  # CSV/ZIP DMA -> Position (streaming, colonnes par nom)
  ingestion.py   # thread lecteur + Queue bornée -> lots
  preprocess.py  # validation, doublons, sauts impossibles, tri + CLI
  simulator.py   # trafic synthétique + anomalies avec vérité terrain
tests/
data/            # CSV générés (ignorés par git)
```

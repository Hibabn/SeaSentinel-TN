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

## Structure
```
seasentinel/
  models.py      # Position : format commun à toutes les sources
  geo.py         # haversine, cap, déplacement
  simulator.py   # trafic synthétique + anomalies avec vérité terrain
tests/
data/            # CSV générés (ignorés par git)
```

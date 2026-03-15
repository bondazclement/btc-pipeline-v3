# BTC Data Collection Pipeline v2

## Projet : TFT Pricing d'Options Binaires sur BTC Court Terme

Pipeline de collecte de donnees et construction de dataset pour entrainer un Temporal Fusion Transformer (TFT).

**Objectif** : estimer mu (drift) et sigma (volatilite realisee forward) sur BTC/USDT pour pricer des options binaires cash-or-nothing sur des horizons de 30 secondes, 1 minute, 3 minutes, et 5 minutes.

**Resolution** : 1 seconde. Sequence TFT : 300 timesteps = 5 minutes de contexte.

---

## Changements v3 -> v2

Sources supprimees (utilite < 3/10 pour horizons 30s-5min) :
- Blockchain transactions / CDD
- UTXO snapshots (HODL waves)
- BGeometrics (MVRV Z-Score, Puell Multiple)
- Blockchair dumps

Bugs corriges :
- **CRITIQUE** : Perte de donnees dans binance_live.py (ecrasement fichier horaire)
- **CRITIQUE** : Boucle Python O(n*horizon) pour sigma -> vectorise avec numpy
- **MODERE** : coin_days_destroyed_block toujours a 0 -> colonne supprimee
- **MODERE** : update_task_state trop frequent -> cache memoire + flush_state()
- **MINEUR** : Colonnes fee mempool renommees (noms refletant la vraie nature)

---

## Sources de donnees et utilite

| Source | Utilite 30s-5min | Decision v2 |
|--------|-----------------|-------------|
| aggTrades spot (buy_ratio) | 10/10 | GARDER |
| bookTicker (spread bid/ask) | 8/10 | GARDER |
| depth5 (book imbalance) | 8/10 | GARDER |
| Futures funding rate | 6/10 | GARDER |
| Futures liquidations | 6/10 | GARDER |
| Blockchain blocs (allege) | 4/10 | GARDER |
| Mempool fee rates | 4/10 | GARDER |
| Glassnode daily | 3/10 | GARDER |

---

## Structure

```
btc_pipeline/
|-- requirements.txt
|-- __init__.py
|-- config.py
|-- storage/
|   +-- gcs_client.py
|-- collectors/
|   |-- binance_historical.py
|   |-- binance_live.py
|   |-- bitcoin_core.py
|   |-- mempool.py
|   +-- glassnode.py
+-- processors/
    |-- bucket_aggregator.py
    |-- feature_engineer.py
    |-- temporal_aligner.py
    |-- label_generator.py
    |-- normalizer.py
    +-- dataset_builder.py

notebooks/
|-- 00_setup_and_checks.ipynb
|-- 01_binance_historical.ipynb
|-- 02_blockchain_blocks.ipynb
|-- 03_mempool_and_onchain.ipynb
|-- 04_websocket_live.ipynb
|-- 05_dataset_builder.ipynb
|-- 06_validation_and_eda.ipynb
+-- 07_data_inventory.ipynb
```

---

## Installation en une commande

```bash
chmod +x install.sh && ./install.sh
```

Le script install.sh est idempotent et installe :
- Dependances systeme
- Bitcoin Core (necessaire pour l'extraction des blocs)
- Packages Python (depuis btc_pipeline/requirements.txt)
- Configuration Bitcoin Core

---

## Configuration

Variables d'environnement requises :

```bash
export GCS_BUCKET_NAME=btc-training-data
export GOOGLE_APPLICATION_CREDENTIALS=/workspace/service-account.json
export GOOGLE_CLOUD_PROJECT=mon-projet-gcp
export BTC_RPC_PASSWORD=votre_mot_de_passe
export GLASSNODE_API_KEY=xxx  # optionnel
```

---

## Ordre d'execution des notebooks

```
JOUR 1 -- Setup + lancement des collectes longues
---------------------------------------------------
1. 00_setup_and_checks.ipynb          -> Verifier l'environnement + demarrer Bitcoin Core
2. 01_binance_historical.ipynb        -> Lancer (24-36h)
3. 02_blockchain_blocks.ipynb         -> Lancer en parallele (24-48h, necessite Bitcoin Core synchro)
4. 04_websocket_live.ipynb            -> Lancer IMMEDIATEMENT (continu)

JOUR 2-3 -- Apres completion Binance + blocs
---------------------------------------------------
5. 03_mempool_and_onchain.ipynb       -> Executer (~1-2h)

APRES TOUTES LES SOURCES
---------------------------------------------------
6. 05_dataset_builder.ipynb           -> Construction dataset (6-12h)
7. 06_validation_and_eda.ipynb        -> Validation
8. 07_data_inventory.ipynb            -> Inventaire final
```

---

## Estimations volumes GCS

| Source | Volume GCS | Duree collecte | Priorite |
|--------|-----------|----------------|----------|
| Binance aggTrades spot | ~90 GB | 24-36h | 1 |
| Binance klines spot | ~4 GB | 2-3h | 1 |
| Binance futures | ~6 GB | 2-3h | 1 |
| WebSocket live | ~1 GB/mois | continu | 1 |
| Blockchain blocs | ~5 GB | 24-48h (RPC) | 1 |
| Mempool snapshots | ~2 GB | continu | 2 |
| Glassnode daily | ~100 MB | 1h | 2 |
| **TOTAL** | **~107 GB** | | |
| Dataset final (features) | ~15-20 GB | 6-12h CPU | |
| **Cout GCS** | **~$2-3/mois** | | |

Noeud Bitcoin Core (disque local) : ~650 GB

---

## Note Bitcoin Core

La synchronisation initiale du noeud Bitcoin Core prend **3-7 jours** et necessite ~650 GB d'espace disque.
Lancer `bitcoind` des le jour 1, le telechargement Binance peut tourner en parallele.

```bash
bitcoind -datadir=/workspace/.bitcoin -daemon
bitcoin-cli -datadir=/workspace/.bitcoin getblockchaininfo
```

---

## Backend de stockage

Le pipeline supporte deux backends :
- `STORAGE_BACKEND=gcs` : Google Cloud Storage (production)
- `STORAGE_BACKEND=local` : Stockage local dans `/workspace/gcs_mirror/` (dev/test)

---

## Deploiement sur RunPod

```
Type      : CPU Pod (pas de GPU pour la collecte)
CPU       : 8-16 vCPUs
RAM       : 64-128 GB
Disque    : 1 TB Network Volume (CRITIQUE pour la persistance)
Image     : runpod/jupyter:latest
```

---

*Pipeline v2 -- Projet de recherche quantitative prive*

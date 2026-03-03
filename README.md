# BTC Data Collection Pipeline v3

## Projet : TFT Pricing d'Options Binaires sur BTC Court Terme

Pipeline complet de collecte de données et construction de dataset pour entraîner un Temporal Fusion Transformer.

---

## Structure

```
/workspace/
├── btc_pipeline/                    # Modules Python partagés
│   ├── config.py                    # Configuration, schémas, constantes
│   ├── storage/
│   │   └── gcs_client.py            # Interface GCS unifiée (streaming)
│   ├── collectors/
│   │   ├── binance_historical.py    # Données Binance data.binance.vision
│   │   ├── binance_live.py          # WebSocket live (4 streams)
│   │   ├── bitcoin_core.py          # RPC blocs, transactions, UTXO
│   │   ├── blockchair.py            # Dumps TSV Blockchair (alt. RPC)
│   │   ├── mempool.py               # API + WS mempool.space
│   │   └── glassnode.py             # Glassnode + BGeometrics
│   └── processors/
│       ├── bucket_aggregator.py     # aggTrades → buckets 1s
│       ├── feature_engineer.py      # Toutes les features
│       ├── temporal_aligner.py      # Fusion multi-sources
│       ├── label_generator.py       # Labels μ, σ, direction
│       ├── normalizer.py            # Rolling z-score
│       └── dataset_builder.py       # Orchestrateur mensuel
│
├── notebooks/                       # Notebooks Jupyter (10 notebooks)
│   ├── 00_setup_and_checks.ipynb    # Vérification environnement
│   ├── 01_binance_historical.ipynb  # Download Binance (24-36h)
│   ├── 02_blockchain_blocks.ipynb   # Extraction blocs (24-48h)
│   ├── 03_blockchain_transactions.ipynb  # Transactions (5-10 jours)
│   ├── 04_blockchain_utxo.ipynb     # Snapshots UTXO
│   ├── 05_mempool_and_onchain.ipynb # Mempool + Glassnode
│   ├── 06_websocket_live.ipynb      # Streams live (continu)
│   ├── 07_dataset_builder.ipynb     # Construction dataset
│   ├── 08_validation_and_eda.ipynb  # Validation & exploration
│   └── 09_data_inventory.ipynb      # Inventaire GCS
│
├── install.sh                       # Script d'installation
└── logs/                            # Logs d'exécution
```

---

## Déploiement sur RunPod

### 1. Configuration du Pod

```
Type      : CPU Pod (pas de GPU pour la collecte)
CPU       : 8-16 vCPUs
RAM       : 64-128 GB
Disque    : 1 TB Network Volume (CRITIQUE pour la persistance)
Image     : runpod/jupyter:latest
```

### 2. Installation

```bash
# Uploader le zip sur le pod via JupyterLab, puis :
cd /workspace
unzip btc_pipeline_v3.zip
chmod +x install.sh
./install.sh
```

### 3. Configuration GCS

1. Créer un bucket GCS (ex: `btc-training-data`)
2. Créer un service account avec le rôle `Storage Admin`
3. Télécharger le JSON et l'uploader dans `/workspace/service-account.json`
4. Configurer les variables d'environnement :

```bash
export GCS_BUCKET_NAME=btc-training-data
export GOOGLE_APPLICATION_CREDENTIALS=/workspace/service-account.json
export GOOGLE_CLOUD_PROJECT=mon-projet-gcp
export GLASSNODE_API_KEY=xxx  # optionnel
```

### 4. Exécution

Ouvrir JupyterLab et suivre l'ordre :

```
JOUR 1 — Setup + lancement des collectes longues
─────────────────────────────────────────────────
1. 00_setup_and_checks.ipynb          → Vérifier l'environnement
2. 01_binance_historical.ipynb        → Lancer (24-36h)
3. 02_blockchain_blocks.ipynb         → Lancer en parallèle (24-48h)
4. 06_websocket_live.ipynb            → Lancer IMMÉDIATEMENT (continu)

JOUR 2-3 — Après complétion Binance + blocs
─────────────────────────────────────────────────
5. 03_blockchain_transactions.ipynb   → Lancer (5-10 jours)
6. 05_mempool_and_onchain.ipynb       → Exécuter (~1-2h)

JOURS 3-5
─────────────────────────────────────────────────
7. 04_blockchain_utxo.ipynb           → Exécuter

APRÈS TOUTES LES SOURCES
─────────────────────────────────────────────────
8. 07_dataset_builder.ipynb           → Construction dataset (6-12h)
9. 08_validation_and_eda.ipynb        → Validation
10. 09_data_inventory.ipynb           → Inventaire final
```

### Exécution en arrière-plan (notebooks longs)

```bash
pip install papermill
nohup papermill notebooks/01_binance_historical.ipynb \
      logs/01_output.ipynb > logs/01_stderr.log 2>&1 &

# Monitoring :
tail -f logs/01_stderr.log
```

---

## Estimations

| Source | Volume GCS | Durée | Priorité |
|--------|-----------|-------|----------|
| Binance aggTrades spot | ~90 GB | 24-36h | 1 |
| Binance klines | ~4 GB | 2-3h | 1 |
| Binance futures | ~6 GB | 2-3h | 1 |
| WebSocket live | ~1 GB/mois | continu | 1 |
| Blockchain blocs | ~5 GB | 24-48h | 1 |
| Blockchain transactions | ~130 GB | 5-10 jours | 2 |
| UTXO snapshots | ~5-10 GB | 3-5 jours | 2 |
| Mempool + Glassnode | ~2 GB | ~1h | 2 |
| **Dataset final (features)** | **~20-30 GB** | 6-12h | — |

**Coût GCS** : ~$5/mois pour 250 GB (Standard storage)

---

## Backend de stockage

Le pipeline supporte deux backends :
- `STORAGE_BACKEND=gcs` : Google Cloud Storage (production)
- `STORAGE_BACKEND=local` : Stockage local dans `/workspace/gcs_mirror/` (dev/test)

---

*Pipeline v3 — Projet de recherche quantitative privé*

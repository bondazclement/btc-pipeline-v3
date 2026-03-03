#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════
# BTC Data Collection Pipeline v3 — Script d'installation RunPod
# ═══════════════════════════════════════════════════════════════════════════
# Exécuter une seule fois sur le pod RunPod :
#   chmod +x install.sh && ./install.sh
# ═══════════════════════════════════════════════════════════════════════════

set -e

echo "═══════════════════════════════════════════════════════════"
echo " BTC Pipeline v3 — Installation sur RunPod"
echo "═══════════════════════════════════════════════════════════"

WORKSPACE="${WORKSPACE_DIR:-/workspace}"
cd "$WORKSPACE"

# 1. Dépendances système
echo "▶ Installation des dépendances système..."
apt-get update -qq
apt-get install -y -qq wget curl unzip screen 2>/dev/null || true

# 2. Packages Python
echo "▶ Installation des packages Python..."
pip install -q \
    polars pandas pyarrow google-cloud-storage gcsfs \
    aiohttp websockets python-bitcoinrpc loguru tqdm \
    ipywidgets python-dateutil requests nest_asyncio \
    papermill matplotlib

# 3. bitcoin-utxo-dump (optionnel)
if ! command -v bitcoin-utxo-dump &>/dev/null; then
    echo "▶ Installation de bitcoin-utxo-dump..."
    wget -q https://github.com/in3rsha/bitcoin-utxo-dump/releases/download/v0.1.2/bitcoin-utxo-dump-linux-amd64 \
        -O /usr/local/bin/bitcoin-utxo-dump 2>/dev/null || echo "⚠️  bitcoin-utxo-dump download failed (optionnel)"
    chmod +x /usr/local/bin/bitcoin-utxo-dump 2>/dev/null || true
fi

# 4. Copier le pipeline au bon endroit
echo "▶ Installation du pipeline..."
if [ -d "$WORKSPACE/btc_pipeline_package" ]; then
    cp -r "$WORKSPACE/btc_pipeline_package/btc_pipeline" "$WORKSPACE/btc_pipeline"
    cp -r "$WORKSPACE/btc_pipeline_package/notebooks" "$WORKSPACE/notebooks"
fi

# 5. Créer les dossiers
mkdir -p "$WORKSPACE/logs"
mkdir -p "$WORKSPACE/tmp"
mkdir -p "$WORKSPACE/.bitcoin"

# 6. Bitcoin Core config (si pas déjà présent)
if [ ! -f "$WORKSPACE/.bitcoin/bitcoin.conf" ]; then
    echo "▶ Création de bitcoin.conf..."
    cat > "$WORKSPACE/.bitcoin/bitcoin.conf" << 'EOF'
# Bitcoin Core configuration pour le pipeline de données
server=1
txindex=1
rpcuser=btcuser
rpcpassword=secret
rpcallowip=127.0.0.1
rpcbind=127.0.0.1:8332
dbcache=4096
maxconnections=40
blocksonly=0
prune=0
EOF
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " ✅ Installation terminée !"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo " Prochaines étapes :"
echo "   1. Uploader service-account.json dans /workspace/"
echo "   2. Ouvrir notebooks/00_setup_and_checks.ipynb"
echo "   3. Suivre l'ordre d'exécution dans le README"
echo ""
echo " Variables d'environnement à configurer :"
echo "   export GCS_BUCKET_NAME=btc-training-data"
echo "   export GOOGLE_APPLICATION_CREDENTIALS=/workspace/service-account.json"
echo "   export GOOGLE_CLOUD_PROJECT=mon-projet-gcp"
echo "   export GLASSNODE_API_KEY=xxx  # optionnel"
echo ""

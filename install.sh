#!/bin/bash
# BTC Pipeline v2 — Installation complète sur RunPod
# Usage : chmod +x install.sh && ./install.sh
# Idempotent : safe à relancer

set -e

echo "═══════════════════════════════════════════════════"
echo " BTC Pipeline v2 — Installation RunPod"
echo "═══════════════════════════════════════════════════"

WORKSPACE="${WORKSPACE_DIR:-/workspace}"
cd "$WORKSPACE"

# ── 1. Dépendances système ──────────────────────────────
echo "▶ Dépendances système..."
apt-get update -qq 2>/dev/null
apt-get install -y -qq wget curl unzip screen software-properties-common gpg 2>/dev/null

# ── 2. Bitcoin Core ─────────────────────────────────────
# Nécessaire pour l'extraction des blocs blockchain
if ! command -v bitcoind &>/dev/null; then
    echo "▶ Installation de Bitcoin Core..."
    # Télécharger la clé de signature officielle Bitcoin Core
    wget -qO - https://bitcoin.org/laanwjh.pub | gpg --import 2>/dev/null || true
    # Ajouter le PPA Ubuntu
    add-apt-repository -y ppa:bitcoin/bitcoin 2>/dev/null || true
    apt-get update -qq 2>/dev/null
    apt-get install -y -qq bitcoind 2>/dev/null || {
        # Fallback : téléchargement direct du binaire
        BTC_VERSION="27.1"
        wget -q "https://bitcoin.org/bin/bitcoin-core-${BTC_VERSION}/bitcoin-${BTC_VERSION}-x86_64-linux-gnu.tar.gz" \
            -O /tmp/bitcoind.tar.gz
        tar -xzf /tmp/bitcoind.tar.gz -C /tmp/
        cp "/tmp/bitcoin-${BTC_VERSION}/bin/bitcoind" /usr/local/bin/
        cp "/tmp/bitcoin-${BTC_VERSION}/bin/bitcoin-cli" /usr/local/bin/
        rm -rf /tmp/bitcoind.tar.gz "/tmp/bitcoin-${BTC_VERSION}"
    }
    echo "  ✅ Bitcoin Core installé : $(bitcoind --version | head -1)"
else
    echo "  ✅ Bitcoin Core déjà installé : $(bitcoind --version | head -1)"
fi

# ── 3. Configuration Bitcoin Core ────────────────────────
mkdir -p "$WORKSPACE/.bitcoin"
if [ ! -f "$WORKSPACE/.bitcoin/bitcoin.conf" ]; then
    echo "▶ Configuration Bitcoin Core..."
    cat > "$WORKSPACE/.bitcoin/bitcoin.conf" << 'EOF'
# Bitcoin Core — Configuration pipeline de données
server=1
txindex=1
rpcuser=btcuser
rpcpassword=btcpassword_change_me
rpcallowip=127.0.0.1
rpcbind=127.0.0.1:8332
dbcache=4096
maxconnections=20
blocksonly=0
prune=0
# Datadir explicite pour le Network Volume RunPod
datadir=/workspace/.bitcoin
EOF
    echo "  ✅ bitcoin.conf créé"
    echo "  ⚠️  IMPORTANT : Modifier rpcpassword dans /workspace/.bitcoin/bitcoin.conf"
else
    echo "  ✅ bitcoin.conf déjà présent"
fi

# ── 4. Packages Python ──────────────────────────────────
echo "▶ Installation des packages Python..."
pip install -q -r "$WORKSPACE/btc_pipeline/requirements.txt"
echo "  ✅ Packages Python installés"

# ── 5. Structure des dossiers ────────────────────────────
echo "▶ Création de la structure..."
mkdir -p "$WORKSPACE/logs"
mkdir -p "$WORKSPACE/tmp"
echo "  ✅ Dossiers créés"

# ── 6. Vérifications finales ─────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo " ✅ Installation terminée !"
echo "═══════════════════════════════════════════════════"
echo ""
echo " PROCHAINES ÉTAPES :"
echo ""
echo " 1. Modifier le mot de passe RPC :"
echo "    nano /workspace/.bitcoin/bitcoin.conf"
echo "    → Changer rpcpassword=btcpassword_change_me"
echo ""
echo " 2. Uploader le fichier GCS :"
echo "    → Déposer service-account.json dans /workspace/"
echo ""
echo " 3. Configurer les variables d'environnement :"
echo "    export GCS_BUCKET_NAME=btc-training-data"
echo "    export GOOGLE_APPLICATION_CREDENTIALS=/workspace/service-account.json"
echo "    export GOOGLE_CLOUD_PROJECT=mon-projet-gcp"
echo "    export BTC_RPC_PASSWORD=btcpassword_change_me"
echo "    export GLASSNODE_API_KEY=xxx  # optionnel"
echo ""
echo " 4. Ouvrir notebooks/00_setup_and_checks.ipynb"
echo "    → Vérifier toutes les connexions avant de lancer"
echo ""
echo " 5. Démarrer Bitcoin Core (synchronisation initiale : 3-7 jours) :"
echo "    bitcoind -datadir=/workspace/.bitcoin -daemon"
echo "    bitcoin-cli -datadir=/workspace/.bitcoin getblockchaininfo"
echo ""
echo " 6. Lancer le téléchargement Binance EN PARALLÈLE :"
echo "    nohup papermill notebooks/01_binance_historical.ipynb logs/01.ipynb &"
echo "    nohup papermill notebooks/04_websocket_live.ipynb logs/04.ipynb &"
echo ""

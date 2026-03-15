"""
Extraction de la blockchain Bitcoin via RPC local.

v2: allégé — seuls les blocs sont extraits.
Les transactions et UTXO snapshots ont été supprimés (utilité < 3/10 pour horizons 30s-5min).

Prérequis :
  - Bitcoin Core synchronisé avec txindex=1
  - ~650 GB disque pour le nœud
  - Env : BTC_RPC_USER, BTC_RPC_PASSWORD

Architecture streaming :
  Les données sont traitées en batches et immédiatement uploadées sur GCS.
  Le disque local n'est utilisé que pour le nœud Bitcoin Core lui-même.
"""

import time
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from btc_pipeline.config import (
    Config, GCS_PATHS, BLOCK_SCHEMA_DTYPES,
    get_halving_epoch, get_blocks_since_halving,
)
from btc_pipeline.storage.gcs_client import StorageClient


# ═══════════════════════════════════════════════════════════════════════════
# RPC CONNECTION
# ═══════════════════════════════════════════════════════════════════════════

def get_rpc(config: Optional[Config] = None):
    """Crée une connexion RPC vers Bitcoin Core."""
    from bitcoinrpc.authproxy import AuthServiceProxy
    config = config or Config()
    return AuthServiceProxy(config.btc_rpc_url, timeout=120)


def get_chain_info(rpc) -> dict:
    """Retourne les infos de la blockchain (hauteur, vérification, etc.)."""
    return rpc.getblockchaininfo()


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def satoshis_to_btc(sats: int) -> float:
    return sats / 1e8


def classify_amount_btc(btc: float) -> str:
    """Classifie un montant BTC dans une tranche."""
    if btc < 0.0001:
        return "below_0001"
    elif btc < 0.01:
        return "0001_001"
    elif btc < 0.1:
        return "001_01"
    elif btc < 1.0:
        return "01_1"
    elif btc < 10.0:
        return "1_10"
    elif btc < 100.0:
        return "10_100"
    elif btc < 1000.0:
        return "above_100"
    else:
        return "above_1000"


def detect_output_type(scriptPubKey: dict) -> str:
    """Détecte le type de sortie (P2PKH, P2SH, P2WPKH, P2TR, OP_RETURN)."""
    tp = scriptPubKey.get("type", "")
    if tp == "pubkeyhash":
        return "p2pkh"
    elif tp == "scripthash":
        return "p2sh"
    elif tp == "witness_v0_keyhash":
        return "p2wpkh"
    elif tp == "witness_v0_scripthash":
        return "p2wsh"
    elif tp == "witness_v1_taproot":
        return "p2tr"
    elif tp == "nulldata":
        return "op_return"
    else:
        return "other"


# ═══════════════════════════════════════════════════════════════════════════
# BLOCK EXTRACTION (Level 1)
# ═══════════════════════════════════════════════════════════════════════════

def extract_block_full(rpc, height: int, prev_timestamp: Optional[int] = None) -> dict:
    """
    Extrait toutes les métriques d'un bloc (BLOCK_SCHEMA complet).
    Calcule distributions de montants, fee percentiles, counts SegWit/Taproot.
    Ne conserve pas les transactions brutes après calcul.

    Performance : ~100-200ms par bloc via RPC local.
    """
    block_hash = rpc.getblockhash(height)
    block = rpc.getblock(block_hash, 2)  # verbosity=2 = toutes les tx

    # ── Metadata ──────────────────────────────────────────────────────────
    record = {
        "block_height": height,
        "block_hash": block["hash"],
        "block_timestamp": block["time"],
        "block_version": block.get("version", 0),
        # v2: removed — prev_block_hash, merkle_root (pas utiles pour le ML)
        "block_size_bytes": block.get("size", 0),
        "block_weight": block.get("weight", 0),
        "block_stripped_size": block.get("strippedsize", 0),
        "difficulty": block.get("difficulty", 0),
        "bits": int(block.get("bits", "0"), 16) if isinstance(block.get("bits"), str) else block.get("bits", 0),
        "nonce": block.get("nonce", 0),
        "tx_count": len(block.get("tx", [])),
    }

    # ── Timing ────────────────────────────────────────────────────────────
    if prev_timestamp is not None:
        record["seconds_since_prev_block"] = block["time"] - prev_timestamp
    else:
        record["seconds_since_prev_block"] = 0

    record["halving_epoch"] = get_halving_epoch(height)
    record["blocks_since_halving"] = get_blocks_since_halving(height)

    # ── Parse all transactions ────────────────────────────────────────────
    fee_rates = []
    tx_amounts = []  # total output in BTC per tx
    tx_input_counts = []
    tx_output_counts = []
    tx_sizes = []
    segwit_count = 0
    native_segwit_count = 0
    taproot_count = 0
    total_input_sats = 0
    total_output_sats = 0
    coinbase_reward_sats = 0
    utxo_created = 0
    utxo_spent = 0
    utxo_created_value = 0
    utxo_spent_value = 0
    # v2: removed — block_cdd (coin_days_destroyed requires individual transactions)
    coinbase_outputs_count = 0

    # Amount distribution counters
    amount_tx_counts = {
        "below_0001": 0, "0001_001": 0, "001_01": 0, "01_1": 0,
        "1_10": 0, "10_100": 0, "above_100": 0, "above_1000": 0,
    }
    amount_volumes = {k: 0.0 for k in amount_tx_counts}

    for tx in block.get("tx", []):
        is_coinbase = "coinbase" in tx.get("vin", [{}])[0] if tx.get("vin") else False

        # Count outputs (UTXO created)
        tx_output_sats = 0
        has_segwit = False
        has_native_segwit = False
        has_taproot = False

        for vout in tx.get("vout", []):
            val_sats = int(round(vout.get("value", 0) * 1e8))
            tx_output_sats += val_sats
            utxo_created += 1
            utxo_created_value += val_sats

            sp = vout.get("scriptPubKey", {})
            otype = detect_output_type(sp)
            if otype in ("p2wpkh", "p2wsh"):
                has_native_segwit = True
                has_segwit = True
            elif otype == "p2tr":
                has_taproot = True
                has_segwit = True
            elif otype == "p2sh":
                # Could be wrapped SegWit, heuristic
                pass

        total_output_sats += tx_output_sats
        tx_output_btc = satoshis_to_btc(tx_output_sats)

        if is_coinbase:
            coinbase_reward_sats = tx_output_sats
            coinbase_outputs_count = len(tx.get("vout", []))
            tx_input_counts.append(1)
            tx_output_counts.append(len(tx.get("vout", [])))
            tx_sizes.append(tx.get("vsize", tx.get("size", 0)))
            continue

        # Count inputs (UTXO spent)
        tx_input_sats = 0
        n_inputs = len(tx.get("vin", []))
        for vin in tx.get("vin", []):
            utxo_spent += 1
            # We don't have the input value from verbosity=2 directly for all versions
            # but we can get it from the prevout field if available (Bitcoin Core >= 22.0)
            prevout = vin.get("prevout", {})
            if prevout:
                val_sats = int(round(prevout.get("value", 0) * 1e8))
                tx_input_sats += val_sats
                utxo_spent_value += val_sats

            # Check for SegWit witness
            if vin.get("txinwitness"):
                has_segwit = True
                witness = vin["txinwitness"]
                # Taproot: single witness element of 64/65 bytes (Schnorr sig)
                if len(witness) >= 1 and len(witness[0]) in (128, 130):
                    has_taproot = True

        total_input_sats += tx_input_sats

        # Fee calculation
        if tx_input_sats > 0:
            fee_sats = tx_input_sats - tx_output_sats
        else:
            fee_sats = 0  # Can't compute without prevout

        vsize = tx.get("vsize", tx.get("size", 1))
        if vsize > 0 and fee_sats > 0:
            fee_rate = fee_sats / vsize
            fee_rates.append(fee_rate)

        # Amount classification
        cat = classify_amount_btc(tx_output_btc)
        amount_tx_counts[cat] += 1
        amount_volumes[cat] += tx_output_btc
        tx_amounts.append(tx_output_btc)

        tx_input_counts.append(n_inputs)
        tx_output_counts.append(len(tx.get("vout", [])))
        tx_sizes.append(vsize)

        if has_segwit:
            segwit_count += 1
        if has_native_segwit:
            native_segwit_count += 1
        if has_taproot:
            taproot_count += 1

    # ── Aggregate metrics ─────────────────────────────────────────────────
    non_coinbase = max(1, record["tx_count"] - 1)

    record["total_input_btc"] = satoshis_to_btc(total_input_sats)
    record["total_output_btc"] = satoshis_to_btc(total_output_sats)
    record["total_fees_btc"] = satoshis_to_btc(max(0, total_input_sats - total_output_sats + coinbase_reward_sats))
    record["coinbase_reward_btc"] = satoshis_to_btc(coinbase_reward_sats)

    # Fee rate percentiles
    if fee_rates:
        fee_rates_sorted = sorted(fee_rates)
        record["fee_rate_min_sat_vbyte"] = fee_rates_sorted[0]
        record["fee_rate_max_sat_vbyte"] = fee_rates_sorted[-1]
        for p, key in [(10, "p10"), (25, "p25"), (50, "p50"), (75, "p75"), (90, "p90"), (99, "p99")]:
            idx = int(len(fee_rates_sorted) * p / 100)
            idx = min(idx, len(fee_rates_sorted) - 1)
            record[f"fee_rate_{key}_sat_vbyte"] = fee_rates_sorted[idx]
    else:
        for k in ["min", "p10", "p25", "p50", "p75", "p90", "p99", "max"]:
            record[f"fee_rate_{k}_sat_vbyte"] = 0.0

    # Amount distribution
    for cat in amount_tx_counts:
        record[f"tx_count_{cat}btc"] = amount_tx_counts[cat]
        record[f"volume_{cat}btc"] = amount_volumes[cat]

    # Transaction structure stats
    if tx_input_counts:
        record["tx_input_count_mean"] = np.mean(tx_input_counts)
        record["tx_input_count_median"] = np.median(tx_input_counts)
        record["tx_output_count_mean"] = np.mean(tx_output_counts)
        record["tx_output_count_median"] = np.median(tx_output_counts)
    else:
        record["tx_input_count_mean"] = 0
        record["tx_input_count_median"] = 0
        record["tx_output_count_mean"] = 0
        record["tx_output_count_median"] = 0

    if tx_sizes:
        record["tx_size_mean_bytes"] = np.mean(tx_sizes)
        record["tx_size_median_bytes"] = np.median(tx_sizes)
        record["tx_size_p90_bytes"] = np.percentile(tx_sizes, 90) if len(tx_sizes) > 1 else tx_sizes[0]
    else:
        record["tx_size_mean_bytes"] = 0
        record["tx_size_median_bytes"] = 0
        record["tx_size_p90_bytes"] = 0

    # SegWit / Taproot
    record["segwit_tx_count"] = segwit_count
    record["segwit_tx_ratio"] = segwit_count / non_coinbase if non_coinbase > 0 else 0
    record["native_segwit_tx_count"] = native_segwit_count
    record["taproot_tx_count"] = taproot_count
    record["taproot_tx_ratio"] = taproot_count / non_coinbase if non_coinbase > 0 else 0

    # UTXO
    record["utxo_created_count"] = utxo_created
    record["utxo_spent_count"] = utxo_spent
    record["utxo_net_change"] = utxo_created - utxo_spent
    record["utxo_created_value_btc"] = satoshis_to_btc(utxo_created_value)
    record["utxo_spent_value_btc"] = satoshis_to_btc(utxo_spent_value)

    record["coinbase_outputs_count"] = coinbase_outputs_count
    # v2: removed — coin_days_destroyed_block (nécessite transactions individuelles, supprimées en v2)

    return record


def run_blocks_extraction(
    storage: StorageClient,
    batch_size: int = 5000,
    config: Optional[Config] = None,
    progress_callback=None,
) -> dict:
    """
    Extrait tous les blocs depuis genesis.
    Reprend depuis last_completed_block_height dans pipeline_state.json.
    Upload chaque batch Parquet sur GCS, libère la RAM.
    """
    config = config or Config()
    rpc = get_rpc(config)

    chain_info = rpc.getblockchaininfo()
    current_height = chain_info["blocks"]

    state = storage.get_pipeline_state()
    task_state = state.get("tasks", {}).get("blockchain_blocks", {})
    start_height = task_state.get("last_completed_height", -1) + 1

    total_blocks = current_height - start_height + 1
    logger.info(f"▶ Blocks extraction: {start_height} → {current_height} ({total_blocks:,} blocks)")

    # Estimate duration
    est_hours = total_blocks * 0.15 / 3600  # ~150ms per block
    logger.info(f"ESTIMATION: ~{est_hours:.1f}h for {total_blocks:,} blocks")

    stats = {"batches": 0, "total_rows": 0, "total_bytes": 0}
    batch = []
    prev_timestamp = None

    # Get previous block's timestamp for the first block
    if start_height > 0:
        prev_hash = rpc.getblockhash(start_height - 1)
        prev_block = rpc.getblockheader(prev_hash)
        prev_timestamp = prev_block["time"]

    t_start = time.time()

    for height in range(start_height, current_height + 1):
        try:
            record = extract_block_full(rpc, height, prev_timestamp)
            prev_timestamp = record["block_timestamp"]
            batch.append(record)
        except Exception as e:
            logger.error(f"Error extracting block {height}: {e}")
            continue

        # Flush batch
        if len(batch) >= batch_size:
            df = pd.DataFrame(batch)
            year = datetime.fromtimestamp(batch[0]["block_timestamp"], tz=timezone.utc).year
            batch_num = stats["batches"]
            gcs_path = GCS_PATHS["blockchain_blocks"].format(year=year, batch=batch_num)

            size = storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)
            stats["batches"] += 1
            stats["total_rows"] += len(batch)
            stats["total_bytes"] += size

            # Save progress
            storage.update_task_state("blockchain_blocks", "last_completed_height", height)
            storage.update_task_state("blockchain_blocks", "batches_uploaded", stats["batches"])
            storage.flush_state()  # v2: persist state after each batch, not each update

            elapsed = time.time() - t_start
            rate = stats["total_rows"] / max(1, elapsed)
            remaining = (current_height - height) / max(1, rate)

            logger.info(
                f"Batch {stats['batches']}: blocks {batch[0]['block_height']}-{batch[-1]['block_height']}, "
                f"{rate:.0f} blocks/s, ETA {remaining/3600:.1f}h"
            )

            if progress_callback:
                progress_callback(height - start_height, total_blocks, {
                    "height": height, "rate": rate, "eta_h": remaining / 3600
                })

            batch.clear()

    # Flush remaining
    if batch:
        df = pd.DataFrame(batch)
        year = datetime.fromtimestamp(batch[0]["block_timestamp"], tz=timezone.utc).year
        gcs_path = GCS_PATHS["blockchain_blocks"].format(year=year, batch=stats["batches"])
        size = storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)
        stats["batches"] += 1
        stats["total_rows"] += len(batch)
        stats["total_bytes"] += size
        storage.update_task_state("blockchain_blocks", "last_completed_height", batch[-1]["block_height"])

    duration = time.time() - t_start
    stats["duration_s"] = duration
    stats["total_gb"] = stats["total_bytes"] / 1e9

    storage.flush_state()  # v2: final state persistence

    logger.info(
        f"✅ Blocks extraction DONE: {stats['total_rows']:,} blocks, "
        f"{stats['total_gb']:.2f} GB, {duration/3600:.1f}h"
    )
    return stats


# v2: removed — parse_transaction_full, extract_transactions_from_block,
#     run_transactions_extraction, get_tx_creation_height, _tx_height_cache
#     (utilité < 3/10 pour horizons 30s-5min)

# v2: removed — extract_utxo_snapshot_aggregated, run_utxo_snapshots
#     (utilité < 3/10 pour horizons 30s-5min)

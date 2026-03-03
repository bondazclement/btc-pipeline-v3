"""
Extraction complète de la blockchain Bitcoin via RPC local.

Prérequis :
  - Bitcoin Core synchronisé avec txindex=1
  - ~650 GB disque pour le nœud
  - bitcoin-utxo-dump installé
  - Env : BTC_RPC_USER, BTC_RPC_PASSWORD

Architecture streaming :
  Les données sont traitées en batches et immédiatement uploadées sur GCS.
  Le disque local n'est utilisé que pour le nœud Bitcoin Core lui-même.

Trois niveaux d'extraction (chacun avec son propre état de progression) :
  1. Blocs (BLOCK_SCHEMA) — 24-48h
  2. Transactions (TRANSACTION_SCHEMA + CDD) — 5-10 jours
  3. UTXO snapshots — 3-5 jours
"""

import os
import subprocess
import csv
import io
import time
import statistics
from datetime import datetime, timezone
from functools import lru_cache
from typing import Iterator, Optional

import numpy as np
import pandas as pd
from loguru import logger

from btc_pipeline.config import (
    Config, GCS_PATHS, BLOCK_SCHEMA_DTYPES, TRANSACTION_SCHEMA_DTYPES,
    get_halving_epoch, get_blocks_since_halving, HALVING_HEIGHTS,
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
        "prev_block_hash": block.get("previousblockhash", ""),
        "merkle_root": block.get("merkleroot", ""),
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
    block_cdd = 0.0
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
    record["coin_days_destroyed_block"] = block_cdd  # Will be computed in tx pass

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

    logger.info(
        f"✅ Blocks extraction DONE: {stats['total_rows']:,} blocks, "
        f"{stats['total_gb']:.2f} GB, {duration/3600:.1f}h"
    )
    return stats


# ═══════════════════════════════════════════════════════════════════════════
# TRANSACTION EXTRACTION (Level 2)
# ═══════════════════════════════════════════════════════════════════════════

# Cache LRU pour les lookups txid → block_height (nécessaire pour CDD)
_tx_height_cache = {}


def get_tx_creation_height(rpc, txid: str) -> int:
    """
    Retrouve le block_height de création d'un UTXO via getrawtransaction.
    Utilise un cache dict pour éviter les appels RPC répétés.
    """
    if txid in _tx_height_cache:
        return _tx_height_cache[txid]

    try:
        raw_tx = rpc.getrawtransaction(txid, True)
        block_hash = raw_tx.get("blockhash", "")
        if block_hash:
            header = rpc.getblockheader(block_hash)
            height = header["height"]
        else:
            height = 0
    except Exception:
        height = 0

    # Limit cache size
    if len(_tx_height_cache) > 500_000:
        # Evict oldest 100k entries (simple strategy)
        keys = list(_tx_height_cache.keys())[:100_000]
        for k in keys:
            del _tx_height_cache[k]

    _tx_height_cache[txid] = height
    return height


def parse_transaction_full(rpc, tx: dict, block_height: int,
                           block_time: int, tx_index: int) -> dict:
    """
    Parse une transaction complète selon TRANSACTION_SCHEMA.
    Calcule CDD, type de transaction, distributions d'outputs.
    """
    is_coinbase = "coinbase" in tx.get("vin", [{}])[0] if tx.get("vin") else False

    # ── Outputs ───────────────────────────────────────────────────────────
    total_output_sats = 0
    max_output = 0
    min_output = float("inf")
    output_types = {"p2pkh": 0, "p2sh": 0, "p2wpkh": 0, "p2tr": 0, "op_return": 0}

    for vout in tx.get("vout", []):
        val_sats = int(round(vout.get("value", 0) * 1e8))
        total_output_sats += val_sats
        max_output = max(max_output, val_sats)
        if val_sats > 0:
            min_output = min(min_output, val_sats)

        otype = detect_output_type(vout.get("scriptPubKey", {}))
        if otype in output_types:
            output_types[otype] += 1

    if min_output == float("inf"):
        min_output = 0

    # ── Inputs + CDD ──────────────────────────────────────────────────────
    total_input_sats = 0
    has_segwit = False
    has_taproot = False
    is_rbf = False
    input_ages = []  # age in blocks

    n_inputs = len(tx.get("vin", []))
    n_outputs = len(tx.get("vout", []))

    if not is_coinbase:
        for vin in tx.get("vin", []):
            # RBF detection
            if vin.get("sequence", 0xFFFFFFFF) < 0xFFFFFFFE:
                is_rbf = True

            # SegWit / Taproot
            if vin.get("txinwitness"):
                has_segwit = True
                witness = vin["txinwitness"]
                if len(witness) >= 1 and len(witness[0]) in (128, 130):
                    has_taproot = True

            # Input value and age (for CDD)
            prevout = vin.get("prevout", {})
            if prevout:
                val_sats = int(round(prevout.get("value", 0) * 1e8))
                total_input_sats += val_sats

            # Get creation height for CDD
            prev_txid = vin.get("txid", "")
            if prev_txid:
                creation_height = get_tx_creation_height(rpc, prev_txid)
                age_blocks = block_height - creation_height
                input_ages.append((age_blocks, int(round(prevout.get("value", 0) * 1e8)) if prevout else 0))

    # CDD calculation
    cdd = 0.0
    if input_ages:
        for age_blocks, val_sats in input_ages:
            if age_blocks > 0 and val_sats > 0:
                age_days = age_blocks * 10.0 / 1440.0  # ~10 min per block
                cdd += satoshis_to_btc(val_sats) * age_days

    avg_input_age = np.mean([a for a, _ in input_ages]) if input_ages else 0
    max_input_age = max([a for a, _ in input_ages]) if input_ages else 0

    # Fee
    fee_sats = max(0, total_input_sats - total_output_sats) if not is_coinbase else 0
    vsize = tx.get("vsize", tx.get("size", 1))
    fee_rate = fee_sats / vsize if vsize > 0 else 0

    # Transaction type heuristics
    is_consolidation = n_inputs > 3 and n_outputs <= 2
    is_batch = n_inputs <= 2 and n_outputs > 5

    return {
        "txid": tx["txid"],
        "block_height": block_height,
        "block_timestamp": block_time,
        "tx_index_in_block": tx_index,
        "is_coinbase": is_coinbase,
        "input_count": n_inputs,
        "output_count": n_outputs,
        "tx_size_bytes": tx.get("size", 0),
        "tx_vsize_bytes": vsize,
        "tx_weight": tx.get("weight", 0),
        "total_input_satoshis": total_input_sats,
        "total_output_satoshis": total_output_sats,
        "fee_satoshis": fee_sats,
        "fee_rate_sat_vbyte": fee_rate,
        "has_segwit_input": has_segwit,
        "has_taproot_input": has_taproot,
        "is_rbf": is_rbf,
        "is_consolidation": is_consolidation,
        "is_batch_payment": is_batch,
        "output_count_p2pkh": output_types["p2pkh"],
        "output_count_p2sh": output_types["p2sh"],
        "output_count_p2wpkh": output_types["p2wpkh"],
        "output_count_p2tr": output_types["p2tr"],
        "output_count_op_return": output_types["op_return"],
        "max_output_satoshis": max_output,
        "min_output_satoshis": min_output,
        "coin_days_destroyed": cdd,
        "avg_input_age_blocks": avg_input_age,
        "max_input_age_blocks": max_input_age,
    }


def extract_transactions_from_block(rpc, block_height: int) -> Iterator[dict]:
    """Extrait les transactions d'un bloc une par une (streaming)."""
    block_hash = rpc.getblockhash(block_height)
    block = rpc.getblock(block_hash, 2)
    for idx, tx in enumerate(block["tx"]):
        yield parse_transaction_full(rpc, tx, block_height, block["time"], idx)


def run_transactions_extraction(
    storage: StorageClient,
    batch_size: int = 100_000,
    config: Optional[Config] = None,
    progress_callback=None,
) -> dict:
    """
    Extrait TOUTES les transactions depuis genesis.
    Reprend depuis (last_block_height, last_tx_index) dans pipeline_state.json.
    Upload chaque batch de 100K transactions en Parquet sur GCS.

    Durée estimée : 5-10 jours pour 900M transactions.
    Volume GCS : ~130 GB Parquet.
    """
    config = config or Config()
    rpc = get_rpc(config)

    chain_info = rpc.getblockchaininfo()
    current_height = chain_info["blocks"]

    state = storage.get_pipeline_state()
    task_state = state.get("tasks", {}).get("blockchain_transactions", {})
    start_height = task_state.get("last_completed_block_height", -1) + 1

    total_blocks = current_height - start_height + 1
    logger.info(f"▶ Transactions extraction: blocks {start_height} → {current_height}")

    stats = {"batches": 0, "total_rows": 0, "total_bytes": 0, "blocks_processed": 0}
    batch = []
    batch_num = task_state.get("batches_uploaded", 0)

    t_start = time.time()

    for height in range(start_height, current_height + 1):
        try:
            for tx_record in extract_transactions_from_block(rpc, height):
                batch.append(tx_record)

                # Flush batch
                if len(batch) >= batch_size:
                    df = pd.DataFrame(batch)
                    ts = batch[0]["block_timestamp"]
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    gcs_path = GCS_PATHS["blockchain_transactions"].format(
                        year=dt.year, month=dt.month, batch=batch_num
                    )

                    size = storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)
                    stats["batches"] += 1
                    stats["total_rows"] += len(batch)
                    stats["total_bytes"] += size
                    batch_num += 1

                    # Save progress
                    storage.update_task_state("blockchain_transactions",
                                             "last_completed_block_height", height)
                    storage.update_task_state("blockchain_transactions",
                                             "batches_uploaded", batch_num)

                    elapsed = time.time() - t_start
                    rate = stats["total_rows"] / max(1, elapsed)

                    logger.info(
                        f"TX Batch {batch_num}: {stats['total_rows']:,} txs total, "
                        f"block {height}, {rate:.0f} tx/s"
                    )

                    if progress_callback:
                        progress_callback(height - start_height, total_blocks, {
                            "height": height, "total_txs": stats["total_rows"], "rate": rate
                        })

                    batch.clear()

        except Exception as e:
            logger.error(f"Error at block {height}: {e}")
            continue

        stats["blocks_processed"] += 1

    # Flush remaining
    if batch:
        df = pd.DataFrame(batch)
        ts = batch[0]["block_timestamp"]
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        gcs_path = GCS_PATHS["blockchain_transactions"].format(
            year=dt.year, month=dt.month, batch=batch_num
        )
        size = storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)
        stats["batches"] += 1
        stats["total_rows"] += len(batch)
        stats["total_bytes"] += size

    duration = time.time() - t_start
    stats["duration_s"] = duration
    stats["total_gb"] = stats["total_bytes"] / 1e9

    logger.info(
        f"✅ Transactions extraction DONE: {stats['total_rows']:,} txs, "
        f"{stats['total_gb']:.2f} GB, {duration/3600:.1f}h"
    )
    return stats


# ═══════════════════════════════════════════════════════════════════════════
# UTXO SNAPSHOT EXTRACTION (Level 3)
# ═══════════════════════════════════════════════════════════════════════════

def extract_utxo_snapshot_aggregated(rpc, storage: StorageClient,
                                     block_height: int, config: Optional[Config] = None) -> dict:
    """
    Lance bitcoin-utxo-dump pour l'état actuel de l'UTXO set.
    Calcule toutes les distributions agrégées (UTXO_SNAPSHOT_SCHEMA).
    """
    config = config or Config()

    # Verify block height matches chain tip
    chain_info = rpc.getblockchaininfo()
    chain_height = chain_info["blocks"]

    logger.info(f"▶ UTXO snapshot at block {block_height} (chain tip: {chain_height})")

    # Output file for bitcoin-utxo-dump
    csv_path = os.path.join(config.temp_dir, f"utxo_dump_{block_height}.csv")
    os.makedirs(config.temp_dir, exist_ok=True)

    # Run bitcoin-utxo-dump
    btc_datadir = os.path.join(config.workspace, ".bitcoin")
    chainstate_db = os.path.join(btc_datadir, "chainstate")

    try:
        cmd = [
            "bitcoin-utxo-dump",
            "-db", chainstate_db,
            "-o", csv_path,
            "-fields", "count,txid,vout,amount,type,height",
        ]
        logger.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

        if result.returncode != 0:
            logger.error(f"bitcoin-utxo-dump failed: {result.stderr}")
            return {"status": "error", "error": result.stderr}

    except FileNotFoundError:
        logger.error("bitcoin-utxo-dump not found. Please install it first.")
        return {"status": "error", "error": "bitcoin-utxo-dump not installed"}
    except subprocess.TimeoutExpired:
        logger.error("bitcoin-utxo-dump timed out (>1h)")
        return {"status": "error", "error": "timeout"}

    # Parse the CSV and compute aggregated metrics
    logger.info(f"Parsing UTXO dump: {csv_path}")

    block_time = rpc.getblockheader(rpc.getblockhash(block_height))["time"]

    # Age buckets (in blocks)
    BLOCKS_PER_DAY = 144
    age_buckets_blocks = {
        "below_1d": BLOCKS_PER_DAY,
        "1d_7d": BLOCKS_PER_DAY * 7,
        "7d_30d": BLOCKS_PER_DAY * 30,
        "30d_90d": BLOCKS_PER_DAY * 90,
        "90d_180d": BLOCKS_PER_DAY * 180,
        "180d_1y": BLOCKS_PER_DAY * 365,
        "1y_2y": BLOCKS_PER_DAY * 730,
        "2y_3y": BLOCKS_PER_DAY * 1095,
        "3y_5y": BLOCKS_PER_DAY * 1825,
    }

    # Initialize counters
    btc_by_age = {k: 0.0 for k in list(age_buckets_blocks.keys()) + ["above_5y"]}
    count_by_age = {
        "below_1d": 0, "1d_7d": 0, "7d_30d": 0, "30d_90d": 0,
        "90d_180d": 0, "180d_1y": 0, "above_1y": 0,
    }
    count_by_value = {
        "dust": 0, "below_001": 0, "001_01": 0, "01_1": 0,
        "1_10": 0, "10_100": 0, "above_100": 0,
    }
    btc_by_value = {k: 0.0 for k in count_by_value}

    total_count = 0
    total_btc = 0.0
    all_values = []

    # Stream parse the CSV
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            amount_btc = float(row.get("amount", 0))
            creation_height = int(row.get("height", 0))
            age_blocks = block_height - creation_height

            total_count += 1
            total_btc += amount_btc
            amount_sats = int(amount_btc * 1e8)

            # Age classification
            if age_blocks < BLOCKS_PER_DAY:
                btc_by_age["below_1d"] += amount_btc
                count_by_age["below_1d"] += 1
            elif age_blocks < BLOCKS_PER_DAY * 7:
                btc_by_age["1d_7d"] += amount_btc
                count_by_age["1d_7d"] += 1
            elif age_blocks < BLOCKS_PER_DAY * 30:
                btc_by_age["7d_30d"] += amount_btc
                count_by_age["7d_30d"] += 1
            elif age_blocks < BLOCKS_PER_DAY * 90:
                btc_by_age["30d_90d"] += amount_btc
                count_by_age["30d_90d"] += 1
            elif age_blocks < BLOCKS_PER_DAY * 180:
                btc_by_age["90d_180d"] += amount_btc
                count_by_age["90d_180d"] += 1
            elif age_blocks < BLOCKS_PER_DAY * 365:
                btc_by_age["180d_1y"] += amount_btc
                count_by_age["180d_1y"] += 1
            elif age_blocks < BLOCKS_PER_DAY * 730:
                btc_by_age["1y_2y"] += amount_btc
                count_by_age["above_1y"] += 1
            elif age_blocks < BLOCKS_PER_DAY * 1095:
                btc_by_age["2y_3y"] += amount_btc
                count_by_age["above_1y"] += 1
            elif age_blocks < BLOCKS_PER_DAY * 1825:
                btc_by_age["3y_5y"] += amount_btc
                count_by_age["above_1y"] += 1
            else:
                btc_by_age["above_5y"] += amount_btc
                count_by_age["above_1y"] += 1

            # Value classification
            if amount_sats < 546:
                count_by_value["dust"] += 1
                btc_by_value["dust"] += amount_btc
            elif amount_btc < 0.01:
                count_by_value["below_001"] += 1
                btc_by_value["below_001"] += amount_btc
            elif amount_btc < 0.1:
                count_by_value["001_01"] += 1
                btc_by_value["001_01"] += amount_btc
            elif amount_btc < 1.0:
                count_by_value["01_1"] += 1
                btc_by_value["01_1"] += amount_btc
            elif amount_btc < 10.0:
                count_by_value["1_10"] += 1
                btc_by_value["1_10"] += amount_btc
            elif amount_btc < 100.0:
                count_by_value["10_100"] += 1
                btc_by_value["10_100"] += amount_btc
            else:
                count_by_value["above_100"] += 1
                btc_by_value["above_100"] += amount_btc

            # Sample values for median
            if total_count <= 1_000_000:
                all_values.append(amount_btc)

    # Delete local CSV immediately
    os.remove(csv_path)
    logger.info(f"Deleted local dump: {csv_path}")

    avg_value = total_btc / total_count if total_count > 0 else 0
    median_value = float(np.median(all_values)) if all_values else 0

    snapshot = {
        "snapshot_block_height": block_height,
        "snapshot_timestamp": block_time,
        "total_utxo_count": total_count,
        "total_btc_in_utxos": total_btc,
        "avg_utxo_value_btc": avg_value,
        "median_utxo_value_btc": median_value,
        # Age distribution (BTC)
        "btc_age_below_1d": btc_by_age["below_1d"],
        "btc_age_1d_7d": btc_by_age["1d_7d"],
        "btc_age_7d_30d": btc_by_age["7d_30d"],
        "btc_age_30d_90d": btc_by_age["30d_90d"],
        "btc_age_90d_180d": btc_by_age["90d_180d"],
        "btc_age_180d_1y": btc_by_age["180d_1y"],
        "btc_age_1y_2y": btc_by_age["1y_2y"],
        "btc_age_2y_3y": btc_by_age["2y_3y"],
        "btc_age_3y_5y": btc_by_age["3y_5y"],
        "btc_age_above_5y": btc_by_age["above_5y"],
        # Age distribution (counts)
        "utxo_count_age_below_1d": count_by_age["below_1d"],
        "utxo_count_age_1d_7d": count_by_age["1d_7d"],
        "utxo_count_age_7d_30d": count_by_age["7d_30d"],
        "utxo_count_age_30d_90d": count_by_age["30d_90d"],
        "utxo_count_age_90d_180d": count_by_age["90d_180d"],
        "utxo_count_age_180d_1y": count_by_age["180d_1y"],
        "utxo_count_age_above_1y": count_by_age["above_1y"],
        # Value distribution (counts)
        "utxo_count_dust": count_by_value["dust"],
        "utxo_count_below_001btc": count_by_value["below_001"],
        "utxo_count_001_01btc": count_by_value["001_01"],
        "utxo_count_01_1btc": count_by_value["01_1"],
        "utxo_count_1_10btc": count_by_value["1_10"],
        "utxo_count_10_100btc": count_by_value["10_100"],
        "utxo_count_above_100btc": count_by_value["above_100"],
        # Value distribution (BTC held)
        "btc_held_by_dust": btc_by_value["dust"],
        "btc_held_below_001btc": btc_by_value["below_001"],
        "btc_held_001_01btc": btc_by_value["001_01"],
        "btc_held_01_1btc": btc_by_value["01_1"],
        "btc_held_1_10btc": btc_by_value["1_10"],
        "btc_held_10_100btc": btc_by_value["10_100"],
        "btc_held_above_100btc": btc_by_value["above_100"],
        # Derived metrics (placeholders — computed at dataset build time)
        "liveliness": 0.0,
        "coin_days_destroyed_7d": 0.0,
        "dormancy_7d": 0.0,
        "realized_cap_usd": 0.0,
    }

    # Upload
    dt = datetime.fromtimestamp(block_time, tz=timezone.utc)
    date_str = dt.strftime("%Y%m%d")
    gcs_path = GCS_PATHS["blockchain_utxo"].format(year=dt.year, date=date_str)

    df = pd.DataFrame([snapshot])
    size = storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)

    logger.info(f"✅ UTXO snapshot at block {block_height}: {total_count:,} UTXOs, {total_btc:.2f} BTC")

    return {"status": "success", "block_height": block_height, "utxo_count": total_count,
            "total_btc": total_btc, "size_bytes": size}


def run_utxo_snapshots(
    storage: StorageClient,
    frequency: str = "weekly",
    start_date: str = "2017-01-01",
    mode: str = "aggregated",
    config: Optional[Config] = None,
    progress_callback=None,
) -> dict:
    """
    Génère des snapshots UTXO à la fréquence demandée.
    Note: pour les snapshots historiques, Bitcoin Core doit être re-indexed
    à chaque hauteur — ceci est très lent. En pratique, on utilise les snapshots
    disponibles ou on reconstitue depuis les transactions.
    """
    config = config or Config()
    rpc = get_rpc(config)

    chain_info = rpc.getblockchaininfo()
    current_height = chain_info["blocks"]

    # For now, only current snapshot is supported directly
    # Historical snapshots would need to be reconstructed from transactions
    logger.info(f"▶ Running UTXO snapshot at current height {current_height}")

    result = extract_utxo_snapshot_aggregated(rpc, storage, current_height, config)

    storage.update_task_state("blockchain_utxo", "last_snapshot_height", current_height)
    storage.update_task_state("blockchain_utxo", "mode", mode)

    return result

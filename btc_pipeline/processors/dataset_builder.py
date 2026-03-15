"""
Dataset Builder
Orchestrateur mensuel : construit le dataset final mois par mois.

Pour chaque mois :
1. Charge les aggTrades → agrège en buckets 1s
2. Ajoute les features rolling
3. Fusionne les données blockchain (blocs, transactions)
4. Fusionne UTXO, mempool, Glassnode
5. Fusionne futures (funding rate)
6. Ajoute les features temporelles
7. Normalise (rolling z-score)
8. Génère les labels
9. Valide (qualité, leakage)
10. Upload features + labels sur GCS
"""

import time
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from btc_pipeline.config import Config, GCS_PATHS
from btc_pipeline.storage.gcs_client import StorageClient
from btc_pipeline.processors.bucket_aggregator import (
    process_aggtrades_month, compute_rolling_features,
)
from btc_pipeline.processors.feature_engineer import build_features
from btc_pipeline.processors.temporal_aligner import (
    load_aggtrades_month, load_klines_month, load_blocks_range,
    # v2: removed — load_utxo_snapshots
    load_mempool_month, load_funding_month,
    load_glassnode_metrics, validate_klines_vs_aggtrades,
)
from btc_pipeline.processors.normalizer import normalize_features, compute_feature_stats, save_scaler_state
from btc_pipeline.processors.label_generator import generate_labels, validate_no_leakage


# ═══════════════════════════════════════════════════════════════════════════
# QUALITY CRITERIA
# ═══════════════════════════════════════════════════════════════════════════

QUALITY_CRITERIA = {
    "min_rows_per_month": 2_000_000,    # ~2.6M seconds per month
    "max_nan_ratio": 0.05,              # < 5% NaN
    "max_price_gap_seconds": 300,       # no gap > 5 minutes
    "min_buy_ratio_std": 0.01,          # buy_ratio shouldn't be constant
    "price_kline_match_pct": 0.01,      # < 0.01% price difference vs klines
}


def check_quality(df_features: pd.DataFrame, df_labels: pd.DataFrame,
                  validation_result: dict = None) -> dict:
    """Vérifie les critères de qualité pour un mois."""
    report = {"checks": {}, "passed": True}

    # Row count
    n_rows = len(df_features)
    report["checks"]["row_count"] = {
        "value": n_rows,
        "threshold": QUALITY_CRITERIA["min_rows_per_month"],
        "passed": n_rows >= QUALITY_CRITERIA["min_rows_per_month"],
    }

    # NaN ratio
    nan_ratio = df_features.isna().sum().sum() / (n_rows * len(df_features.columns))
    report["checks"]["nan_ratio"] = {
        "value": round(nan_ratio, 6),
        "threshold": QUALITY_CRITERIA["max_nan_ratio"],
        "passed": nan_ratio <= QUALITY_CRITERIA["max_nan_ratio"],
    }

    # Price gaps
    if "timestamp_s" in df_features.columns:
        ts_diff = df_features["timestamp_s"].diff()
        max_gap = ts_diff.max()
        report["checks"]["max_gap_seconds"] = {
            "value": int(max_gap) if not pd.isna(max_gap) else 0,
            "threshold": QUALITY_CRITERIA["max_price_gap_seconds"],
            "passed": (max_gap <= QUALITY_CRITERIA["max_price_gap_seconds"]) if not pd.isna(max_gap) else True,
        }

    # Buy ratio variance
    if "buy_ratio" in df_features.columns:
        br_std = df_features["buy_ratio"].std()
        report["checks"]["buy_ratio_std"] = {
            "value": round(br_std, 6),
            "threshold": QUALITY_CRITERIA["min_buy_ratio_std"],
            "passed": br_std >= QUALITY_CRITERIA["min_buy_ratio_std"],
        }

    # Kline validation
    if validation_result:
        report["checks"]["kline_validation"] = validation_result

    # Label validity
    valid_labels = df_labels.dropna().shape[0]
    report["checks"]["valid_labels_ratio"] = {
        "value": round(valid_labels / max(1, len(df_labels)), 4),
        "passed": valid_labels / max(1, len(df_labels)) > 0.95,
    }

    # Overall
    report["passed"] = all(c.get("passed", True) for c in report["checks"].values()
                           if isinstance(c, dict))
    report["n_features"] = len([c for c in df_features.columns if c != "timestamp_s"])
    report["n_rows"] = n_rows

    return report


# ═══════════════════════════════════════════════════════════════════════════
# MONTHLY BUILD
# ═══════════════════════════════════════════════════════════════════════════

def build_month(
    storage: StorageClient,
    year: int,
    month: int,
    priority: int = 1,
    normalize: bool = True,
    config: Optional[Config] = None,
) -> dict:
    """
    Construit le dataset pour un mois complet.

    v2: priority max = 2 (UTXO et BGeometrics supprimés).

    Priority levels :
      1 : aggTrades + blocs + temporel (dataset minimal entraînable)
      2 : + futures + mempool + Glassnode (dataset complet)
    """
    config = config or Config()
    t_start = time.time()

    month_str = f"{year}-{month:02d}"
    logger.info(f"═══ Building dataset for {month_str} (priority={priority}) ═══")

    # ── 1. Load aggTrades and aggregate to 1s ──────────────────────────────
    df_raw = load_aggtrades_month(storage, year, month)
    if df_raw.empty:
        logger.error(f"No aggTrades for {month_str} — skipping")
        return {"status": "no_data", "month": month_str}

    df_1s = process_aggtrades_month(df_raw, fill_gaps=True)
    del df_raw  # Free memory
    logger.info(f"  1s buckets: {len(df_1s):,} rows")

    # ── 2. Rolling features on 1s data ──────────────────────────────────────
    df_1s = compute_rolling_features(df_1s)
    logger.info(f"  Rolling features added")

    # ── 3. Load auxiliary data sources ──────────────────────────────────────
    df_blocks = pd.DataFrame()
    df_mempool = pd.DataFrame()
    df_funding = pd.DataFrame()
    glassnode_dfs = {}

    # Priority 1: blocks
    df_blocks = load_blocks_range(storage, year)
    if not df_blocks.empty:
        # Filter to this month's timerange
        ts_min = df_1s["timestamp_s"].min()
        ts_max = df_1s["timestamp_s"].max()
        # Include blocks slightly before for forward-fill
        df_blocks = df_blocks[
            (df_blocks["block_timestamp"] >= ts_min - 86400) &
            (df_blocks["block_timestamp"] <= ts_max)
        ]
        logger.info(f"  Blocks loaded: {len(df_blocks):,}")

    # Priority 2: futures, mempool, Glassnode
    if priority >= 2:
        df_funding = load_funding_month(storage, year, month)
        if not df_funding.empty:
            logger.info(f"  Funding rate loaded: {len(df_funding):,} records")

        df_mempool = load_mempool_month(storage, year, month)
        if not df_mempool.empty:
            logger.info(f"  Mempool loaded: {len(df_mempool):,} snapshots")

        # v2: Glassnode moved from priority 3 to priority 2 (3/10 utilité, on garde tout >= 3/10)
        glassnode_dfs = load_glassnode_metrics(storage, year)
        if glassnode_dfs:
            logger.info(f"  Glassnode metrics loaded: {list(glassnode_dfs.keys())}")

    # v2: removed — Priority 3 (UTXO + BGeometrics, utilité < 3/10)

    # ── 4. Feature engineering (merge all sources) ─────────────────────────
    df_features = build_features(
        df_1s,
        df_blocks=df_blocks,
        df_mempool=df_mempool,
        df_funding=df_funding,
        glassnode_dfs=glassnode_dfs,
    )

    # Free memory
    del df_1s, df_blocks, df_mempool, df_funding
    logger.info(f"  Features built: {len(df_features.columns)} columns")

    # ── 5. Normalization ───────────────────────────────────────────────────
    if normalize:
        df_features = normalize_features(df_features)
        logger.info(f"  Features normalized")

    # ── 6. Generate labels ─────────────────────────────────────────────────
    df_labels = generate_labels(df_features)
    logger.info(f"  Labels generated")

    # ── 7. Validation ──────────────────────────────────────────────────────
    # Cross-validate with klines
    df_klines = load_klines_month(storage, year, month, "1m")
    kline_validation = {}
    if not df_klines.empty:
        kline_validation = validate_klines_vs_aggtrades(df_features, df_klines)
        del df_klines

    # Leakage test
    no_leakage = validate_no_leakage(df_features, df_labels)

    # Quality report
    quality = check_quality(df_features, df_labels, kline_validation)
    quality["no_leakage"] = no_leakage
    quality["month"] = month_str
    quality["priority"] = priority

    # ── 8. Upload to GCS ───────────────────────────────────────────────────
    features_path = GCS_PATHS["features"].format(year=year, month=month)
    labels_path = GCS_PATHS["labels"].format(year=year, month=month)
    quality_path = GCS_PATHS["quality"].format(year=year, month=month)

    features_size = storage.stream_upload_parquet(df_features, features_path, skip_if_exists=False)
    labels_size = storage.stream_upload_parquet(df_labels, labels_path, skip_if_exists=False)

    import json
    quality_json = json.dumps(quality, indent=2, default=str).encode("utf-8")
    storage.stream_upload_bytes(quality_json, quality_path, skip_if_exists=False)

    # Save feature stats for this year
    stats = compute_feature_stats(df_features)
    save_scaler_state(storage, stats, year)

    duration = time.time() - t_start

    result = {
        "status": "success" if quality["passed"] else "quality_warning",
        "month": month_str,
        "rows": len(df_features),
        "features": len(df_features.columns),
        "features_size_mb": features_size / 1e6,
        "labels_size_mb": labels_size / 1e6,
        "duration_s": duration,
        "quality": quality,
    }

    logger.info(
        f"  ✅ {month_str}: {len(df_features):,} rows, "
        f"{len(df_features.columns)} features, "
        f"{features_size/1e6:.1f} MB features + {labels_size/1e6:.1f} MB labels, "
        f"{duration:.0f}s"
    )

    return result


# ═══════════════════════════════════════════════════════════════════════════
# FULL DATASET BUILD (all months)
# ═══════════════════════════════════════════════════════════════════════════

def build_full_dataset(
    storage: StorageClient,
    start_year: int = 2017,
    start_month: int = 8,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None,
    priority: int = 1,
    config: Optional[Config] = None,
    progress_callback=None,
) -> dict:
    """
    Construit le dataset complet mois par mois.
    Reprend depuis le dernier mois complété.
    """
    config = config or Config()

    if end_year is None:
        now = datetime.now(timezone.utc)
        end_year = now.year
        end_month = now.month - 1  # Don't process current month
        if end_month == 0:
            end_year -= 1
            end_month = 12

    # Generate month list
    months = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    # Check pipeline state for already completed months
    state = storage.get_pipeline_state()
    completed = set(state.get("tasks", {}).get("dataset_builder", {}).get("completed_months", []))
    months_to_process = [(y, m) for y, m in months if f"{y}-{m:02d}" not in completed]

    logger.info(f"▶ Dataset build: {len(months_to_process)} months to process (of {len(months)} total)")

    all_results = []
    total_bytes = 0

    for i, (year, month) in enumerate(months_to_process):
        result = build_month(storage, year, month, priority=priority, config=config)
        all_results.append(result)

        if result["status"] in ("success", "quality_warning"):
            total_bytes += result.get("features_size_mb", 0) * 1e6 + result.get("labels_size_mb", 0) * 1e6
            completed.add(f"{year}-{month:02d}")
            storage.update_task_state("dataset_builder", "completed_months", list(completed))
            storage.update_task_state("dataset_builder", "last_completed", f"{year}-{month:02d}")

        if progress_callback:
            progress_callback(i + 1, len(months_to_process), result)

    # Summary
    success = sum(1 for r in all_results if r["status"] in ("success", "quality_warning"))
    failed = sum(1 for r in all_results if r["status"] not in ("success", "quality_warning"))
    total_gb = total_bytes / 1e9

    summary = {
        "months_processed": len(all_results),
        "success": success,
        "failed": failed,
        "total_gb": total_gb,
        "priority": priority,
        "results": all_results,
    }

    logger.info(
        f"═══ Dataset build complete: {success} success, {failed} failed, {total_gb:.2f} GB ═══"
    )

    return summary

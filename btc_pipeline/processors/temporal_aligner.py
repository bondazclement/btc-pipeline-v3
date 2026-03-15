"""
Temporal Aligner
Aligne toutes les sources de données sur la résolution 1 seconde.
Gère les différentes fréquences via forward-fill et interpolation.

v2: UTXO loader supprimé (snapshots UTXO retirés du pipeline).

Sources et résolutions :
  - aggTrades → 1s (natif après agrégation)
  - klines 1m → 60s (validation + backup)
  - Blockchain blocs → ~600s (forward-fill)
  - Mempool → ~60s (forward-fill)
  - Glassnode → daily (forward-fill)
  - Futures funding → 8h (forward-fill)
"""

import pandas as pd
import numpy as np
from loguru import logger

from btc_pipeline.storage.gcs_client import StorageClient
from btc_pipeline.config import GCS_PATHS


def load_aggtrades_month(storage: StorageClient, year: int, month: int) -> pd.DataFrame:
    """Charge les aggTrades d'un mois depuis GCS."""
    gcs_path = GCS_PATHS["binance_spot_aggtrades"].format(year=year, month=month)
    if not storage.exists(gcs_path):
        logger.warning(f"aggTrades not found: {gcs_path}")
        return pd.DataFrame()
    return storage.download_parquet(gcs_path)


def load_klines_month(storage: StorageClient, year: int, month: int,
                      interval: str = "1m") -> pd.DataFrame:
    """Charge les klines d'un mois depuis GCS."""
    gcs_path = GCS_PATHS["binance_spot_klines"].format(interval=interval, year=year, month=month)
    if not storage.exists(gcs_path):
        return pd.DataFrame()
    return storage.download_parquet(gcs_path)


def load_blocks_range(storage: StorageClient, year: int) -> pd.DataFrame:
    """Charge toutes les données de blocs d'une année depuis GCS."""
    prefix = f"raw/blockchain/blocks/year={year}/"
    files = storage.list_files(prefix)
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        if f.endswith(".parquet"):
            try:
                df = storage.download_parquet(f)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Error loading {f}: {e}")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


# v2: removed — load_utxo_snapshots (UTXO snapshots supprimés, utilité < 3/10 pour horizons 30s-5min)


def load_mempool_month(storage: StorageClient, year: int, month: int) -> pd.DataFrame:
    """Charge les données mempool d'un mois."""
    prefix = f"raw/mempool/snapshots/year={year}/month={month:02d}/"
    files = storage.list_files(prefix)
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        if f.endswith(".parquet"):
            try:
                dfs.append(storage.download_parquet(f))
            except Exception as e:
                logger.warning(f"Error loading {f}: {e}")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def load_funding_month(storage: StorageClient, year: int, month: int) -> pd.DataFrame:
    """Charge les données funding rate d'un mois."""
    gcs_path = GCS_PATHS["binance_futures_funding"].format(year=year, month=month)
    if not storage.exists(gcs_path):
        return pd.DataFrame()
    return storage.download_parquet(gcs_path)


def load_glassnode_metrics(storage: StorageClient, year: int) -> dict[str, pd.DataFrame]:
    """Charge toutes les métriques Glassnode d'une année."""
    prefix = f"raw/onchain_metrics/glassnode/"
    files = storage.list_files(prefix)

    result = {}
    for f in files:
        if f.endswith(".parquet") and f"year={year}" in f:
            # Extract metric name from path
            parts = f.split("/")
            for p in parts:
                if p.startswith("metric="):
                    metric_name = p.replace("metric=", "")
                    try:
                        df = storage.download_parquet(f)
                        result[metric_name] = df
                    except Exception as e:
                        logger.warning(f"Error loading {f}: {e}")
    return result


def validate_klines_vs_aggtrades(df_1s: pd.DataFrame, df_klines: pd.DataFrame) -> dict:
    """
    Validation croisée aggTrades vs klines 1m.
    Vérifie que les close prices matchent à ±0.01%.
    """
    if df_1s.empty or df_klines.empty:
        return {"status": "skipped", "reason": "missing data"}

    # Aggregate 1s to 1m for comparison
    df_1s_copy = df_1s.copy()
    df_1s_copy["minute"] = (df_1s_copy["timestamp_s"] // 60) * 60

    agg_1m = df_1s_copy.groupby("minute").agg(
        close=("close", "last"),
        volume=("volume", "sum"),
    ).reset_index()

    # Merge with klines
    if "open_time" in df_klines.columns:
        df_klines = df_klines.copy()
        df_klines["minute"] = (df_klines["open_time"] // 1000).astype("int64")

    merged = agg_1m.merge(df_klines[["minute", "close", "volume"]],
                           on="minute", suffixes=("_agg", "_kline"), how="inner")

    if len(merged) == 0:
        return {"status": "no_overlap", "matched": 0}

    # Price match
    price_diff = (merged["close_agg"] - merged["close_kline"]).abs()
    price_match_pct = (price_diff / merged["close_kline"]).mean() * 100

    # Volume match (less strict — aggregation rounding)
    vol_diff = (merged["volume_agg"] - merged["volume_kline"]).abs()
    vol_match_pct = (vol_diff / merged["volume_kline"].replace(0, 1)).mean() * 100

    result = {
        "status": "ok" if price_match_pct < 0.01 else "warning",
        "matched_minutes": len(merged),
        "price_diff_mean_pct": round(price_match_pct, 6),
        "volume_diff_mean_pct": round(vol_match_pct, 4),
    }

    if price_match_pct > 0.01:
        logger.warning(f"Price mismatch: {price_match_pct:.4f}%")
    else:
        logger.info(f"Validation OK: {len(merged)} minutes matched, price diff {price_match_pct:.6f}%")

    return result

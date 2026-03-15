"""
Feature Engineer
Calcule toutes les features dérivées à partir des données brutes.
Inclut : features Binance rolling, features blockchain (CDD, whale, taproot),
features UTXO (HODL waves, liveliness), features mempool, features temporelles.

Toutes les features sont alignées sur la résolution 1 seconde.
Les sources lentes (blockchain, UTXO, Glassnode) sont forward-fillées.
"""

import numpy as np
import pandas as pd
from loguru import logger


# ═══════════════════════════════════════════════════════════════════════════
# TEMPORAL FEATURES
# ═══════════════════════════════════════════════════════════════════════════

def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajoute des features temporelles cycliques.
    timestamp_s → sin/cos encodings pour heure, jour de la semaine, mois.
    """
    result = df.copy()

    dt = pd.to_datetime(result["timestamp_s"], unit="s", utc=True)

    # Hour of day (0-23) → sin/cos
    hour = dt.dt.hour + dt.dt.minute / 60.0
    result["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    result["hour_cos"] = np.cos(2 * np.pi * hour / 24)

    # Day of week (0=Monday, 6=Sunday) → sin/cos
    dow = dt.dt.dayofweek
    result["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    result["dow_cos"] = np.cos(2 * np.pi * dow / 7)

    # Month (1-12) → sin/cos
    month = dt.dt.month
    result["month_sin"] = np.sin(2 * np.pi * month / 12)
    result["month_cos"] = np.cos(2 * np.pi * month / 12)

    # Day of month → sin/cos
    dom = dt.dt.day
    result["dom_sin"] = np.sin(2 * np.pi * dom / 31)
    result["dom_cos"] = np.cos(2 * np.pi * dom / 31)

    # Is weekend
    result["is_weekend"] = (dow >= 5).astype("float32")

    # Distance to UTC midnight (seconds)
    result["seconds_since_midnight"] = (dt.dt.hour * 3600 + dt.dt.minute * 60 + dt.dt.second).astype("int32")

    return result


# ═══════════════════════════════════════════════════════════════════════════
# BLOCKCHAIN FEATURES (forward-filled from ~10min resolution)
# ═══════════════════════════════════════════════════════════════════════════

def merge_block_features(df_1s: pd.DataFrame, df_blocks: pd.DataFrame) -> pd.DataFrame:
    """
    Fusionne les métriques de blocs sur le DataFrame 1s.
    Forward-fill : chaque seconde hérite des métriques du dernier bloc miné.
    """
    if df_blocks.empty:
        logger.warning("No block data to merge")
        return df_1s

    # Select key block features
    block_features = [
        "block_timestamp", "tx_count", "total_fees_btc",
        "fee_rate_p50_sat_vbyte", "fee_rate_p90_sat_vbyte",
        "segwit_tx_ratio", "taproot_tx_ratio",
        "utxo_net_change", "seconds_since_prev_block",
        "difficulty", "halving_epoch", "blocks_since_halving",
        # v2: removed — coin_days_destroyed_block (toujours 0, nécessite transactions)
    ]

    # Filter available columns
    available = [c for c in block_features if c in df_blocks.columns]
    blocks = df_blocks[available].copy()
    blocks = blocks.rename(columns={"block_timestamp": "timestamp_s"})

    # Prefix block columns
    rename_map = {c: f"block_{c}" if not c.startswith("block_") and c != "timestamp_s" else c
                  for c in blocks.columns}
    blocks = blocks.rename(columns=rename_map)

    # Merge asof (forward-fill)
    df_1s = df_1s.sort_values("timestamp_s")
    blocks = blocks.sort_values("timestamp_s")

    result = pd.merge_asof(
        df_1s, blocks, on="timestamp_s", direction="backward"
    )

    # Whale / retail ratios from block data
    if "tx_count_above_100btc" in df_blocks.columns:
        whale_blocks = df_blocks[["block_timestamp", "tx_count_above_100btc", "tx_count",
                                   "volume_above_100btc", "total_output_btc"]].copy()
        whale_blocks = whale_blocks.rename(columns={"block_timestamp": "timestamp_s"})
        whale_blocks["block_whale_tx_ratio"] = (
            whale_blocks["tx_count_above_100btc"] / whale_blocks["tx_count"].replace(0, 1)
        )
        whale_blocks["block_whale_volume_ratio"] = (
            whale_blocks["volume_above_100btc"] / whale_blocks["total_output_btc"].replace(0, 1)
        )
        whale_blocks = whale_blocks[["timestamp_s", "block_whale_tx_ratio", "block_whale_volume_ratio"]]
        whale_blocks = whale_blocks.sort_values("timestamp_s")
        result = pd.merge_asof(result, whale_blocks, on="timestamp_s", direction="backward")

    return result


# v2: removed — merge_utxo_features (UTXO snapshots supprimés, utilité < 3/10 pour horizons 30s-5min)


# ═══════════════════════════════════════════════════════════════════════════
# MEMPOOL FEATURES (forward-filled from ~60s resolution)
# ═══════════════════════════════════════════════════════════════════════════

def merge_mempool_features(df_1s: pd.DataFrame, df_mempool: pd.DataFrame) -> pd.DataFrame:
    """Fusionne les métriques mempool sur le DataFrame 1s."""
    if df_mempool.empty:
        logger.warning("No mempool data to merge")
        return df_1s

    # v2: updated column names to match renamed mempool fee columns
    fee_cols = []
    for c in ["fee_standard_sat_vbyte", "fee_urgent_sat_vbyte",
              "fee_p50_sat_vbyte", "fee_p90_sat_vbyte"]:
        if c in df_mempool.columns:
            fee_cols.append(c)
    base_cols = ["timestamp", "mempool_tx_count", "mempool_vsize_bytes"]
    available_cols = [c for c in base_cols + fee_cols if c in df_mempool.columns]
    mempool = df_mempool[available_cols].copy()
    mempool = mempool.rename(columns={"timestamp": "timestamp_s"})

    # Prefix
    rename_map = {c: f"mempool_{c}" if not c.startswith("mempool_") and c != "timestamp_s" else c
                  for c in mempool.columns}
    mempool = mempool.rename(columns=rename_map)
    mempool = mempool.sort_values("timestamp_s")

    result = pd.merge_asof(
        df_1s.sort_values("timestamp_s"),
        mempool,
        on="timestamp_s",
        direction="backward",
    )

    return result


# ═══════════════════════════════════════════════════════════════════════════
# GLASSNODE FEATURES (forward-filled from daily resolution)
# ═══════════════════════════════════════════════════════════════════════════

def merge_glassnode_features(df_1s: pd.DataFrame, glassnode_dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Fusionne les métriques Glassnode quotidiennes sur le DataFrame 1s.
    glassnode_dfs : dict de {metric_name: DataFrame}
    """
    result = df_1s.copy()

    for metric_name, df_metric in glassnode_dfs.items():
        if df_metric.empty:
            continue

        metric_df = df_metric.copy()

        # Normalize column names
        if "timestamp" in metric_df.columns:
            metric_df = metric_df.rename(columns={"timestamp": "timestamp_s"})
        if "value" in metric_df.columns:
            metric_df = metric_df.rename(columns={"value": f"gn_{metric_name}"})

        cols = ["timestamp_s", f"gn_{metric_name}"]
        available = [c for c in cols if c in metric_df.columns]
        if len(available) < 2:
            continue

        metric_df = metric_df[available].sort_values("timestamp_s")

        result = pd.merge_asof(
            result.sort_values("timestamp_s"),
            metric_df,
            on="timestamp_s",
            direction="backward",
        )

    return result


# ═══════════════════════════════════════════════════════════════════════════
# FUTURES FEATURES
# ═══════════════════════════════════════════════════════════════════════════

def merge_futures_features(df_1s: pd.DataFrame, df_funding: pd.DataFrame,
                           df_futures_aggtrades: pd.DataFrame = None) -> pd.DataFrame:
    """Fusionne les données futures (funding rate, basis, etc.) sur le DataFrame 1s."""
    result = df_1s.copy()

    if not df_funding.empty:
        funding = df_funding.copy()
        # Funding rate is reported every 8 hours
        if "calc_time" in funding.columns:
            funding["timestamp_s"] = (funding["calc_time"] // 1000).astype("int64")
        elif "timestamp" in funding.columns:
            funding["timestamp_s"] = funding["timestamp"]

        if "last_funding_rate" in funding.columns:
            funding = funding.rename(columns={"last_funding_rate": "funding_rate"})

        if "mark_price" in funding.columns:
            funding = funding.rename(columns={"mark_price": "futures_mark_price"})

        cols = ["timestamp_s"]
        for c in ["funding_rate", "futures_mark_price"]:
            if c in funding.columns:
                cols.append(c)

        funding = funding[cols].sort_values("timestamp_s")

        result = pd.merge_asof(
            result.sort_values("timestamp_s"),
            funding,
            on="timestamp_s",
            direction="backward",
        )

        # Basis = (futures_price - spot_price) / spot_price
        if "futures_mark_price" in result.columns and "close" in result.columns:
            result["basis"] = (
                (result["futures_mark_price"] - result["close"]) /
                result["close"].replace(0, np.nan)
            ).fillna(0)

    return result


# ═══════════════════════════════════════════════════════════════════════════
# MASTER FEATURE PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def build_features(
    df_1s: pd.DataFrame,
    df_blocks: pd.DataFrame = None,
    df_mempool: pd.DataFrame = None,
    df_funding: pd.DataFrame = None,
    glassnode_dfs: dict = None,
) -> pd.DataFrame:
    """
    Pipeline complet de feature engineering.
    Prend le DataFrame 1s (aggTrades agrégés) et fusionne toutes les sources.

    v2: paramètre df_utxo supprimé (UTXO snapshots retirés du pipeline).
    """
    logger.info(f"Building features for {len(df_1s):,} timesteps")

    result = df_1s.copy()

    # 1. Temporal features
    result = add_temporal_features(result)
    logger.info(f"  ✓ Temporal features added")

    # 2. Block features
    if df_blocks is not None and not df_blocks.empty:
        result = merge_block_features(result, df_blocks)
        logger.info(f"  ✓ Block features merged")

    # v2: removed — UTXO features merge (utilité < 3/10 pour horizons 30s-5min)

    # 3. Mempool features
    if df_mempool is not None and not df_mempool.empty:
        result = merge_mempool_features(result, df_mempool)
        logger.info(f"  ✓ Mempool features merged")

    # 5. Futures features
    if df_funding is not None and not df_funding.empty:
        result = merge_futures_features(result, df_funding)
        logger.info(f"  ✓ Futures features merged")

    # 6. Glassnode daily metrics
    if glassnode_dfs:
        result = merge_glassnode_features(result, glassnode_dfs)
        logger.info(f"  ✓ Glassnode features merged")

    # 7. Forward-fill all remaining NaNs (from merge_asof)
    result = result.ffill()

    # Count features and NaN ratio
    n_features = len([c for c in result.columns if c != "timestamp_s"])
    nan_ratio = result.isna().sum().sum() / (len(result) * len(result.columns))
    logger.info(f"  Total features: {n_features}, NaN ratio: {nan_ratio:.4%}")

    return result

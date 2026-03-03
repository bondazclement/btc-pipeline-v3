"""
Bucket Aggregator
Convertit les aggTrades bruts en time buckets de 1 seconde.
C'est le cœur de la résolution temporelle du dataset.

Pour chaque seconde :
  - OHLCV (price, volume)
  - Buy ratio (proportion de trades buyer-maker)
  - VWAP
  - Trade count
  - Volume imbalance
"""

import numpy as np
import pandas as pd
from loguru import logger


def aggregate_aggtrades_to_1s(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège les aggTrades en buckets de 1 seconde.

    Input columns :
      timestamp (ms), price, quantity, is_buyer_maker

    Output columns :
      timestamp_s, open, high, low, close, volume, quote_volume,
      buy_volume, sell_volume, buy_ratio, trade_count, vwap,
      volume_imbalance, price_range, price_return
    """
    if df.empty:
        return pd.DataFrame()

    # Timestamp en secondes (floor)
    df = df.copy()
    df["timestamp_s"] = (df["timestamp"] // 1000).astype("int64")

    # Pre-compute
    df["quote_amount"] = df["price"] * df["quantity"]
    df["is_buy"] = ~df["is_buyer_maker"]  # is_buyer_maker=True means seller initiated

    grouped = df.groupby("timestamp_s")

    result = pd.DataFrame({
        "timestamp_s": grouped["timestamp_s"].first(),
        "open": grouped["price"].first(),
        "high": grouped["price"].max(),
        "low": grouped["price"].min(),
        "close": grouped["price"].last(),
        "volume": grouped["quantity"].sum(),
        "quote_volume": grouped["quote_amount"].sum(),
        "trade_count": grouped["price"].count(),
    })

    # Buy/sell volume
    buy_vol = df[df["is_buy"]].groupby("timestamp_s")["quantity"].sum()
    sell_vol = df[~df["is_buy"]].groupby("timestamp_s")["quantity"].sum()

    result["buy_volume"] = buy_vol.reindex(result.index, fill_value=0.0)
    result["sell_volume"] = sell_vol.reindex(result.index, fill_value=0.0)

    # Derived
    result["buy_ratio"] = result["buy_volume"] / result["volume"].replace(0, np.nan)
    result["buy_ratio"] = result["buy_ratio"].fillna(0.5)

    result["vwap"] = result["quote_volume"] / result["volume"].replace(0, np.nan)
    result["vwap"] = result["vwap"].fillna(result["close"])

    result["volume_imbalance"] = (result["buy_volume"] - result["sell_volume"]) / result["volume"].replace(0, np.nan)
    result["volume_imbalance"] = result["volume_imbalance"].fillna(0.0)

    result["price_range"] = result["high"] - result["low"]
    result["price_return"] = result["close"].pct_change().fillna(0.0)

    result = result.reset_index(drop=True)
    result = result.sort_values("timestamp_s").reset_index(drop=True)

    return result


def fill_missing_seconds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remplit les secondes manquantes (quand il n'y a pas de trades).
    Forward-fill du close price, volume=0, buy_ratio=0.5.
    """
    if df.empty:
        return df

    ts_min = df["timestamp_s"].min()
    ts_max = df["timestamp_s"].max()

    full_range = pd.DataFrame({
        "timestamp_s": np.arange(ts_min, ts_max + 1, dtype="int64")
    })

    merged = full_range.merge(df, on="timestamp_s", how="left")

    # Forward-fill prices
    for col in ["open", "high", "low", "close", "vwap"]:
        if col in merged.columns:
            merged[col] = merged[col].ffill()

    # Fill volume-related with 0
    for col in ["volume", "quote_volume", "buy_volume", "sell_volume",
                "trade_count", "price_range"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)

    # Fill ratios with neutral
    if "buy_ratio" in merged.columns:
        merged["buy_ratio"] = merged["buy_ratio"].fillna(0.5)
    if "volume_imbalance" in merged.columns:
        merged["volume_imbalance"] = merged["volume_imbalance"].fillna(0.0)
    if "price_return" in merged.columns:
        merged["price_return"] = merged["price_return"].fillna(0.0)

    return merged


def process_aggtrades_month(df_aggtrades: pd.DataFrame, fill_gaps: bool = True) -> pd.DataFrame:
    """
    Pipeline complet pour un mois d'aggTrades :
    1. Agrège en buckets 1s
    2. Remplit les secondes manquantes
    3. Retourne le DataFrame prêt pour le feature engineering

    Input: raw aggTrades DataFrame (from Parquet)
    Output: 1-second OHLCV + buy_ratio + vwap DataFrame
    """
    logger.info(f"Aggregating {len(df_aggtrades):,} aggTrades to 1s buckets")

    buckets = aggregate_aggtrades_to_1s(df_aggtrades)
    logger.info(f"  → {len(buckets):,} unique seconds with trades")

    if fill_gaps:
        buckets = fill_missing_seconds(buckets)
        logger.info(f"  → {len(buckets):,} seconds after gap-fill")

    return buckets


def compute_rolling_features(df: pd.DataFrame, windows: list[int] = None) -> pd.DataFrame:
    """
    Ajoute des features rolling sur les buckets 1s.

    Windows en secondes : 30s, 60s, 300s (5min), 900s (15min), 3600s (1h)
    """
    if windows is None:
        windows = [30, 60, 300, 900, 3600]

    result = df.copy()

    for w in windows:
        suffix = f"_{w}s"

        # Rolling volume
        result[f"volume_sum{suffix}"] = result["volume"].rolling(w, min_periods=1).sum()
        result[f"quote_volume_sum{suffix}"] = result["quote_volume"].rolling(w, min_periods=1).sum()

        # Rolling buy ratio
        buy_sum = result["buy_volume"].rolling(w, min_periods=1).sum()
        vol_sum = result["volume"].rolling(w, min_periods=1).sum()
        result[f"buy_ratio{suffix}"] = (buy_sum / vol_sum.replace(0, np.nan)).fillna(0.5)

        # Price volatility (realized vol as std of returns)
        result[f"volatility{suffix}"] = result["price_return"].rolling(w, min_periods=2).std().fillna(0)

        # VWAP rolling
        result[f"vwap{suffix}"] = (
            result["quote_volume"].rolling(w, min_periods=1).sum() /
            result["volume"].rolling(w, min_periods=1).sum().replace(0, np.nan)
        ).fillna(result["close"])

        # Trade count rolling
        result[f"trade_count{suffix}"] = result["trade_count"].rolling(w, min_periods=1).sum()

        # Volume imbalance rolling
        buy_roll = result["buy_volume"].rolling(w, min_periods=1).sum()
        sell_roll = result["sell_volume"].rolling(w, min_periods=1).sum()
        total_roll = buy_roll + sell_roll
        result[f"volume_imbalance{suffix}"] = ((buy_roll - sell_roll) / total_roll.replace(0, np.nan)).fillna(0)

        # Price momentum (return over window)
        result[f"return{suffix}"] = result["close"].pct_change(w).fillna(0)

        # High-low range over window
        result[f"range{suffix}"] = (
            result["high"].rolling(w, min_periods=1).max() -
            result["low"].rolling(w, min_periods=1).min()
        )

    return result

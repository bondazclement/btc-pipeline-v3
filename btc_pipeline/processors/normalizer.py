"""
Normalizer
Rolling z-score normalization pour les features.

Règles :
  - Features trading (prix, volume, etc.) : fenêtre 72h (259200 secondes)
  - Features on-chain journalières (Glassnode, UTXO) : fenêtre 365 jours
  - Features cycliques (sin/cos) : pas de normalisation
  - Features booléennes : pas de normalisation

Le rolling z-score est CAUSAL : il n'utilise que des données passées.
z(t) = (x(t) - mean(x[t-W:t])) / std(x[t-W:t])
"""

import pickle
import json
import numpy as np
import pandas as pd
from loguru import logger

from btc_pipeline.storage.gcs_client import StorageClient


# ═══════════════════════════════════════════════════════════════════════════
# FEATURE CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════

# Columns that should NOT be normalized
NO_NORMALIZE = {
    "timestamp_s",
    # Cyclical (already bounded)
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "month_sin", "month_cos", "dom_sin", "dom_cos",
    # Boolean / categorical
    "is_weekend", "halving_epoch",
    # Ratios already in [0,1]
    "buy_ratio", "segwit_tx_ratio", "taproot_tx_ratio",
    "block_whale_tx_ratio", "block_whale_volume_ratio",
    "utxo_btc_age_below_7d_ratio", "utxo_btc_age_above_1y_ratio",
    "utxo_btc_age_above_2y_ratio",
}

# Daily-resolution features (use 365-day window)
DAILY_FEATURES = {
    "gn_active_addresses", "gn_new_addresses", "gn_transaction_count",
    "gn_transfer_volume", "gn_exchange_inflow", "gn_exchange_outflow",
    "gn_sopr", "gn_hashrate_mean", "gn_difficulty", "gn_supply_last_active_1y",
    "utxo_liveliness", "utxo_dormancy_7d", "utxo_realized_cap_usd",
    "utxo_total_utxo_count", "utxo_total_btc_in_utxos",
}


def rolling_zscore(series: pd.Series, window: int, min_periods: int = None) -> pd.Series:
    """
    Compute causal rolling z-score.
    z(t) = (x(t) - rolling_mean) / rolling_std
    """
    if min_periods is None:
        min_periods = max(1, window // 10)

    roll_mean = series.rolling(window, min_periods=min_periods).mean()
    roll_std = series.rolling(window, min_periods=min_periods).std()

    # Avoid division by zero
    roll_std = roll_std.replace(0, np.nan)

    z = (series - roll_mean) / roll_std

    # Clip extreme values (>6 sigma is likely an error)
    z = z.clip(-6, 6)

    # Fill NaN (beginning of series) with 0
    z = z.fillna(0)

    return z


def normalize_features(df: pd.DataFrame, window_fast: int = 259200,
                       window_slow: int = 31536000) -> pd.DataFrame:
    """
    Applique le rolling z-score à toutes les features numériques.

    Args:
        window_fast: fenêtre pour les features haute fréquence (72h = 259200s)
        window_slow: fenêtre pour les features journalières (365j ≈ 31.5M secondes,
                     mais puisque les données sont à 1s, on utilise le nombre de
                     points effectifs)
    """
    result = df.copy()
    normalized_count = 0

    for col in result.columns:
        if col in NO_NORMALIZE:
            continue
        if not pd.api.types.is_numeric_dtype(result[col]):
            continue

        # Choose window based on feature type
        if col in DAILY_FEATURES:
            # Daily features : ~365 data points at 1/day resolution
            # But in 1s DataFrame they're forward-filled
            # Use a large window to smooth
            window = min(window_slow, len(result))
        else:
            window = min(window_fast, len(result))

        result[col] = rolling_zscore(result[col], window)
        normalized_count += 1

    logger.info(f"Normalized {normalized_count} features (skipped {len(result.columns) - normalized_count})")
    return result


# ═══════════════════════════════════════════════════════════════════════════
# SCALER STATE (for inference)
# ═══════════════════════════════════════════════════════════════════════════

def compute_feature_stats(df: pd.DataFrame) -> dict:
    """
    Calcule les statistiques globales de chaque feature.
    Utile pour l'inférence en live (on peut appliquer un z-score global
    quand le rolling n'est pas encore suffisant).
    """
    stats = {}
    for col in df.columns:
        if col == "timestamp_s":
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            stats[col] = {
                "mean": float(df[col].mean()),
                "std": float(df[col].std()),
                "min": float(df[col].min()),
                "max": float(df[col].max()),
                "median": float(df[col].median()),
                "q01": float(df[col].quantile(0.01)),
                "q99": float(df[col].quantile(0.99)),
                "nan_ratio": float(df[col].isna().mean()),
            }
    return stats


def save_scaler_state(storage: StorageClient, stats: dict, year: int):
    """Sauvegarde les statistiques des features sur GCS."""
    stats_json = json.dumps(stats, indent=2).encode("utf-8")
    storage.stream_upload_bytes(stats_json, f"scalers/feature_stats_{year}.json", skip_if_exists=False)

    # Also save as pickle for convenience
    import pickle
    import io
    buf = io.BytesIO()
    pickle.dump(stats, buf)
    buf.seek(0)
    storage.stream_upload_bytes(buf.getvalue(), f"scalers/feature_stats_{year}.pkl", skip_if_exists=False)

    logger.info(f"Scaler state saved for year {year}")

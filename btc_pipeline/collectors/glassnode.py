"""
Glassnode + BGeometrics On-Chain Metrics Collector
Métriques journalières : SOPR, active addresses, exchange flows, hashrate, MVRV Z-Score.

Sources :
  - Glassnode free tier API (résolution journalière)
  - BGeometrics free API (MVRV Z-Score, Puell Multiple)
"""

import time
from datetime import datetime, timezone
from typing import Optional

import requests
import pandas as pd
from loguru import logger

from btc_pipeline.config import Config, GCS_PATHS
from btc_pipeline.storage.gcs_client import StorageClient


# ═══════════════════════════════════════════════════════════════════════════
# GLASSNODE FREE TIER METRICS
# ═══════════════════════════════════════════════════════════════════════════

GLASSNODE_METRICS = {
    # metric_path: (endpoint, resolution, since)
    "active_addresses": ("/v1/metrics/addresses/active_count", "24h", "2009-01-03"),
    "new_addresses": ("/v1/metrics/addresses/new_non_zero_count", "24h", "2009-01-03"),
    "transaction_count": ("/v1/metrics/transactions/count", "24h", "2009-01-03"),
    "transfer_volume": ("/v1/metrics/transactions/transfers_volume_sum", "24h", "2009-01-03"),
    "exchange_inflow": ("/v1/metrics/transactions/transfers_volume_to_exchanges_sum", "24h", "2012-01-01"),
    "exchange_outflow": ("/v1/metrics/transactions/transfers_volume_from_exchanges_sum", "24h", "2012-01-01"),
    "sopr": ("/v1/metrics/indicators/sopr", "24h", "2010-01-01"),
    "hashrate_mean": ("/v1/metrics/mining/hash_rate_mean", "24h", "2009-01-03"),
    "difficulty": ("/v1/metrics/mining/difficulty_latest", "24h", "2009-01-03"),
    "supply_last_active_1y": ("/v1/metrics/supply/active_more_1y_percent", "24h", "2010-07-01"),
}


def fetch_glassnode_metric(
    metric_name: str,
    api_key: str,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> pd.DataFrame:
    """Récupère une métrique Glassnode depuis l'API free tier."""
    if metric_name not in GLASSNODE_METRICS:
        raise ValueError(f"Unknown Glassnode metric: {metric_name}")

    endpoint, resolution, default_since = GLASSNODE_METRICS[metric_name]
    since = since or default_since
    base = "https://api.glassnode.com"

    params = {
        "a": "BTC",
        "api_key": api_key,
        "s": int(datetime.strptime(since, "%Y-%m-%d").timestamp()),
        "i": resolution,
    }
    if until:
        params["u"] = int(datetime.strptime(until, "%Y-%m-%d").timestamp())

    try:
        resp = requests.get(f"{base}{endpoint}", params=params, timeout=30)
        if resp.status_code == 429:
            logger.warning("Glassnode rate limit — waiting 60s")
            time.sleep(60)
            resp = requests.get(f"{base}{endpoint}", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"Glassnode API error for {metric_name}: {e}")
        return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    if "t" in df.columns:
        df = df.rename(columns={"t": "timestamp", "v": "value"})
    elif "timestamp" not in df.columns and len(df.columns) == 2:
        df.columns = ["timestamp", "value"]

    df["metric"] = metric_name
    return df


def run_glassnode_collection(
    storage: StorageClient,
    config: Optional[Config] = None,
    metrics: Optional[list[str]] = None,
    since: Optional[str] = None,
    progress_callback=None,
) -> dict:
    """
    Collecte toutes les métriques Glassnode disponibles en free tier.
    Upload chaque métrique par année sur GCS.
    """
    config = config or Config()
    api_key = config.glassnode_api_key

    if not api_key:
        logger.warning("No Glassnode API key — skipping collection")
        return {"status": "no_api_key", "metrics_collected": 0}

    metrics = metrics or list(GLASSNODE_METRICS.keys())
    stats = {"metrics_collected": 0, "total_rows": 0, "total_bytes": 0}

    for i, metric_name in enumerate(metrics):
        logger.info(f"Fetching Glassnode: {metric_name} ({i+1}/{len(metrics)})")

        df = fetch_glassnode_metric(metric_name, api_key, since=since)

        if df.empty:
            logger.warning(f"Empty data for {metric_name}")
            continue

        # Upload by year
        if "timestamp" in df.columns:
            df["year"] = pd.to_datetime(df["timestamp"], unit="s").dt.year

            for year, year_df in df.groupby("year"):
                gcs_path = GCS_PATHS["glassnode"].format(metric=metric_name, year=year)
                size = storage.stream_upload_parquet(
                    year_df.drop(columns=["year"]), gcs_path, skip_if_exists=False
                )
                stats["total_bytes"] += size

        stats["metrics_collected"] += 1
        stats["total_rows"] += len(df)

        if progress_callback:
            progress_callback(i + 1, len(metrics), {"metric": metric_name, "rows": len(df)})

        # Rate limiting — Glassnode free tier is limited
        time.sleep(2)

    stats["total_gb"] = stats["total_bytes"] / 1e9
    logger.info(
        f"✅ Glassnode done: {stats['metrics_collected']} metrics, "
        f"{stats['total_rows']:,} rows, {stats['total_gb']:.4f} GB"
    )

    storage.update_task_state("glassnode", "metrics_collected", stats["metrics_collected"])
    storage.update_task_state("glassnode", "last_run", datetime.now(timezone.utc).isoformat())

    return stats


# ═══════════════════════════════════════════════════════════════════════════
# BGEOMETRICS — MVRV Z-Score, Puell Multiple
# ═══════════════════════════════════════════════════════════════════════════

BGEOMETRICS_METRICS = {
    "mvrv_zscore": "https://bitcoin-data.com/v1/mvrv-zscore",
    "puell_multiple": "https://bitcoin-data.com/v1/puell-multiple",
    "pi_cycle_top": "https://bitcoin-data.com/v1/pi-cycle-top",
    "golden_ratio": "https://bitcoin-data.com/v1/golden-ratio-multiplier",
}


def fetch_bgeometrics_metric(metric_name: str) -> pd.DataFrame:
    """Récupère une métrique depuis BGeometrics/bitcoin-data.com."""
    if metric_name not in BGEOMETRICS_METRICS:
        raise ValueError(f"Unknown BGeometrics metric: {metric_name}")

    url = BGEOMETRICS_METRICS[metric_name]

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"BGeometrics error for {metric_name}: {e}")
        return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["metric"] = metric_name
    return df


def run_bgeometrics_collection(
    storage: StorageClient,
    metrics: Optional[list[str]] = None,
    progress_callback=None,
) -> dict:
    """Collecte toutes les métriques BGeometrics."""
    metrics = metrics or list(BGEOMETRICS_METRICS.keys())
    stats = {"metrics_collected": 0, "total_rows": 0, "total_bytes": 0}

    for i, metric_name in enumerate(metrics):
        logger.info(f"Fetching BGeometrics: {metric_name} ({i+1}/{len(metrics)})")

        df = fetch_bgeometrics_metric(metric_name)
        if df.empty:
            continue

        # Try to extract year from date column
        date_col = None
        for c in df.columns:
            if "date" in c.lower() or "time" in c.lower() or c == "d":
                date_col = c
                break

        if date_col:
            df["_date"] = pd.to_datetime(df[date_col], errors="coerce")
            df["year"] = df["_date"].dt.year

            for year, year_df in df.groupby("year"):
                if pd.isna(year):
                    continue
                gcs_path = GCS_PATHS["bgeometrics"].format(metric=metric_name, year=int(year))
                year_df_clean = year_df.drop(columns=["_date", "year"], errors="ignore")
                size = storage.stream_upload_parquet(year_df_clean, gcs_path, skip_if_exists=False)
                stats["total_bytes"] += size
        else:
            gcs_path = f"raw/onchain_metrics/bgeometrics/metric={metric_name}/all.parquet"
            size = storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)
            stats["total_bytes"] += size

        stats["metrics_collected"] += 1
        stats["total_rows"] += len(df)

        if progress_callback:
            progress_callback(i + 1, len(metrics), {"metric": metric_name})

        time.sleep(1)

    stats["total_gb"] = stats["total_bytes"] / 1e9
    logger.info(f"✅ BGeometrics done: {stats['metrics_collected']} metrics, {stats['total_rows']:,} rows")

    storage.update_task_state("bgeometrics", "metrics_collected", stats["metrics_collected"])
    return stats

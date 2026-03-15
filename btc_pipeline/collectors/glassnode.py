"""
Glassnode On-Chain Metrics Collector
Métriques journalières : SOPR, active addresses, exchange flows, hashrate.

v2: BGeometrics supprimé (utilité < 3/10 pour horizons 30s-5min).

Source :
  - Glassnode free tier API (résolution journalière)
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
    storage.flush_state()  # v2: persist state after collection

    return stats


# v2: removed — BGEOMETRICS_METRICS, fetch_bgeometrics_metric, run_bgeometrics_collection
#     (utilité < 3/10 pour horizons 30s-5min)

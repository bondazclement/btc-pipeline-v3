"""
Blockchair Data Collector
Télécharge les dumps TSV pré-parsés depuis gz.blockchair.com/bitcoin.
Alternative au nœud Bitcoin Core local pour les données blocs.

Flux : TSV.gz → décompression streaming → parse → Parquet → GCS → suppression locale.

URLs :
  https://gz.blockchair.com/bitcoin/blocks/blockchair_bitcoin_blocks_{YYYYMMDD}.tsv.gz
  https://gz.blockchair.com/bitcoin/transactions/blockchair_bitcoin_transactions_{YYYYMMDD}.tsv.gz

Note : Blockchair throttle à ~10 KB/s en free tier.
"""

import io
import gzip
from datetime import date, timedelta
from typing import Optional

import requests
import pandas as pd
from loguru import logger

from btc_pipeline.config import Config, GCS_PATHS
from btc_pipeline.storage.gcs_client import StorageClient


def build_blockchair_url(data_type: str, dt: date) -> str:
    """Construit l'URL Blockchair pour une date donnée."""
    ds = dt.strftime("%Y%m%d")
    return f"https://gz.blockchair.com/bitcoin/{data_type}/blockchair_bitcoin_{data_type}_{ds}.tsv.gz"


def download_blockchair_day(
    storage: StorageClient,
    data_type: str,
    dt: date,
) -> dict:
    """
    Télécharge un jour de données Blockchair (blocks ou transactions).
    Parse le TSV.gz en streaming, convertit en Parquet, upload sur GCS.
    """
    url = build_blockchair_url(data_type, dt)
    gcs_path = f"raw/blockchain/blockchair/{data_type}/{dt.strftime('%Y/%m')}/{dt.strftime('%Y-%m-%d')}.parquet"

    if storage.exists(gcs_path):
        return {"status": "skipped", "rows": 0}

    try:
        resp = requests.get(url, timeout=600, stream=True)
        if resp.status_code == 404:
            return {"status": "not_found", "rows": 0}
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Blockchair download error: {e}")
        return {"status": "error", "error": str(e), "rows": 0}

    # Decompress gzip in memory
    try:
        raw = gzip.decompress(resp.content)
        df = pd.read_csv(io.BytesIO(raw), sep="\t")
    except Exception as e:
        logger.error(f"Parse error for {url}: {e}")
        return {"status": "error", "error": str(e), "rows": 0}

    if len(df) == 0:
        return {"status": "empty", "rows": 0}

    size = storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)

    logger.info(f"✅ Blockchair {data_type} {dt}: {len(df):,} rows, {size/1e6:.1f} MB")
    return {"status": "success", "rows": len(df), "size_bytes": size}


def run_blockchair_blocks(
    storage: StorageClient,
    start_date: str = "2009-01-03",
    end_date: Optional[str] = None,
    progress_callback=None,
) -> dict:
    """Télécharge tous les dumps Blockchair blocks jour par jour."""
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date) if end_date else date.today() - timedelta(days=1)

    state = storage.get_pipeline_state()
    last = state.get("tasks", {}).get("blockchair_blocks", {}).get("last_date")
    if last:
        start = max(start, date.fromisoformat(last) + timedelta(days=1))

    current = start
    stats = {"completed": 0, "failed": 0, "total_bytes": 0}
    total_days = (end - start).days + 1

    logger.info(f"▶ Blockchair blocks: {start} → {end} ({total_days} days)")

    i = 0
    while current <= end:
        result = download_blockchair_day(storage, "blocks", current)
        if result["status"] == "success":
            stats["completed"] += 1
            stats["total_bytes"] += result.get("size_bytes", 0)
            storage.update_task_state("blockchair_blocks", "last_date", current.isoformat())
        elif result["status"] not in ("skipped", "not_found"):
            stats["failed"] += 1

        i += 1
        if progress_callback:
            progress_callback(i, total_days, {"date": current.isoformat()})

        current += timedelta(days=1)

    stats["total_gb"] = stats["total_bytes"] / 1e9
    logger.info(f"✅ Blockchair blocks done: {stats['completed']} days, {stats['total_gb']:.2f} GB")
    return stats


def run_blockchair_transactions(
    storage: StorageClient,
    start_date: str = "2009-01-03",
    end_date: Optional[str] = None,
    progress_callback=None,
) -> dict:
    """Télécharge tous les dumps Blockchair transactions jour par jour."""
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date) if end_date else date.today() - timedelta(days=1)

    state = storage.get_pipeline_state()
    last = state.get("tasks", {}).get("blockchair_transactions", {}).get("last_date")
    if last:
        start = max(start, date.fromisoformat(last) + timedelta(days=1))

    current = start
    stats = {"completed": 0, "failed": 0, "total_bytes": 0}
    total_days = (end - start).days + 1

    logger.info(f"▶ Blockchair transactions: {start} → {end} ({total_days} days)")

    i = 0
    while current <= end:
        result = download_blockchair_day(storage, "transactions", current)
        if result["status"] == "success":
            stats["completed"] += 1
            stats["total_bytes"] += result.get("size_bytes", 0)
            storage.update_task_state("blockchair_transactions", "last_date", current.isoformat())
        elif result["status"] not in ("skipped", "not_found"):
            stats["failed"] += 1

        i += 1
        if progress_callback:
            progress_callback(i, total_days, {"date": current.isoformat()})

        current += timedelta(days=1)

    stats["total_gb"] = stats["total_bytes"] / 1e9
    logger.info(f"✅ Blockchair transactions done: {stats['completed']} days, {stats['total_gb']:.2f} GB")
    return stats

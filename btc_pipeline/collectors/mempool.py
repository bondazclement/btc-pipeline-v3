"""
Mempool.space Data Collector
API REST pour historique + WebSocket pour données live.

Métriques collectées :
  - Mempool tx count, vsize, total fees
  - Fee rates par niveau de confirmation (pas de vrais percentiles)
  - Block estimations

v2: les colonnes de frais sont renommées pour refléter leur vraie nature
    (recommandations de délai de confirmation, pas des percentiles du mempool).

Sources :
  - https://mempool.space/api (REST)
  - wss://mempool.space/api/v1/ws (WebSocket)
"""

import time
import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
import pandas as pd
from loguru import logger

from btc_pipeline.config import Config, GCS_PATHS, MEMPOOL_SCHEMA_DTYPES
from btc_pipeline.storage.gcs_client import StorageClient


# ═══════════════════════════════════════════════════════════════════════════
# REST API — Historical / Snapshot
# ═══════════════════════════════════════════════════════════════════════════

def fetch_mempool_snapshot(config: Optional[Config] = None) -> dict:
    """Récupère l'état actuel du mempool via l'API REST."""
    config = config or Config()
    base = config.mempool_api_base

    try:
        # Mempool stats
        stats_resp = requests.get(f"{base}/mempool", timeout=15)
        stats_resp.raise_for_status()
        stats = stats_resp.json()

        # Recommended fees
        fees_resp = requests.get(f"{base}/v1/fees/recommended", timeout=15)
        fees_resp.raise_for_status()
        fees = fees_resp.json()

        # Fee histogram (for percentiles)
        hist_resp = requests.get(f"{base}/mempool/recent", timeout=15)
        hist_resp.raise_for_status()

        # Build record
        now_ts = int(time.time())

        # Estimate fee percentiles from the histogram if available
        # The mempool API gives us basic fee levels
        # v2: renamed fee columns — these are confirmation delay recommendations,
        # not true statistical percentiles of the mempool fee distribution
        record = {
            "timestamp": now_ts,
            "mempool_tx_count": stats.get("count", 0),
            "mempool_vsize_bytes": stats.get("vsize", 0),
            "mempool_total_fee_sat": int(stats.get("total_fee", 0) * 1e8) if isinstance(stats.get("total_fee"), float) else stats.get("total_fee", 0),
            "fee_minimum_sat_vbyte":  fees.get("minimumFee", 1),      # minimum relay fee
            "fee_economy_sat_vbyte":  fees.get("economyFee", 2),      # ~1h confirmation
            "fee_standard_sat_vbyte": fees.get("hourFee", 5),         # ~1h confirmation
            "fee_priority_sat_vbyte": fees.get("halfHourFee", 10),    # ~30min confirmation
            "fee_urgent_sat_vbyte":   fees.get("fastestFee", 20),     # next block
            "blocks_estimated_1h": 6,   # ~6 blocks/hour average
            "blocks_estimated_6h": 36,
        }

        return record

    except Exception as e:
        logger.error(f"Mempool API error: {e}")
        return {}


def fetch_fee_history_2h(config: Optional[Config] = None) -> list[dict]:
    """Récupère l'historique des frais recommandés sur les 2 dernières heures."""
    config = config or Config()
    # mempool.space provides fee history via blocks
    try:
        resp = requests.get(f"{config.mempool_api_base}/v1/mining/blocks/fee-rates/2h", timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.debug(f"Fee history not available: {e}")
    return []


def run_mempool_collection(
    storage: StorageClient,
    interval_seconds: int = 60,
    duration_hours: float = 24,
    config: Optional[Config] = None,
    progress_callback=None,
) -> dict:
    """
    Collecte des snapshots mempool périodiques pendant duration_hours.
    Upload vers GCS en micro-batches horaires.
    """
    config = config or Config()
    buffer = []
    stats = {"snapshots": 0, "uploads": 0, "total_bytes": 0}

    t_start = time.time()
    t_end = t_start + duration_hours * 3600
    last_hour_flush = None

    logger.info(f"▶ Mempool collection: every {interval_seconds}s for {duration_hours}h")

    while time.time() < t_end:
        snapshot = fetch_mempool_snapshot(config)
        if snapshot:
            buffer.append(snapshot)
            stats["snapshots"] += 1

        # Flush every hour
        now = datetime.now(timezone.utc)
        current_hour = now.strftime("%Y-%m-%d-%H")

        if last_hour_flush != current_hour and buffer:
            df = pd.DataFrame(buffer)
            gcs_path = GCS_PATHS["mempool_snapshots"].format(
                year=now.year, month=now.month, day=now.day
            )
            size = storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)
            stats["uploads"] += 1
            stats["total_bytes"] += size
            buffer.clear()
            last_hour_flush = current_hour

            logger.info(f"Mempool flush: {stats['snapshots']} snapshots, {size/1e3:.1f} KB")

        if progress_callback:
            elapsed = time.time() - t_start
            progress_callback(elapsed, duration_hours * 3600, stats)

        time.sleep(interval_seconds)

    # Final flush
    if buffer:
        now = datetime.now(timezone.utc)
        df = pd.DataFrame(buffer)
        gcs_path = GCS_PATHS["mempool_snapshots"].format(
            year=now.year, month=now.month, day=now.day
        )
        storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)

    logger.info(f"✅ Mempool collection done: {stats['snapshots']} snapshots")
    return stats


# ═══════════════════════════════════════════════════════════════════════════
# WEBSOCKET — Live mempool data
# ═══════════════════════════════════════════════════════════════════════════

async def run_mempool_websocket(
    storage: StorageClient,
    flush_interval: int = 60,
    stop_event: Optional[asyncio.Event] = None,
    config: Optional[Config] = None,
):
    """
    WebSocket live mempool data from mempool.space.
    Subscribes to mempool updates and flushes to GCS.
    """
    import websockets

    config = config or Config()
    url = config.mempool_ws_url
    buffer = []
    last_flush = time.time()
    backoff = 1

    while True:
        if stop_event and stop_event.is_set():
            break

        try:
            async with websockets.connect(url) as ws:
                backoff = 1
                logger.info("✅ Connected to mempool.space WebSocket")

                # Subscribe to mempool updates
                await ws.send(json.dumps({
                    "action": "init",
                    "data": ["stats", "blocks"]
                }))

                while True:
                    if stop_event and stop_event.is_set():
                        break

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=flush_interval)
                        msg = json.loads(raw)

                        # Parse mempool stats from WS messages
                        if "mempoolInfo" in msg:
                            info = msg["mempoolInfo"]
                            # v2: renamed fee columns to reflect confirmation delay recommendations
                            record = {
                                "timestamp": int(time.time()),
                                "mempool_tx_count": info.get("size", 0),
                                "mempool_vsize_bytes": info.get("bytes", 0),
                                "mempool_total_fee_sat": 0,  # not directly available
                                "fee_minimum_sat_vbyte": 0,
                                "fee_economy_sat_vbyte": 0,
                                "fee_standard_sat_vbyte": 0,
                                "fee_priority_sat_vbyte": 0,
                                "fee_urgent_sat_vbyte": 0,
                                "blocks_estimated_1h": 0,
                                "blocks_estimated_6h": 0,
                            }
                            # Get fee rates if available
                            if "fees" in msg:
                                fees = msg["fees"]
                                record["fee_economy_sat_vbyte"] = fees.get("economyFee", 0)
                                record["fee_standard_sat_vbyte"] = fees.get("hourFee", 0)
                                record["fee_priority_sat_vbyte"] = fees.get("halfHourFee", 0)
                                record["fee_urgent_sat_vbyte"] = fees.get("fastestFee", 0)

                            buffer.append(record)

                    except asyncio.TimeoutError:
                        pass

                    # Periodic flush
                    if time.time() - last_flush >= flush_interval and buffer:
                        df = pd.DataFrame(buffer)
                        now = datetime.now(timezone.utc)
                        gcs_path = GCS_PATHS["mempool_snapshots"].format(
                            year=now.year, month=now.month, day=now.day
                        )
                        storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)
                        buffer.clear()
                        last_flush = time.time()

        except Exception as e:
            logger.warning(f"Mempool WS error: {e}. Reconnecting in {backoff}s")
            if buffer:
                try:
                    df = pd.DataFrame(buffer)
                    now = datetime.now(timezone.utc)
                    gcs_path = GCS_PATHS["mempool_snapshots"].format(
                        year=now.year, month=now.month, day=now.day
                    )
                    storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)
                    buffer.clear()
                except Exception:
                    pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

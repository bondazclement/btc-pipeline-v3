"""
Binance WebSocket Live Collector
Daemon continu — flush vers GCS toutes les 60 secondes.
Streams : @aggTrade, @bookTicker, @depth5@100ms, @forceOrder (liquidations)

Usage :
    await run_all_live_streams(storage)
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional

import pandas as pd
import websockets
from loguru import logger

from btc_pipeline.config import Config, GCS_PATHS
from btc_pipeline.storage.gcs_client import StorageClient


# ═══════════════════════════════════════════════════════════════════════════
# STREAM DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════

STREAMS = {
    "aggtrade": {
        "url_suffix": "btcusdt@aggTrade",
        "base_url": "wss://stream.binance.com:9443/ws/",
        "gcs_key": "live_aggtrade",
    },
    "bookticker": {
        "url_suffix": "btcusdt@bookTicker",
        "base_url": "wss://stream.binance.com:9443/ws/",
        "gcs_key": "live_bookticker",
    },
    "depth5": {
        "url_suffix": "btcusdt@depth5@100ms",
        "base_url": "wss://stream.binance.com:9443/ws/",
        "gcs_key": "live_depth5",
    },
    "liquidations": {
        "url_suffix": "btcusdt@forceOrder",
        "base_url": "wss://fstream.binance.com/ws/",
        "gcs_key": "live_liquidations",
    },
}


# ═══════════════════════════════════════════════════════════════════════════
# MESSAGE PARSERS
# ═══════════════════════════════════════════════════════════════════════════

def parse_aggtrade(msg: dict) -> dict:
    return {
        "event_time": msg.get("E", 0),
        "agg_trade_id": msg.get("a", 0),
        "price": float(msg.get("p", 0)),
        "quantity": float(msg.get("q", 0)),
        "first_trade_id": msg.get("f", 0),
        "last_trade_id": msg.get("l", 0),
        "trade_time": msg.get("T", 0),
        "is_buyer_maker": msg.get("m", False),
    }


def parse_bookticker(msg: dict) -> dict:
    return {
        "update_id": msg.get("u", 0),
        "event_time": msg.get("E", 0),
        "best_bid_price": float(msg.get("b", 0)),
        "best_bid_qty": float(msg.get("B", 0)),
        "best_ask_price": float(msg.get("a", 0)),
        "best_ask_qty": float(msg.get("A", 0)),
    }


def parse_depth5(msg: dict) -> dict:
    bids = msg.get("bids", [])
    asks = msg.get("asks", [])
    record = {
        "event_time": msg.get("E", int(time.time() * 1000)),
        "last_update_id": msg.get("lastUpdateId", 0),
    }
    for i in range(5):
        if i < len(bids):
            record[f"bid_{i}_price"] = float(bids[i][0])
            record[f"bid_{i}_qty"] = float(bids[i][1])
        else:
            record[f"bid_{i}_price"] = 0.0
            record[f"bid_{i}_qty"] = 0.0
        if i < len(asks):
            record[f"ask_{i}_price"] = float(asks[i][0])
            record[f"ask_{i}_qty"] = float(asks[i][1])
        else:
            record[f"ask_{i}_price"] = 0.0
            record[f"ask_{i}_qty"] = 0.0
    return record


def parse_liquidation(msg: dict) -> dict:
    order = msg.get("o", {})
    return {
        "event_time": msg.get("E", 0),
        "symbol": order.get("s", ""),
        "side": order.get("S", ""),
        "order_type": order.get("o", ""),
        "time_in_force": order.get("f", ""),
        "quantity": float(order.get("q", 0)),
        "price": float(order.get("p", 0)),
        "avg_price": float(order.get("ap", 0)),
        "order_status": order.get("X", ""),
        "trade_time": order.get("T", 0),
    }


PARSERS = {
    "aggtrade": parse_aggtrade,
    "bookticker": parse_bookticker,
    "depth5": parse_depth5,
    "liquidations": parse_liquidation,
}


# ═══════════════════════════════════════════════════════════════════════════
# BUFFER + FLUSH
# ═══════════════════════════════════════════════════════════════════════════

class StreamBuffer:
    """Buffer RAM pour un stream WebSocket. Flush vers GCS toutes les N secondes."""

    def __init__(self, stream_name: str, storage: StorageClient, flush_interval: int = 60):
        self.stream_name = stream_name
        self.storage = storage
        self.flush_interval = flush_interval
        self.buffer: list[dict] = []
        self.last_flush = time.time()
        self.total_flushed = 0
        self.total_bytes = 0
        self.msg_count = 0

    def add(self, record: dict):
        self.buffer.append(record)
        self.msg_count += 1

    def should_flush(self) -> bool:
        return (time.time() - self.last_flush) >= self.flush_interval and len(self.buffer) > 0

    def flush(self) -> int:
        """Flush le buffer vers GCS. Retourne le nombre de rows flushées."""
        if not self.buffer:
            return 0

        df = pd.DataFrame(self.buffer)
        now = datetime.now(timezone.utc)

        gcs_key = STREAMS[self.stream_name]["gcs_key"]
        gcs_path = GCS_PATHS[gcs_key].format(
            year=now.year, month=now.month, day=now.day, hour=now.hour
        )

        # Pour les fichiers horaires, on append (ou on écrase le dernier)
        # Stratégie : un fichier par heure, on écrase à chaque flush
        # (les données de la même heure sont cumulées dans le buffer)
        size = self.storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)

        rows = len(self.buffer)
        self.total_flushed += rows
        self.total_bytes += size
        self.buffer.clear()
        self.last_flush = time.time()

        logger.debug(
            f"FLUSH {self.stream_name}: {rows} rows, {size/1e3:.1f} KB → {gcs_path}"
        )
        return rows

    def get_stats(self) -> dict:
        return {
            "stream": self.stream_name,
            "buffer_size": len(self.buffer),
            "total_flushed": self.total_flushed,
            "total_bytes": self.total_bytes,
            "msg_count": self.msg_count,
            "msg_per_sec": self.msg_count / max(1, time.time() - self.last_flush),
        }


# ═══════════════════════════════════════════════════════════════════════════
# WEBSOCKET STREAM HANDLER
# ═══════════════════════════════════════════════════════════════════════════

async def run_stream(stream_name: str, storage: StorageClient,
                     flush_interval: int = 60, stop_event: Optional[asyncio.Event] = None):
    """
    Lance un stream WebSocket Binance et flush vers GCS périodiquement.
    Reconnexion automatique avec backoff exponentiel.
    """
    stream_cfg = STREAMS[stream_name]
    url = stream_cfg["base_url"] + stream_cfg["url_suffix"]
    parser = PARSERS[stream_name]
    buffer = StreamBuffer(stream_name, storage, flush_interval)

    backoff = 1
    max_backoff = 60

    while True:
        if stop_event and stop_event.is_set():
            buffer.flush()
            logger.info(f"Stream {stream_name} stopped by event")
            break

        try:
            logger.info(f"Connecting to {stream_name}: {url}")
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1  # Reset backoff on success
                logger.info(f"✅ Connected: {stream_name}")

                while True:
                    if stop_event and stop_event.is_set():
                        break

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=flush_interval)
                        msg = json.loads(raw)
                        record = parser(msg)
                        buffer.add(record)

                    except asyncio.TimeoutError:
                        pass  # Flush on next check

                    # Periodic flush
                    if buffer.should_flush():
                        buffer.flush()

        except (websockets.exceptions.ConnectionClosed, ConnectionError, OSError) as e:
            buffer.flush()  # Flush whatever we have before reconnecting
            logger.warning(f"Stream {stream_name} disconnected: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

        except Exception as e:
            buffer.flush()
            logger.error(f"Stream {stream_name} error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


# ═══════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════

async def run_all_live_streams(storage: StorageClient, flush_interval: int = 60,
                                stop_event: Optional[asyncio.Event] = None):
    """
    Lance les 4 streams WebSocket en parallèle (asyncio.gather).
    Chaque stream a son propre buffer et flush indépendant.
    """
    logger.info("▶ Starting all live WebSocket streams...")

    tasks = [
        asyncio.create_task(
            run_stream(name, storage, flush_interval, stop_event),
            name=f"ws_{name}"
        )
        for name in STREAMS
    ]

    # Monitoring task
    async def monitor():
        while True:
            if stop_event and stop_event.is_set():
                break
            await asyncio.sleep(300)  # Log stats every 5 min
            logger.info("── Live stream status ──")
            for task in tasks:
                logger.info(f"  {task.get_name()}: {'running' if not task.done() else 'STOPPED'}")

    tasks.append(asyncio.create_task(monitor(), name="ws_monitor"))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("All live streams cancelled")


def start_live_streams_sync(storage: StorageClient, flush_interval: int = 60):
    """Wrapper synchrone pour lancer les streams (utilisable dans un notebook)."""
    stop_event = asyncio.Event()
    try:
        asyncio.run(run_all_live_streams(storage, flush_interval, stop_event))
    except KeyboardInterrupt:
        stop_event.set()
        logger.info("Live streams stopped by user")

"""
Configuration centralisée du pipeline BTC.
Toutes les constantes, chemins GCS, schémas, et variables d'environnement.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Config:
    """Configuration unique — lue depuis les variables d'environnement."""

    # ── GCS ────────────────────────────────────────────────────────────────
    gcs_bucket: str = field(default_factory=lambda: os.environ.get("GCS_BUCKET_NAME", "btc-training-data"))
    gcs_project: str = field(default_factory=lambda: os.environ.get("GOOGLE_CLOUD_PROJECT", "mon-projet-gcp"))
    gcs_credentials: str = field(default_factory=lambda: os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/workspace/service-account.json"))

    # ── Bitcoin Core RPC ──────────────────────────────────────────────────
    btc_rpc_user: str = field(default_factory=lambda: os.environ.get("BTC_RPC_USER", "btcuser"))
    btc_rpc_password: str = field(default_factory=lambda: os.environ.get("BTC_RPC_PASSWORD", "secret"))
    btc_rpc_host: str = field(default_factory=lambda: os.environ.get("BTC_RPC_HOST", "127.0.0.1"))
    btc_rpc_port: int = field(default_factory=lambda: int(os.environ.get("BTC_RPC_PORT", "8332")))

    @property
    def btc_rpc_url(self) -> str:
        return f"http://{self.btc_rpc_user}:{self.btc_rpc_password}@{self.btc_rpc_host}:{self.btc_rpc_port}"

    # ── API Keys ──────────────────────────────────────────────────────────
    glassnode_api_key: Optional[str] = field(default_factory=lambda: os.environ.get("GLASSNODE_API_KEY"))

    # ── Disque local ──────────────────────────────────────────────────────
    workspace: str = field(default_factory=lambda: os.environ.get("WORKSPACE_DIR", "/workspace"))
    temp_dir: str = field(default_factory=lambda: os.environ.get("TEMP_DIR", "/workspace/tmp"))
    disk_warning_gb: float = 150.0
    disk_critical_gb: float = 80.0

    # ── Binance ───────────────────────────────────────────────────────────
    binance_base_url: str = "https://data.binance.vision"
    binance_start_date: str = "2017-08-17"
    binance_futures_start: str = "2019-09-01"
    binance_ws_base: str = "wss://stream.binance.com:9443/ws"
    binance_fws_base: str = "wss://fstream.binance.com/ws"

    # ── Mempool ───────────────────────────────────────────────────────────
    mempool_api_base: str = "https://mempool.space/api"
    mempool_ws_url: str = "wss://mempool.space/api/v1/ws"

    # ── Blockchair ────────────────────────────────────────────────────────
    blockchair_base: str = "https://gz.blockchair.com/bitcoin"

    # ── Pipeline ──────────────────────────────────────────────────────────
    pipeline_state_path: str = "metadata/pipeline_state.json"
    batch_size_blocks: int = 5000
    batch_size_transactions: int = 100_000
    ws_flush_interval_s: int = 60
    parquet_compression: str = "snappy"


# ═══════════════════════════════════════════════════════════════════════════
# GCS PATH TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════

GCS_PATHS = {
    # Binance raw
    "binance_spot_aggtrades":   "raw/binance/spot_aggtrades/year={year}/month={month:02d}/BTCUSDT-aggTrades-{year}-{month:02d}.parquet",
    "binance_spot_klines":      "raw/binance/spot_klines_{interval}/year={year}/month={month:02d}/BTCUSDT-klines-{interval}-{year}-{month:02d}.parquet",
    "binance_spot_bookticker":  "raw/binance/spot_bookticker/year={year}/month={month:02d}/BTCUSDT-bookTicker-{year}-{month:02d}.parquet",
    "binance_futures_aggtrades":"raw/binance/futures_aggtrades/year={year}/month={month:02d}/BTCUSDT-aggTrades-{year}-{month:02d}.parquet",
    "binance_futures_klines":   "raw/binance/futures_klines_{interval}/year={year}/month={month:02d}/BTCUSDT-klines-{interval}-{year}-{month:02d}.parquet",
    "binance_futures_funding":  "raw/binance/futures_funding/year={year}/month={month:02d}/BTCUSDT-fundingRate-{year}-{month:02d}.parquet",
    # Binance live
    "live_aggtrade":     "raw/binance/live/aggtrade/year={year}/month={month:02d}/day={day:02d}/{hour:02d}.parquet",
    "live_bookticker":   "raw/binance/live/bookticker/year={year}/month={month:02d}/day={day:02d}/{hour:02d}.parquet",
    "live_depth5":       "raw/binance/live/depth5/year={year}/month={month:02d}/day={day:02d}/{hour:02d}.parquet",
    "live_liquidations": "raw/binance/live/liquidations/year={year}/month={month:02d}/day={day:02d}/{hour:02d}.parquet",
    # Blockchain
    "blockchain_blocks":       "raw/blockchain/blocks/year={year}/blocks_{year}_batch_{batch:04d}.parquet",
    "blockchain_transactions": "raw/blockchain/transactions/year={year}/month={month:02d}/txs_{year}-{month:02d}_batch_{batch:04d}.parquet",
    "blockchain_utxo":         "raw/blockchain/utxo_snapshots/year={year}/utxo_snapshot_{date}.parquet",
    # Mempool
    "mempool_snapshots": "raw/mempool/snapshots/year={year}/month={month:02d}/day={day:02d}/mempool_snapshots.parquet",
    "mempool_fee":       "raw/mempool/fee_history/year={year}/month={month:02d}/fee_history.parquet",
    # On-chain
    "glassnode":   "raw/onchain_metrics/glassnode/metric={metric}/year={year}/daily.parquet",
    "bgeometrics": "raw/onchain_metrics/bgeometrics/metric={metric}/year={year}/daily.parquet",
    # Processed
    "features":   "processed/features_1s/year={year}/month={month:02d}/features_{year}-{month:02d}.parquet",
    "labels":     "processed/labels/year={year}/month={month:02d}/labels_{year}-{month:02d}.parquet",
    "quality":    "processed/validation_reports/{year}-{month:02d}_quality_report.json",
}


# ═══════════════════════════════════════════════════════════════════════════
# BINANCE DOWNLOAD TASKS
# ═══════════════════════════════════════════════════════════════════════════

BINANCE_DOWNLOAD_TASKS = [
    # (market, data_type, symbol, interval, start_date)
    ("spot",       "aggTrades",           "BTCUSDT", None,  "2017-08-17"),
    ("spot",       "klines",              "BTCUSDT", "1s",  "2020-01-01"),
    ("spot",       "klines",              "BTCUSDT", "1m",  "2017-08-17"),
    ("spot",       "klines",              "BTCUSDT", "5m",  "2017-08-17"),
    ("spot",       "klines",              "BTCUSDT", "1h",  "2017-08-17"),
    ("spot",       "bookTicker",          "BTCUSDT", None,  "2020-01-01"),
    ("futures/um", "aggTrades",           "BTCUSDT", None,  "2019-09-01"),
    ("futures/um", "klines",              "BTCUSDT", "1m",  "2019-09-01"),
    ("futures/um", "fundingRate",         "BTCUSDT", None,  "2019-09-01"),
    ("futures/um", "liquidationSnapshot", "BTCUSDT", None,  "2020-01-01"),
]


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMAS — dtype maps for Parquet
# ═══════════════════════════════════════════════════════════════════════════

AGGTRADE_COLUMNS = [
    "agg_trade_id", "price", "quantity", "first_trade_id",
    "last_trade_id", "timestamp", "is_buyer_maker", "is_best_match",
]

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
]

BLOCK_SCHEMA_DTYPES = {
    "block_height": "int32", "block_hash": "str", "block_timestamp": "int64",
    "block_version": "int32", "prev_block_hash": "str", "merkle_root": "str",
    "tx_count": "int32", "total_input_btc": "float64", "total_output_btc": "float64",
    "total_fees_btc": "float64", "coinbase_reward_btc": "float64",
    "block_size_bytes": "int32", "block_weight": "int32", "block_stripped_size": "int32",
    "fee_rate_min_sat_vbyte": "float32", "fee_rate_p10_sat_vbyte": "float32",
    "fee_rate_p25_sat_vbyte": "float32", "fee_rate_p50_sat_vbyte": "float32",
    "fee_rate_p75_sat_vbyte": "float32", "fee_rate_p90_sat_vbyte": "float32",
    "fee_rate_p99_sat_vbyte": "float32", "fee_rate_max_sat_vbyte": "float32",
    "tx_count_below_0001btc": "int32", "tx_count_0001_001btc": "int32",
    "tx_count_001_01btc": "int32", "tx_count_01_1btc": "int32",
    "tx_count_1_10btc": "int32", "tx_count_10_100btc": "int32",
    "tx_count_above_100btc": "int32", "tx_count_above_1000btc": "int32",
    "volume_below_0001btc": "float32", "volume_0001_001btc": "float32",
    "volume_001_01btc": "float32", "volume_01_1btc": "float32",
    "volume_1_10btc": "float32", "volume_10_100btc": "float32",
    "volume_above_100btc": "float32", "volume_above_1000btc": "float32",
    "tx_input_count_mean": "float32", "tx_input_count_median": "float32",
    "tx_output_count_mean": "float32", "tx_output_count_median": "float32",
    "tx_size_mean_bytes": "float32", "tx_size_median_bytes": "float32",
    "tx_size_p90_bytes": "float32",
    "segwit_tx_count": "int32", "segwit_tx_ratio": "float32",
    "native_segwit_tx_count": "int32",
    "taproot_tx_count": "int32", "taproot_tx_ratio": "float32",
    "utxo_created_count": "int32", "utxo_spent_count": "int32",
    "utxo_net_change": "int32", "utxo_created_value_btc": "float64",
    "utxo_spent_value_btc": "float64",
    "seconds_since_prev_block": "int32", "blocks_since_halving": "int32",
    "halving_epoch": "int8",
    "difficulty": "float64", "bits": "int32", "nonce": "int32",
    "coinbase_outputs_count": "int8",
    "coin_days_destroyed_block": "float64",
}

TRANSACTION_SCHEMA_DTYPES = {
    "txid": "str", "block_height": "int32", "block_timestamp": "int64",
    "tx_index_in_block": "int16", "is_coinbase": "bool",
    "input_count": "int16", "output_count": "int16",
    "tx_size_bytes": "int32", "tx_vsize_bytes": "int32", "tx_weight": "int32",
    "total_input_satoshis": "int64", "total_output_satoshis": "int64",
    "fee_satoshis": "int64", "fee_rate_sat_vbyte": "float32",
    "has_segwit_input": "bool", "has_taproot_input": "bool",
    "is_rbf": "bool", "is_consolidation": "bool", "is_batch_payment": "bool",
    "output_count_p2pkh": "int16", "output_count_p2sh": "int16",
    "output_count_p2wpkh": "int16", "output_count_p2tr": "int16",
    "output_count_op_return": "int16",
    "max_output_satoshis": "int64", "min_output_satoshis": "int64",
    "coin_days_destroyed": "float64", "avg_input_age_blocks": "float32",
    "max_input_age_blocks": "int32",
}

UTXO_SNAPSHOT_SCHEMA_DTYPES = {
    "snapshot_block_height": "int32", "snapshot_timestamp": "int64",
    "total_utxo_count": "int64", "total_btc_in_utxos": "float64",
    "avg_utxo_value_btc": "float64", "median_utxo_value_btc": "float64",
    "btc_age_below_1d": "float64", "btc_age_1d_7d": "float64",
    "btc_age_7d_30d": "float64", "btc_age_30d_90d": "float64",
    "btc_age_90d_180d": "float64", "btc_age_180d_1y": "float64",
    "btc_age_1y_2y": "float64", "btc_age_2y_3y": "float64",
    "btc_age_3y_5y": "float64", "btc_age_above_5y": "float64",
    "utxo_count_age_below_1d": "int64", "utxo_count_age_1d_7d": "int64",
    "utxo_count_age_7d_30d": "int64", "utxo_count_age_30d_90d": "int64",
    "utxo_count_age_90d_180d": "int64", "utxo_count_age_180d_1y": "int64",
    "utxo_count_age_above_1y": "int64",
    "utxo_count_dust": "int64", "utxo_count_below_001btc": "int64",
    "utxo_count_001_01btc": "int64", "utxo_count_01_1btc": "int64",
    "utxo_count_1_10btc": "int64", "utxo_count_10_100btc": "int64",
    "utxo_count_above_100btc": "int64",
    "btc_held_by_dust": "float64", "btc_held_below_001btc": "float64",
    "btc_held_001_01btc": "float64", "btc_held_01_1btc": "float64",
    "btc_held_1_10btc": "float64", "btc_held_10_100btc": "float64",
    "btc_held_above_100btc": "float64",
    "liveliness": "float64", "coin_days_destroyed_7d": "float64",
    "dormancy_7d": "float64", "realized_cap_usd": "float64",
}

MEMPOOL_SCHEMA_DTYPES = {
    "timestamp": "int64", "mempool_tx_count": "int32",
    "mempool_vsize_bytes": "int64", "mempool_total_fee_sat": "int64",
    "fee_p10_sat_vbyte": "float32", "fee_p25_sat_vbyte": "float32",
    "fee_p50_sat_vbyte": "float32", "fee_p75_sat_vbyte": "float32",
    "fee_p90_sat_vbyte": "float32", "fee_p99_sat_vbyte": "float32",
    "blocks_estimated_1h": "int8", "blocks_estimated_6h": "int8",
}

# ═══════════════════════════════════════════════════════════════════════════
# HALVING BLOCK HEIGHTS
# ═══════════════════════════════════════════════════════════════════════════

HALVING_HEIGHTS = [0, 210_000, 420_000, 630_000, 840_000]
# epoch 0: genesis → 210k, epoch 1: 210k → 420k, etc.

def get_halving_epoch(block_height: int) -> int:
    for i in range(len(HALVING_HEIGHTS) - 1, -1, -1):
        if block_height >= HALVING_HEIGHTS[i]:
            return i
    return 0

def get_blocks_since_halving(block_height: int) -> int:
    epoch = get_halving_epoch(block_height)
    return block_height - HALVING_HEIGHTS[epoch]

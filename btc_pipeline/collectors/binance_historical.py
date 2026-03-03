"""
Binance Historical Data Collector
Télécharge toutes les données BTCUSDT depuis data.binance.vision.
Flux : ZIP téléchargé → décompression en RAM → parse Polars → Parquet → GCS → ZIP supprimé.

Sources :
  - Spot aggTrades (2017-08-17 → aujourd'hui)
  - Spot klines 1s/1m/5m/1h
  - Spot bookTicker
  - Futures aggTrades, klines 1m, fundingRate, liquidationSnapshot
"""

import io
import os
import csv
import zipfile
import tempfile
from datetime import datetime, date, timedelta
from typing import Optional, Generator

import requests
import pandas as pd
import polars as pl
from loguru import logger

from btc_pipeline.config import (
    Config, GCS_PATHS, BINANCE_DOWNLOAD_TASKS,
    AGGTRADE_COLUMNS, KLINE_COLUMNS,
)
from btc_pipeline.storage.gcs_client import StorageClient


# ═══════════════════════════════════════════════════════════════════════════
# URL BUILDERS
# ═══════════════════════════════════════════════════════════════════════════

def build_monthly_url(market: str, data_type: str, symbol: str,
                      year: int, month: int, interval: Optional[str] = None) -> str:
    """Construit l'URL data.binance.vision pour un mois donné."""
    base = "https://data.binance.vision/data"
    if market == "spot":
        if data_type == "klines" and interval:
            return f"{base}/spot/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year}-{month:02d}.zip"
        elif data_type == "aggTrades":
            return f"{base}/spot/monthly/aggTrades/{symbol}/{symbol}-aggTrades-{year}-{month:02d}.zip"
        elif data_type == "bookTicker":
            return f"{base}/spot/monthly/bookTicker/{symbol}/{symbol}-bookTicker-{year}-{month:02d}.zip"
    elif market == "futures/um":
        if data_type == "klines" and interval:
            return f"{base}/futures/um/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year}-{month:02d}.zip"
        elif data_type == "aggTrades":
            return f"{base}/futures/um/monthly/aggTrades/{symbol}/{symbol}-aggTrades-{year}-{month:02d}.zip"
        elif data_type == "fundingRate":
            return f"{base}/futures/um/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{year}-{month:02d}.zip"
        elif data_type == "liquidationSnapshot":
            return f"{base}/futures/um/monthly/liquidationSnapshot/{symbol}/{symbol}-liquidationSnapshot-{year}-{month:02d}.zip"
    raise ValueError(f"Unknown combination: {market}/{data_type}/{interval}")


def build_daily_url(market: str, data_type: str, symbol: str,
                    dt: date, interval: Optional[str] = None) -> str:
    """Construit l'URL journalière (pour compléter les mois récents non encore publiés en monthly)."""
    base = "https://data.binance.vision/data"
    ds = dt.strftime("%Y-%m-%d")
    if market == "spot":
        if data_type == "klines" and interval:
            return f"{base}/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{ds}.zip"
        elif data_type == "aggTrades":
            return f"{base}/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{ds}.zip"
        elif data_type == "bookTicker":
            return f"{base}/spot/daily/bookTicker/{symbol}/{symbol}-bookTicker-{ds}.zip"
    elif market == "futures/um":
        if data_type == "klines" and interval:
            return f"{base}/futures/um/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{ds}.zip"
        elif data_type == "aggTrades":
            return f"{base}/futures/um/daily/aggTrades/{symbol}/{symbol}-aggTrades-{ds}.zip"
        elif data_type == "fundingRate":
            return f"{base}/futures/um/daily/fundingRate/{symbol}/{symbol}-fundingRate-{ds}.zip"
        elif data_type == "liquidationSnapshot":
            return f"{base}/futures/um/daily/liquidationSnapshot/{symbol}/{symbol}-liquidationSnapshot-{ds}.zip"
    raise ValueError(f"Unknown daily combination: {market}/{data_type}/{interval}")


# ═══════════════════════════════════════════════════════════════════════════
# GCS PATH BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def get_gcs_path(market: str, data_type: str, year: int, month: int,
                 interval: Optional[str] = None) -> str:
    """Détermine le chemin GCS de destination pour un fichier Parquet."""
    if market == "spot":
        if data_type == "aggTrades":
            return GCS_PATHS["binance_spot_aggtrades"].format(year=year, month=month)
        elif data_type == "klines":
            return GCS_PATHS["binance_spot_klines"].format(interval=interval, year=year, month=month)
        elif data_type == "bookTicker":
            return GCS_PATHS["binance_spot_bookticker"].format(year=year, month=month)
    elif market == "futures/um":
        if data_type == "aggTrades":
            return GCS_PATHS["binance_futures_aggtrades"].format(year=year, month=month)
        elif data_type == "klines":
            return GCS_PATHS["binance_futures_klines"].format(interval=interval, year=year, month=month)
        elif data_type == "fundingRate":
            return GCS_PATHS["binance_futures_funding"].format(year=year, month=month)
        elif data_type == "liquidationSnapshot":
            # Use funding path pattern for now
            return f"raw/binance/futures_liquidations/year={year}/month={month:02d}/BTCUSDT-liquidationSnapshot-{year}-{month:02d}.parquet"
    raise ValueError(f"No GCS path for {market}/{data_type}/{interval}")


# ═══════════════════════════════════════════════════════════════════════════
# MONTH GENERATOR
# ═══════════════════════════════════════════════════════════════════════════

def generate_months(start_date: str, end_date: Optional[str] = None) -> list[tuple[int, int]]:
    """Génère la liste (year, month) depuis start_date jusqu'à aujourd'hui."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else date.today()
    months = []
    current = start.replace(day=1)
    while current <= end:
        months.append((current.year, current.month))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


# ═══════════════════════════════════════════════════════════════════════════
# CSV PARSERS (ZIP → DataFrame) — tout en mémoire, rien sur disque
# ═══════════════════════════════════════════════════════════════════════════

def parse_aggtrades_csv(csv_bytes: bytes) -> pd.DataFrame:
    """Parse aggTrades CSV depuis bytes. Pas de header dans le fichier Binance."""
    df = pd.read_csv(
        io.BytesIO(csv_bytes),
        header=None,
        names=AGGTRADE_COLUMNS,
        dtype={
            "agg_trade_id": "int64",
            "price": "float64",
            "quantity": "float64",
            "first_trade_id": "int64",
            "last_trade_id": "int64",
            "timestamp": "int64",
            "is_buyer_maker": "bool",
            "is_best_match": "bool",
        },
    )
    return df


def parse_klines_csv(csv_bytes: bytes) -> pd.DataFrame:
    """Parse klines CSV depuis bytes."""
    df = pd.read_csv(
        io.BytesIO(csv_bytes),
        header=None,
        names=KLINE_COLUMNS,
        dtype={
            "open_time": "int64", "open": "float64", "high": "float64",
            "low": "float64", "close": "float64", "volume": "float64",
            "close_time": "int64", "quote_volume": "float64",
            "trade_count": "int64",
            "taker_buy_base_volume": "float64",
            "taker_buy_quote_volume": "float64",
        },
    )
    # Drop la colonne 'ignore'
    df = df.drop(columns=["ignore"], errors="ignore")
    return df


def parse_bookticker_csv(csv_bytes: bytes) -> pd.DataFrame:
    """Parse bookTicker CSV."""
    columns = ["update_id", "best_bid_price", "best_bid_qty",
                "best_ask_price", "best_ask_qty", "transaction_time", "event_time"]
    try:
        df = pd.read_csv(io.BytesIO(csv_bytes), header=None, names=columns)
    except Exception:
        # Some files have headers
        df = pd.read_csv(io.BytesIO(csv_bytes))
        df.columns = columns[:len(df.columns)]
    return df


def parse_funding_csv(csv_bytes: bytes) -> pd.DataFrame:
    """Parse fundingRate CSV."""
    columns = ["calc_time", "funding_interval_hours", "last_funding_rate",
                "mark_price", "symbol"]
    try:
        df = pd.read_csv(io.BytesIO(csv_bytes), header=None, names=columns)
    except Exception:
        df = pd.read_csv(io.BytesIO(csv_bytes))
    return df


def parse_liquidation_csv(csv_bytes: bytes) -> pd.DataFrame:
    """Parse liquidationSnapshot CSV."""
    try:
        df = pd.read_csv(io.BytesIO(csv_bytes))
    except Exception:
        df = pd.read_csv(io.BytesIO(csv_bytes), header=None)
    return df


def parse_csv_by_type(csv_bytes: bytes, data_type: str, interval: Optional[str] = None) -> pd.DataFrame:
    """Dispatch vers le bon parser selon le type de données."""
    if data_type == "aggTrades":
        return parse_aggtrades_csv(csv_bytes)
    elif data_type == "klines":
        return parse_klines_csv(csv_bytes)
    elif data_type == "bookTicker":
        return parse_bookticker_csv(csv_bytes)
    elif data_type == "fundingRate":
        return parse_funding_csv(csv_bytes)
    elif data_type == "liquidationSnapshot":
        return parse_liquidation_csv(csv_bytes)
    else:
        raise ValueError(f"Unknown data_type: {data_type}")


# ═══════════════════════════════════════════════════════════════════════════
# CORE DOWNLOAD + PROCESS + UPLOAD (une seule fonction unifiée)
# ═══════════════════════════════════════════════════════════════════════════

def download_and_upload_month(
    storage: StorageClient,
    market: str,
    data_type: str,
    symbol: str,
    year: int,
    month: int,
    interval: Optional[str] = None,
    config: Optional[Config] = None,
) -> dict:
    """
    Télécharge un mois de données Binance, parse, convertit en Parquet, upload sur GCS.
    Retourne un dict avec les statistiques (rows, size_bytes, duration).

    Flux complet : URL → ZIP (RAM) → CSV (RAM) → DataFrame → Parquet bytes → GCS
    Aucun fichier intermédiaire sur disque.
    """
    config = config or Config()
    gcs_path = get_gcs_path(market, data_type, year, month, interval)

    # Idempotence : skip si déjà uploadé
    if storage.exists(gcs_path):
        logger.debug(f"SKIP (exists): {gcs_path}")
        return {"status": "skipped", "gcs_path": gcs_path, "rows": 0, "size_bytes": 0}

    url = build_monthly_url(market, data_type, symbol, year, month, interval)
    t0 = datetime.now()

    # 1. Téléchargement du ZIP en mémoire
    try:
        resp = requests.get(url, timeout=300, stream=True)
        if resp.status_code == 404:
            logger.warning(f"NOT FOUND (404): {url}")
            return {"status": "not_found", "gcs_path": gcs_path, "rows": 0, "size_bytes": 0}
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"DOWNLOAD ERROR: {url} — {e}")
        return {"status": "error", "gcs_path": gcs_path, "error": str(e), "rows": 0, "size_bytes": 0}

    zip_bytes = resp.content
    logger.debug(f"Downloaded ZIP: {len(zip_bytes)/1e6:.1f} MB from {url}")

    # 2. Extraction du CSV depuis le ZIP (en mémoire)
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                logger.error(f"No CSV in ZIP: {url}")
                return {"status": "error", "gcs_path": gcs_path, "error": "no_csv_in_zip", "rows": 0, "size_bytes": 0}
            csv_bytes = zf.read(csv_names[0])
    except zipfile.BadZipFile as e:
        logger.error(f"Bad ZIP: {url} — {e}")
        return {"status": "error", "gcs_path": gcs_path, "error": str(e), "rows": 0, "size_bytes": 0}

    del zip_bytes  # Libérer la mémoire du ZIP

    # 3. Parse CSV → DataFrame
    try:
        df = parse_csv_by_type(csv_bytes, data_type, interval)
    except Exception as e:
        logger.error(f"PARSE ERROR: {url} — {e}")
        return {"status": "error", "gcs_path": gcs_path, "error": str(e), "rows": 0, "size_bytes": 0}

    del csv_bytes  # Libérer la mémoire du CSV brut

    rows = len(df)
    if rows == 0:
        logger.warning(f"Empty DataFrame for {url}")
        return {"status": "empty", "gcs_path": gcs_path, "rows": 0, "size_bytes": 0}

    # 4. Upload Parquet vers GCS (streaming, pas de disque)
    size_bytes = storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)
    duration = (datetime.now() - t0).total_seconds()

    logger.info(
        f"✅ {market}/{data_type} {year}-{month:02d}: "
        f"{rows:,} rows, {size_bytes/1e6:.1f} MB, {duration:.0f}s"
    )

    return {
        "status": "success",
        "gcs_path": gcs_path,
        "rows": rows,
        "size_bytes": size_bytes,
        "duration_s": duration,
    }


# ═══════════════════════════════════════════════════════════════════════════
# DAILY GAP FILLER (pour les derniers jours non encore publiés en monthly)
# ═══════════════════════════════════════════════════════════════════════════

def download_and_upload_day(
    storage: StorageClient,
    market: str,
    data_type: str,
    symbol: str,
    dt: date,
    interval: Optional[str] = None,
) -> dict:
    """Télécharge un jour unique (pour combler les gaps des mois récents)."""
    # Build a daily GCS path
    gcs_path = f"raw/binance/daily/{market.replace('/', '_')}/{data_type}/"
    if interval:
        gcs_path += f"{interval}/"
    gcs_path += f"{dt.strftime('%Y-%m-%d')}.parquet"

    if storage.exists(gcs_path):
        return {"status": "skipped", "rows": 0}

    url = build_daily_url(market, data_type, symbol, dt, interval)

    try:
        resp = requests.get(url, timeout=120)
        if resp.status_code == 404:
            return {"status": "not_found", "rows": 0}
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        return {"status": "error", "error": str(e), "rows": 0}

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                return {"status": "error", "error": "no_csv", "rows": 0}
            csv_bytes = zf.read(csv_names[0])
    except zipfile.BadZipFile:
        return {"status": "error", "error": "bad_zip", "rows": 0}

    df = parse_csv_by_type(csv_bytes, data_type, interval)
    if len(df) == 0:
        return {"status": "empty", "rows": 0}

    size_bytes = storage.stream_upload_parquet(df, gcs_path, skip_if_exists=False)
    return {"status": "success", "rows": len(df), "size_bytes": size_bytes}


# ═══════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR — Lance toutes les tâches pour un type de données
# ═══════════════════════════════════════════════════════════════════════════

def run_binance_download(
    storage: StorageClient,
    market: str,
    data_type: str,
    symbol: str = "BTCUSDT",
    interval: Optional[str] = None,
    start_date: str = "2017-08-17",
    end_date: Optional[str] = None,
    progress_callback=None,
) -> dict:
    """
    Télécharge tous les mois disponibles pour une tâche Binance donnée.
    Reprend depuis le dernier mois complété (via pipeline_state).

    Args:
        progress_callback: callable(current, total, info_dict) pour les barres de progression

    Returns:
        dict avec completed_count, failed_months, total_gb, duration
    """
    task_key = f"binance_{market.replace('/', '_')}_{data_type}"
    if interval:
        task_key += f"_{interval}"

    months = generate_months(start_date, end_date)
    total_months = len(months)

    # Récupérer le state pour reprendre
    state = storage.get_pipeline_state()
    task_state = state.get("tasks", {}).get(task_key, {})
    last_completed = task_state.get("last_completed_month", None)

    # Filtrer les mois déjà complétés
    if last_completed:
        ly, lm = map(int, last_completed.split("-"))
        months = [(y, m) for y, m in months if (y, m) > (ly, lm)]

    logger.info(f"▶ {task_key}: {len(months)} months to download (of {total_months} total)")

    stats = {
        "completed": 0, "skipped": 0, "failed": 0, "not_found": 0,
        "total_rows": 0, "total_bytes": 0, "failed_months": [],
    }

    t_start = datetime.now()

    for i, (year, month) in enumerate(months):
        # Check disk space
        disk_status = storage.check_disk_space()
        if disk_status == "critical":
            logger.error("DISK CRITICAL — pausing downloads")
            storage.cleanup_local_temp()
            break

        result = download_and_upload_month(
            storage, market, data_type, symbol, year, month, interval
        )

        if result["status"] == "success":
            stats["completed"] += 1
            stats["total_rows"] += result["rows"]
            stats["total_bytes"] += result["size_bytes"]
            # Sauvegarder la progression
            storage.update_task_state(task_key, "last_completed_month", f"{year}-{month:02d}")
            storage.update_task_state(task_key, "last_completed_rows", result["rows"])
        elif result["status"] == "skipped":
            stats["skipped"] += 1
        elif result["status"] == "not_found":
            stats["not_found"] += 1
        else:
            stats["failed"] += 1
            stats["failed_months"].append(f"{year}-{month:02d}")

        if progress_callback:
            progress_callback(i + 1, len(months), {
                "year": year, "month": month,
                "status": result["status"],
                "rows": result.get("rows", 0),
            })

    duration = (datetime.now() - t_start).total_seconds()
    stats["duration_s"] = duration
    stats["total_gb"] = stats["total_bytes"] / 1e9

    logger.info(
        f"✅ {task_key} DONE: {stats['completed']} completed, "
        f"{stats['skipped']} skipped, {stats['failed']} failed, "
        f"{stats['not_found']} not_found, "
        f"{stats['total_gb']:.2f} GB, {duration:.0f}s"
    )

    return stats


def run_all_binance_tasks(storage: StorageClient, priority: int = 1) -> dict:
    """
    Lance toutes les tâches de téléchargement Binance.
    priority=1 : aggTrades + klines 1m seulement
    priority=2 : tout sauf liquidations
    priority=3 : tout
    """
    all_stats = {}

    priority_map = {
        ("spot", "aggTrades"): 1,
        ("spot", "klines", "1m"): 1,
        ("spot", "klines", "1s"): 2,
        ("spot", "klines", "5m"): 2,
        ("spot", "klines", "1h"): 2,
        ("spot", "bookTicker"): 2,
        ("futures/um", "aggTrades"): 2,
        ("futures/um", "klines", "1m"): 2,
        ("futures/um", "fundingRate"): 2,
        ("futures/um", "liquidationSnapshot"): 3,
    }

    for market, data_type, symbol, interval, start_date in BINANCE_DOWNLOAD_TASKS:
        key = (market, data_type) if interval is None else (market, data_type, interval)
        task_priority = priority_map.get(key, 3)

        if task_priority > priority:
            logger.info(f"SKIP (priority {task_priority} > {priority}): {market}/{data_type}/{interval}")
            continue

        stats = run_binance_download(
            storage, market, data_type, symbol, interval, start_date
        )
        task_name = f"{market}_{data_type}" + (f"_{interval}" if interval else "")
        all_stats[task_name] = stats

    return all_stats

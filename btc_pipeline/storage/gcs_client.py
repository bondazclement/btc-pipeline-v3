"""
Interface unique GCS avec streaming pour économiser le disque local.
Toutes les opérations I/O du pipeline passent EXCLUSIVEMENT par ce module.

Supporte deux backends :
  - "gcs"   : Google Cloud Storage (production sur RunPod)
  - "local" : système de fichiers local (développement / tests)
"""

import io
import os
import json
import time
import shutil
import glob
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger


class StorageClient:
    """Client de stockage unifié GCS / local."""

    def __init__(self, backend: Optional[str] = None, bucket_name: Optional[str] = None):
        self.backend = backend or os.environ.get("STORAGE_BACKEND", "gcs")
        self.bucket_name = bucket_name or os.environ.get("GCS_BUCKET_NAME", "btc-training-data")
        self.workspace = os.environ.get("WORKSPACE_DIR", "/workspace")
        self.temp_dir = os.path.join(self.workspace, "tmp")
        os.makedirs(self.temp_dir, exist_ok=True)

        if self.backend == "gcs":
            self._init_gcs()
        else:
            # Local backend — stocke tout sous workspace/gcs_mirror/
            self.local_root = os.path.join(self.workspace, "gcs_mirror")
            os.makedirs(self.local_root, exist_ok=True)
            logger.info(f"StorageClient: backend=local, root={self.local_root}")

    def _init_gcs(self):
        from google.cloud import storage as gcs_lib
        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
        if creds_path and os.path.exists(creds_path):
            self.gcs_client = gcs_lib.Client.from_service_account_json(creds_path)
        else:
            self.gcs_client = gcs_lib.Client()
        self.bucket = self.gcs_client.bucket(self.bucket_name)
        logger.info(f"StorageClient: backend=gcs, bucket={self.bucket_name}")

    # ═══════════════════════════════════════════════════════════════════════
    # EXISTENCE CHECK
    # ═══════════════════════════════════════════════════════════════════════

    def exists(self, gcs_path: str) -> bool:
        """Vérifie si un objet existe. Check avant tout upload (idempotence)."""
        if self.backend == "gcs":
            blob = self.bucket.blob(gcs_path)
            return blob.exists()
        else:
            return os.path.exists(os.path.join(self.local_root, gcs_path))

    # ═══════════════════════════════════════════════════════════════════════
    # UPLOAD — STREAMING (RAM → GCS, pas de disque intermédiaire)
    # ═══════════════════════════════════════════════════════════════════════

    def stream_upload_parquet(self, df: pd.DataFrame, gcs_path: str,
                              compression: str = "snappy",
                              skip_if_exists: bool = True) -> int:
        """
        Écrit le DataFrame directement vers GCS sans passer par le disque local.
        Retourne la taille en bytes du fichier uploadé.
        """
        if skip_if_exists and self.exists(gcs_path):
            logger.debug(f"SKIP (exists): {gcs_path}")
            return 0

        buf = io.BytesIO()
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, buf, compression=compression)
        size_bytes = buf.tell()
        buf.seek(0)

        if self.backend == "gcs":
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_file(buf, content_type="application/octet-stream")
        else:
            local_path = os.path.join(self.local_root, gcs_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(buf.getvalue())

        logger.info(f"UPLOADED: {gcs_path} ({size_bytes / 1e6:.1f} MB)")
        return size_bytes

    def stream_upload_file(self, local_path: str, gcs_path: str,
                           delete_after: bool = True) -> int:
        """Upload un fichier local vers GCS, supprime le local si demandé."""
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")

        size_bytes = os.path.getsize(local_path)

        if self.backend == "gcs":
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
        else:
            dest = os.path.join(self.local_root, gcs_path)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copy2(local_path, dest)

        logger.info(f"UPLOADED file: {gcs_path} ({size_bytes / 1e6:.1f} MB)")

        if delete_after:
            os.remove(local_path)
            logger.debug(f"DELETED local: {local_path}")

        return size_bytes

    def stream_upload_bytes(self, data: bytes, gcs_path: str,
                            skip_if_exists: bool = True) -> int:
        """Upload raw bytes vers GCS."""
        if skip_if_exists and self.exists(gcs_path):
            return 0
        if self.backend == "gcs":
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_string(data, content_type="application/octet-stream")
        else:
            local_path = os.path.join(self.local_root, gcs_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(data)
        return len(data)

    # ═══════════════════════════════════════════════════════════════════════
    # DOWNLOAD
    # ═══════════════════════════════════════════════════════════════════════

    def download_parquet(self, gcs_path: str) -> pd.DataFrame:
        """Lit un Parquet depuis GCS directement en RAM."""
        if self.backend == "gcs":
            blob = self.bucket.blob(gcs_path)
            buf = io.BytesIO()
            blob.download_to_file(buf)
            buf.seek(0)
            return pq.read_table(buf).to_pandas()
        else:
            local_path = os.path.join(self.local_root, gcs_path)
            return pq.read_table(local_path).to_pandas()

    def download_to_local(self, gcs_path: str, local_path: str) -> None:
        """Télécharge un fichier GCS vers le disque local."""
        if self.backend == "gcs":
            blob = self.bucket.blob(gcs_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob.download_to_filename(local_path)
        else:
            src = os.path.join(self.local_root, gcs_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            shutil.copy2(src, local_path)

    def download_json(self, gcs_path: str) -> dict:
        """Télécharge et parse un JSON depuis GCS."""
        if self.backend == "gcs":
            blob = self.bucket.blob(gcs_path)
            data = blob.download_as_bytes()
            return json.loads(data)
        else:
            local_path = os.path.join(self.local_root, gcs_path)
            with open(local_path, "r") as f:
                return json.load(f)

    # ═══════════════════════════════════════════════════════════════════════
    # LIST FILES
    # ═══════════════════════════════════════════════════════════════════════

    def list_files(self, prefix: str) -> list[str]:
        """Liste tous les objets avec ce préfixe."""
        if self.backend == "gcs":
            blobs = self.bucket.list_blobs(prefix=prefix)
            return [b.name for b in blobs]
        else:
            base = os.path.join(self.local_root, prefix)
            if not os.path.exists(base):
                return []
            results = []
            for root, dirs, files in os.walk(base):
                for f in files:
                    full = os.path.join(root, f)
                    rel = os.path.relpath(full, self.local_root)
                    results.append(rel)
            return sorted(results)

    # ═══════════════════════════════════════════════════════════════════════
    # PIPELINE STATE
    # ═══════════════════════════════════════════════════════════════════════

    def get_pipeline_state(self) -> dict:
        """Charge l'état du pipeline depuis GCS."""
        state_path = "metadata/pipeline_state.json"
        try:
            return self.download_json(state_path)
        except Exception:
            logger.warning("No existing pipeline_state.json — starting fresh")
            return {"created_at": datetime.now(timezone.utc).isoformat(), "tasks": {}}

    def save_pipeline_state(self, state: dict) -> None:
        """Sauvegarde l'état du pipeline sur GCS."""
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        state_json = json.dumps(state, indent=2, default=str).encode("utf-8")
        # Force overwrite — state is always updated
        if self.backend == "gcs":
            blob = self.bucket.blob("metadata/pipeline_state.json")
            blob.upload_from_string(state_json, content_type="application/json")
        else:
            local_path = os.path.join(self.local_root, "metadata/pipeline_state.json")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(state_json)
        logger.info("Pipeline state saved")

    def update_task_state(self, task_name: str, key: str, value) -> None:
        """Met à jour un champ spécifique dans le state d'une tâche."""
        state = self.get_pipeline_state()
        if "tasks" not in state:
            state["tasks"] = {}
        if task_name not in state["tasks"]:
            state["tasks"][task_name] = {}
        state["tasks"][task_name][key] = value
        self.save_pipeline_state(state)

    # ═══════════════════════════════════════════════════════════════════════
    # DISK MONITORING
    # ═══════════════════════════════════════════════════════════════════════

    def get_local_disk_usage_gb(self) -> tuple[float, float, float]:
        """Retourne (total_gb, used_gb, free_gb) du disque local."""
        total, used, free = shutil.disk_usage(self.workspace)
        return total / 1e9, used / 1e9, free / 1e9

    def check_disk_space(self, warning_gb: float = 150.0, critical_gb: float = 80.0) -> str:
        """
        Vérifie l'espace disque. Retourne "ok", "warning", ou "critical".
        """
        _, _, free_gb = self.get_local_disk_usage_gb()
        if free_gb < critical_gb:
            logger.error(f"🚨 CRITICAL: {free_gb:.1f} GB free — below {critical_gb} GB threshold")
            return "critical"
        elif free_gb < warning_gb:
            logger.warning(f"⚠️ WARNING: {free_gb:.1f} GB free — below {warning_gb} GB threshold")
            return "warning"
        else:
            return "ok"

    def cleanup_local_temp(self, max_age_minutes: int = 30) -> float:
        """Supprime les fichiers temporaires non uploadés depuis > N minutes.
        Retourne le nombre de GB libérés."""
        freed = 0.0
        cutoff = time.time() - (max_age_minutes * 60)
        if os.path.exists(self.temp_dir):
            for f in glob.glob(os.path.join(self.temp_dir, "**", "*"), recursive=True):
                if os.path.isfile(f) and os.path.getmtime(f) < cutoff:
                    size = os.path.getsize(f)
                    os.remove(f)
                    freed += size
                    logger.debug(f"Cleaned up: {f} ({size/1e6:.1f} MB)")
        freed_gb = freed / 1e9
        if freed_gb > 0:
            logger.info(f"Cleanup freed {freed_gb:.2f} GB")
        return freed_gb

    # ═══════════════════════════════════════════════════════════════════════
    # INVENTORY
    # ═══════════════════════════════════════════════════════════════════════

    def get_inventory(self) -> dict:
        """Construit un inventaire complet du bucket."""
        inventory = {}
        prefixes = [
            "raw/binance/spot_aggtrades",
            "raw/binance/spot_klines_1s",
            "raw/binance/spot_klines_1m",
            "raw/binance/spot_klines_5m",
            "raw/binance/spot_klines_1h",
            "raw/binance/spot_bookticker",
            "raw/binance/futures_aggtrades",
            "raw/binance/futures_klines_1m",
            "raw/binance/futures_funding",
            "raw/binance/live",
            "raw/blockchain/blocks",
            "raw/blockchain/transactions",
            "raw/blockchain/utxo_snapshots",
            "raw/mempool",
            "raw/onchain_metrics",
            "processed/features_1s",
            "processed/labels",
        ]
        for prefix in prefixes:
            files = self.list_files(prefix)
            if files:
                total_size = 0
                if self.backend == "gcs":
                    for f in files:
                        blob = self.bucket.blob(f)
                        blob.reload()
                        total_size += blob.size or 0
                inventory[prefix] = {
                    "file_count": len(files),
                    "total_size_gb": total_size / 1e9 if self.backend == "gcs" else -1,
                    "first_file": files[0] if files else None,
                    "last_file": files[-1] if files else None,
                }
        return inventory

"""
Logging pour les jobs Spark de transformation.

Meme format que les connecteurs d'ingestion (Fahd_openalex.py, fahd_datagov.py,
generic_crawler.py) : fichier local horodate + stdout. En plus, chaque job ecrit
un log de run structure (JSON) dans le bucket raw-logs via MinIOClient, pour que
la tracabilite soit uniforme sur toute la plateforme (ingestion ET transformation).
"""

import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from configs.spark_config import LOG_DIR, RAW_LOGS_BUCKET
from src.storage.minio.fahd_client import MinIOClient


def setup_logger(job_name: str) -> logging.Logger:
    log_dir = Path(LOG_DIR)
    log_dir.mkdir(exist_ok=True, parents=True)

    logger = logging.getLogger(job_name)
    if logger.handlers:
        # deja configure (ex: appel multiple dans le meme process) -> ne pas dupliquer
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = logging.FileHandler(
        log_dir / f"spark_{job_name}_{datetime.now():%Y%m%d}.log", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def new_run_id() -> str:
    return str(uuid.uuid4())


def write_run_log(
    job_name: str,
    run_id: str,
    status: str,
    records_read: int = 0,
    records_written: int = 0,
    records_quarantined: int = 0,
    duplicates_dropped: int = 0,
    error: str | None = None,
    extra: dict | None = None,
) -> None:
    """
    Ecrit un log de run structure dans raw-logs, meme convention de chemin que
    les connecteurs d'ingestion : source=<job>/year=/month=/day=/run_<id>.json
    """
    now = datetime.now(timezone.utc)
    payload = {
        "run_id": run_id,
        "job": job_name,
        "layer": "spark_transformation",
        "status": status,
        "timestamp": now.isoformat(),
        "records_read": records_read,
        "records_written": records_written,
        "records_quarantined": records_quarantined,
        "duplicates_dropped": duplicates_dropped,
        "error": error,
    }
    if extra:
        payload.update(extra)

    object_name = (
        f"source={job_name}/year={now:%Y}/month={now:%m}/day={now:%d}/run_{run_id}.json"
    )

    client = MinIOClient()
    client.upload_json(bucket_name=RAW_LOGS_BUCKET, object_name=object_name, data=payload)
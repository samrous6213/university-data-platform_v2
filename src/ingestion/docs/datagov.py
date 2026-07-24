"""
Ingestion documents/fichiers -> MinIO (zone raw).
Source : Portail Open Data du Maroc (data.gov.ma), plateforme CKAN.
"""

import hashlib
import json
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.storage.minio.ayoub_client import MinIOClient

# ─── Paramètres ─────────────────────────────────────────────
CONNECTOR_VERSION = "1.0.1"
SOURCE_NAME = "data_gov_ma"

CKAN_BASE_URL = "https://data.gov.ma/data/api/3/action"

RAW_DOCUMENTS_BUCKET = "raw-documents"
RAW_JSON_BUCKET = "raw-json"
LOG_BUCKET = "raw-logs"

DATASET_IDS = [
    "universites-marocaines-2014",
    "etablissements-de-l-enseignement-superieur-universitaire-public-ouverts",
]

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"ingestion_datagovma_{datetime.now():%Y%m%d}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(SOURCE_NAME)

def build_session(max_retries: int = 5) -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def calculate_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def fetch_package(session: requests.Session, package_id: str) -> dict:
    response = session.get(f"{CKAN_BASE_URL}/package_show", params={"id": package_id}, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise RuntimeError(f"CKAN a renvoyé success=false pour '{package_id}': {payload.get('error')}")
    return payload["result"]

def download_resource(session: requests.Session, resource_url: str) -> tuple[bytes, int]:
    response = session.get(resource_url, timeout=60)
    response.raise_for_status()
    return response.content, response.status_code

def ingest_dataset(client: MinIOClient, session: requests.Session, package_id: str) -> dict:
    logger.info("Début ingestion dataset='%s'", package_id)

    now = datetime.now()
    year_str, month_str, day_str = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    timestamp_str = now.isoformat()
    ingestion_id = str(uuid.uuid4())

    resources_ingested = 0
    resources_failed = 0
    error_message = None

    try:
        package = fetch_package(session, package_id)
        resources = package.get("resources", [])

        package_object_name = (
            f"source={SOURCE_NAME}/entity={package_id}/"
            f"year={year_str}/month={month_str}/day={day_str}/package_metadata.json"
        )
        
        # CORRECTION ICI : Retrait de l'argument metadata non supporté par ayoub_client
        client.upload_json(
            bucket_name=RAW_JSON_BUCKET,
            object_name=package_object_name,
            data=package
        )

        for resource in resources:
            resource_id = resource.get("id", "unknown")
            resource_url = resource.get("url")
            resource_format = (resource.get("format") or "").strip().lower()

            if not resource_url:
                continue

            try:
                content, status = download_resource(session, resource_url)
                content_hash = calculate_sha256(content)
                filename = resource_url.rstrip("/").split("/")[-1] or f"{resource_id}.bin"
                bucket = RAW_JSON_BUCKET if resource_format == "json" else RAW_DOCUMENTS_BUCKET

                object_name = (
                    f"source={SOURCE_NAME}/entity={package_id}/"
                    f"year={year_str}/month={month_str}/day={day_str}/"
                    f"{content_hash[:10]}_{filename}"
                )

                # CORRECTION ICI : Retrait de l'argument metadata non supporté par ayoub_client
                client.upload_binary(
                    bucket_name=bucket,
                    object_name=object_name,
                    data=content,
                    content_type=resource.get("mimetype") or "application/octet-stream"
                )
                resources_ingested += 1

            except Exception as e:
                resources_failed += 1
                logger.exception("Échec ressource dataset=%s resource=%s : %s", package_id, resource_id, e)

    except Exception as e:
        error_message = str(e)
        logger.exception("Erreur critique dataset='%s' : %s", package_id, error_message)

    finally:
        log_payload = {
            "ingestion_id": ingestion_id,
            "source": SOURCE_NAME,
            "dataset_id": package_id,
            "timestamp": timestamp_str,
            "resources_ingested": resources_ingested,
            "resources_failed": resources_failed,
            "error": error_message,
            "connector_version": CONNECTOR_VERSION,
        }
        log_object_name = (
            f"source={SOURCE_NAME}/entity={package_id}/"
            f"year={year_str}/month={month_str}/day={day_str}/run_{ingestion_id}.json"
        )
        client.upload_json(bucket_name=LOG_BUCKET, object_name=log_object_name, data=log_payload)

    return {
        "dataset_id": package_id,
        "resources_ingested": resources_ingested,
        "resources_failed": resources_failed,
        "error": error_message,
    }

def run(dataset_ids: list[str]) -> dict:
    session = build_session()
    client = MinIOClient()
    summaries = [ingest_dataset(client, session, pkg_id) for pkg_id in dataset_ids]
    total_failed = sum(1 for s in summaries if s["error"] or s["resources_failed"] > 0)
    result = {"summaries": summaries, "datasets_with_errors": total_failed}
    if total_failed:
        raise RuntimeError(f"Ingestion data.gov.ma terminée avec erreurs : {result}")
    return result

def main() -> None:
    try:
        run(DATASET_IDS)
    except RuntimeError:
        sys.exit(1)

if __name__ == "__main__":
    main()
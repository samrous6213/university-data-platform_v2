"""
Ingestion API -> MinIO (zone raw).
Source : API Publique ORCID (Profils de chercheurs).
"""

import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.storage.minio.ayoub_client import MinIOClient

CONNECTOR_VERSION = "1.0.1"
SOURCE_NAME = "orcid_api"
API_BASE_URL = "https://pub.orcid.org/v3.0"

RAW_JSON_BUCKET = "raw-json"
LOG_BUCKET = "raw-logs"

# Identifiants ORCID à récupérer
# Remplacement des profils de démonstration par de vrais chercheurs de l'USMBA
ORCID_IDS = [
    "0009-0002-8430-6789", # Abdelhadi Razouki
    "0000-0002-3372-1168"  # Anas BOUAYAD
]

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"ingestion_orcid_{datetime.now():%Y%m%d}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(SOURCE_NAME)

def build_session(max_retries: int = 5) -> requests.Session:
    session = requests.Session()
    # On force l'API à nous répondre en JSON
    session.headers.update({"Accept": "application/json"}) 
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def ingest_orcid_profile(client: MinIOClient, session: requests.Session, orcid_id: str) -> dict:
    logger.info(f"Début ingestion profil ORCID : {orcid_id}")
    now = datetime.now()
    year_str, month_str, day_str = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    timestamp_str = now.isoformat()
    ingestion_id = str(uuid.uuid4())
    error_message = None

    try:
        url = f"{API_BASE_URL}/{orcid_id}"
        response = session.get(url, timeout=30)
        response.raise_for_status()
        profile_data = response.json() 

        # Architecture partitionnée pour Spark
        object_name = (
            f"source={SOURCE_NAME}/entity={orcid_id}/"
            f"year={year_str}/month={month_str}/day={day_str}/profile.json"
        )

        client.upload_json(
            bucket_name=RAW_JSON_BUCKET,
            object_name=object_name,
            data=profile_data
        )
        logger.info(f"Profil stocké avec succès : s3://{RAW_JSON_BUCKET}/{object_name}")

    except Exception as e:
        error_message = str(e)
        logger.error(f"Erreur lors de l'ingestion de {orcid_id}: {error_message}")

    finally:
        log_payload = {
            "ingestion_id": ingestion_id,
            "source": SOURCE_NAME,
            "orcid_id": orcid_id,
            "timestamp": timestamp_str,
            "status": "success" if not error_message else "failed",
            "error": error_message
        }
        log_object_name = (
            f"source={SOURCE_NAME}/entity={orcid_id}/"
            f"year={year_str}/month={month_str}/day={day_str}/run_{ingestion_id}.json"
        )
        client.upload_json(bucket_name=LOG_BUCKET, object_name=log_object_name, data=log_payload)

    return {"orcid_id": orcid_id, "error": error_message}

def main():
    client = MinIOClient()
    session = build_session()
    
    for orcid_id in ORCID_IDS:
        ingest_orcid_profile(client, session, orcid_id)

if __name__ == "__main__":
    main()
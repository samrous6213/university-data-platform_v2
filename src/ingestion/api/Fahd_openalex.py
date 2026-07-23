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

from  src.storage.minio.fahd_client import MinIOClient 

# ─── Paramètres ─────────────────────────────────────────────
CONNECTOR_VERSION = "1.0.0"
SOURCE_NAME = "openalex"

# Buckets conformes au brief
RAW_BUCKET = "raw-json"
LOG_BUCKET = "raw-logs"

BASE_URL = "https://api.openalex.org"
MAILTO = "data-team@example.ma"  # polite pool OpenAlex : moins de throttling
PER_PAGE = 100

# Institutions cibles -> OpenAlex institution ID (format "Ixxxxxxxxx").
# Recherche : https://api.openalex.org/institutions?search=<nom universite>
INSTITUTIONS = {
    "um5": "I126477371",
    "uca": "I119856527",
    "usmba": "I81605866",
    "uh2c": "I99297268",
}

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"ingestion_openalex_{datetime.now():%Y%m%d}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(SOURCE_NAME)


def build_session(max_retries: int = 5) -> requests.Session:
    """Session avec retry/backoff reel sur les erreurs transitoires (fix #1)."""
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=2,  # 2s, 4s, 8s, 16s, 32s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def calculate_sha256(data: list) -> str:
    """Hash SHA-256 sur la liste des résultats purs (idempotence stricte)."""
    data_str = json.dumps(data, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(data_str.encode("utf-8")).hexdigest()


def extract_openalex_works(session: requests.Session, institution_id: str, limit: int) -> tuple[list, int, str]:
    """
    Recupere les publications ('works') d'une institution, avec pagination reelle
    par curseur (fix #2) et filtre institution (fix #3), jusqu'a `limit` records
    (fix #4 : `limit` est desormais reellement applique).
    """
    results = []
    last_status = None
    last_url = None
    cursor = "*"

    while cursor and len(results) < limit:
        params = {
            "filter": f"institutions.id:{institution_id}",
            "per-page": min(PER_PAGE, limit - len(results)),
            "cursor": cursor,
            "mailto": MAILTO,
        }
        response = session.get(f"{BASE_URL}/works", params=params, timeout=30)
        last_status = response.status_code
        last_url = response.url
        response.raise_for_status()

        data = response.json()
        page_results = data.get("results", [])
        results.extend(page_results)

        cursor = data.get("meta", {}).get("next_cursor")
        if not page_results:
            break

    return results[:limit], last_status, last_url


def run(institution_key: str, institution_id: str, limit: int = 200) -> None:
    logger.info("Debut ingestion OpenAlex | institution=%s limit=%s", institution_key, limit)

    session = build_session()
    client = MinIOClient()
    now = datetime.now()

    year_str = now.strftime("%Y")
    month_str = now.strftime("%m")
    day_str = now.strftime("%d")
    timestamp_str = now.isoformat()

    ingestion_id = str(uuid.uuid4())
    status = 500
    records = 0
    error_message = None
    raw_object_path = None
    content_hash = ""
    source_url = f"{BASE_URL}/works?filter=institutions.id:{institution_id}"

    try:
        results, status, source_url = extract_openalex_works(session, institution_id, limit)
        records = len(results)
        content_hash = calculate_sha256(results)

        object_name = (
            f"source={SOURCE_NAME}/entity=works_{institution_key}/"
            f"year={year_str}/month={month_str}/day={day_str}/"
            f"openalex_{content_hash[:10]}.json"
        )
        raw_object_path = f"s3://{RAW_BUCKET}/{object_name}"

        payload = {
            "metadata": {
                "source": SOURCE_NAME,
                "institution": institution_key,
                "ingestion_id": ingestion_id,
                "connector_version": CONNECTOR_VERSION,
                "ingested_at": timestamp_str,
                "url_source": source_url,
                "http_status": status,
                "record_count": records,
                "content_hash": content_hash,
                "raw_object_path": raw_object_path,
            },
            "data": results,
        }

        client.upload_json(bucket_name=RAW_BUCKET, object_name=object_name, data=payload)
        logger.info("Raw stocke : %s (records=%s)", raw_object_path, records)

    except Exception as e:
        error_message = str(e)
        logger.exception("Erreur critique institution=%s : %s", institution_key, error_message)

    finally:
        log_payload = {
            "ingestion_id": ingestion_id,
            "source": SOURCE_NAME,
            "institution": institution_key,
            "status": status,
            "records_extracted": records,
            "timestamp": timestamp_str,
            "error": error_message,
            "connector_version": CONNECTOR_VERSION,
            "raw_object_path": raw_object_path,  # traçabilité raw <- log
        }

        log_object_name = (
            f"source={SOURCE_NAME}/entity=works_{institution_key}/"
            f"year={year_str}/month={month_str}/day={day_str}/"
            f"run_{ingestion_id}.json"
        )

        client.upload_json(bucket_name=LOG_BUCKET, object_name=log_object_name, data=log_payload)
        logger.info("Log stocke : s3://%s/%s", LOG_BUCKET, log_object_name)

        if error_message:
            raise RuntimeError(f"Ingestion OpenAlex en echec pour '{institution_key}': {error_message}")


def main() -> None:
    failures = 0
    for institution_key, institution_id in INSTITUTIONS.items():
        if institution_id.startswith("I_REPLACE_ME"):
            logger.warning("Institution '%s' ignoree : ID OpenAlex non configure.", institution_key)
            continue
        try:
            run(institution_key, institution_id, limit=200)
        except RuntimeError:
            failures += 1

    if failures:
        sys.exit(1)  # code de sortie non-nul : utile pour l'alerting cron / Airflow


if __name__ == "__main__":
    main()
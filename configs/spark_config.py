"""
Configuration centrale pour la couche transformation (Spark / Hudi / Hive).

Reprend EXACTEMENT les memes variables d'environnement que src/storage/minio/fahd_client.py
(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE) pour eviter toute
divergence de configuration entre l'ingestion et la transformation.

Ne pas dupliquer ce fichier ailleurs (pas de spark_config.py ou job_config.py concurrent
dans transformations/spark/config/) : spark_session.py importe ce module.
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ── MinIO / S3A ──────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").strip().lower() in {"1", "true", "yes", "y", "on"}

S3A_ENDPOINT = f"{'https' if MINIO_SECURE else 'http'}://{MINIO_ENDPOINT}"

# ── Buckets raw (doivent matcher les connecteurs d'ingestion) ───────────
RAW_WEB_HTML_BUCKET = "raw-web-html"
RAW_JSON_BUCKET = "raw-json"
RAW_DOCUMENTS_BUCKET = "raw-documents"
RAW_LOGS_BUCKET = "raw-logs"

# ── Hive Metastore ───────────────────────────────────────────────────────
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

# ── Hudi : chemin de base par table curated ─────────────────────────────
HUDI_WAREHOUSE_PATH = os.getenv("HUDI_WAREHOUSE_PATH", "s3a://curated/hudi")

HUDI_TABLES = {
    "faculty_profiles": {
        "base_path": f"{HUDI_WAREHOUSE_PATH}/faculty_profiles",
        "recordkey_field": "record_id",
        "precombine_field": "crawl_timestamp",
        "partitionpath_field": "source_system",
    },
    "course_catalog": {
        "base_path": f"{HUDI_WAREHOUSE_PATH}/course_catalog",
        "recordkey_field": "record_id",
        "precombine_field": "crawl_timestamp",
        "partitionpath_field": "source_system",
    },
}

# ── Divers ────────────────────────────────────────────────────────────
APP_NAME = "university-data-platform-transform"
LOG_DIR = os.getenv("SPARK_LOG_DIR", "logs")
MAX_WRITE_RETRIES = int(os.getenv("MAX_WRITE_RETRIES", "3"))
RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", "2"))
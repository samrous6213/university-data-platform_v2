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
# FIX : "hive-metastore" (nom de service docker-compose) n'est resoluble que
# DANS le reseau Docker. Le job Spark tourne ici nativement sous Windows
# (.venv), donc le nom d'hote doit etre "localhost" (ou l'IP de la machine
# Docker si WSL2/Docker Desktop expose differemment), a condition que le
# port 9083 soit bien mappe dans docker-compose.yml (ports: "9083:9083").
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://localhost:9083")

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

# ── Postgres analytique (dashboard Metabase) ─────────────────────────────
# Base separee ("analytics") sur la MEME instance Postgres que le Hive
# Metastore (pas de nouveau conteneur/port). Ne jamais utiliser la base
# "metastore" ici : ce sont les tables internes de Hive (TBLS, SDS, ...).
# Spark tourne nativement (.venv), donc host = localhost, comme pour
# HIVE_METASTORE_URI ci-dessus.
POSTGRES_ANALYTICS_HOST = os.getenv("POSTGRES_ANALYTICS_HOST", "127.0.0.1")
POSTGRES_ANALYTICS_PORT = os.getenv("POSTGRES_ANALYTICS_PORT", "5432")
POSTGRES_ANALYTICS_DB = os.getenv("POSTGRES_ANALYTICS_DB", "analytics")
POSTGRES_ANALYTICS_USER = os.getenv("POSTGRES_ANALYTICS_USER", "hive")
POSTGRES_ANALYTICS_PASSWORD = os.getenv("POSTGRES_ANALYTICS_PASSWORD", "hive")

POSTGRES_ANALYTICS_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_ANALYTICS_HOST}:{POSTGRES_ANALYTICS_PORT}/{POSTGRES_ANALYTICS_DB}"
)

# Chemin local vers le driver JDBC deja utilise par hive-metastore
# (./jdbc/postgresql-42.7.3.jar). Charge dynamiquement dans le SparkContext
# par postgres_writer.py, pas besoin de toucher spark_session.py.
JDBC_DRIVER_JAR = os.getenv("JDBC_DRIVER_JAR", os.path.join("jdbc", "postgresql-42.7.3.jar"))

# table curated Hudi -> table Postgres cible (meme nom pour l'instant)
POSTGRES_TABLES = {
    "faculty_profiles": "faculty_profiles",
    "course_catalog": "course_catalog",
}

# ── Elasticsearch (index de recherche) ───────────────────────────────────
# Meme logique que Postgres : Spark tourne nativement (.venv), donc host =
# localhost (le port 9200 est mappe dans docker-compose.yml).
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "127.0.0.1")
ELASTICSEARCH_PORT = int(os.getenv("ELASTICSEARCH_PORT", "9200"))
ELASTICSEARCH_URL = f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"

# Un seul index pour les deux entites (faculty_profiles + course_catalog),
# distinguees par le champ "entity_type" -- permet une recherche unifiee
# (ex: chercher "intelligence artificielle" et trouver profils ET formations).
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "university_search")

# ── Divers ────────────────────────────────────────────────────────────
APP_NAME = "university-data-platform-transform"
LOG_DIR = os.getenv("SPARK_LOG_DIR", "logs")
MAX_WRITE_RETRIES = int(os.getenv("MAX_WRITE_RETRIES", "3"))
RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", "2"))
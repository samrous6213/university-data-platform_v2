import logging
import hashlib
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/spark/work-dir")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from bs4 import BeautifulSoup

from src.storage.minio.hiba_client import MinIOClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BUCKET       = "raw-web-html"
OUTPUT_PATH  = os.getenv(
    "HTML_PARSER_OUTPUT",
    "/opt/spark/work-dir/data/processed_html",
)
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
CRAWLER_VERSION = os.getenv("CRAWLER_VERSION", "chaimae_html_parser_v1")
MAX_TEXT_LEN = 100_000

# Schéma explicite : évite les erreurs d'inférence Spark si un champ est vide
RECORD_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("source_system", StringType(), True),
    StructField("storage_path", StringType(), False),
    StructField("page_title", StringType(), True),
    StructField("normalized_text", StringType(), True),
    StructField("http_status", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawler_version", StringType(), True),
    StructField("crawl_timestamp", StringType(), False),
])


def extract_source_system(object_name: str) -> str:
    for part in object_name.split("/"):
        if part.startswith("source="):
            return part.replace("source=", "")
    return "unknown"


def compute_content_hash(raw_bytes: bytes) -> str:
    return hashlib.md5(raw_bytes).hexdigest()


def build_record(object_name: str, html_content: str, content_hash: str, http_status: str) -> dict:
    """Parse le HTML et construit un enregistrement structuré."""

    try:
        soup  = BeautifulSoup(html_content, "html.parser")
        title = soup.title.get_text(strip=True) if soup.title else ""
        text  = soup.get_text(separator=" ", strip=True)
    except Exception as e:
        logger.warning(f"[PARSE] BeautifulSoup failed for {object_name}: {e}")
        title, text = "", ""

    return {
        "record_id": hashlib.md5(object_name.encode("utf-8")).hexdigest(),
        "source_system": extract_source_system(object_name),
        "storage_path": object_name,
        "page_title": title,
        "normalized_text": text[:MAX_TEXT_LEN],
        "http_status": http_status,
        "content_hash": content_hash,
        "crawler_version": CRAWLER_VERSION,
        "crawl_timestamp": datetime.now().isoformat(),
    }


def fetch_html_objects(client: MinIOClient, bucket: str):
    """Liste et télécharge tous les fichiers .html depuis MinIO."""

    logger.info(f"Listing objects in bucket '{bucket}' ...")

    try:
        objects = list(client.client.list_objects(bucket, recursive=True))
    except Exception as e:
        logger.error(f"Could not list objects in '{bucket}': {e}")
        return []

    html_objects = [obj for obj in objects if obj.object_name.endswith(".html")]
    logger.info(f"Found {len(html_objects)} HTML files")

    records = []
    errors  = 0

    for i, obj in enumerate(html_objects, start=1):

        logger.info(f"Processing HTML file: {obj.object_name}")

        try:
            response = client.client.get_object(bucket, obj.object_name)

            # http_status n'est pas fourni par le SDK MinIO pour un objet déjà stocké ;
            # on conserve le code de statut HTTP renvoyé par l'appel S3 lui-même.
            http_status = str(getattr(response, "status", "unknown"))

            try:
                raw_bytes = response.read()
                html = raw_bytes.decode("utf-8", errors="ignore")
            finally:
                # Toujours fermer la connexion, même si le decode échoue
                response.close()
                response.release_conn()

            content_hash = compute_content_hash(raw_bytes)

            records.append(
                build_record(obj.object_name, html, content_hash, http_status)
            )

        except Exception as e:
            errors += 1
            logger.error(f"[FETCH] Failed on {obj.object_name}: {e}")

        if i % 100 == 0:
            logger.info(f"{i}/{len(html_objects)} files processed...")

    logger.info(f"{len(records)} records built | {errors} errors")
    return records


def main():

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName("HTML Parser")
        .getOrCreate()
    )

    logger.info("Connecting to MinIO ...")

    client = MinIOClient(
        endpoint=os.getenv("MINIO_ENDPOINT", "university-minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )

    logger.info(f"Reading HTML files from '{BUCKET}' ...")
    records = fetch_html_objects(client, BUCKET)

    if not records:
        logger.warning("No HTML files found. Stopping job.")
        spark.stop()
        return

    # Schéma explicite — pas d'inférence risquée sur gros volumes
    df = spark.createDataFrame(records, schema=RECORD_SCHEMA)

    logger.info("Schema:")
    df.printSchema()

    logger.info("Sample rows:")
    df.show(5, truncate=False)

    row_count = df.count()

    try:
        (
            df.write
            .mode("overwrite")
            .parquet(OUTPUT_PATH)
        )
        logger.info(f"Parquet saved to: {OUTPUT_PATH}")
        logger.info(f"Rows written: {row_count}")

    except Exception as e:
        logger.error(f"Failed to write Parquet output: {e}")
        raise

    finally:
        logger.info("HTML parsing completed successfully.")
        spark.stop()


if __name__ == "__main__":
    main()
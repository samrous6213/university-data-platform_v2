import os
import sys
import json
import hashlib
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *

PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    "/opt/spark/work-dir"
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.storage.minio.hiba_client import MinIOClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

BUCKETS = [
    "raw-json",
    "raw-documents",
    "raw-web-html",
    "raw-logs"
]

OUTPUT_PATH = os.getenv(
    "JSON_PARSER_OUTPUT",
    "/opt/spark/work-dir/data/processed_json"
)

SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "spark://spark-master:7077"
)

CRAWLER_VERSION = os.getenv(
    "CRAWLER_VERSION",
    "chaimae_json_parser_v1"
)

MAX_TEXT_LEN = 100000

SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("bucket_name", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("storage_path", StringType(), False),
    StructField("file_name", StringType(), True),
    StructField("document_type", StringType(), True),
    StructField("extracted_text", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawler_version", StringType(), True),
    StructField("crawl_timestamp", StringType(), False),
])


def extract_source_system(path):

    for part in path.split("/"):

        if part.startswith("source="):
            return part.replace("source=", "")

    return "unknown"


def compute_content_hash(raw_bytes):

    return hashlib.md5(
        raw_bytes
    ).hexdigest()


def json_to_text(obj):

    if isinstance(obj, dict):

        parts = []

        for key, value in obj.items():

            parts.append(
                f"{key}: {json_to_text(value)}"
            )

        return " ".join(parts)

    elif isinstance(obj, list):

        return " ".join(
            json_to_text(item)
            for item in obj
        )

    return str(obj)


def build_record(
    bucket_name,
    object_name,
    text,
    content_hash
):

    return {

        "record_id":
            hashlib.md5(
                f"{bucket_name}:{object_name}".encode("utf-8")
            ).hexdigest(),

        "bucket_name":
            bucket_name,

        "source_system":
            extract_source_system(
                object_name
            ),

        "storage_path":
            object_name,

        "file_name":
            os.path.basename(
                object_name
            ),

        "document_type":
            "json",

        "extracted_text":
            text[:MAX_TEXT_LEN],

        "content_hash":
            content_hash,

        "crawler_version":
            CRAWLER_VERSION,

        "crawl_timestamp":
            datetime.now().isoformat()
    }


def main():

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName("Universal JSON Parser")
        .getOrCreate()
    )

    logger.info(
        "Connecting to MinIO..."
    )

    client = MinIOClient(
        endpoint=os.getenv(
            "MINIO_ENDPOINT",
            "university-minio:9000"
        ),
        access_key=os.getenv(
            "MINIO_ACCESS_KEY",
            "minioadmin"
        ),
        secret_key=os.getenv(
            "MINIO_SECRET_KEY",
            "minioadmin"
        ),
        secure=os.getenv(
            "MINIO_SECURE",
            "false"
        ).lower() == "true"
    )

    records = []

    processed = 0

    for bucket in BUCKETS:

        logger.info(
            f"Scanning bucket: {bucket}"
        )

        try:

            for obj in client.client.list_objects(
                bucket,
                recursive=True
            ):

                if not obj.object_name.lower().endswith(".json"):
                    continue

                logger.info(
                    f"Processing JSON file: {bucket}/{obj.object_name}"
                )

                try:

                    response = client.client.get_object(
                        bucket,
                        obj.object_name
                    )

                    try:

                        raw_bytes = response.read()

                        content = raw_bytes.decode(
                            "utf-8",
                            errors="ignore"
                        )

                    finally:

                        response.close()
                        response.release_conn()

                    content_hash = compute_content_hash(
                        raw_bytes
                    )

                    data = json.loads(
                        content
                    )

                    text = json_to_text(
                        data
                    )

                    records.append(
                        build_record(
                            bucket,
                            obj.object_name,
                            text,
                            content_hash
                        )
                    )

                    processed += 1

                    if processed % 50 == 0:

                        logger.info(
                            f"{processed} JSON processed..."
                        )

                except Exception as e:

                    logger.error(
                        f"Failed: {bucket}/{obj.object_name}"
                    )

                    logger.error(
                        str(e)
                    )

        except Exception as e:

            logger.error(
                f"Cannot scan bucket {bucket}: {e}"
            )

    logger.info(
        f"Total JSON files = {processed}"
    )

    if not records:

        logger.warning(
            "No JSON files found. Stopping Spark gracefully."
        )

        spark.stop()
        return

    df = spark.createDataFrame(
        records,
        schema=SCHEMA
    )

    logger.info(
        "Spark schema:"
    )

    df.printSchema()

    logger.info(
        "Sample rows:"
    )

    df.show(
        10,
        truncate=False
    )

    (
        df.write
        .mode("overwrite")
        .parquet(
            OUTPUT_PATH
        )
    )

    logger.info(
        f"Saved to {OUTPUT_PATH}"
    )

    logger.info(
        f"Rows = {df.count()}"
    )

    logger.info(
        "JSON parsing completed successfully."
    )

    spark.stop()


if __name__ == "__main__":
    main()
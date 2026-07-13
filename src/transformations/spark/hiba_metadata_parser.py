import os
import sys
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
    "raw-web-html",
    "raw-documents",
    "raw-images",
    "raw-json",
    "raw-logs"
]

OUTPUT_PATH = os.getenv(
    "METADATA_PARSER_OUTPUT",
    "/opt/spark/work-dir/data/processed_metadata"
)

SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "spark://spark-master:7077"
)

CRAWLER_VERSION = os.getenv(
    "CRAWLER_VERSION",
    "chaimae_metadata_parser_v1"
)

SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("bucket_name", StringType(), False),
    StructField("source_system", StringType(), True),
    StructField("storage_path", StringType(), False),
    StructField("file_name", StringType(), True),
    StructField("file_extension", StringType(), True),
    StructField("file_size", LongType(), True),
    StructField("content_hash", StringType(), True),
    StructField("last_modified", StringType(), True),
    StructField("etag", StringType(), True),
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


def build_record(
    bucket_name,
    object_name,
    file_size,
    content_hash,
    last_modified,
    etag
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

        "file_extension":
            os.path.splitext(
                object_name
            )[1]
            .lower()
            .replace(".", ""),

        "file_size":
            file_size,

        "content_hash":
            content_hash,

        "last_modified":
            str(last_modified) if last_modified else None,

        "etag":
            etag,

        "crawler_version":
            CRAWLER_VERSION,

        "crawl_timestamp":
            datetime.now().isoformat()
    }


def main():

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName("Raw Zone Metadata Scanner")
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

    success_count = 0
    failure_count = 0
    processed_count = 0

    for bucket in BUCKETS:

        logger.info(
            f"Scanning bucket: {bucket}"
        )

        try:

            objects = list(
                client.client.list_objects(
                    bucket,
                    recursive=True
                )
            )

        except Exception as e:

            logger.error(
                f"Cannot scan bucket {bucket}: {e}"
            )

            continue

        logger.info(
            f"Objects found in {bucket} = {len(objects)}"
        )

        for obj in objects:

            logger.info(
                f"Processing object: {bucket}/{obj.object_name}"
            )

            try:

                response = client.client.get_object(
                    bucket,
                    obj.object_name
                )

                raw_bytes = response.read()

                response.close()
                response.release_conn()

                file_size = len(raw_bytes)

                content_hash = compute_content_hash(
                    raw_bytes
                )

                last_modified = getattr(
                    obj,
                    "last_modified",
                    None
                )

                etag = getattr(
                    obj,
                    "etag",
                    None
                )

                records.append(
                    build_record(
                        bucket,
                        obj.object_name,
                        file_size,
                        content_hash,
                        last_modified,
                        etag
                    )
                )

                success_count += 1

            except Exception as e:

                failure_count += 1

                logger.error(
                    f"Failed: {bucket}/{obj.object_name}"
                )

                logger.error(
                    str(e)
                )

            finally:

                processed_count += 1

                if processed_count % 50 == 0:

                    logger.info(
                        f"{processed_count} objects processed..."
                    )

    logger.info(
        f"Total objects processed = {processed_count}"
    )

    logger.info(
        f"Successful = {success_count}"
    )

    logger.info(
        f"Failed = {failure_count}"
    )

    if len(records) == 0:

        logger.warning(
            "No objects found across raw buckets. Stopping Spark gracefully."
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
        "Raw zone metadata scan completed successfully."
    )

    spark.stop()


if __name__ == "__main__":
    main()
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

BUCKET = "raw-logs"

OUTPUT_PATH = os.getenv(
    "LOGS_PARSER_OUTPUT",
    "/opt/spark/work-dir/data/processed_logs"
)

SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "spark://spark-master:7077"
)

CRAWLER_VERSION = os.getenv(
    "CRAWLER_VERSION",
    "chaimae_logs_parser_v1"
)

SUPPORTED_EXTENSIONS = [
    ".log",
    ".txt",
    ".json",
    ".csv"
]

MAX_TEXT_LEN = 100000

SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("source_system", StringType(), True),
    StructField("storage_path", StringType(), False),
    StructField("file_name", StringType(), True),
    StructField("log_type", StringType(), True),
    StructField("line_count", IntegerType(), True),
    StructField("file_size", LongType(), True),
    StructField("log_preview", StringType(), True),
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


def build_record(
    object_name,
    log_type,
    line_count,
    file_size,
    log_preview,
    content_hash
):

    return {

        "record_id":
            hashlib.md5(
                object_name.encode("utf-8")
            ).hexdigest(),

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

        "log_type":
            log_type,

        "line_count":
            line_count,

        "file_size":
            file_size,

        "log_preview":
            log_preview,

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
        .appName("Logs Parser")
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

    logger.info(
        f"Scanning bucket: {BUCKET}"
    )

    log_objects = []

    for obj in client.client.list_objects(
        BUCKET,
        recursive=True
    ):

        extension = (
            os.path.splitext(
                obj.object_name
            )[1]
            .lower()
        )

        if extension not in SUPPORTED_EXTENSIONS:

            logger.info(
                f"Skipping unsupported file: {obj.object_name}"
            )

            continue

        log_objects.append(obj)

    logger.info(
        f"Log files found = {len(log_objects)}"
    )

    if len(log_objects) == 0:

        logger.warning(
            "No log files found in bucket. Stopping Spark gracefully."
        )

        spark.stop()

        return

    records = []

    success_count = 0
    failure_count = 0
    processed_count = 0

    for obj in log_objects:

        extension = (
            os.path.splitext(
                obj.object_name
            )[1]
            .lower()
        )

        logger.info(
            f"Processing log file: {obj.object_name}"
        )

        try:

            response = client.client.get_object(
                BUCKET,
                obj.object_name
            )

            raw_bytes = response.read()

            response.close()
            response.release_conn()

            file_size = len(raw_bytes)

            content_hash = compute_content_hash(
                raw_bytes
            )

            text = raw_bytes.decode(
                "utf-8",
                errors="ignore"
            )

            lines = text.splitlines()

            line_count = len(lines)

            log_preview = "\n".join(
                lines[:20]
            )[:MAX_TEXT_LEN]

            logger.info(
                f"Lines = {line_count}, size = {file_size} bytes"
            )

            records.append(
                build_record(
                    obj.object_name,
                    extension.replace(".", ""),
                    line_count,
                    file_size,
                    log_preview,
                    content_hash
                )
            )

            success_count += 1

        except Exception as e:

            failure_count += 1

            logger.error(
                f"Failed: {obj.object_name}"
            )

            logger.error(
                str(e)
            )

        finally:

            processed_count += 1

            if processed_count % 25 == 0:

                logger.info(
                    f"{processed_count} log files processed..."
                )

    logger.info(
        f"Total log files processed = {processed_count}"
    )

    logger.info(
        f"Successful = {success_count}"
    )

    logger.info(
        f"Failed = {failure_count}"
    )

    if len(records) == 0:

        logger.warning(
            "No valid log records extracted. Stopping Spark gracefully."
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
        "Logs parsing completed successfully."
    )

    spark.stop()


if __name__ == "__main__":
    main()
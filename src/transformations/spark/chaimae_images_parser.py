import os
import sys
import hashlib
import logging
from datetime import datetime
from io import BytesIO

from PIL import Image

from pyspark.sql import SparkSession
from pyspark.sql.types import *

PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    "/opt/spark/work-dir"
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.storage.minio.chaimae_client import MinIOClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

BUCKET = "raw-images"

OUTPUT_PATH = os.getenv(
    "IMAGE_PARSER_OUTPUT",
    "/opt/spark/work-dir/data/processed_images"
)

SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "spark://spark-master:7077"
)

CRAWLER_VERSION = os.getenv(
    "CRAWLER_VERSION",
    "chaimae_image_parser_v1"
)

SUPPORTED_EXTENSIONS = [
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".svg"
]

SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("source_system", StringType(), True),
    StructField("storage_path", StringType(), False),
    StructField("file_name", StringType(), True),
    StructField("image_type", StringType(), True),
    StructField("image_width", IntegerType(), True),
    StructField("image_height", IntegerType(), True),
    StructField("image_mode", StringType(), True),
    StructField("image_format", StringType(), True),
    StructField("file_size", LongType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawler_version", StringType(), True),
    StructField("crawl_timestamp", StringType(), False),
])


def extract_source_system(path):

    for part in path.split("/"):

        if part.startswith("source="):
            return part.replace("source=", "")

    return "unknown"


def compute_content_hash(file_bytes):

    return hashlib.md5(
        file_bytes
    ).hexdigest()


def extract_image_metadata(
    object_name,
    image_bytes
):

    extension = (
        os.path.splitext(
            object_name
        )[1]
        .lower()
    )

    width = None
    height = None
    mode = None
    image_format = None

    if extension == ".svg":

        image_format = "svg"

        logger.info(
            f"SVG file detected, skipping pixel metadata extraction: {object_name}"
        )

    else:

        image = Image.open(
            BytesIO(image_bytes)
        )

        image.verify()

        image = Image.open(
            BytesIO(image_bytes)
        )

        width, height = image.size
        mode = image.mode
        image_format = image.format

        logger.info(
            f"Dimensions = {width}x{height}, "
            f"format = {image_format}, "
            f"mode = {mode}"
        )

    return width, height, mode, image_format


def build_record(
    object_name,
    image_type,
    width,
    height,
    mode,
    image_format,
    file_size,
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

        "image_type":
            image_type,

        "image_width":
            width,

        "image_height":
            height,

        "image_mode":
            mode,

        "image_format":
            image_format,

        "file_size":
            file_size,

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
        .appName("Image Metadata Parser")
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

    image_objects = []

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

        image_objects.append(obj)

    logger.info(
        f"Image files found = {len(image_objects)}"
    )

    if len(image_objects) == 0:

        logger.warning(
            "No images found in bucket. Stopping Spark gracefully."
        )

        spark.stop()

        return

    records = []

    success_count = 0
    failure_count = 0
    processed_count = 0

    for obj in image_objects:

        extension = (
            os.path.splitext(
                obj.object_name
            )[1]
            .lower()
        )

        logger.info(
            f"Processing image: {obj.object_name}"
        )

        try:

            response = client.client.get_object(
                BUCKET,
                obj.object_name
            )

            image_bytes = response.read()

            response.close()
            response.release_conn()

            file_size = len(image_bytes)

            content_hash = compute_content_hash(
                image_bytes
            )

            width, height, mode, image_format = extract_image_metadata(
                obj.object_name,
                image_bytes
            )

            records.append(
                build_record(
                    obj.object_name,
                    extension.replace(".", ""),
                    width,
                    height,
                    mode,
                    image_format,
                    file_size,
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
                    f"{processed_count} images processed..."
                )

    logger.info(
        f"Total images processed = {processed_count}"
    )

    logger.info(
        f"Successful = {success_count}"
    )

    logger.info(
        f"Failed = {failure_count}"
    )

    if len(records) == 0:

        logger.warning(
            "No valid image records extracted. Stopping Spark gracefully."
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
        "Image metadata parsing completed successfully."
    )

    spark.stop()


if __name__ == "__main__":
    main()
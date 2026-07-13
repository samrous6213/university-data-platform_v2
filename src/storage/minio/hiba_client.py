import json
import logging
import os
from io import BytesIO

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOClient:

    def __init__(
        self,
        endpoint=None,
        access_key=None,
        secret_key=None,
        secure=None,
    ):

        endpoint = endpoint or os.getenv(
            "MINIO_ENDPOINT",
            "minio:9000"
        )

        access_key = access_key or os.getenv(
            "MINIO_ACCESS_KEY",
            "minioadmin"
        )

        secret_key = secret_key or os.getenv(
            "MINIO_SECRET_KEY",
            "minioadmin"
        )

        if secure is None:
            secure = (
                os.getenv(
                    "MINIO_SECURE",
                    "false"
                ).lower()
                == "true"
            )

        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def create_bucket_if_not_exists(
        self,
        bucket_name,
    ):

        try:

            if not self.client.bucket_exists(
                bucket_name
            ):
                self.client.make_bucket(
                    bucket_name
                )

                logger.info(
                    f"Bucket created: {bucket_name}"
                )

        except S3Error as e:

            logger.error(
                f"Bucket error: {e}"
            )

            raise

    def upload_json(
        self,
        bucket_name,
        object_name,
        data,
    ):

        self.create_bucket_if_not_exists(
            bucket_name
        )

        payload = json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")

        self.client.put_object(
            bucket_name,
            object_name,
            BytesIO(payload),
            len(payload),
            content_type="application/json",
        )

        logger.info(
            f"JSON uploaded -> {object_name}"
        )

    def upload_binary(
        self,
        bucket_name,
        object_name,
        data,
        content_type,
    ):

        self.create_bucket_if_not_exists(
            bucket_name
        )

        self.client.put_object(
            bucket_name,
            object_name,
            BytesIO(data),
            len(data),
            content_type=content_type,
        )

        logger.info(
            f"File uploaded -> {object_name}"
        )

    def list_objects(
        self,
        bucket_name,
        prefix="",
    ):

        return self.client.list_objects(
            bucket_name,
            prefix=prefix,
            recursive=True,
        )

    def object_exists(
        self,
        bucket_name,
        object_name,
    ):

        try:

            self.client.stat_object(
                bucket_name,
                object_name,
            )

            return True

        except Exception:

            return False
import json
import logging
import os
from io import BytesIO
from typing import Optional, Union

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOClient:

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: Optional[bool] = None,
    ):

        # Pour exécution locale Windows
        endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")

        access_key = access_key or os.getenv(
            "MINIO_ACCESS_KEY",
            "minioadmin"
        )

        secret_key = secret_key or os.getenv(
            "MINIO_SECRET_KEY",
            "minioadmin"
        )

        if secure is None:
            secure_env = os.getenv(
                "MINIO_SECURE",
                "false"
            ).strip().lower()

            secure = secure_env in {
                "1",
                "true",
                "yes",
                "y",
                "on"
            }

        print("\n==============================")
        print(f"MINIO ENDPOINT : {endpoint}")
        print(f"MINIO USER     : {access_key}")
        print(f"MINIO SECURE   : {secure}")
        print("==============================\n")

        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def create_bucket_if_not_exists(
        self,
        bucket_name: str
    ) -> None:

        try:

            if not self.client.bucket_exists(bucket_name):

                self.client.make_bucket(bucket_name)

                logger.info(
                    f"Bucket created: {bucket_name}"
                )

        except S3Error as e:

            logger.error(
                f"Failed to create bucket '{bucket_name}': {e}"
            )

            raise

    def upload_json(
        self,
        bucket_name: str,
        object_name: str,
        data: Union[dict, list],
    ) -> None:

        self.create_bucket_if_not_exists(bucket_name)

        payload = json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")

        try:

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

        except S3Error as e:

            logger.error(
                f"Failed to upload JSON '{object_name}': {e}"
            )

            raise

    def upload_binary(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        content_type: str,
    ) -> None:

        self.create_bucket_if_not_exists(bucket_name)

        try:

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

        except S3Error as e:

            logger.error(
                f"Failed to upload file '{object_name}': {e}"
            )

            raise
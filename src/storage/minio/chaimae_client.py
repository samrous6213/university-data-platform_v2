from minio import Minio
import json
from io import BytesIO


class MinIOClient:

    def __init__(self):

        self.client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )

    def create_bucket_if_not_exists(
        self,
        bucket_name
    ):

        if not self.client.bucket_exists(
            bucket_name
        ):
            self.client.make_bucket(
                bucket_name
            )

    def upload_json(
        self,
        bucket_name,
        object_name,
        data
    ):

        self.create_bucket_if_not_exists(
            bucket_name
        )

        payload = json.dumps(
            data,
            ensure_ascii=False,
            indent=2
        ).encode("utf-8")

        self.client.put_object(
            bucket_name,
            object_name,
            BytesIO(payload),
            len(payload),
            content_type="application/json"
        )

        print(
            f"JSON uploaded -> {object_name}"
        )

    def upload_binary(
        self,
        bucket_name,
        object_name,
        data,
        content_type
    ):

        self.create_bucket_if_not_exists(
            bucket_name
        )

        self.client.put_object(
            bucket_name,
            object_name,
            BytesIO(data),
            len(data),
            content_type=content_type
        )

        print(
            f"File uploaded -> {object_name}"
        )
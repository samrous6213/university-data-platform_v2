from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession


class SparkConfig:
    def __init__(
        self,
        app_name: str = "UniversityDataPlatform",
        master: Optional[str] = None,
        minio_endpoint: Optional[str] = None,
        minio_access_key: Optional[str] = None,
        minio_secret_key: Optional[str] = None,
        hive_metastore_uris: Optional[str] = None,
        warehouse_dir: Optional[str] = None,
    ) -> None:
        self.app_name = app_name
        self.master = master or os.getenv("SPARK_MASTER", "local[*]")
        self.minio_endpoint = minio_endpoint or os.getenv(
            "MINIO_ENDPOINT", "http://university-minio:9000"
        )
        self.minio_access_key = minio_access_key or os.getenv(
            "MINIO_ACCESS_KEY", "minioadmin"
        )
        self.minio_secret_key = minio_secret_key or os.getenv(
            "MINIO_SECRET_KEY", "minioadmin"
        )
        self.hive_metastore_uris = hive_metastore_uris or os.getenv(
            "HIVE_METASTORE_URIS", "thrift://hive-metastore:9083"
        )
        self.warehouse_dir = warehouse_dir or os.getenv(
            "WAREHOUSE_DIR", "/user/hive/warehouse"
        )

    def build(self) -> SparkSession:
        builder = SparkSession.builder.appName(self.app_name)

        if self.master:
            builder = builder.master(self.master)

        return (
            builder
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.executor.memory", "1536m")
            .config("spark.sql.shuffle.partitions", "800")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.kryoserializer.buffer.max", "256m")
            .config(
                "spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("hive.metastore.uris", self.hive_metastore_uris)
            .config("spark.sql.warehouse.dir", self.warehouse_dir)
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .enableHiveSupport()
            .getOrCreate()
        )

"""
Construction centralisee de la SparkSession pour tous les jobs de transformation.

Un seul endroit definit les jars Hudi, la config S3A (MinIO) et l'URI Hive Metastore,
pour garantir que faculty_profiles_job.py et course_catalog_job.py utilisent
exactement la meme configuration (pas de divergence entre jobs).
"""

from pyspark.sql import SparkSession

from configs.spark_config import (
    APP_NAME,
    HIVE_METASTORE_URI,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    S3A_ENDPOINT,
)

HUDI_PACKAGE = "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1"
AWS_BUNDLE_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


def get_spark_session(app_name: str | None = None) -> SparkSession:
    """
    Retourne une SparkSession configuree pour :
      - lire/ecrire sur MinIO via S3A
      - lire/ecrire des tables Hudi
      - se synchroniser avec Hive Metastore (SQL access, brief section 3)
    """
    builder = (
        SparkSession.builder.appName(app_name or APP_NAME)
        .config("spark.jars.packages", f"{HUDI_PACKAGE},{AWS_BUNDLE_PACKAGE}")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        # ── S3A / MinIO ──
        .config("spark.hadoop.fs.s3a.endpoint", S3A_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Hive Metastore ──
        .config("hive.metastore.uris", HIVE_METASTORE_URI)
        .enableHiveSupport()
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
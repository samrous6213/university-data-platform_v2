r"""
Configuration Spark/MinIO/Hudi/Hive + écriture des 4 tables curated.
Point d'entrée unique de l'étape 6.

Peut être lancé directement :
    cd D:\university-data-platform_v2\src\transformations\spark
    python write_hudi.py
"""

import os
import sys
from pathlib import Path

# --- Calcule la racine du projet dynamiquement, peu importe d'où le script est lancé ---
# Ce fichier est à : <ROOT>/src/transformations/spark/write_hudi.py
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]  # remonte : spark -> transformations -> src -> ROOT
sys.path.insert(0, str(PROJECT_ROOT))

from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession

from src.transformations.spark.transform_faculty import transform_faculty
from src.transformations.spark.transform_courses import transform_courses
from src.transformations.spark.transform_publications import transform_publications
from src.transformations.spark.transform_news import transform_news

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
HIVE_METASTORE_URIS = os.getenv("HIVE_METASTORE_URIS", "thrift://hive-metastore:9083")
HUDI_WAREHOUSE_PATH = os.getenv("HUDI_WAREHOUSE_PATH", "s3a://curated-zone/hudi_warehouse")
HIVE_DATABASE = "university_lakehouse"

RAW_PATHS = {
    "faculty": "s3a://raw-json/faculty_profiles/",
    "courses": "s3a://raw-json/source=mit_ocw/",
    "publications": "s3a://raw-json/source=crossref/",
    "news": "s3a://raw-json/university_news/",
}


def get_spark_session(app_name: str = "university-etape6") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ]),
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("hive.metastore.uris", HIVE_METASTORE_URIS)
        .enableHiveSupport()
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@dataclass
class HudiTableConfig:
    name: str
    record_key: str
    partition_field: str
    precombine_field: str = "crawl_timestamp"
    table_type: str = "COPY_ON_WRITE"

    @property
    def base_path(self) -> str:
        return f"{HUDI_WAREHOUSE_PATH}/{self.name}"

    def hudi_options(self) -> dict:
        return {
            "hoodie.table.name": self.name,
            "hoodie.datasource.write.table.type": self.table_type,
            "hoodie.datasource.write.recordkey.field": self.record_key,
            "hoodie.datasource.write.partitionpath.field": self.partition_field,
            "hoodie.datasource.write.precombine.field": self.precombine_field,
            "hoodie.datasource.write.hive_style_partitioning": "true",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.database": HIVE_DATABASE,
            "hoodie.datasource.hive_sync.table": self.name,
            "hoodie.datasource.hive_sync.partition_fields": self.partition_field,
            "hoodie.datasource.hive_sync.partition_extractor_class": (
                "org.apache.hudi.hive.MultiPartKeysValueExtractor"
            ),
            "hoodie.datasource.hive_sync.mode": "hms",
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            "hoodie.datasource.hive_sync.metastore.uris": HIVE_METASTORE_URIS,   # <- LIGNE AJOUTÉE
        }


FACULTY_PROFILES = HudiTableConfig("faculty_profiles", "record_id", "faculty")
COURSE_CATALOG = HudiTableConfig("course_catalog", "record_id", "department")
RESEARCH_PUBLICATIONS = HudiTableConfig("research_publications", "record_id", "publication_year")
UNIVERSITY_NEWS = HudiTableConfig("university_news", "record_id", "publication_year")


def ensure_database_exists(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")


def write_to_hudi(df: DataFrame, table_config: HudiTableConfig) -> None:
    count = df.count()
    print(f"→ Upsert vers Hudi : {table_config.name} ({count} lignes) → {table_config.base_path}")
    if count == 0:
        print(f"⚠️  Aucune ligne pour '{table_config.name}', on saute.")
        return
    (
        df.write.format("hudi")
        .options(**table_config.hudi_options())
        .mode("append")
        .save(table_config.base_path)
    )
    print(f"✅ Table '{HIVE_DATABASE}.{table_config.name}' mise à jour et synchronisée avec Hive.")


def run_faculty(spark: SparkSession) -> None:
    raw_df = spark.read.option("multiline", "true").json(RAW_PATHS["faculty"])
    write_to_hudi(transform_faculty(raw_df), FACULTY_PROFILES)


def run_courses(spark: SparkSession) -> None:
    raw_df = spark.read.option("multiline", "true").json(RAW_PATHS["courses"])
    write_to_hudi(transform_courses(raw_df), COURSE_CATALOG)


def run_publications(spark: SparkSession) -> None:
    raw_df = spark.read.option("multiline", "true").json(RAW_PATHS["publications"])
    write_to_hudi(transform_publications(raw_df), RESEARCH_PUBLICATIONS)


def run_news(spark: SparkSession) -> None:
    raw_df = spark.read.option("multiline", "true").json(RAW_PATHS["news"])
    write_to_hudi(transform_news(raw_df), UNIVERSITY_NEWS)


def run_all() -> None:
    spark = get_spark_session()
    ensure_database_exists(spark)

    print("\n[1/4] USMS Faculty -> faculty_profiles")
    run_faculty(spark)
    print("\n[2/4] MIT OCW -> course_catalog")
    run_courses(spark)
    print("\n[3/4] Crossref -> research_publications")
    run_publications(spark)
    print("\n[4/4] USMS News -> university_news")
    run_news(spark)

    print("\n✅ Étape 6 terminée : 4 tables Hudi écrites et synchronisées avec Hive.")
    spark.stop()


if __name__ == "__main__":
    run_all()
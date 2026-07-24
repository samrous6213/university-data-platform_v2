"""
Initialisation de la SparkSession avec prise en charge de S3A (MinIO) et Apache Hudi.
"""

import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession

# Ajout dynamique de la racine du projet au PYTHONPATH pour importer spark_config.py
sys.path.append(str(Path(__file__).resolve().parents[4]))

from configs.spark_config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY
)

def get_spark_session(app_name: str = "USMBA_Data_Platform") -> SparkSession:
    """
    Crée et retourne une SparkSession parfaitement configurée pour le Lakehouse.
    """
# Force Spark à utiliser le Python actuel (contourne le bug du Windows Store)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # --- LE BYPASS WINDOWS POUR HADOOP ---
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = os.environ.get("PATH", "") + ";C:\\hadoop\\bin"
    # -------------------------------------

    # Définition des dépendances Maven (Compatibilité Spark 3.3.x / 3.4.x)
    packages = [
        "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.0", # Moteur Apache Hudi
        "org.apache.hadoop:hadoop-aws:3.3.2",              # Connecteur S3A pour MinIO
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026"      # SDK AWS requis par hadoop-aws
    ]

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.datetime.java8API.enabled", "true") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
        .config("spark.local.dir", "C:/tmp/spark") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # On réduit le niveau de log à WARN pour ne pas inonder le terminal
    spark.sparkContext.setLogLevel("WARN")

    return spark
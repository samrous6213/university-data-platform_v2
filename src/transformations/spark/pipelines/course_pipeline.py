"""
Pipeline complet pour le catalogue des cours (Web USMBA).
Flux : Reader -> Transform -> Quality -> Hudi Writer
"""
from pyspark.sql import SparkSession
import sys
from pathlib import Path

# Ajout dynamique pour les imports absolus
sys.path.append(str(Path(__file__).resolve().parents[4]))

from configs.spark_config import RAW_ZONE_HTML, SILVER_ZONE, QUARANTINE_ZONE, TABLE_COURSE_CATALOG
from src.transformations.spark.readers.html_reader import read_raw_html
from src.transformations.spark.transforms.course_catalog_transform import transform_course_catalog
from src.transformations.spark.transforms.quality_checks import apply_quality_checks
from src.lakehouse.hudi_writer import write_to_hudi, write_to_quarantine

def run_course_pipeline(spark: SparkSession):
    print("🚀 Démarrage du pipeline : Course Catalog")

    # 1. Extraction
    # On lit tous les fichiers HTML de l'usmba
    raw_path = f"{RAW_ZONE_HTML}/source=web_usmba/*/*/*/*/*.html"
    df_raw = read_raw_html(spark, raw_path)

    # 2. Transformation
    df_transformed = transform_course_catalog(df_raw)

    # 3. Contrôle Qualité (Clé primaire technique : course_id)
    df_valid, df_rejected = apply_quality_checks(df_transformed, primary_key="course_id")

    # 4. Chargement (Écriture Hudi & Quarantaine)
    write_to_hudi(
        df=df_valid,
        table_name=TABLE_COURSE_CATALOG,
        primary_key="course_id",
        precombine_field="ingestion_timestamp",
        base_path=SILVER_ZONE
    )

    write_to_quarantine(
        df_rejected=df_rejected,
        table_name=TABLE_COURSE_CATALOG,
        quarantine_path=QUARANTINE_ZONE
    )

    print("✅ Pipeline Course Catalog terminé avec succès.")
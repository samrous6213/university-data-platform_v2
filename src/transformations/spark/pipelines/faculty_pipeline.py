"""
Pipeline complet pour les profils des professeurs (ORCID).
Flux : Reader -> Transform -> Quality -> Hudi Writer
"""
from pyspark.sql import SparkSession
import sys
from pathlib import Path

# Ajout dynamique pour les imports absolus
sys.path.append(str(Path(__file__).resolve().parents[4]))

from configs.spark_config import RAW_ZONE_JSON, SILVER_ZONE, QUARANTINE_ZONE, TABLE_FACULTY_PROFILES
from src.transformations.spark.readers.json_reader import read_raw_json
from src.transformations.spark.transforms.faculty_profiles_transform import transform_faculty_profiles
from src.transformations.spark.transforms.quality_checks import apply_quality_checks
from src.lakehouse.hudi_writer import write_to_hudi, write_to_quarantine

def run_faculty_pipeline(spark: SparkSession):
    print("🚀 Démarrage du pipeline : Faculty Profiles")

    # 1. Extraction
    # On lit tous les JSON de la source orcid_api, peu importe la date
    raw_path = f"{RAW_ZONE_JSON}/source=orcid_api/*/*/*/*/*.json"
    df_raw = read_raw_json(spark, raw_path)

    # 2. Transformation
    df_transformed = transform_faculty_profiles(df_raw)

    # 3. Contrôle Qualité (Clé primaire métier : orcid_id)
    df_valid, df_rejected = apply_quality_checks(df_transformed, primary_key="orcid_id")

    # 4. Chargement (Écriture Hudi & Quarantaine)
    write_to_hudi(
        df=df_valid,
        table_name=TABLE_FACULTY_PROFILES,
        primary_key="orcid_id",
        precombine_field="ingestion_timestamp",
        base_path=SILVER_ZONE
    )

    write_to_quarantine(
        df_rejected=df_rejected,
        table_name=TABLE_FACULTY_PROFILES,
        quarantine_path=QUARANTINE_ZONE
    )

    print("✅ Pipeline Faculty Profiles terminé avec succès.")
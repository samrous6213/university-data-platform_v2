"""
Champs communs obligatoires pour toutes les tables Silver.
"""
from pyspark.sql.types import StructField, StringType, TimestampType

def get_common_fields():
    return [
        StructField("record_id", StringType(), False),          # Identifiant unique de la ligne
        StructField("ingestion_timestamp", TimestampType(), False), # Date de traitement
        StructField("source_system", StringType(), False)       # Web ou API
    ]
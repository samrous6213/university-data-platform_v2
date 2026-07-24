"""
Lecteur distribué pour les fichiers JSON bruts (ex: profils ORCID).
"""
from pyspark.sql import SparkSession, DataFrame

def read_raw_json(spark: SparkSession, s3_path: str) -> DataFrame:
    """
    Lit les fichiers JSON depuis la Raw Zone de MinIO.
    L'option 'multiline' garantit que même les JSON formatés sur plusieurs lignes sont bien lus.
    """
    return spark.read \
        .option("multiline", "true") \
        .json(s3_path)
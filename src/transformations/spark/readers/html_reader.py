"""
Lecteur distribué pour les fichiers HTML bruts (ex: pages USMBA).
"""
from pyspark.sql import SparkSession, DataFrame

def read_raw_html(spark: SparkSession, s3_path: str) -> DataFrame:
    """
    Lit le code source HTML depuis la Raw Zone comme du texte brut distribué.
    L'option 'wholetext' permet de lire tout le fichier dans une seule cellule de DataFrame.
    """
    df = spark.read \
        .option("wholetext", "true") \
        .text(s3_path)
    
    # Spark nomme la colonne contenant le texte "value" par défaut, on la renomme pour plus de clarté
    return df.withColumnRenamed("value", "raw_content")
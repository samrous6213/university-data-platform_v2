"""
Module de contrôle qualité (Data Quality) pour séparer les données valides des enregistrements rejetés.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def apply_quality_checks(df: DataFrame, primary_key: str) -> tuple[DataFrame, DataFrame]:
    """
    Applique les règles de gestion strictes sur le DataFrame transformé.
    Retourne deux DataFrames : (df_valid, df_rejected).
    """
    
    # 1. Règle : La clé primaire ne doit pas être nulle
    # Tout ce qui n'a pas de clé primaire part directement en rejet (Quarantaine)
    df_rejected = df.filter(F.col(primary_key).isNull() | (F.trim(F.col(primary_key)) == ""))
    
    # 2. Règle : Les données valides
    df_valid = df.filter(F.col(primary_key).isNotNull() & (F.trim(F.col(primary_key)) != ""))
    
    # 3. Règle : Pas de doublons stricts dans le flux valide
    # Si le scraping ou l'API a ramené deux fois la même ligne, on ne garde qu'une occurrence
    df_valid = df_valid.dropDuplicates([primary_key])
    
    return df_valid, df_rejected
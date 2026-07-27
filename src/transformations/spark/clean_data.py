"""
Fonctions de nettoyage et de normalisation communes,
utilisées par tous les scripts transform_*.py avant l'écriture vers Hudi.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_record_id(df: DataFrame, key_cols: list, out_col: str = "record_id") -> DataFrame:
    """Génère un identifiant unique (SHA-256) à partir d'une ou plusieurs colonnes clés."""
    key_expr = F.coalesce(*[F.col(c).cast("string") for c in key_cols])
    return df.withColumn(out_col, F.sha2(key_expr, 256))


def add_content_hash(df: DataFrame, cols: list, out_col: str = "content_hash") -> DataFrame:
    """Calcule un hash de contenu, utile pour détecter les changements entre 2 crawls."""
    concat_expr = F.concat_ws(
        "|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    )
    return df.withColumn(out_col, F.sha2(concat_expr, 256))


def normalize_text(df: DataFrame, source_col: str, out_col: str = "normalized_text") -> DataFrame:
    """Nettoie et met en minuscule un champ texte, pour la recherche / le dédoublonnage."""
    return df.withColumn(
        out_col,
        F.lower(
            F.trim(F.regexp_replace(F.coalesce(F.col(source_col), F.lit("")), r"\s+", " "))
        ),
    )


def drop_null_and_duplicates(df: DataFrame, not_null_col: str, dedup_key: str = "record_id") -> DataFrame:
    """Retire les lignes sans valeur clé et les doublons sur record_id."""
    return df.filter(F.col(not_null_col).isNotNull()).dropDuplicates([dedup_key])
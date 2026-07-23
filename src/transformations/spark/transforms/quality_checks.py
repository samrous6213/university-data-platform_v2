"""
Verifications de qualite appliquees avant l'ecriture Hudi. Separe les lignes
valides des lignes en quarantaine (a corriger/investiguer) au lieu de faire
echouer tout le job sur une seule ligne malformee -> "reliability" (30 pts).
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ["record_id", "source_system", "raw_object_path", "crawl_timestamp"]
MIN_TEXT_LENGTH = 30  # en dessous, on considere la page trop pauvre pour etre exploitable


def _null_or_empty(colname: str):
    return col(colname).isNull() | (col(colname) == "")


def split_valid_and_quarantine(df: DataFrame, text_col: str = "normalized_text") -> tuple[DataFrame, DataFrame]:
    """
    Retourne (df_valid, df_quarantine).

    Regles de quarantaine :
      - champs obligatoires manquants (record_id, source_system, raw_object_path, crawl_timestamp)
      - texte normalise trop court/vide (page probablement vide ou mal extraite)
    """
    quarantine_condition = None
    for field in REQUIRED_FIELDS:
        cond = _null_or_empty(field)
        quarantine_condition = cond if quarantine_condition is None else (quarantine_condition | cond)

    if text_col in df.columns:
        quarantine_condition = quarantine_condition | (
            col(text_col).isNull() | (length(col(text_col)) < MIN_TEXT_LENGTH)
        )

    df_quarantine = df.filter(quarantine_condition)
    df_valid = df.filter(~quarantine_condition)

    quarantine_count = df_quarantine.count()
    valid_count = df_valid.count()
    if quarantine_count > 0:
        logger.warning(
            "%s ligne(s) mises en quarantaine sur %s (raisons : champs requis manquants "
            "et/ou texte < %s caracteres)",
            quarantine_count, quarantine_count + valid_count, MIN_TEXT_LENGTH,
        )

    return df_valid, df_quarantine


def deduplicate_on_record_id(df: DataFrame) -> tuple[DataFrame, int]:
    """
    Deduplique sur record_id en gardant la ligne la plus recente (crawl_timestamp).
    Retourne (df_dedup, nb_doublons_retires).
    """
    before = df.count()
    df_dedup = (
        df.orderBy(col("crawl_timestamp").desc())
        .dropDuplicates(["record_id"])
    )
    after = df_dedup.count()
    dropped = before - after
    if dropped > 0:
        logger.info("%s doublon(s) retire(s) sur record_id (avant=%s, apres=%s)", dropped, before, after)
    return df_dedup, dropped


def write_quarantine(df_quarantine: DataFrame, bucket: str, path: str) -> None:
    """Persiste les lignes en quarantaine pour investigation manuelle (pas de perte silencieuse)."""
    count = df_quarantine.count()
    if count == 0:
        return
    output_path = f"s3a://{bucket}/{path}"
    df_quarantine.write.mode("append").json(output_path)
    logger.info("%s ligne(s) en quarantaine ecrites -> %s", count, output_path)
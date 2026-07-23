"""
Fonctions de nettoyage/normalisation partagees entre faculty_profiles_transform.py
et course_catalog_transform.py.
"""

import hashlib
import logging
import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, regexp_replace, trim, udf, when
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)

_FR_STOPWORDS_HINT = {"le", "la", "les", "des", "une", "et", "de", "du"}
_EN_STOPWORDS_HINT = {"the", "and", "of", "for", "with"}


@udf(returnType=StringType())
def _record_id_udf(source_url: str, content_hash: str) -> str:
    """
    record_id DETERMINISTE (pas d'UUID aleatoire) : indispensable pour que les
    upserts Hudi soient idempotents d'un rerun a l'autre (meme source_url ->
    meme record_id -> mise a jour au lieu de duplication).
    Fallback sur content_hash si source_url est absent (ex: docs sans URL stable).
    """
    basis = source_url or content_hash or ""
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


@udf(returnType=StringType())
def _detect_language_udf(text: str) -> str:
    """Heuristique simple FR/EN/inconnu, suffisante pour un MVP (pas de lib NLP lourde)."""
    if not text:
        return "unknown"
    tokens = set(re.findall(r"[a-zàâäéèêëïîôöùûüç]+", text.lower())[:200])
    fr_hits = len(tokens & _FR_STOPWORDS_HINT)
    en_hits = len(tokens & _EN_STOPWORDS_HINT)
    if fr_hits == 0 and en_hits == 0:
        return "unknown"
    return "fr" if fr_hits >= en_hits else "en"


def normalize_text_column(df: DataFrame, source_col: str, target_col: str = "normalized_text") -> DataFrame:
    """
    Nettoyage generique : espaces multiples, trim, suppression des caracteres de
    controle. Ne touche pas a la casse d'origine (utile pour l'affichage/recherche).
    """
    return df.withColumn(
        target_col,
        trim(regexp_replace(regexp_replace(col(source_col), r"\s+", " "), r"[\x00-\x1f]", "")),
    )


def add_record_id(df: DataFrame, url_col: str = "source_url", hash_col: str = "content_hash") -> DataFrame:
    return df.withColumn("record_id", _record_id_udf(col(url_col), col(hash_col)))


def add_language(df: DataFrame, text_col: str = "normalized_text") -> DataFrame:
    return df.withColumn("language", _detect_language_udf(col(text_col)))


def add_is_deleted_flag(df: DataFrame) -> DataFrame:
    """Toujours False a l'ingestion initiale ; reserve pour les futurs runs de reconciliation
    (comparaison avec un snapshot precedent pour detecter les pages supprimees)."""
    return df.withColumn("is_deleted", when(col("record_id").isNotNull(), False).otherwise(False))
"""
Transformation : pages web classifiees 'faculty_profiles' -> table curated faculty_profiles.
Enrichissement : jointure avec les publications OpenAlex (publication_count par institution).

Entree attendue (depuis readers.json_reader.read_web_crawler_json) :
    source_url, extraction_timestamp, http_status, content_checksum,
    connector_version, school_id, school_name, entity_type, extracted_text,
    html_object_path, raw_object_path (absent tel quel -> reconstruit)

Entree OpenAlex (depuis readers.json_reader.read_openalex_json) :
    institution_key, raw_object_path, ingested_at, source_url, content_hash, work
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    lit,
    regexp_extract,
    to_timestamp,
)

from src.transformations.spark.transforms.text_cleaning import (
    add_is_deleted_flag,
    add_language,
    add_record_id,
    normalize_text_column,
)

logger = logging.getLogger(__name__)

# Heuristiques d'extraction depuis le texte brut de la page (fallback simple,
# a affiner avec de vraies donnees ; un titre academique precede souvent le nom).
_TITLE_PATTERN = r"(Professeur[e]?\s(?:assistant|habilite)?|Maitre de conference[s]?|Enseignant[e]?-chercheur[e]?)"
_EMAIL_PATTERN = r"[\w\.-]+@[\w\.-]+\.\w+"

# Mapping school_id (schools_config.json) -> institution OpenAlex (Fahd_openalex.py)
_SCHOOL_TO_OPENALEX_INSTITUTION = {
    "faculty_fstm": "uh2c",
    "faculty_fsac": "uh2c",
    "faculty_enset": "uh2c",
    "faculty_ensam": "uh2c",
}


def _map_school_to_institution(df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import when as _when

    known_school_ids = set(_SCHOOL_TO_OPENALEX_INSTITUTION.keys())
    df_school_ids = {row["school_id"] for row in df.select("school_id").distinct().collect()}
    unmapped = df_school_ids - known_school_ids
    if unmapped:
        logger.warning(
            "Ecole(s) sans correspondance OpenAlex dans _SCHOOL_TO_OPENALEX_INSTITUTION : %s "
            "-> publication_count restera null pour ces lignes. Ajoute le mapping si ces "
            "ecoles dependent d'une des institutions deja suivies dans Fahd_openalex.py.",
            unmapped,
        )

    mapping_expr = None
    for school_id, institution_key in _SCHOOL_TO_OPENALEX_INSTITUTION.items():
        mapping_expr = (
            _when(col("school_id") == school_id, lit(institution_key))
            if mapping_expr is None
            else mapping_expr.when(col("school_id") == school_id, lit(institution_key))
        )
    return df.withColumn("openalex_institution_id", mapping_expr)


def _build_openalex_publication_counts(df_openalex: DataFrame) -> DataFrame:
    """Agrege le nombre de publications par institution."""
    return (
        df_openalex.groupBy("institution_key")
        .agg(count(lit(1)).alias("publication_count"))
        .withColumnRenamed("institution_key", "openalex_institution_id")
    )


def transform_faculty_profiles(df_web: DataFrame, df_openalex: DataFrame | None = None) -> DataFrame:
    """
    df_web      : sortie de read_web_crawler_json(spark, "faculty_profiles")
    df_openalex : sortie de read_openalex_json(spark), optionnel (enrichissement)
    """
    df = df_web

    # Reconstruction de raw_object_path si absent (le JSON stocke html_object_path,
    # on l'utilise comme reference raw principale pour la tracabilite)
    if "raw_object_path" not in df.columns:
        df = df.withColumn("raw_object_path", coalesce(col("html_object_path"), col("json_object_path")))

    df = normalize_text_column(df, source_col="extracted_text", target_col="normalized_text")
    df = add_record_id(df, url_col="source_url", hash_col="content_checksum")
    df = add_language(df, text_col="normalized_text")
    df = add_is_deleted_flag(df)

    df = df.withColumn("title", regexp_extract(col("extracted_text"), _TITLE_PATTERN, 1))
    df = df.withColumn("email", regexp_extract(col("extracted_text"), _EMAIL_PATTERN, 0))
    df = df.withColumn("full_name", lit(None).cast("string"))   # a affiner : NER ou regles par site
    df = df.withColumn("department", lit(None).cast("string"))  # a affiner : mapping par gabarit de page
    df = df.withColumn("research_areas", lit(None).cast("array<string>"))
    df = df.withColumn("profile_url", col("source_url"))

    df = df.withColumn("crawl_timestamp", to_timestamp(col("extraction_timestamp")))
    df = df.withColumn("business_timestamp", col("crawl_timestamp"))
    df = df.withColumnRenamed("connector_version", "_connector_version")  # non retenu dans le schema final
    df = df.withColumn("source_system", lit("web_crawler"))

    df = _map_school_to_institution(df)

    if df_openalex is not None:
        df_counts = _build_openalex_publication_counts(df_openalex)
        df = df.join(df_counts, on="openalex_institution_id", how="left")
    else:
        df = df.withColumn("publication_count", lit(None).cast("int"))

    final_columns = [
        "record_id", "source_system", "source_url", "raw_object_path", "content_hash",
        "crawl_timestamp", "business_timestamp", "is_deleted", "language",
        "school_id", "school_name", "full_name", "title", "department",
        "research_areas", "email", "profile_url", "openalex_institution_id",
        "publication_count", "normalized_text",
    ]
    df = df.withColumnRenamed("content_checksum", "content_hash")
    df = df.select(*final_columns)

    logger.info("Transformation faculty_profiles terminee : %s lignes", df.count())
    return df
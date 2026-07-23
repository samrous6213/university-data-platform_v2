"""
Schema de la table curated `faculty_profiles` (obligatoire, section 3 du brief).

Alimentee par :
  - generic_crawler.py -> raw-web-html / raw-json, entity_type="faculty_profiles"
  - Fahd_openalex.py   -> raw-json (enrichissement : publication_count par institution)
"""

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from .common import with_common_fields

FACULTY_PROFILES_FIELDS = [
    StructField("school_id", StringType(), nullable=False),      # cf. schools_config.json
    StructField("school_name", StringType(), nullable=True),
    StructField("full_name", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),           # ex: "Professeur", "Maitre de conference"
    StructField("department", StringType(), nullable=True),
    StructField("research_areas", ArrayType(StringType()), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("profile_url", StringType(), nullable=True),     # = source_url la plupart du temps
    StructField("openalex_institution_id", StringType(), nullable=True),
    StructField("publication_count", IntegerType(), nullable=True),  # enrichi via jointure OpenAlex
    StructField("normalized_text", StringType(), nullable=True), # texte brut nettoye, fallback recherche
]

FACULTY_PROFILES_SCHEMA = StructType(with_common_fields(FACULTY_PROFILES_FIELDS))
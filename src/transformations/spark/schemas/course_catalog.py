"""
Schema de la table curated `course_catalog` (obligatoire, section 3 du brief).

Alimentee par generic_crawler.py -> raw-web-html / raw-json, entity_type="course_catalog".
"""

from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

from .common import with_common_fields

COURSE_CATALOG_FIELDS = [
    StructField("school_id", StringType(), nullable=False),
    StructField("school_name", StringType(), nullable=True),
    StructField("program_name", StringType(), nullable=True),   # ex: "Licence Informatique"
    StructField("program_level", StringType(), nullable=True),  # licence | master | ingenieur
    StructField("department", StringType(), nullable=True),
    StructField("keywords_matched", ArrayType(StringType()), nullable=True),  # mots-cles ayant classe la page
    StructField("catalog_url", StringType(), nullable=True),    # = source_url
    StructField("normalized_text", StringType(), nullable=True),
]

COURSE_CATALOG_SCHEMA = StructType(with_common_fields(COURSE_CATALOG_FIELDS))
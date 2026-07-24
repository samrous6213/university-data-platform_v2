"""
Schéma cible pour la table Hudi : course_catalog
"""
from pyspark.sql.types import StructType, StructField, StringType
from .common import get_common_fields

def get_course_catalog_schema() -> StructType:
    fields = get_common_fields() + [
        StructField("course_id", StringType(), False),      # Clé primaire (générée via hash de l'URL ou du titre)
        StructField("faculty_name", StringType(), True),    # Ex: FSDM, EST...
        StructField("department", StringType(), True),      # Ex: Informatique, Mathématiques
        StructField("course_title", StringType(), True),    # Nom de la formation
        StructField("degree_level", StringType(), True)     # Licence, Master, BUT...
    ]
    return StructType(fields)
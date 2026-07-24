"""
Schéma cible pour la table Hudi : faculty_profiles
"""
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from .common import get_common_fields

def get_faculty_profiles_schema() -> StructType:
    fields = get_common_fields() + [
        StructField("orcid_id", StringType(), False),              # Clé primaire métier
        StructField("full_name", StringType(), True),
        StructField("university_affiliation", StringType(), True),
        StructField("publications", ArrayType(StringType()), True) # Liste des titres de publications
    ]
    return StructType(fields)
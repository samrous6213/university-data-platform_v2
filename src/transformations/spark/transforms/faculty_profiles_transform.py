"""
Transformation du JSON ORCID brut vers le schéma Silver faculty_profiles.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform_faculty_profiles(df_raw: DataFrame) -> DataFrame:
    """
    Extrait et aplatit les informations clés du profil JSON d'ORCID.
    """
    # 1. Extraction déterministe de l'ORCID ID depuis la structure ou le nom de fichier
    df_transformed = df_raw.select(
        # Clé métier
       F.col("orcid-identifier.path").alias("orcid_id"),
        
        # Nom complet
        F.concat_ws(
            " ",
            F.col("person.name.given-names.value"),
            F.col("person.name.family-name.value")
        ).alias("full_name"),
        
        # Affiliation universitaire
        F.lit("Université Sidi Mohamed Ben Abdellah (USMBA)").alias("university_affiliation"),
        
        # Liste des publications (titres)
        F.expr("`activities-summary`.works.group.`work-summary`[0].title.title.value").alias("publications")
    )

    # 2. Ajout des champs communs de traçabilité
    df_final = df_transformed.withColumn(
        "record_id", F.concat(F.lit("orcid_"), F.col("orcid_id"))
    ).withColumn(
        "ingestion_timestamp", F.current_timestamp()
    ).withColumn(
        "source_system", F.lit("orcid_api")
    )

    return df_final
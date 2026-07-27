"""
faculty_transformer.py
=======================

Transformer pour la table ``faculty_profiles`` du projet
University Data Platform.

Ce module transforme les donnees brutes explodees (apres
``inline_outer`` sur un champ de type array dans le JSON source)
en un DataFrame propre conforme au schema standardise
``faculty_profiles``.

Le schema de sortie est le suivant :

    - record_id          (StringType)   : identifiant unique (MD5)
    - name               (StringType)   : nom complet du membre du corps
    - title              (StringType)   : titre / grade academique
    - department         (StringType)   : departement ou unite d'affiliation
    - email              (StringType)   : adresse email
    - research_interests (StringType)   : domaines de recherche
    - source_system      (StringType)   : nom de la source d'origine
    - source_url         (StringType)   : URL de la page source
    - crawl_timestamp    (TimestampType) : horodatage de l'extraction
    - year               (IntegerType)  : annee academique ou d'embauche

Le transformer gere les variantes de noms de champs provenant de
sources heterogenes (web scraping, APIs, documents) via des
appels ``coalesce``.

Le record_id est genere de maniere deterministic par hachage MD5
des champs cles (nom + departement + source) pour garantir la
deduplication dans la table Hudi.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.transformations.utils.logger import get_logger

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Schema de sortie
# --------------------------------------------------------------------------- #

FACULTY_PROFILES_COLUMNS = [
    "record_id",
    "name",
    "title",
    "department",
    "email",
    "research_interests",
    "source_system",
    "source_url",
    "crawl_timestamp",
    "year",
]


# --------------------------------------------------------------------------- #
# Constantes de log
# --------------------------------------------------------------------------- #

LOG_PREFIX: str = "[faculty_transformer]"


# --------------------------------------------------------------------------- #
# Helper : colonnes potentiellement absentes
# --------------------------------------------------------------------------- #


def safe_col(df: DataFrame, column_name: str):
    """
    Retourne ``F.col(column_name)`` si la colonne existe dans le
    DataFrame, sinon ``F.lit(None)``.

    Cela evite les ``AnalysisException`` lors de l'utilisation de
    ``F.coalesce()`` ou d'autres fonctions Spark sur des colonnes
    qui peuvent etre absentes des sources heterogenes.

    Args:
        df: DataFrame source.
        column_name: nom de la colonne a verifier.

    Returns:
        Column: expression Spark sure.
    """
    if column_name in df.columns:
        return F.col(column_name)
    return F.lit(None)


# --------------------------------------------------------------------------- #
# 1. Transformation principale
# --------------------------------------------------------------------------- #


def transform_faculty_profiles(exploded_df: DataFrame) -> DataFrame:
    """
    Transforme les donnees brutes d'un source en un DataFrame propre
    conforme au schema ``faculty_profiles``.

    Cette fonction effectue les etapes suivantes :
        1. Resolution des variantes de noms de champs (coalesce)
        2. Nettoyage des chaines de caracteres (trim, null)
        3. Validation des enregistrements critiques (nom non null)
        4. Generation du record_id par hachage MD5
        5. Extraction du source_system depuis _source_prefix
        6. Conversion des types (year -> IntegerType, crawl_timestamp)
        7. Selection et reordonnancement du schema de sortie

    Le DataFrame en entree est le resultat d'un ``inline_outer`` sur
    un champ de type array dans le JSON source, avec les colonnes
    additionnelles ``_source_file`` et ``_source_prefix`` ajoutees
    en amont par le pipeline ETL.

    Args:
        exploded_df: DataFrame contenant les enregistrements explodes
            depuis le tableau JSON source, plus les colonnes
            ``_source_file`` et ``_source_prefix``.

    Returns:
        DataFrame: DataFrame transforme avec le schema standardise
            ``faculty_profiles``. Les enregistrements sans nom
            (null) sont filtres.

    Raises:
        Aucune exception levee. Les enregistrements invalides sont
            logges et filtres.
    """
    logger.info(f"{LOG_PREFIX} Debut de la transformation faculty_profiles")

    raw_count = exploded_df.count()
    logger.info(f"{LOG_PREFIX} Nombre d'enregistrements en entree : {raw_count}")

    transformed = exploded_df

    # ----- 1. Resolution des variantes de noms de champs -----

    transformed = _resolve_field_variants(transformed)

    # ----- 2. Nettoyage des chaines de caracteres -----

    transformed = _clean_string_fields(transformed)

    # ----- 3. Validation des enregistrements critiques -----

    transformed = _validate_records(transformed)

    # ----- 4. Generation du record_id -----

    transformed = _generate_record_id(transformed)

    # ----- 5. Extraction du source_system -----

    transformed = transformed.withColumn(
        "source_system",
        safe_col(transformed, "_source_prefix"),
    )

    # ----- 6. Conversion des types -----

    transformed = _cast_types(transformed)

    # ----- 7. Selection du schema de sortie -----

    transformed = transformed.select(*FACULTY_PROFILES_COLUMNS)

    final_count = transformed.count()
    logger.info(
        f"{LOG_PREFIX} Transformation terminee : "
        f"{final_count} enregistrements valides "
        f"(filtres : {raw_count - final_count})"
    )

    return transformed


# --------------------------------------------------------------------------- #
# 2. Resolution des variantes de noms de champs
# --------------------------------------------------------------------------- #


def _resolve_field_variants(df: DataFrame) -> DataFrame:
    """
    Resout les variantes de noms de champs provenant de sources
    heterogenes.

    Chaque champ de sortie est obtenu par ``coalesce`` sur les noms
    de colonnes possibles dans les donnees sources. Le premier nom
    non-null trouve est utilise. Si aucun nom n'est present, la
    valeur reste null.

    Args:
        df: DataFrame avec les colonnes brutes du JSON source.

    Returns:
        DataFrame: DataFrame avec les colonnes standardisees.
    """
    logger.info(f"{LOG_PREFIX} Resolution des variantes de champs")

    df = df.withColumn(
        "name",
        F.coalesce(
            safe_col(df, "name"),
            safe_col(df, "faculty_name"),
            safe_col(df, "full_name"),
            safe_col(df, "professor_name"),
            safe_col(df, "lecturer_name"),
            safe_col(df, "enseignant"),
            safe_col(df, "nom"),
            safe_col(df, "nom_complet"),
        ),
    )

    df = df.withColumn(
        "title",
        F.coalesce(
            safe_col(df, "title"),
            safe_col(df, "academic_title"),
            safe_col(df, "position"),
            safe_col(df, "role"),
            safe_col(df, "rank"),
            safe_col(df, "grade"),
            safe_col(df, "titre"),
            safe_col(df, "grade_academique"),
        ),
    )

    df = df.withColumn(
        "department",
        F.coalesce(
            safe_col(df, "department"),
            safe_col(df, "dept"),
            safe_col(df, "faculty_department"),
            safe_col(df, "unit"),
            safe_col(df, "division"),
            safe_col(df, "faculty"),
            safe_col(df, "departement"),
            safe_col(df, "unite"),
        ),
    )

    df = df.withColumn(
        "email",
        F.coalesce(
            safe_col(df, "email"),
            safe_col(df, "email_address"),
            safe_col(df, "contact_email"),
            safe_col(df, "mail"),
            safe_col(df, "adresse_email"),
        ),
    )

    df = df.withColumn(
        "research_interests",
        F.coalesce(
            safe_col(df, "research_interests"),
            safe_col(df, "research"),
            safe_col(df, "interests"),
            safe_col(df, "research_areas"),
            safe_col(df, "specialization"),
            safe_col(df, "expertise"),
            safe_col(df, "domaines_recherche"),
            safe_col(df, "thematiques"),
        ),
    )

    df = df.withColumn(
        "source_url",
        F.coalesce(
            safe_col(df, "source_url"),
            safe_col(df, "url"),
            safe_col(df, "profile_url"),
            safe_col(df, "link"),
            safe_col(df, "faculty_url"),
            safe_col(df, "page_url"),
            safe_col(df, "lien"),
        ),
    )

    df = df.withColumn(
        "year",
        F.coalesce(
            safe_col(df, "year"),
            safe_col(df, "academic_year"),
            safe_col(df, "join_year"),
            safe_col(df, "hiring_year"),
            safe_col(df, "annee"),
            safe_col(df, "annee_academique"),
        ),
    )

    return df


# --------------------------------------------------------------------------- #
# 3. Nettoyage des chaines de caracteres
# --------------------------------------------------------------------------- #


def _clean_string_fields(df: DataFrame) -> DataFrame:
    """
    Nettoie les champs de type string en appliquant un ``trim``
    pour supprimer les espaces en debut et fin de chaine.

    Les champs traites sont : name, title, department, email,
    research_interests, source_url.

    Args:
        df: DataFrame avec les colonnes standardisees.

    Returns:
        DataFrame: DataFrame avec les champs string nettoyes.
    """
    logger.info(f"{LOG_PREFIX} Nettoyage des champs string")

    string_columns = [
        "name",
        "title",
        "department",
        "email",
        "research_interests",
        "source_url",
    ]

    for col_name in string_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    return df


# --------------------------------------------------------------------------- #
# 4. Validation des enregistrements
# --------------------------------------------------------------------------- #


def _validate_records(df: DataFrame) -> DataFrame:
    """
    Filtre les enregistrements qui ne remplissent pas les
    conditions minimales de qualite.

    Regles de validation :
        - Le champ ``name`` ne doit pas etre null ou vide.
          Un profil sans nom est considere comme invalide.

    Les enregistrements invalides sont logges avant filtrage
    pour faciliter le diagnostic.

    Args:
        df: DataFrame avec les colonnes standardisees et nettoyees.

    Returns:
        DataFrame: DataFrame ne contenant que les enregistrements valides.
    """
    logger.info(f"{LOG_PREFIX} Validation des enregistrements")

    total_before = df.count()

    df = df.filter(
        F.col("name").isNotNull()
        & (F.length(F.col("name")) > 0)
    )

    total_after = df.count()
    filtered_count = total_before - total_after

    if filtered_count > 0:
        logger.warning(
            f"{LOG_PREFIX} {filtered_count} enregistrements filtres "
            f"(name null ou vide)"
        )

    return df


# --------------------------------------------------------------------------- #
# 5. Generation du record_id
# --------------------------------------------------------------------------- #


def _generate_record_id(df: DataFrame) -> DataFrame:
    """
    Genere un identifiant unique (record_id) par hachage MD5.

    Le record_id est construit a partir de la concatenation de :
        - le nom du membre du corps (name)
        - le departement (department)
        - la source (source_system ou _source_prefix)

    Cette approche deterministic garantit que les memes donnees
    produisent toujours le meme record_id, ce qui permet a Hudi
    de dedupliquer correctement lors des operations upsert.

    Si le departement est null, une chaine vide est utilisee dans
    le hash pour eviter les collisions.

    Args:
        df: DataFrame avec les colonnes standardisees.

    Returns:
        DataFrame: DataFrame avec la colonne ``record_id`` generee.
    """
    logger.info(f"{LOG_PREFIX} Generation du record_id")

    hash_input = F.concat_ws(
        "::",
        F.coalesce(safe_col(df, "name"), F.lit("")),
        F.coalesce(safe_col(df, "department"), F.lit("")),
        F.coalesce(safe_col(df, "_source_prefix"), F.lit("")),
    )

    df = df.withColumn(
        "record_id",
        F.md5(hash_input),
    )

    return df


# --------------------------------------------------------------------------- #
# 6. Conversion des types
# --------------------------------------------------------------------------- #


def _cast_types(df: DataFrame) -> DataFrame:
    """
    Convertit les champs dans les types cibles du schema.

    Conversions appliquees :
        - ``year`` : cast en IntegerType, avec gestion des
          valeurs non convertibles (null sur echec).
        - ``crawl_timestamp`` : cast en TimestampType depuis la
          chaine ISO ou le timestamp existant, avec gestion
          des valeurs non convertibles.

    Args:
        df: DataFrame avec les colonnes standardisees.

    Returns:
        DataFrame: DataFrame avec les types cibles appliques.
    """
    logger.info(f"{LOG_PREFIX} Conversion des types")

    df = df.withColumn(
        "year",
        F.when(
            F.col("year").isNotNull(),
            F.col("year").cast(T.IntegerType()),
        ).otherwise(F.lit(None).cast(T.IntegerType())),
    )

    if "crawl_timestamp" in df.columns:
        df = df.withColumn(
            "crawl_timestamp",
            F.when(
                F.col("crawl_timestamp").isNotNull(),
                F.col("crawl_timestamp").cast(T.TimestampType()),
            ).otherwise(F.current_timestamp()),
        )
    else:
        df = df.withColumn("crawl_timestamp", F.current_timestamp())

    return df

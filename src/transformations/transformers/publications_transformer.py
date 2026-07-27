"""
publications_transformer.py
============================

Transformer pour la table ``research_publications`` du projet
University Data Platform.

Ce module transforme les enregistrements bruts lus depuis MinIO
(en JSON aplati ou apres explosion de tableau ``results``) en un
DataFrame propre conforme au schema standardise
``research_publications``.

Le schema de sortie est le suivant :

    - record_id         (StringType)   : identifiant unique (MD5)
    - title             (StringType)   : titre de la publication
    - authors           (StringType)   : auteurs (chaine concatenee)
    - journal           (StringType)   : revue / journal / conference
    - doi               (StringType)   : identifiant DOI
    - abstract          (StringType)   : resume de la publication
    - keywords          (StringType)   : mots-cles (chaine concatenee)
    - department        (StringType)   : departement ou affiliation
    - source_system     (StringType)   : nom de la source d'origine
    - source_url        (StringType)   : URL de la page source
    - crawl_timestamp   (TimestampType) : horodatage de l'extraction
    - year              (IntegerType)  : annee de publication

Le transformer gere les variantes de noms de champs provenant de
sources heterogenes (OpenAlex, web scraping, APIs) via des
appels ``coalesce``.

Le record_id est genere de maniere deterministic par hachage MD5
des champs cles (title + authors ou doi + source) pour garantir
la deduplication dans la table Hudi.
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

RESEARCH_PUBLICATIONS_COLUMNS = [
    "record_id",
    "title",
    "authors",
    "journal",
    "doi",
    "abstract",
    "keywords",
    "department",
    "source_system",
    "source_url",
    "crawl_timestamp",
    "year",
]


# --------------------------------------------------------------------------- #
# Constantes de log
# --------------------------------------------------------------------------- #

LOG_PREFIX: str = "[publications_transformer]"


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


def transform_research_publications(raw_df: DataFrame) -> DataFrame:
    """
    Transforme les enregistrements bruts d'une source en un DataFrame
    propre conforme au schema ``research_publications``.

    Cette fonction effectue les etapes suivantes :
        1. Resolution des variantes de noms de champs (coalesce)
        2. Normalisation des champs composes (authors, keywords)
        3. Nettoyage des chaines de caracteres (trim, null)
        4. Validation des enregistrements critiques (title non null)
        5. Generation du record_id par hachage MD5
        6. Extraction du source_system depuis _source_prefix
        7. Conversion des types (year -> IntegerType, crawl_timestamp)
        8. Selection et reordonnancement du schema de sortie

    Le DataFrame en entree est le resultat de ``read_raw_records`` avec
    la colonne additionnelle ``_source_prefix`` ajoutee en amont par le
    reader.

    Args:
        raw_df: DataFrame contenant les enregistrements bruts depuis
            le JSON source, plus la colonne ``_source_prefix``.

    Returns:
        DataFrame: DataFrame transforme avec le schema standardise
            ``research_publications``. Les enregistrements sans titre
            (null) sont filtres.

    Raises:
        Aucune exception levee. Les enregistrements invalides sont
            logges et filtres.
    """
    logger.info(f"{LOG_PREFIX} Debut de la transformation research_publications")

    raw_count = raw_df.count()
    logger.info(f"{LOG_PREFIX} Nombre d'enregistrements en entree : {raw_count}")

    transformed = raw_df

    # ----- 1. Resolution des variantes de noms de champs -----

    transformed = _resolve_field_variants(transformed)

    # ----- 2. Normalisation des champs composes -----

    transformed = _normalize_composite_fields(transformed)

    # ----- 3. Nettoyage des chaines de caracteres -----

    transformed = _clean_string_fields(transformed)

    # ----- 4. Validation des enregistrements critiques -----

    transformed = _validate_records(transformed)

    # ----- 5. Generation du record_id -----

    transformed = _generate_record_id(transformed)

    # ----- 6. Extraction du source_system -----

    transformed = transformed.withColumn(
        "source_system",
        safe_col(transformed, "_source_prefix"),
    )

    # ----- 7. Conversion des types -----

    transformed = _cast_types(transformed)

    # ----- 8. Selection du schema de sortie -----

    transformed = transformed.select(*RESEARCH_PUBLICATIONS_COLUMNS)

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
        "title",
        F.coalesce(
            safe_col(df, "title"),
            safe_col(df, "paper_title"),
            safe_col(df, "publication_title"),
            safe_col(df, "name"),
            safe_col(df, "titre"),
            safe_col(df, "titre_publication"),
        ),
    )

    df = df.withColumn(
        "authors",
        F.coalesce(
            safe_col(df, "authors"),
            safe_col(df, "author"),
            safe_col(df, "authorships"),
            safe_col(df, "creator"),
            safe_col(df, "auteurs"),
            safe_col(df, "auteur"),
        ),
    )

    df = df.withColumn(
        "journal",
        F.coalesce(
            safe_col(df, "journal"),
            safe_col(df, "venue"),
            safe_col(df, "publication_venue"),
            safe_col(df, "source"),
            safe_col(df, "container_title"),
            safe_col(df, "revue"),
            safe_col(df, "conference"),
            safe_col(df, "source_title"),
        ),
    )

    df = df.withColumn(
        "doi",
        F.coalesce(
            safe_col(df, "doi"),
            safe_col(df, "DOI"),
            safe_col(df, "digital_object_identifier"),
            safe_col(df, "identifier_doi"),
        ),
    )

    df = df.withColumn(
        "abstract",
        F.coalesce(
            safe_col(df, "abstract"),
            safe_col(df, "abstract_inverted"),
            safe_col(df, "summary"),
            safe_col(df, "resume"),
        ),
    )

    df = df.withColumn(
        "keywords",
        F.coalesce(
            safe_col(df, "keywords"),
            safe_col(df, "keyword"),
            safe_col(df, "topics"),
            safe_col(df, "concepts"),
            safe_col(df, "subject"),
            safe_col(df, "mots_cles"),
            safe_col(df, "theme"),
        ),
    )

    df = df.withColumn(
        "department",
        F.coalesce(
            safe_col(df, "department"),
            safe_col(df, "dept"),
            safe_col(df, "affiliation"),
            safe_col(df, "institution"),
            safe_col(df, "faculty"),
            safe_col(df, "departement"),
            safe_col(df, "organisme"),
        ),
    )

    df = df.withColumn(
        "source_url",
        F.coalesce(
            safe_col(df, "source_url"),
            safe_col(df, "url"),
            safe_col(df, "link"),
            safe_col(df, "page_url"),
            safe_col(df, "landing_page_url"),
            safe_col(df, "lien"),
        ),
    )

    df = df.withColumn(
        "year",
        F.coalesce(
            safe_col(df, "year"),
            safe_col(df, "publication_year"),
            safe_col(df, "publication_date"),
            safe_col(df, "annee"),
            safe_col(df, "annee_publication"),
        ),
    )

    return df


# --------------------------------------------------------------------------- #
# 3. Normalisation des champs composes
# --------------------------------------------------------------------------- #


def _normalize_composite_fields(df: DataFrame) -> DataFrame:
    """
    Normalise les champs contenant des donnees composes (listes,
    objets imbriques) en les convertissant en chaines de caracteres
    lisibles.

    Champs traites :
        - ``authors`` : peut etre un array de noms, un array d'objets
          avec une cle ``name`` ou ``display_name``, ou une chaine simple.
          Normalise en une chaine ``"Auteur1, Auteur2, ..."``.
        - ``keywords`` : peut etre un array de mots-cles, un array
          d'objets avec une cle ``display_name`` ou ``name``, ou une
          chaine simple. Normalise en une chaine ``"mot1, mot2, ..."``.

    Si le champ est deja une chaine, il est conserve tel quel (cast en
    StringType pour garantir le type de sortie).

    Cette fonction inspecte le schema Spark du DataFrame pour determiner
    le type reel de chaque colonne et appliquer la transformation adaptee
    sans risque d'AnalysisException.

    Args:
        df: DataFrame avec les colonnes standardisees.

    Returns:
        DataFrame: DataFrame avec les champs composes normalises.
    """
    logger.info(f"{LOG_PREFIX} Normalisation des champs composes")

    # --- authors ---
    if "authors" in df.columns:
        df = _normalize_array_field(
            df,
            "authors",
            preferred_field_names=("name", "display_name"),
        )

    # --- keywords ---
    if "keywords" in df.columns:
        df = _normalize_array_field(
            df,
            "keywords",
            preferred_field_names=("display_name", "name"),
        )

    return df


def _normalize_array_field(
    df: DataFrame,
    col_name: str,
    preferred_field_names: tuple = ("name", "display_name"),
) -> DataFrame:
    """
    Normalise un champ qui peut etre un array de structs, un array de
    strings, ou une string simple en une string concatenee.

    Inspection du schema Spark pour determiner le type reel :
        - ArrayType(StructType) : extrait le premier champ nom matchant
          et le concatene avec ", ".
        - ArrayType(StringType) : concatene directement avec ", ".
        - StringType ou tout autre type : cast en StringType.

    Args:
        df: DataFrame contenant la colonne a normaliser.
        col_name: nom de la colonne a normaliser.
        preferred_field_names: tuple de noms de champs a chercher dans
            les structs, par ordre de priorite.

    Returns:
        DataFrame avec la colonne normalisee en StringType.
    """
    col_type = df.schema[col_name].dataType

    if isinstance(col_type, T.ArrayType):
        element_type = col_type.elementType

        if isinstance(element_type, T.StructType):
            struct_field_names = {f.name for f in element_type.fields}
            matched_field = next(
                (fn for fn in preferred_field_names if fn in struct_field_names),
                None,
            )
            if matched_field is not None:
                return df.withColumn(
                    col_name,
                    F.concat_ws(
                        ", ",
                        F.transform(F.col(col_name), lambda x: x[matched_field]),
                    ),
                )
            return df.withColumn(col_name, F.col(col_name).cast(T.StringType()))

        if isinstance(element_type, T.StringType):
            return df.withColumn(
                col_name,
                F.concat_ws(", ", F.col(col_name)),
            )

        return df.withColumn(col_name, F.col(col_name).cast(T.StringType()))

    return df.withColumn(col_name, F.col(col_name).cast(T.StringType()))


# --------------------------------------------------------------------------- #
# 4. Nettoyage des chaines de caracteres
# --------------------------------------------------------------------------- #


def _clean_string_fields(df: DataFrame) -> DataFrame:
    """
    Nettoie les champs de type string en appliquant un ``trim``
    pour supprimer les espaces en debut et fin de chaine.

    Les champs traites sont : title, authors, journal, doi, abstract,
    keywords, department, source_url.

    Args:
        df: DataFrame avec les colonnes standardisees.

    Returns:
        DataFrame: DataFrame avec les champs string nettoyes.
    """
    logger.info(f"{LOG_PREFIX} Nettoyage des champs string")

    string_columns = [
        "title",
        "authors",
        "journal",
        "doi",
        "abstract",
        "keywords",
        "department",
        "source_url",
    ]

    for col_name in string_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    return df


# --------------------------------------------------------------------------- #
# 5. Validation des enregistrements
# --------------------------------------------------------------------------- #


def _validate_records(df: DataFrame) -> DataFrame:
    """
    Filtre les enregistrements qui ne remplissent pas les
    conditions minimales de qualite.

    Regles de validation :
        - Le champ ``title`` ne doit pas etre null ou vide.
          Une publication sans titre est consideree comme invalide.

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
        F.col("title").isNotNull()
        & (F.length(F.col("title")) > 0)
    )

    total_after = df.count()
    filtered_count = total_before - total_after

    if filtered_count > 0:
        logger.warning(
            f"{LOG_PREFIX} {filtered_count} enregistrements filtres "
            f"(title null ou vide)"
        )

    return df


# --------------------------------------------------------------------------- #
# 6. Generation du record_id
# --------------------------------------------------------------------------- #


def _generate_record_id(df: DataFrame) -> DataFrame:
    """
    Genere un identifiant unique (record_id) par hachage MD5.

    Le record_id est construit a partir de la concatenation de :
        - le titre de la publication (title)
        - les auteurs (authors) ou le DOI si disponible
        - la source (source_system ou _source_prefix)

    Si le DOI est disponible, il est utilise en complement du titre
    pour reduire les risques de collision (un meme titre peut
    correspondre a des publications differentes).

    Cette approche deterministic garantit que les memes donnees
    produisent toujours le meme record_id, ce qui permet a Hudi
    de dedupliquer correctement lors des operations upsert.

    Args:
        df: DataFrame avec les colonnes standardisees.

    Returns:
        DataFrame: DataFrame avec la colonne ``record_id`` generee.
    """
    logger.info(f"{LOG_PREFIX} Generation du record_id")

    hash_input = F.concat_ws(
        "::",
        F.coalesce(safe_col(df, "title"), F.lit("")),
        F.coalesce(safe_col(df, "doi"), safe_col(df, "authors"), F.lit("")),
        F.coalesce(safe_col(df, "_source_prefix"), F.lit("")),
    )

    df = df.withColumn(
        "record_id",
        F.md5(hash_input),
    )

    return df


# --------------------------------------------------------------------------- #
# 7. Conversion des types
# --------------------------------------------------------------------------- #


def _cast_types(df: DataFrame) -> DataFrame:
    """
    Convertit les champs dans les types cibles du schema.

    Conversions appliquees :
        - ``year`` : cast en IntegerType, avec gestion des
          valeurs non convertibles (null sur echec). Si la valeur
          est une chaine de 4 caracteres (ex : ``"2024"``), elle
          est directement convertie. Si elle contient un tiret
          (ex : ``"2024-01-15"``), seule la premiere partie est
          extraite.
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
            F.col("year").isNotNull()
            & F.col("year").cast(T.StringType()).rlike(r"^\d{4}$"),
            F.col("year").cast(T.IntegerType()),
        ).when(
            F.col("year").isNotNull()
            & F.col("year").cast(T.StringType()).rlike(r"^\d{4}-"),
            F.substring(F.col("year").cast(T.StringType()), 1, 4).cast(
                T.IntegerType()
            ),
        ).when(
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

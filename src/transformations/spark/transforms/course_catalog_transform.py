"""
Parsing distribué du HTML brut USMBA vers le schéma Silver course_catalog.
"""
from bs4 import BeautifulSoup
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Schéma temporaire retourné par l'UDF de parsing HTML
PARSED_HTML_SCHEMA = ArrayType(
    StructType([
        StructField("faculty_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("course_title", StringType(), True),
        StructField("degree_level", StringType(), True)
    ])
)

def _parse_usmba_html(html_content: str):
    """
    Fonction Python exécutée par chaque worker Spark pour parser le texte HTML via BeautifulSoup.
    """
    if not html_content:
        return []

    soup = BeautifulSoup(html_content, "html.parser")
    results = []

    # Détection de l'entité (Faculté) dans le titre ou l'en-tête
    page_text = soup.get_text()
    faculty_name = "USMBA"
    if "EST" in page_text or "Technologie" in page_text:
        faculty_name = "EST Fès"
    elif "FSDM" in page_text or "Sciences Dhar El Mehraz" in page_text:
        faculty_name = "FSDM Fès"
    elif "FST" in page_text or "Techniques" in page_text:
        faculty_name = "FST Fès"

    # Recherche des éléments de formations/départements dans les menus et liens
    links = soup.find_all("a")
    for link in links:
        text = link.get_text(strip=True)
        # Filtrage ciblé sur les mots-clés de formations universitaires
        if any(keyword in text.lower() for keyword in ["licence", "master", "dut", "ingénieur", "département", "filière"]):
            degree = "Licence/DUT" if "licence" in text.lower() or "dut" in text.lower() else "Formation Supérieure"
            results.append({
                "faculty_name": faculty_name,
                "department": "Département Académique",
                "course_title": text,
                "degree_level": degree
            })

    # Si aucun lien spécifique n'est trouvé, enregistrer une ligne globale pour l'entité
    if not results:
        results.append({
            "faculty_name": faculty_name,
            "department": "Général",
            "course_title": "Portail des Formations",
            "degree_level": "Tous Niveaux"
        })

    return results

# Déclaration de l'UDF PySpark
parse_html_udf = F.udf(_parse_usmba_html, PARSED_HTML_SCHEMA)


def transform_course_catalog(df_raw_html: DataFrame) -> DataFrame:
    """
    Applique le parsing UDF distribué sur le code HTML et extrait les formations.
    """
    # 1. Application de l'UDF pour extraire la liste des cours
    df_parsed = df_raw_html.withColumn("parsed_courses", parse_html_udf(F.col("raw_content")))

    # 2. Aplatissement (explode) de la liste pour obtenir 1 ligne par cours/formation
    df_exploded = df_parsed.select(F.explode(F.col("parsed_courses")).alias("course"))

    # 3. Extraction des colonnes individuelles et génération de la clé primaire
    df_final = df_exploded.select(
        F.concat(
            F.lit("course_"),
            F.md5(F.concat_ws("_", F.col("course.faculty_name"), F.col("course.course_title")))
        ).alias("course_id"),
        F.col("course.faculty_name").alias("faculty_name"),
        F.col("course.department").alias("department"),
        F.col("course.course_title").alias("course_title"),
        F.col("course.degree_level").alias("degree_level")
    )

    # 4. Ajout des champs communs de traçabilité
    df_final = df_final.withColumn(
        "record_id", F.col("course_id")
    ).withColumn(
        "ingestion_timestamp", F.current_timestamp()
    ).withColumn(
        "source_system", F.lit("web_usmba")
    )

    return df_final
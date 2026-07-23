"""
debug/test_json_reader.py

Objectif : verifier que json_reader.py lit correctement les deux formats
possibles du bucket raw-json :
  1) format PLAT (web_crawler / data.gov.ma)     -> read_web_crawler_json()
  2) format IMBRIQUE (openalex, metadata/data)   -> read_openalex_json()

A lancer APRES avoir valide test_spark_session.py.

Pre-requis : au moins quelques fichiers deja presents sous
  s3a://raw-json/source=*/entity=faculty_profiles/.../*.json
  s3a://raw-json/source=*/entity=course_catalog/.../*.json
  s3a://raw-json/source=openalex/entity=works_*/.../*.json
Sinon les .count() renverront 0 -- ce n'est pas une erreur en soi, juste un
signal qu'il faut d'abord lancer l'ingestion sur au moins 1 source.

Usage :
    python -m debug.test_json_reader
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("debug.test_json_reader")


def check_dataframe(df, label: str, expected_min_cols=None):
    """Petit garde-fou commun : affiche schema/sample, alerte si vide ou colonnes manquantes."""
    print(f"\n=== {label} ===")
    print(f"Colonnes : {df.columns}")
    df.printSchema()

    count = df.count()
    print(f"Nombre de lignes : {count}")

    if count == 0:
        logger.warning(
            "%s : 0 ligne lue. Verifie que des fichiers existent bien sous le "
            "prefixe attendu dans MinIO (l'ingestion a-t-elle deja tourne ?).",
            label,
        )
        return False

    print("Echantillon (5 lignes) :")
    df.show(5, truncate=80)

    if expected_min_cols:
        missing = [c for c in expected_min_cols if c not in df.columns]
        if missing:
            logger.error("%s : colonnes attendues manquantes : %s", label, missing)
            return False

    return True


def main() -> int:
    try:
        from src.transformations.spark.config.spark_session import get_spark_session
    except ImportError as e:
        logger.error("Import de get_spark_session() impossible : %s", e)
        return 1

    from src.transformations.spark.readers.json_reader import (
        read_web_crawler_json,
        read_openalex_json,
    )

    spark = get_spark_session()
    all_ok = True

    # 1) faculty_profiles (format plat, web_crawler)
    try:
        df_faculty = read_web_crawler_json(spark, entity_type="faculty_profiles")
        ok = check_dataframe(
            df_faculty,
            "faculty_profiles (web_crawler, format plat)",
            expected_min_cols=["source_url", "raw_object_path", "extracted_text"],
        )
        all_ok = all_ok and ok
    except Exception:
        logger.exception("Echec read_web_crawler_json(entity_type='faculty_profiles')")
        all_ok = False

    # 2) course_catalog (format plat, web_crawler)
    try:
        df_course = read_web_crawler_json(spark, entity_type="course_catalog")
        ok = check_dataframe(
            df_course,
            "course_catalog (web_crawler, format plat)",
            expected_min_cols=["source_url", "raw_object_path", "extracted_text"],
        )
        all_ok = all_ok and ok
    except Exception:
        logger.exception("Echec read_web_crawler_json(entity_type='course_catalog')")
        all_ok = False

    # 3) openalex (format imbrique, aplati par le reader)
    try:
        df_openalex = read_openalex_json(spark)
        ok = check_dataframe(
            df_openalex,
            "openalex (format imbrique, aplati)",
            expected_min_cols=["institution_key", "raw_object_path", "work"],
        )
        all_ok = all_ok and ok

        if ok:
            # Verifie que le champ "work" (struct explode) contient bien des
            # sous-champs exploitables -- sinon le futur transform va planter
            # silencieusement en lisant des colonnes vides.
            print("\nSchema du champ imbrique 'work' :")
            df_openalex.select("work.*").printSchema()
    except Exception:
        logger.exception("Echec read_openalex_json()")
        all_ok = False

    # 4) Detection de fichiers corrompus (le reader les filtre deja, mais on
    #    veut savoir si ca arrive souvent -- ca remonte dans les logs WARNING
    #    du reader lui-meme, donc rien a faire ici, juste un rappel visuel).
    print(
        "\n(rappel) Si des WARNING 'fichier(s) JSON corrompu(s) ignore(s)' sont "
        "apparus ci-dessus dans les logs, verifie ces fichiers manuellement sur MinIO."
    )

    spark.stop()

    if all_ok:
        logger.info("Tous les reads JSON sont OK. Tu peux passer a test_text_cleaning.py.")
        return 0
    else:
        logger.warning(
            "Au moins un probleme detecte (0 ligne, colonne manquante, ou "
            "exception). Corrige avant de continuer sur les transforms."
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
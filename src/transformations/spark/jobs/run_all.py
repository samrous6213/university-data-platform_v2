"""
Orchestrateur simple (sans Airflow, hors scope du brief) : execute les jobs
faculty_profiles puis course_catalog sequentiellement, dans le meme process
Spark ou en sous-process selon le contexte de deploiement.

Usage (cron quotidien) :
    python -m src.transformations.spark.jobs.run_all
"""

import sys

from src.transformations.spark.config.spark_session import get_spark_session
from src.transformations.spark.pipelines.course_pipeline import run_course_pipeline
from src.transformations.spark.pipelines.faculty_pipeline import run_faculty_pipeline
from src.transformations.spark.utils.logging_setup import (
    new_run_id,
    setup_logger,
    write_run_log,
)

JOB_NAME = "run_all"

PIPELINES = {
    "faculty_profiles": run_faculty_pipeline,
    "course_catalog": run_course_pipeline,
}


def main() -> int:
    logger = setup_logger(JOB_NAME)
    spark = get_spark_session(app_name="transform-run-all")

    failures = 0
    for pipeline_name, pipeline_fn in PIPELINES.items():
        run_id = new_run_id()
        logger.info("Debut pipeline '%s' (run_id=%s)", pipeline_name, run_id)
        try:
            summary = pipeline_fn(spark)
            write_run_log(job_name=pipeline_name, run_id=run_id, status="success", **summary)
            logger.info("Pipeline '%s' termine : %s", pipeline_name, summary)
        except Exception as e:
            failures += 1
            logger.exception("Pipeline '%s' en echec : %s", pipeline_name, e)
            write_run_log(job_name=pipeline_name, run_id=run_id, status="failed", error=str(e))
            # on continue avec les autres pipelines plutot que d'arreter tout le run
            continue

    spark.stop()

    if failures:
        logger.error("%s pipeline(s) en echec sur %s", failures, len(PIPELINES))
        return 1

    logger.info("Tous les pipelines ont reussi (%s/%s)", len(PIPELINES), len(PIPELINES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
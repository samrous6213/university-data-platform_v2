"""
Point d'entree CLI du job course_catalog.
Execution : spark-submit ... jobs/course_catalog_job.py
"""

import sys

from src.transformations.spark.config.spark_session import get_spark_session
from src.transformations.spark.pipelines.course_pipeline import run_course_pipeline
from src.transformations.spark.utils.logging_setup import (
    new_run_id,
    setup_logger,
    write_run_log,
)

JOB_NAME = "course_catalog"


def main() -> int:
    logger = setup_logger(JOB_NAME)
    run_id = new_run_id()
    logger.info("Debut job '%s' (run_id=%s)", JOB_NAME, run_id)

    spark = get_spark_session(app_name=f"transform-{JOB_NAME}")

    try:
        summary = run_course_pipeline(spark)
        write_run_log(
            job_name=JOB_NAME,
            run_id=run_id,
            status="success",
            records_read=summary["records_read"],
            records_written=summary["records_written"],
            records_quarantined=summary["records_quarantined"],
            duplicates_dropped=summary["duplicates_dropped"],
        )
        logger.info("Job '%s' termine avec succes : %s", JOB_NAME, summary)
        return 0

    except Exception as e:
        logger.exception("Job '%s' en echec : %s", JOB_NAME, e)
        write_run_log(job_name=JOB_NAME, run_id=run_id, status="failed", error=str(e))
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
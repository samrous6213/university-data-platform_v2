#!/usr/bin/env python3
from __future__ import annotations

import sys
import traceback
from typing import List, Tuple

from src.transformations.config.spark_config import SparkConfig
from src.transformations.etl.course_catalog_etl import run_course_catalog_etl
from src.transformations.etl.documents_registry_etl import run_documents_registry_etl
from src.transformations.etl.faculty_profiles_etl import run_faculty_profiles_etl
from src.transformations.etl.research_publications_etl import run_research_publications_etl
from src.transformations.etl.university_news_etl import run_university_news_etl
from src.transformations.utils.logger import get_logger

logger = get_logger(__name__)


def run_all() -> List[Tuple[str, int, bool]]:
    spark = None
    results: List[Tuple[str, int, bool]] = []

    try:
        config = SparkConfig()
        spark = config.build()
        logger.info("SparkSession created successfully")

        faculty_count = run_faculty_profiles_etl(spark)
        results.append(("faculty_profiles", faculty_count, True))

        course_count = run_course_catalog_etl(spark)
        results.append(("course_catalog", course_count, True))

        news_count = run_university_news_etl(spark)
        results.append(("university_news", news_count, True))

        pubs_count = run_research_publications_etl(spark)
        results.append(("research_publications", pubs_count, True))

        docs_count = run_documents_registry_etl(spark)
        results.append(("documents_registry", docs_count, True))

    except Exception:
        logger.error(f"ETL pipeline failed: {traceback.format_exc()}")
        if not results:
            results.extend([
                ("faculty_profiles", 0, False),
                ("course_catalog", 0, False),
                ("university_news", 0, False),
                ("research_publications", 0, False),
                ("documents_registry", 0, False),
            ])

    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped")

    return results


def print_summary(results: List[Tuple[str, int, bool]]) -> None:
    print()
    print("=" * 60)
    print("  ETL PIPELINE SUMMARY")
    print("=" * 60)
    all_ok = True
    for name, count, ok in results:
        status = "OK" if ok else "FAILED"
        print(f"  {name:25s}  {status:8s}  {count:6d} records")
        if not ok:
            all_ok = False
    print("=" * 60)
    if all_ok:
        print("  All ETLs completed successfully")
    else:
        print("  Some ETLs failed \u2014 check logs above")
    print("=" * 60)


if __name__ == "__main__":
    results = run_all()
    print_summary(results)
    sys.exit(0 if all(ok for _, _, ok in results) else 1)

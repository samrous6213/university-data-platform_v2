"""
Lecteur pour les documents PDF stockes dans raw-documents (fahd_datagov.py).

Non utilise directement par faculty_profiles/course_catalog (qui viennent du web
crawler), mais disponible pour enrichir ces tables ou pour une future table
"documents_registry" (backlog, cf. document d'architecture). Fournit un DataFrame
(source_url_equivalent, content, extracted_text, ...) exploitable en jointure.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from configs.spark_config import RAW_DOCUMENTS_BUCKET

logger = logging.getLogger(__name__)


@udf(returnType=StringType())
def _extract_pdf_text_udf(pdf_bytes: bytes) -> str:
    """
    Extraction texte PDF via pypdf. Volontairement simple (pas d'OCR) pour un MVP :
    suffisant pour des rapports/etudes texte natif ; les scans necessiteraient
    une etape OCR distincte, hors scope du brief actuel.
    """
    if not pdf_bytes:
        return ""
    try:
        from io import BytesIO

        from pypdf import PdfReader

        reader = PdfReader(BytesIO(pdf_bytes))
        pages_text = [page.extract_text() or "" for page in reader.pages]
        return " ".join(pages_text).strip()
    except Exception as e:  # pragma: no cover - defense, ne doit pas casser le job
        logger.warning("Echec extraction texte PDF : %s", e)
        return ""


def read_raw_pdfs(spark: SparkSession, dataset_prefix: str | None = None) -> DataFrame:
    """
    Lit les PDF sous raw-documents (dataset_prefix optionnel pour filtrer un
    dataset data.gov.ma precis, ex: 'source=data_gov_ma/entity=universites-marocaines-2014').
    """
    prefix = dataset_prefix or "source=data_gov_ma"
    path_glob = f"s3a://{RAW_DOCUMENTS_BUCKET}/{prefix}/*/*/*/*.pdf"
    logger.info("Lecture PDF bruts : %s", path_glob)

    df = spark.read.format("binaryFile").load(path_glob)
    df = df.withColumn("extracted_text", _extract_pdf_text_udf(col("content")))
    df = df.drop("content")

    logger.info("PDF lus et parses : %s", df.count())
    return df
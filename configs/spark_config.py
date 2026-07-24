"""
Configuration globale pour Apache Spark, MinIO (S3A) et Apache Hudi.
"""

# ---------------------------------------------------------
# 1. Configuration de connexion MinIO (S3A)
# ---------------------------------------------------------
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# ---------------------------------------------------------
# 2. Chemins des zones du Datalake (Utilisation du préfixe s3a:// pour Spark)
# ---------------------------------------------------------
RAW_ZONE_JSON = "s3a://raw-json"
RAW_ZONE_HTML = "s3a://raw-html"
RAW_ZONE_DOCS = "s3a://raw-documents"

QUARANTINE_ZONE = "s3a://quarantine-zone"
SILVER_ZONE = "s3a://silver-zone"

# ---------------------------------------------------------
# 3. Métadonnées Hudi
# ---------------------------------------------------------
HUDI_DATABASE_NAME = "usmba_lakehouse"
TABLE_FACULTY_PROFILES = "faculty_profiles"
TABLE_COURSE_CATALOG = "course_catalog"
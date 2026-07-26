# src/processing/index_to_elasticsearch.py
"""
Indexation Elasticsearch - Version simplifiée
"""
import subprocess
import logging
import sys
import os

sys.path.append('/opt/airflow')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def index_elasticsearch():
    logger.info("="*60)
    logger.info("🚀 INDEXATION ELASTICSEARCH")
    logger.info("="*60)
    cmd = ["python", "-m", "src.processing.es_indexer_simple"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        logger.info("✅ Indexation réussie")
        return True
    else:
        logger.error(f"❌ Erreur: {result.stderr}")
        return False

if __name__ == "__main__":
    success = index_elasticsearch()
    sys.exit(0 if success else 1)
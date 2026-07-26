# src/processing/spark_transform.py
"""
Transformation Spark : lecture MinIO → écriture Hudi/Parquet
"""

import subprocess
import logging
import sys
import os

# Ajouter le chemin du projet
sys.path.append('/opt/airflow')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_spark_transform():
    """Exécute la transformation Spark via pyspark"""
    logger.info("="*60)
    logger.info("🚀 LANCEMENT DE LA TRANSFORMATION SPARK")
    logger.info("="*60)
    
    # Utiliser Python directement avec pyspark
    cmd = [
        "python",
        "-m", "src.processing.transform_script"
    ]
    
    logger.info(f"📝 Commande: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        logger.info("✅ Transformation Spark réussie")
        logger.info(result.stdout)
        return True
    else:
        logger.error(f"❌ Erreur: {result.stderr}")
        return False

if __name__ == "__main__":
    success = run_spark_transform()
    sys.exit(0 if success else 1)
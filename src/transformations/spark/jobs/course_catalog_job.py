"""
Point d'entrée exécutable pour le pipeline du catalogue de cours.
"""
import sys
from pathlib import Path

# Permet à Python de trouver le dossier 'src' et 'configs' depuis ce sous-dossier
sys.path.append(str(Path(__file__).resolve().parents[4]))

from src.transformations.spark.config.spark_session import get_spark_session
from src.transformations.spark.pipelines.course_pipeline import run_course_pipeline

if __name__ == "__main__":
    # Instanciation du moteur
    spark = get_spark_session(app_name="Job_Course_Catalog")
    
    try:
        run_course_pipeline(spark)
    finally:
        # On s'assure que Spark s'éteint proprement même en cas d'erreur
        spark.stop()
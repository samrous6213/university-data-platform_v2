"""
Master Orchestrator : Lance l'ensemble des pipelines du Datalake de manière séquentielle.
Inclut une logique de relance (Retry Logic) et une journalisation fichier (File Logging).
Renvoie un exit_code non-nul (1) si un des flux plante définitivement.
"""
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

# Définition de la racine du projet pour les imports absolus
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.append(str(PROJECT_ROOT))

# --- CONFIGURATION DU LOGGING ---
log_dir = PROJECT_ROOT / "logs"
log_dir.mkdir(exist_ok=True) # Crée le dossier s'il n'existe pas

# Création d'un nom de fichier unique avec horodatage
log_file = log_dir / f"master_orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout) # Affiche aussi les logs dans le terminal
    ]
)
logger = logging.getLogger(__name__)
# --------------------------------

from src.transformations.spark.config.spark_session import get_spark_session
from src.transformations.spark.pipelines.faculty_pipeline import run_faculty_pipeline
from src.transformations.spark.pipelines.course_pipeline import run_course_pipeline

def run_with_retry(job_name, job_func, spark_session, max_retries=3, wait_time=5):
    """
    Exécute un job avec une logique de relance automatique en cas d'échec partiel.
    Garantit la résilience (Reliability) de l'orchestration.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[RUN] Lancement du pipeline : {job_name} (Tentative {attempt}/{max_retries})")
            job_func(spark_session)
            logger.info(f"[OK] {job_name} termine avec succes.")
            return True
        except Exception as e:
            logger.error(f"[ERREUR] dans {job_name} : {e}")
            if attempt < max_retries:
                logger.warning(f"[WAIT] Attente de {wait_time} secondes avant la prochaine tentative...")
                time.sleep(wait_time)
            else:
                logger.critical(f"[CRITIQUE] Echec definitif de {job_name} apres {max_retries} tentatives.")
                return False

if __name__ == "__main__":
    logger.info("==========================================================")
    logger.info("[START] DEMARRAGE DU MASTER ORCHESTRATOR SPARK LAKEHOUSE")
    logger.info("==========================================================")

    success = True
    spark = None

    try:
        # Instanciation unique pour économiser la RAM
        spark = get_spark_session(app_name="Master_Orchestrator_USMBA")
        
        # Exécution avec Retry Logic
        if not run_with_retry("Faculty Profiles", run_faculty_pipeline, spark):
            success = False

        if not run_with_retry("Course Catalog", run_course_pipeline, spark):
            success = False

    except Exception as e:
        logger.critical(f"[CRITIQUE] Echec fatal de l'orchestrateur : {e}")
        success = False

    finally:
        # Le bloc 'finally' garantit que Spark s'arrêtera TOUJOURS, crash ou pas.
        if spark is not None:
            spark.stop()
            logger.info("[STOP] Moteur Spark arrete proprement.")

    logger.info("==========================================================")
    if success:
        logger.info("[SUCCESS] TOUS LES PIPELINES ONT ETE EXECUTES AVEC SUCCES")
        sys.exit(0) # Succès total
    else:
        logger.critical("[FAIL] ECHEC D'UN OU PLUSIEURS PIPELINES. VOIR LES LOGS.")
        sys.exit(1) # Alerte système pour le planificateur externe
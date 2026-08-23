"""
DAG d'orchestration du pipeline University Data Platform - v2 IN-CONTAINER.

CHANGEMENT PAR RAPPORT A LA v1 (SSHOperator + .bat) : toutes les taches
tournent maintenant DIRECTEMENT dans le conteneur airflow-scheduler, qui
utilise desormais une image custom (voir Dockerfile.airflow) contenant
Java 17 + PySpark + toutes les dependances du projet (requirements.txt).

Le code du projet est monte en volume (./src:/opt/airflow/src,
./configs:/opt/airflow/configs, cf. docker-compose.yml) exactement comme il
etait execute nativement sous Windows : meme commande "python -m
src...." , seul l'environnement change (les hosts localhost/127.0.0.1 sont
desormais les noms de service Docker : minio, hive-metastore, elasticsearch,
postgres -- voir les overrides d'environnement dans docker-compose.yml).

Plus besoin de :
  - SSHOperator / apache-airflow-providers-ssh
  - connexion SSH "windows_host_ssh"
  - OpenSSH Server sur l'hote Windows
  - fichiers run_ingestion_*.bat / run_faculty_profiles.bat / run_course_catalog.bat
    (peuvent etre conserves pour du debug manuel en local, mais ne sont
    plus dans le chemin d'execution du DAG)

PERIMETRE : Sources (API/web/documents) -> MinIO -> Spark -> Hudi ->
Elasticsearch. Metabase reste hors DAG (dashboard consulte manuellement,
pas de refresh automatique requis pour le MVP).

ORDRE D'EXECUTION : entierement sequentiel, comme la v1, pour rester
conservateur pendant le stress-test live du jury (un seul conteneur
scheduler execute a la fois les appels reseau d'ingestion et les jobs
Spark gourmands en CPU/RAM). Voir la variante parallele commentee en bas
de fichier si le temps d'execution total devient un probleme.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "university-data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# WORKDIR /opt/airflow est defini dans Dockerfile.airflow ; "cd /opt/airflow"
# reste explicite ici par securite / lisibilite (au cas ou le WORKDIR de
# l'image venait a changer).
WORKDIR = "/opt/airflow"

with DAG(
    dag_id="university_data_platform_daily",
    description="Sources (API/web/documents) -> MinIO -> Spark -> Hudi -> Elasticsearch, in-container, quotidien",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["university-data-platform", "mvp"],
) as dag:

    # ── 1) Ingestion : 3 sources exigees par le brief ──────────────
    run_ingestion_openalex = BashOperator(
        task_id="run_ingestion_openalex",
        bash_command=f"cd {WORKDIR} && python -m src.ingestion.api.Fahd_openalex",
        execution_timeout=timedelta(minutes=15),  # API paginee, generalement rapide ; marge large
    )

    run_ingestion_datagov = BashOperator(
        task_id="run_ingestion_datagov",
        bash_command=f"cd {WORKDIR} && python -m src.ingestion.docs.fahd_datagov",
        execution_timeout=timedelta(minutes=15),  # telechargement de fichiers, taille variable
    )

    run_ingestion_web = BashOperator(
        task_id="run_ingestion_web",
        bash_command=f"cd {WORKDIR} && python -m src.ingestion.web.generic_crawler",
        execution_timeout=timedelta(minutes=30),  # crawl recursif sur 4 etablissements, le plus long des 3
    )

    # ── 2) Transformation Spark : ecrit Hudi + sync Elasticsearch ──
    # (postgres_writer + es_writer sont deja appeles a l'interieur des
    # pipelines run_faculty_pipeline / run_course_pipeline)
    run_faculty_profiles_pipeline = BashOperator(
        task_id="run_faculty_profiles_pipeline",
        bash_command=f"cd {WORKDIR} && python -m src.transformations.spark.jobs.faculty_profiles_job",
        execution_timeout=timedelta(minutes=30),
    )

    run_course_catalog_pipeline = BashOperator(
        task_id="run_course_catalog_pipeline",
        bash_command=f"cd {WORKDIR} && python -m src.transformations.spark.jobs.course_catalog_job",
        execution_timeout=timedelta(minutes=30),
    )

    # Chaine entierement sequentielle : ingestion (3 sources) -> transformation (2 tables)
    (
        run_ingestion_openalex
        >> run_ingestion_datagov
        >> run_ingestion_web
        >> run_faculty_profiles_pipeline
        >> run_course_catalog_pipeline
    )

    # ── Variante plus rapide (non activee) ──────────────────────────
    # Avec LocalExecutor (voir docker-compose.yml), les 3 ingestions
    # peuvent tourner en parallele entre elles (independantes, I/O-bound
    # reseau), tout en gardant Spark sequentiel apres :
    #
    # [run_ingestion_openalex, run_ingestion_datagov, run_ingestion_web] \
    #     >> run_faculty_profiles_pipeline >> run_course_catalog_pipeline
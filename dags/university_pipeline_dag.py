"""
DAG d'orchestration du pipeline University Data Platform.

ARCHITECTURE : Airflow tourne dans Docker (conteneurs airflow-*), mais les
jobs Python (ingestion) et Spark (transformation) tournent nativement sur
l'hote Windows (.venv), car Airflow n'est pas supporte nativement sous
Windows (conflit de dependances pydantic/typing_extensions) et le
conteneur Airflow n'a pas pyspark/les jars Hudi installes.

Chaque tache utilise donc SSHOperator pour declencher l'execution reelle
sur l'hote via `host.docker.internal` (l'adresse depuis laquelle un
conteneur Docker Desktop atteint la machine hote), en appelant un fichier
.bat dedie par tache : aucune commande complexe avec guillemets imbriques
n'est transmise via SSH/cmd.exe (cause du bug #1 initial), et chaque .bat
force l'encodage UTF-8 de la console (chcp 65001 + PYTHONIOENCODING,
fix du bug #2 sur les caracteres arabes).

Necessite :
  - Le package apache-airflow-providers-ssh installe dans les conteneurs
    airflow-* (cf. _PIP_ADDITIONAL_REQUIREMENTS dans docker-compose.yml)
  - Une connexion SSH nommee "windows_host_ssh" configuree dans
    Admin -> Connections (host.docker.internal, port 22, user + password
    Windows)
  - OpenSSH Server actif sur l'hote Windows (service sshd Running)
  - Les 3 fichiers d'ingestion a la racine du projet :
      run_ingestion_openalex.bat
      run_ingestion_datagov.bat
      run_ingestion_web.bat
    (memes chemins/conventions que run_faculty_profiles.bat et
    run_course_catalog.bat, deja valides)

Respecte l'exigence du brief section 3 : "Support daily execution with
logs and retry logic" -- ET ferme la boucle Sources -> MinIO -> Spark ->
Hudi de bout en bout : le DAG va desormais chercher de nouvelles donnees
aux sources avant chaque transformation, au lieu de retransformer les
memes objets MinIO deja presents.

ORDRE D'EXECUTION : entierement sequentiel (ingestion_openalex ->
ingestion_datagov -> ingestion_web -> spark_faculty_profiles ->
spark_course_catalog). Choix deliberement conservateur : un seul hote
Windows execute a la fois des appels reseau (ingestion) et des jobs Spark
gourmands en CPU/RAM ; du parallelisme entre taches augmenterait le
risque de timeout MinIO/Postgres/Elasticsearch deja rencontre en
developpement, un risque a eviter particulierement pendant le
stress-test live du jury. Si le temps d'execution total devient un
probleme, les 3 taches d'ingestion peuvent etre parallelisees entre
elles (elles sont independantes et majoritairement I/O-bound reseau,
contrairement aux jobs Spark) : voir la variante commentee en bas de
fichier.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

PROJECT_ROOT = r"C:\Users\Fahds\university-data-platform_v2"
SSH_CONN_ID = "windows_host_ssh"

default_args = {
    "owner": "university-data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="university_data_platform_daily",
    description="Sources (API/web/documents) -> MinIO -> Spark -> Hudi -> Postgres (dashboard) -> Elasticsearch (search), quotidien",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["university-data-platform", "mvp"],
) as dag:

    # ── 1) Ingestion : 3 sources exigees par le brief ──────────────
    run_ingestion_openalex = SSHOperator(
        task_id="run_ingestion_openalex",
        ssh_conn_id=SSH_CONN_ID,
        command=rf"{PROJECT_ROOT}\run_ingestion_openalex.bat",
        cmd_timeout=900,  # API paginee, generalement rapide ; marge large
    )

    run_ingestion_datagov = SSHOperator(
        task_id="run_ingestion_datagov",
        ssh_conn_id=SSH_CONN_ID,
        command=rf"{PROJECT_ROOT}\run_ingestion_datagov.bat",
        cmd_timeout=900,  # telechargement de fichiers, taille variable
    )

    run_ingestion_web = SSHOperator(
        task_id="run_ingestion_web",
        ssh_conn_id=SSH_CONN_ID,
        command=rf"{PROJECT_ROOT}\run_ingestion_web.bat",
        cmd_timeout=1800,  # crawl recursif sur 4 etablissements, le plus long des 3
    )

    # ── 2) Transformation Spark : deja valide (existant, inchange) ─
    run_faculty_profiles_pipeline = SSHOperator(
        task_id="run_faculty_profiles_pipeline",
        ssh_conn_id=SSH_CONN_ID,
        command=rf"{PROJECT_ROOT}\run_faculty_profiles.bat",
        cmd_timeout=None,
    )

    run_course_catalog_pipeline = SSHOperator(
        task_id="run_course_catalog_pipeline",
        ssh_conn_id=SSH_CONN_ID,
        command=rf"{PROJECT_ROOT}\run_course_catalog.bat",
        cmd_timeout=None,
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
    # Si le temps total de run devient genant en demo, les 3 ingestions
    # peuvent etre paralleles entre elles (independantes, I/O-bound),
    # tout en gardant Spark sequentiel apres :
    #
    # [run_ingestion_openalex, run_ingestion_datagov, run_ingestion_web] \
    #     >> run_faculty_profiles_pipeline >> run_course_catalog_pipeline
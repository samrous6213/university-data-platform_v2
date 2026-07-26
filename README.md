# University Data Platform — MVP

Plateforme de données pour un environnement universitaire : ingestion depuis 3 types de sources, stockage brut traçable, transformation et curation en tables analytiques, exposition SQL, dashboard BI et recherche full-text.

```
Sources (API / Web / Documents) -> MinIO (raw) -> Spark (transform) -> Hudi/Hive (curated + SQL)
                                                                              |
                                                                +-------------+-------------+
                                                                |                           |
                                                        Metabase (dashboard)        Elasticsearch (search)
```

Projet réalisé dans le cadre du **University Data Platform Challenge** (Best-of-9 MVP, 3 semaines).

---

## 1. Vue d'ensemble

| Élément | Détail |
|---|---|
| Objectif | Pipeline de bout en bout, exécuté quotidiennement, reproductible |
| Tables curated | `faculty_profiles`, `course_catalog` |
| Sources ingérées | 1 API (OpenAlex), 1 web statique (sites d'établissements), 1 fichiers/documents (data.gov.ma / CKAN) |
| Orchestration | Airflow (Docker) + `SSHOperator` vers l'hôte Windows |
| Traitement | Apache Spark (natif sur l'hôte, `.venv`) |
| Stockage brut | MinIO (S3-compatible) |
| Lakehouse | Apache Hudi |
| Catalogue SQL | Hive Metastore |
| BI | Metabase (via Postgres) |
| Recherche | Elasticsearch + API de recherche (`src/api/search_api.py`) |

---


## Technologies

- Python 3.11
- Apache Spark
- Apache Hudi
- Apache Hive Metastore
- Apache Airflow
- MinIO
- PostgreSQL
- Elasticsearch
- FastAPI
- Docker Compose
- Metabase


## 2. Pourquoi Airflow tourne dans Docker mais Spark tourne sur l'hôte

Airflow n'est pas supporté nativement sous Windows (conflits de dépendances `pydantic`/`typing_extensions`), et le conteneur Airflow n'embarque pas PySpark ni les jars Hudi. Le choix retenu :

- **Airflow** : conteneurs Docker (`docker-compose.yml`)
- **Ingestion + Spark** : exécution native sur l'hôte Windows, dans l'environnement virtuel `.venv`
- **Liaison** : chaque tâche Airflow est un `SSHOperator` qui déclenche un script `.bat` sur l'hôte via `host.docker.internal` (adresse par laquelle un conteneur Docker Desktop atteint la machine hôte)

Chaque tâche appelle un fichier `.bat` dédié plutôt qu'une commande inline, pour deux raisons :
- éviter les problèmes de guillemets imbriqués mal transmis par `Win32-OpenSSH` → `cmd.exe`
- forcer l'encodage UTF-8 de la console (`chcp 65001` + `set PYTHONIOENCODING=utf-8`), indispensable car les données scrapées contiennent des caractères arabes (établissements marocains bilingues fr/ar) que `cmd.exe` encode par défaut en `cp1252` en session SSH non-interactive

---

## 3. Structure du dépôt

```
university-data-platform_v2/
├── dags/
│   └── university_pipeline_dag.py       # DAG unique : ingestion -> Spark
├── src/
│   ├── api/
│   │   └── search_api.py                # API de recherche (FastAPI + Elasticsearch)
│   ├── ingestion/
│   │   ├── api/Fahd_openalex.py         # Source 1 : API OpenAlex
│   │   ├── docs/fahd_datagov.py         # Source 2 : fichiers/documents (CKAN data.gov.ma)
│   │   └── web/generic_crawler.py       # Source 3 : web statique (BeautifulSoup)
│   ├── storage/minio/fahd_client.py     # Client MinIO partagé
│   ├── lakehouse/
│   │   ├── hudi/hudi_writer.py
│   │   ├── postgres/postgres_writer.py  # Sync vers Postgres (Metabase)
│   │   └── elasticsearch/es_writer.py   # Sync vers Elasticsearch
│   └── transformations/spark/
│       ├── config/spark_session.py
│       ├── jobs/                        # faculty_profiles_job.py, course_catalog_job.py, run_all.py
│       ├── pipelines/                   # faculty_pipeline.py, course_pipeline.py
│       ├── readers/                     # csv, html, json, pdf, xlsx
│       ├── schemas/                     # faculty_profiles.py, course_catalog.py, common.py
│       ├── transforms/                  # nettoyage, contrôles qualité, normalisation
│       └── utils/                       # logging, retry
├── configs/
│   └── schools_config.json              # Liste des établissements à crawler
├── debug/                               # Scripts de vérification manuelle
│   ├── test_json_reader.py
│   ├── test_quality_checks.py
│   ├── test_spark_session.py
│   ├── test_elasticsearch.py
│   └── test_hudi_writer.py
├── jars/                                # Jars Spark (Hudi, connecteurs...)
├── jdbc/                                # Driver JDBC Postgres
├── run_ingestion_openalex.bat
├── run_ingestion_datagov.bat
├── run_ingestion_web.bat
├── run_faculty_profiles.bat
├── run_course_catalog.bat
├── docker-compose.yml
├── requirements.txt
└── .gitignore
```

> `airflow_home/` (config Airflow locale, logs, clés SSH) n'est **pas** versionné — voir `.gitignore`.

---

## 4. Prérequis

- Windows avec **OpenSSH Server** actif (service `sshd` en `Running`) — nécessaire pour que `SSHOperator` atteigne l'hôte depuis les conteneurs Airflow
- **Docker Desktop** (pour Airflow, MinIO, Postgres, Elasticsearch, Hive Metastore — selon ce que couvre `docker-compose.yml`)
- **Python 3.11** + environnement virtuel `.venv` sur l'hôte, avec `pip install -r requirements.txt`
- **PySpark** + jars Hudi disponibles dans `jars/` (les jobs Spark tournent nativement sur l'hôte, pas dans un conteneur)
- Une paire de clés SSH générée pour la connexion Airflow → hôte, référencée dans `airflow_home/ssh_keys/` (non versionnée)

---

## 5. Installation

```powershell
# 1. Cloner le dépôt
git clone https://github.com/samrous6213/university-data-platform_v2
cd university-data-platform_v2

# 2. Créer et activer l'environnement virtuel
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# 3. Lancer les services Docker (Airflow, MinIO, Postgres, Elasticsearch, Hive Metastore)
docker-compose up -d

# 4. Générer une paire de clés SSH pour Airflow -> hôte (si pas déjà fait)
ssh-keygen -t ed25519 -f airflow_home\ssh_keys\windows_host_key -N '""'
# Ajouter la clé publique correspondante au fichier authorized_keys de l'hôte Windows

# 5. Dans l'UI Airflow (Admin -> Connections), créer une connexion :
#    Conn Id   : windows_host_ssh
#    Conn Type : SSH
#    Host      : host.docker.internal
#    Port      : 22
#    Username / clé privée : selon l'utilisateur Windows configuré
```

---

## 6. Exécution du pipeline

### Automatique (quotidien)

Le DAG `university_data_platform_daily` est planifié `@daily` dans `dags/university_pipeline_dag.py`. Il suffit de l'activer (unpause) dans l'UI Airflow — pas de déclenchement manuel nécessaire en usage normal.

### Manuelle (test / démo)

Depuis l'UI Airflow : `DAGs` → `university_data_platform_daily` → bouton **Trigger DAG**.

Ordre d'exécution (entièrement séquentiel, choix volontaire pour la stabilité) :

```
run_ingestion_openalex
        ↓
run_ingestion_datagov
        ↓
run_ingestion_web
        ↓
run_faculty_profiles_pipeline
        ↓
run_course_catalog_pipeline
```

Les 3 tâches d'ingestion sont indépendantes et pourraient être parallélisées (I/O-bound réseau), mais restent séquentielles pour éviter de saturer CPU/RAM de l'hôte pendant que Spark tourne — un risque déjà rencontré en développement (timeouts MinIO/Postgres/Elasticsearch) et à éviter particulièrement pendant le stress-test live du jury.

### En local, sans Airflow (debug)

Chaque script peut être lancé directement :

```powershell
python -m src.ingestion.api.Fahd_openalex
python -m src.ingestion.docs.fahd_datagov
python -m src.ingestion.web.generic_crawler
python -m src.transformations.spark.jobs.faculty_profiles_job
python -m src.transformations.spark.jobs.course_catalog_job
```

---

## 7. Où regarder les résultats

| Couche | Où | Ce qu'on y trouve |
|---|---|---|
| Raw | MinIO — buckets `raw-json`, `raw-web-html`, `raw-documents`, `raw-logs` | Données brutes + métadonnées de traçabilité (source, timestamp, checksum) |
| Curated | Hudi / Hive | Tables `faculty_profiles`, `course_catalog`, interrogeables en SQL |
| BI | Metabase (via Postgres) | Dashboard KPIs |
| Recherche | Elasticsearch + `src/api/search_api.py` | Endpoint de recherche mot-clé |

Chaque enregistrement curated et chaque objet raw porte un `content_hash`/`checksum` et un `source_url`/`raw_object_path`, ce qui permet de remonter de n'importe quel résultat (dashboard, recherche) jusqu'à l'objet brut d'origine dans MinIO.

---

## 8. Traçabilité et idempotence

- Les noms d'objets MinIO sont basés sur le hash du contenu (`content_hash[:10-12]`) → un rerun sans changement de données ne crée pas de doublon.
- Chaque run d'ingestion écrit un log JSON dans `raw-logs/` (source, nombre d'enregistrements, erreurs, `ingestion_id`).
- Les tables Hudi supportent l'upsert : les jobs Spark peuvent être rejoués sans dupliquer les données déjà présentes (`duplicates_dropped` suivi dans les logs de job).

---

## 9. Tests

Dossier `debug/` — scripts de vérification manuelle, à lancer individuellement :

```powershell
python debug\test_json_reader.py
python debug\test_quality_checks.py
python debug\test_spark_session.py
python debug\test_elasticsearch.py
python debug\test_hudi_writer.py
```

---

## 10. Limites connues / hors périmètre MVP

Conformément au brief, sont explicitement hors scope : Qdrant/embeddings/RAG, Kafka, Keycloak, gouvernance d'entreprise avancée, automatisation navigateur lourde (Playwright/Selenium) sauf nécessité stricte.

---


# University Data Platform — RUNBOOK (Guide d'exploitation)

**Version :** 1.0

> Guide opérationnel : démarrer la plateforme, exécuter l'ingestion et la transformation Spark, vérifier les données, surveiller les logs, récupérer en cas d'erreur et arrêter l'ensemble.

---

## 0) Prérequis

- **Docker Desktop** (Windows/macOS) ou **Docker Engine** (Linux) — aucun autre logiciel requis sur l'hôte
- Le dépôt cloné, avec un fichier `.env` créé à partir de `.env.example` (`cp .env.example .env`)

Toute la plateforme est conteneurisée. L'ingestion et la transformation Spark s'exécutent directement dans le conteneur `airflow-scheduler` (image custom définie par `Dockerfile.airflow` : Java 17 + PySpark + dépendances du projet).

---

## 1) Démarrer la plateforme ▶

Depuis la racine du projet :

```bash
# Premier démarrage uniquement : builder l'image Airflow custom
docker compose build airflow-init airflow-webserver airflow-scheduler

docker compose up -d
```

### Vérifications

```bash
docker compose ps
```

Attendus (`Up`, sauf `airflow-init` qui passe à `Exited (0)` une fois son travail fait) :
- `university-minio`
- `university-postgres`
- `hive-metastore`
- `spark-master`, `spark-worker`
- `university-elasticsearch`
- `university-metabase`
- `airflow-postgres`, `airflow-webserver`, `airflow-scheduler`

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Doit renvoyer une liste vide (aucune erreur de parsing du DAG).

---

## 2) Accès aux services

| Service | URL | Port | Credentials |
|---|---:|---:|---|
| MinIO (Console) | http://localhost:9001 | 9001 | `minioadmin` / `minioadmin` |
| Metabase | http://localhost:3000 | 3000 | configuré au premier lancement |
| Airflow (Web) | http://localhost:8081 | 8081 | `admin` / `admin` |
| Elasticsearch | http://localhost:9200 | 9200 | — (sécurité désactivée en local) |
| API de recherche (FastAPI) | http://localhost:8000 | 8000 | — (à démarrer manuellement, section 5) |
| PostgreSQL (Metastore/analytics) | localhost:5432 | 5432 | `hive` / `hive` |
| PostgreSQL (metadata Airflow) | localhost:5433 | 5433 | `airflow` / `airflow` |
| Hive Metastore (thrift) | localhost:9083 | 9083 | — |

---

## 3) Exécuter l'ingestion

Toutes les commandes s'exécutent dans le conteneur `airflow-scheduler` :

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow && python -m src.ingestion.api.Fahd_openalex"
docker compose exec airflow-scheduler bash -c "cd /opt/airflow && python -m src.ingestion.docs.fahd_datagov"
docker compose exec airflow-scheduler bash -c "cd /opt/airflow && python -m src.ingestion.web.generic_crawler"
```

Les scripts écrivent dans MinIO (`raw-json`, `raw-web-html`, `raw-documents`, `raw-logs`).

### Lancer le pipeline complet via Airflow

Depuis l'UI (http://localhost:8081) : DAG `university_data_platform_daily` → bouton ▶ **Trigger DAG**.

Ou en CLI :
```bash
docker compose exec airflow-scheduler airflow dags trigger university_data_platform_daily
```

Le DAG exécute chaque tâche via `BashOperator`, de façon strictement séquentielle :

```
run_ingestion_openalex → run_ingestion_datagov → run_ingestion_web
    → run_faculty_profiles_pipeline → run_course_catalog_pipeline
```

---

## 4) Exécuter la transformation Spark

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow && python -m src.transformations.spark.jobs.faculty_profiles_job"
docker compose exec airflow-scheduler bash -c "cd /opt/airflow && python -m src.transformations.spark.jobs.course_catalog_job"
```

Chaque job :
1. Lit depuis MinIO (`s3a://raw-json/...`, `s3a://raw-web-html/...`, `s3a://raw-documents/...`)
2. Écrit en upsert dans la table Hudi correspondante (`faculty_profiles` ou `course_catalog`), synchronisée dans le schéma Hive `default`
3. Synchronise automatiquement vers PostgreSQL (`postgres_writer.py`) et Elasticsearch (`es_writer.py`) — pas d'étape séparée à lancer

Log de fin typique :
```
Job 'faculty_profiles' termine avec succes : {'records_read': ..., 'records_written': ...,
'records_quarantined': ..., 'duplicates_dropped': ...,
'records_synced_postgres': ..., 'records_synced_elasticsearch': ...}
```

---

## 5) Lancer l'API de recherche

Toutes les dépendances (`fastapi`, `uvicorn`, `elasticsearch`) sont déjà dans l'image `airflow-scheduler` — pas besoin d'environnement séparé.

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow && uvicorn src.api.search_api:app --host 0.0.0.0 --port 8000"
```

> `--host 0.0.0.0` est nécessaire pour que le port exposé (`8000:8000` dans `docker-compose.yml`) soit accessible depuis l'hôte — le défaut d'uvicorn (`127.0.0.1`) resterait invisible depuis l'extérieur du conteneur.

Endpoints :
- http://localhost:8000/docs — documentation Swagger interactive
- http://localhost:8000/health — vérifie l'état de l'API et d'Elasticsearch
- http://localhost:8000/search?q=intelligence+artificielle — recherche libre, avec filtres optionnels `entity_type` (`faculty_profiles` | `course_catalog`) et `school_id`

Cette commande occupe le terminal (serveur au premier plan) ; ouvrir un second terminal pour continuer à utiliser `docker compose exec` en parallèle, ou l'ajouter comme process en arrière-plan avec `-d` sur `docker compose exec` n'est pas supporté — préférer `screen`/`tmux` dans le conteneur, ou l'ajouter comme service dédié dans `docker-compose.yml` si un usage prolongé est nécessaire.

---

## 6) Vérifier les données

### 6.1 MinIO
http://localhost:9001 → vérifier les buckets `raw-json`, `raw-web-html`, `raw-documents`, `raw-logs`.

### 6.2 Tables Hudi/Hive
```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow && python -c \"
from src.transformations.spark.config.spark_session import get_spark_session
spark = get_spark_session('debug-check')
spark.sql('SHOW TABLES').show()
spark.sql('SELECT COUNT(*) FROM faculty_profiles').show()
spark.sql('SELECT COUNT(*) FROM course_catalog').show()
\""
```

### 6.3 PostgreSQL
```bash
docker compose exec postgres psql -U hive -d analytics -c "SELECT count(*) FROM faculty_profiles;"
docker compose exec postgres psql -U hive -d analytics -c "SELECT count(*) FROM course_catalog;"
```

### 6.4 Elasticsearch
```bash
curl -s http://localhost:9200/university_search/_count?pretty
curl -s 'http://localhost:9200/university_search/_search?size=3' -H 'Content-Type: application/json' | python -m json.tool
```

### 6.5 API de recherche
```bash
curl -s "http://localhost:8000/search?q=informatique" | python -m json.tool
```

### 6.6 Scripts de vérification (`debug/`)

Le dossier `debug/` contient des scripts de vérification manuelle unitaires, utiles pour isoler un composant sans lancer tout le pipeline :

| Script | Vérifie |
|---|---|
| `test_spark_session.py` | La SparkSession se construit et se connecte au Hive Metastore |
| `test_hudi_writer.py` | Écriture/lecture d'une table Hudi |
| `test_json_reader.py` | Lecture des fichiers JSON bruts depuis MinIO |
| `test_quality_checks.py` | Les contrôles de qualité (quarantaine, dédoublonnage) |
| `test_show_tables.py` | Liste les tables enregistrées dans Hive |
| `test_elasticsearch.py` | Connexion et indexation Elasticsearch |

Exécution (même principe, dans le conteneur) :
```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow && python debug/test_show_tables.py"
```

### 6.7 État du DAG Airflow
http://localhost:8081 → DAG `university_data_platform_daily` → vérifier que toutes les tâches sont vertes ; cliquer sur une tâche → "Logs" pour le détail.

---

## 7) Monitoring & logs

```bash
docker compose logs -f --tail=200 airflow-scheduler
```

Autres conteneurs utiles à surveiller : `university-minio`, `university-elasticsearch`, `university-postgres`, `hive-metastore`.

Chaque run d'ingestion écrit aussi un log JSON dans le bucket `raw-logs/` (source, nombre d'enregistrements, erreurs, `ingestion_id`), consultable via la console MinIO.

---

## 8) Recovery

### Redémarrer un service en échec
```bash
docker compose restart elasticsearch
```

### Réexécuter une tâche
- Depuis l'UI Airflow : clic sur la tâche → "Clear" pour la rejouer
- Ou relancer directement la commande correspondante (sections 3/4)

Les tables Hudi supportant l'upsert, un rerun ne duplique pas les données déjà présentes (`duplicates_dropped` dans les logs de job) — c'est le comportement attendu lors d'un rerun ou d'un test de récupération après incident.

### Reset complet (⚠️ perte de données)
```bash
docker compose down -v
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d
```

---

## 9) Problèmes courants

**Erreur d'import du DAG** (`airflow dags list-import-errors` non vide)
→ Lire le message complet ; vérifier que `./src` et `./configs` sont bien montés (`docker compose config`) et que toute dépendance importée par le DAG figure dans `requirements.txt`.

**MinIO/Hive Metastore/Elasticsearch injoignables depuis Spark**
→ Vérifier `docker compose ps` ; vérifier dans `docker-compose.yml` (section `x-airflow-common.environment`) que `HIVE_METASTORE_URI`, `ELASTICSEARCH_HOST`, `MINIO_ENDPOINT` pointent bien vers les noms de service Docker (`hive-metastore`, `elasticsearch`, `minio`), pas vers `localhost`.

**Un dossier `metastore_db/` apparaît à la racine**
→ Signe que Spark est retombé sur un metastore Derby local au lieu du vrai Hive Metastore. Supprimer le dossier, vérifier `HIVE_METASTORE_URI` ci-dessus, puis relancer.

**Premier run Spark lent (téléchargement de jars)**
→ Normal uniquement après un `docker compose down -v` (le cache Ivy, persisté dans le volume `airflow_ivy_cache`, a été supprimé). Sinon, les jars Hudi/hadoop-aws sont pré-résolus au build de l'image.

**Elasticsearch renvoie 0 documents**
→ Vérifier que le job Spark s'est bien terminé (`records_synced_elasticsearch` dans les logs) ; relancer le job de transformation si besoin.

---

## 10) Arrêter la plateforme

```bash
docker compose down          # conserve les volumes (données)
docker compose down -v       # ⚠️ supprime aussi les volumes
```

---

## Annexe — DAG Airflow

`university_data_platform_daily` (`dags/university_pipeline_dag.py`), 5 tâches `BashOperator` exécutées dans `airflow-scheduler`, entièrement séquentielles :

1. `run_ingestion_openalex`
2. `run_ingestion_datagov`
3. `run_ingestion_web`
4. `run_faculty_profiles_pipeline`
5. `run_course_catalog_pipeline`
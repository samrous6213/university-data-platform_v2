# University Data Platform — RUNBOOK (Guide d'exploitation)

**Version :** 1.0

> Ce runbook décrit les étapes **opérationnelles** pour démarrer la plateforme, exécuter les scripts d'ingestion, lancer la transformation Spark, synchroniser vers Postgres/Elasticsearch, vérifier les données, surveiller les logs, récupérer en cas d'erreur et arrêter l'ensemble.

---

## 0) Prérequis

### Matériel / logiciels
- Windows avec **OpenSSH Server** actif (service `sshd` en `Running`)
- Docker Desktop (ou Docker Engine)
- Docker Compose
- Python 3.11 + environnement virtuel `.venv` sur l'hôte (`pip install -r requirements.txt`)
- PySpark + jars Hudi disponibles dans `jars/`
- Accès au projet : racine du dépôt `university-data-platform_v2`

### Avertissement (configuration)
- Les identifiants sont ceux définis dans `docker-compose.yml` (MinIO/PostgreSQL/Airflow).
- **Airflow tourne dans Docker**, mais **l'ingestion et Spark tournent nativement sur l'hôte** (`.venv`), déclenchés depuis les conteneurs Airflow via `SSHOperator` + scripts `.bat`.
- La connexion Airflow → hôte (`windows_host_ssh`) doit être configurée (voir README, section Installation) avant tout déclenchement via l'UI.

---

## 1) Démarrer la plateforme ▶

Depuis la racine du projet :

```bash
docker compose up -d
```

### Vérifications
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

Attendus (conteneurs) :
- `minio`
- `postgres` (Hive Metastore / Metabase)
- `hive-metastore`
- `elasticsearch`
- `metabase`
- `airflow-webserver`, `airflow-scheduler`, `airflow-postgres`

### Vérifier que Spark/l'environnement hôte est prêt
```powershell
.venv\Scripts\activate
python -c "import pyspark; print(pyspark.__version__)"
```

---

## 2) Accès aux services

| Service | URL | Port | Credentials |
|---|---:|---:|---|
| MinIO (Console) | http://localhost:9001 | 9001 | `minioadmin` / `minioadmin` |
| Metabase | http://localhost:3000 | 3000 | configuré au premier lancement |
| Airflow (Web) | http://localhost:8081 | 8081 | `admin` / `admin` |
| Elasticsearch | http://localhost:9200 | 9200 | désactivé (sécurité désactivée dans compose) |
| API de recherche (FastAPI) | http://localhost:8000 | 8000 | — |
| PostgreSQL (Metastore/Metabase) | localhost:5435 | 5435 | `hive` / `hive` |

> ⚠️ Ports indicatifs alignés sur `docker-compose.yml` — à vérifier/ajuster selon ta configuration exacte si elle diffère.

---

## 3) Exécuter les ingestions individuellement

⚠️ **Important :** contrairement à un setup 100 % Docker, ces commandes s'exécutent **depuis l'hôte Windows**, dans l'environnement virtuel `.venv` (et non dans un conteneur), car Spark/PySpark et les jars Hudi n'y sont disponibles que côté hôte.

```powershell
.venv\Scripts\activate
```

Les scripts écrivent dans **MinIO** via `src/storage/minio/fahd_client.py`.

### 3.1 OpenAlex (publications)
```powershell
python -m src.ingestion.api.Fahd_openalex
```

### 3.2 data.gov.ma / CKAN (fichiers/documents)
```powershell
python -m src.ingestion.docs.fahd_datagov
```

### 3.3 Web statique (faculty + cours)
```powershell
python -m src.ingestion.web.generic_crawler
```

### 3.4 Lancer le pipeline complet (via Airflow)

Depuis l'UI Airflow :
1. Ouvrir http://localhost:8081
2. Cliquer sur le DAG `university_data_platform_daily`
3. Cliquer sur ▶ **"Trigger DAG"**

**OU en ligne de commande :**
```bash
docker compose exec airflow-webserver airflow dags trigger university_data_platform_daily
```

> Le DAG déclenche, via `SSHOperator`, les scripts `.bat` correspondants sur l'hôte (`run_ingestion_openalex.bat`, `run_ingestion_datagov.bat`, `run_ingestion_web.bat`, `run_faculty_profiles.bat`, `run_course_catalog.bat`), de façon **strictement séquentielle** pour éviter de saturer CPU/RAM de l'hôte.

---

## 4) Exécuter la transformation Spark

La transformation lit des données depuis MinIO et écrit dans la zone curated (Hudi) + crée/rafraîchit les tables Hive.

### Commandes (jobs)
Sur l'hôte, `.venv` activé :

```powershell
python -m src.transformations.spark.jobs.faculty_profiles_job
python -m src.transformations.spark.jobs.course_catalog_job
```

> Ou via les scripts `.bat` équivalents : `run_faculty_profiles.bat`, `run_course_catalog.bat`.

### Attendus
- Lecture depuis MinIO (`s3a://raw-json/...`, `s3a://raw-web-html/...`, `s3a://raw-documents/...`)
- Écriture Hudi dans la zone curated
- Création/refresh de tables Hive :
  - `faculty_profiles`
  - `course_catalog`
- Synchronisation automatique vers PostgreSQL (`postgres_writer.py`) et Elasticsearch (`es_writer.py`)

---

## 5) Synchroniser vers Postgres et Elasticsearch

La synchronisation est déclenchée en fin de job Spark (via `lakehouse/postgres/postgres_writer.py` et `lakehouse/elasticsearch/es_writer.py`), il n'y a donc normalement pas de commande séparée à lancer.

### Vérification rapide côté Elasticsearch
```bash
curl -s http://localhost:9200/university_search/_count?pretty
```

### Vérification rapide côté Postgres (Metabase)
```powershell
python debug\test_elasticsearch.py
```

---

## 6) Vérifier les données (étape par étape)

### 6.1 Vérifier MinIO (buckets + contenu)
1. Ouvrir : http://localhost:9001
2. Vérifier les buckets :
   - `raw-json`, `raw-web-html`, `raw-documents`, `raw-logs`

**Objectif opérationnel :** confirmer que les scripts d'ingestion ont écrit des objets récents.

### 6.2 Vérifier Hive / tables curated
```sql
SHOW TABLES;
SELECT COUNT(*) FROM faculty_profiles;
SELECT COUNT(*) FROM course_catalog;
```
(à exécuter dans un contexte Spark SQL sur l'hôte)

### 6.3 Vérifier Elasticsearch

```bash
curl -s http://localhost:9200/university_search/_count?pretty
```

- Vérifier un sample :

```bash
curl -s 'http://localhost:9200/university_search/_search?size=3' -H 'Content-Type: application/json' | python -m json.tool
```

### 6.4 Vérifier l'API de recherche
```bash
curl -s "http://localhost:8000/search?q=informatique" | python -m json.tool
```

### 6.5 Vérifier l'état du DAG Airflow

1. Ouvrir http://localhost:8081
2. Vérifier que toutes les tâches sont en **vert** ✅
3. Vérifier les logs : cliquer sur une tâche → onglet "Logs"
4. Vérifier l'historique : les DAG runs récents doivent être en succès

---

## 7) Monitoring & logs

### 7.1 Logs Docker

```bash
docker compose logs -f --tail=200 airflow-webserver
```

Autres services (à adapter) :
- `minio`
- `elasticsearch`
- `airflow-scheduler`
- `postgres`

### 7.2 Logs d'ingestion (MinIO)
Chaque run d'ingestion écrit un log JSON dans le bucket `raw-logs/` (source, nombre d'enregistrements, erreurs, `ingestion_id`).

### 7.3 Airflow UI
1. Ouvrir : http://localhost:8081
2. Aller sur DAG : `university_data_platform_daily`
3. Vérifier l'historique : tâches `run_ingestion_*` → `run_faculty_profiles_pipeline` → `run_course_catalog_pipeline`

---

## 8) Recovery (redémarrage / reprise)

### 8.1 Redémarrer un service Docker en échec
Exemple : redémarrer Elasticsearch

```bash
docker compose restart elasticsearch
```

### 8.2 Réexécuter une ingestion
- Relancer la commande correspondante (OpenAlex / data.gov.ma / web) depuis l'hôte, ou relancer la tâche depuis l'UI Airflow.

### 8.3 Réexécuter la transformation
Procéder dans cet ordre :
```powershell
python -m src.transformations.spark.jobs.faculty_profiles_job
python -m src.transformations.spark.jobs.course_catalog_job
```

> Les tables Hudi supportant l'upsert, un rerun ne duplique pas les données déjà présentes (`duplicates_dropped` suivi dans les logs de job).

---

## 9) Problèmes courants & solutions rapides

### 9.1 SSHOperator ne joint pas l'hôte
Symptômes : la tâche Airflow échoue avec une erreur de connexion SSH.

Actions :
- Vérifier que le service `sshd` est bien `Running` sur l'hôte Windows
- Vérifier la connexion Airflow (`windows_host_ssh`) dans Admin → Connections
- Vérifier que `host.docker.internal` est bien résolu depuis le conteneur

### 9.2 Caractères arabes mal encodés dans les logs
- Vérifier que le script `.bat` force bien `chcp 65001` et `set PYTHONIOENCODING=utf-8`

### 9.3 MinIO inaccessible depuis Spark
Symptômes : la transformation échoue sur S3A (endpoint/credentials).

Actions :
- Vérifier que le conteneur MinIO tourne :
  ```bash
  docker ps | grep minio
  ```
- Vérifier l'endpoint utilisé par Spark : `http://localhost:9000` (accès depuis l'hôte, contrairement à `http://minio:9000` en contexte 100 % Docker)

### 9.4 Elasticsearch : zéro documents
- Vérifier que le job Spark (`es_writer.py`) s'est bien terminé (logs du job)
- Vérifier le count :
  ```bash
  curl -s http://localhost:9200/university_search/_count?pretty
  ```
- Si besoin : relancer le job de transformation correspondant.

---

## 10) Arrêter la plateforme

Arrêt des conteneurs (sans supprimer les volumes) :

```bash
docker compose down
```

Arrêt + suppression des conteneurs/réseaux (sans effacer les volumes par défaut) :
- Utilisez `docker compose down -v` **uniquement si vous acceptez de perdre les données**.

---

## Annexe — Référence : DAG Airflow

DAG : `university_data_platform_daily` (`dags/university_pipeline_dag.py`)

Chaîne (séquentielle) :
1. `run_ingestion_openalex`
2. `run_ingestion_datagov`
3. `run_ingestion_web`
4. `run_faculty_profiles_pipeline`
5. `run_course_catalog_pipeline`
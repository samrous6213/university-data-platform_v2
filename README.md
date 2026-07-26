# University Data Platform — MVP
![Status](https://img.shields.io/badge/status-mvp-blue)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Spark](https://img.shields.io/badge/Spark-native--host-orange)
![Docker](https://img.shields.io/badge/docker-compose-blue)
![Airflow](https://img.shields.io/badge/Airflow-docker--SSHOperator-blue)

> **Plateforme d'ingestion & d'analyse de données académiques** pour un environnement universitaire.
>
> Données : **OpenAlex (API)**, **data.gov.ma / CKAN (documents)**, **sites d'établissements (Web statique)** → normalisation → stockage data lake + analytics (Spark/Hudi, Hive/Metastore, PostgreSQL, Elasticsearch) + visualisation **Metabase** + recherche via **FastAPI**.

---

## 1) Présentation générale

**University Data Platform** est une plateforme data **end-to-end**, réalisée dans le cadre du **University Data Platform Challenge** (Best-of-9 MVP, 3 semaines), conçue pour automatiser :

- **L'ingestion** de données depuis 3 types de sources hétérogènes :
  - **OpenAlex** (API) : publications et métadonnées académiques
  - **data.gov.ma / CKAN** (fichiers/documents) : jeux de données ouvertes
  - **Sites d'établissements** (web statique, `BeautifulSoup`) : profils faculty, catalogues de cours
- **La transformation** et la structuration :
  - via **Apache Spark** (jobs `faculty_profiles_job`, `course_catalog_job`)
  - avec une couche **curated** en **Apache Hudi**
  - et un **catalogue SQL** via **Hive Metastore**
- **La synchronisation** vers les couches de restitution :
  - **PostgreSQL** (pour Metabase)
  - **Elasticsearch** (pour la recherche)
- **La recherche** :
  - via une **API FastAPI** (`src/api/search_api.py`) adossée à Elasticsearch
- **La restitution BI** :
  - via **Metabase** (dashboard KPIs)
- **L'orchestration** :
  - via **Apache Airflow** (conteneur Docker), avec exécution des tâches Spark/ingestion sur l'hôte via `SSHOperator`

---

## 2) Architecture (description + diagramme textuel)

```text
[OpenAlex API] [data.gov.ma / CKAN] [Sites Web (BeautifulSoup)]
                        |
                        v
                [MinIO S3 - raw zone]
                        |
                        v
        [Spark - transformation, native sur l'hôte]
                        |
                        v
              [Apache Hudi - curated zone]
                        |
                        v
              [Hive Metastore - catalogue SQL]
                 /                       \
                v                         v
     [PostgreSQL] --> [Metabase]   [Elasticsearch] --> [FastAPI search_api]
```

### Flux fonctionnel (high-level)

1. **Ingestion**
   - `Fahd_openalex.py`, `fahd_datagov.py`, `generic_crawler.py` récupèrent les données.
   - Les scripts écrivent des **fichiers bruts** et du **JSON structuré** dans **MinIO** via `fahd_client.py`.
2. **Transformation**
   - Spark lit depuis MinIO (`s3a://...`).
   - Écrit les données en **curated** (Hudi) et crée/rafraîchit les tables **Hive**.
3. **Synchronisation**
   - `postgres_writer.py` alimente PostgreSQL pour Metabase.
   - `es_writer.py` alimente Elasticsearch pour la recherche.
4. **BI & Recherche**
   - Metabase lit les tables structurées (via Postgres) et affiche les KPIs.
   - `search_api.py` expose un endpoint de recherche mot-clé sur Elasticsearch.

### Particularité : Airflow (Docker) + Spark (hôte)

Airflow n'est pas supporté nativement sous Windows (conflits de dépendances `pydantic`/`typing_extensions`), et le conteneur Airflow n'embarque pas PySpark ni les jars Hudi. Le choix retenu :

- **Airflow** : conteneur Docker (`docker-compose.yml`)
- **Ingestion + Spark** : exécution native sur l'hôte Windows, dans l'environnement virtuel `.venv`
- **Liaison** : chaque tâche Airflow est un `SSHOperator` qui déclenche un script `.bat` sur l'hôte via `host.docker.internal`

Chaque tâche appelle un fichier `.bat` dédié plutôt qu'une commande inline, pour deux raisons :
- éviter les problèmes de guillemets imbriqués mal transmis par `Win32-OpenSSH` → `cmd.exe`
- forcer l'encodage UTF-8 de la console (`chcp 65001` + `set PYTHONIOENCODING=utf-8`), indispensable car les données scrapées contiennent des caractères arabes (établissements marocains bilingues fr/ar)

---

## 3) Stack technologique

- **Langage / runtime** : Python 3.11
- **Orchestration** : **Apache Airflow** (Docker + `SSHOperator`)
- **Ingestion** : scripts Python (API, documents, web)
- **Stockage Data Lake** : **MinIO (S3 compatible)**
- **Transformation** : **Apache Spark** (natif sur l'hôte)
- **Curated format** : **Apache Hudi**
- **Catalog SQL** : **Hive Metastore**
- **Données structurées** : **PostgreSQL**
- **Recherche** : **Elasticsearch** + **FastAPI**
- **BI** : **Metabase**
- **Conteneurisation** : **Docker Compose**

---

## 4) Structure du projet

```text
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
├── jars/                                # Jars Spark (Hudi, connecteurs...)
├── jdbc/                                # Driver JDBC Postgres
├── run_ingestion_openalex.bat
├── run_ingestion_datagov.bat
├── run_ingestion_web.bat
├── run_faculty_profiles.bat
├── run_course_catalog.bat
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 5) Installation & démarrage (Docker Compose)

### Pré-requis
- Windows avec **OpenSSH Server** actif (service `sshd` en `Running`)
- **Docker Desktop**
- **Python 3.11** + environnement virtuel `.venv` sur l'hôte
- **PySpark** + jars Hudi disponibles dans `jars/`
- Une paire de clés SSH pour la connexion Airflow → hôte

### Étapes

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

### Vérification rapide
```bash
docker ps
```

---

## 6) Sources de données & volumes

### Sources externes
1. **OpenAlex** (API)
   - publications et métadonnées académiques, pagination `offset/rows`
2. **data.gov.ma / CKAN** (fichiers/documents)
   - jeux de données ouvertes (CSV/JSON)
3. **Sites d'établissements** (web statique)
   - profils faculty
   - catalogue de cours

### MinIO buckets (data lake)

Buckets utilisés par le pipeline :
- `raw-json` — Données structurées JSON
- `raw-web-html` — Pages HTML brutes
- `raw-documents` — Fichiers/documents CKAN
- `raw-logs` — Logs d'ingestion (JSON, par run)
- Zone **curated** (Hudi) — tables transformées

### Organisation (partitionnement)
- Objets nommés à partir du hash du contenu (`content_hash[:10-12]`) pour l'idempotence
- Organisation par date (`year=YYYY/month=MM/day=DD`) selon la source

---

## 7) Commandes d'exécution (ingestion, transformation)

> En production, l'ordre est piloté par le DAG Airflow `university_data_platform_daily`, entièrement séquentiel (choix volontaire pour la stabilité de l'hôte).

### 7.1 Ingestion

```powershell
python -m src.ingestion.api.Fahd_openalex
python -m src.ingestion.docs.fahd_datagov
python -m src.ingestion.web.generic_crawler
```

### 7.2 Transformation Spark (curated + Hive)

```powershell
python -m src.transformations.spark.jobs.faculty_profiles_job
python -m src.transformations.spark.jobs.course_catalog_job
```

> La transformation lit depuis MinIO puis écrit dans la zone curated (Hudi), crée/rafraîchit les tables Hive `faculty_profiles` et `course_catalog`, et synchronise vers PostgreSQL et Elasticsearch.

### 7.3 Ordre d'exécution du DAG Airflow

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

### 7.4 Déclenchement manuel (démo)

Depuis l'UI Airflow : `DAGs` → `university_data_platform_daily` → bouton **Trigger DAG**.

---

## 8) Accès aux services (ports & endpoints)

| Service | URL | Port |
|---|---:|---:|
| **MinIO Console** | http://localhost:9001 | 9001 |
| **MinIO S3** | http://localhost:9000 | 9000 |
| **Metabase** | http://localhost:3000 | 3000 |
| **Airflow Webserver** | http://localhost:8081 | 8081 |
| **Elasticsearch** | http://localhost:9200 | 9200 |
| **API de recherche (FastAPI)** | http://localhost:8000 | 8000 |
| **PostgreSQL (Hive/Metabase)** | — | 5435 |

> ⚠️ Ports indicatifs alignés sur `docker-compose.yml` — à vérifier/ajuster selon ta configuration exacte si elle diffère.

---

## 9) Traçabilité et idempotence

- Chaque enregistrement curated et chaque objet raw porte un `content_hash`/`checksum` et un `source_url`/`raw_object_path`, ce qui permet de remonter de n'importe quel résultat (dashboard, recherche) jusqu'à l'objet brut d'origine dans MinIO.
- Les noms d'objets MinIO sont basés sur le hash du contenu → un rerun sans changement de données ne crée pas de doublon.
- Chaque run d'ingestion écrit un log JSON dans `raw-logs/` (source, nombre d'enregistrements, erreurs, `ingestion_id`).
- Les tables Hudi supportent l'upsert : les jobs Spark peuvent être rejoués sans dupliquer les données déjà présentes (`duplicates_dropped` suivi dans les logs de job).

---

## 10) Auteurs

- Projet réalisé dans le cadre du challenge **University Data Platform** (Best-of-9 MVP, 3 semaines).
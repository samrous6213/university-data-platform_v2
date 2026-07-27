# University Data Platform V2

![Status](https://img.shields.io/badge/status-production--ready-brightgreen)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Spark](https://img.shields.io/badge/Spark-3.5.1-orange)
![Hudi](https://img.shields.io/badge/Hudi-0.15.0-blue)
![Docker](https://img.shields.io/badge/docker--compose-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.10.0-blue)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.11.0-yellow)

> **Plateforme d'ingestion & d'analyse de données académiques** pour les universités marocaines.
>
> Données : **OpenAlex (API)**, **UH2C (Web Scraping)**, **HCP (Documents)** → normalisation → stockage data lake + analytics (Spark/Hudi/Parquet, Hive/Metastore, PostgreSQL) + recherche **Elasticsearch** + visualisation **Metabase**.

---

## 1) Présentation générale

**University Data Platform V2** est une plateforme data **end-to-end** conçue pour automatiser :

- **L'ingestion** de données depuis des sources académiques hétérogènes :
  - **OpenAlex API** : auteurs & publications académiques
  - **UH2C Web** (scraping HTML + crawling BFS) : actualités & profils faculty de 4 institutions marocaines (FSJESM, FSBM, ENSCASA, ENCGCASA)
  - **HCP Documents** (crawling BFS) : documents officiels (PDF, Word, Excel, CSV, archives)
- **La transformation** et la structuration :
  - via **Apache Spark 3.5.1**
  - avec une couche **curated** (Apache Hudi 0.15.0 / Parquet)
  - et un **catalog SQL** via **Hive Metastore**
- **L'indexation pour la recherche** :
  - via **Elasticsearch 8.11**
- **La restitution BI** :
  - via **Metabase** (dashboards)
- **L'orchestration** :
  - via **Apache Airflow 2.10** (DAG `hiba_pipeline`)

---

## 2) Architecture

### Flux fonctionnel (high-level)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION                                    │
│  ┌──────────┐  ┌──────────────────┐  ┌──────────┐                  │
│  │ OpenAlex │  │  UH2C (4 écoles) │  │   HCP    │                  │
│  │  (API)   │  │  FSJESM/FSBM/    │  │  (Docs)  │                  │
│  │          │  │  ENSCASA/ENCGCASA │  │          │                  │
│  └────┬─────┘  └────┬─────────────┘  └────┬─────┘                  │
│       │              │                      │                        │
│       ▼              ▼                      ▼                        │
│  ┌─────────────────────────────────────────────────┐                │
│  │              MinIO (Data Lake)                   │                │
│  │  raw-json │ raw-web-html │ raw-documents        │                │
│  │  raw-images │ raw-logs │ raw-html               │                │
│  └──────────────────────┬──────────────────────────┘                │
└─────────────────────────┼───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION (Spark)                            │
│  ┌────────────────┐  ┌────────────────────┐                         │
│  │ faculty_       │  │ research_          │                         │
│  │ profiles_etl   │  │ publications_etl   │                         │
│  └────────┬───────┘  └────────┬───────────┘                         │
│  ┌────────────────┐  ┌────────────────────┐                         │
│  │ university_    │  │ documents_         │                         │
│  │ news_etl       │  │ registry_etl       │                         │
│  └────────┬───────┘  └────────┬───────────┘                         │
│           │                   │                                      │
│           ▼                   ▼                                      │
│  ┌─────────────────────────────────────────┐                        │
│  │      Apache Hudi (Curated Layer)         │                        │
│  │   s3a://hudi/ → Hive Metastore sync      │                        │
│  └──────────────────────┬──────────────────┘                        │
└─────────────────────────┼───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    INDEXATION & RECHERCHE                            │
│  ┌──────────────────────────────────────────┐                      │
│  │            Elasticsearch 8.11             │                      │
│  │   4 index : faculty / publications /     │                      │
│  │            news / documents               │                      │
│  └──────────────────────┬───────────────────┘                      │
└─────────────────────────┼───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BI & VISUALISATION                                │
│  ┌──────────────────────────────────────────┐                      │
│  │              Metabase                     │                      │
│  │         Dashboards & KPIs                 │                      │
│  └──────────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3) Stack technologique

| Catégorie | Technologie | Version |
|-----------|-------------|---------|
| **Langage / runtime** | Python | 3.11 |
| **Orchestration** | Apache Airflow | 2.10.0 |
| **Ingestion** | Scrapers Python (requests, BeautifulSoup4, lxml) | — |
| **Transformation** | Apache Spark | 3.5.1 |
| **Curated format** | Apache Hudi | 0.15.0 |
| **Catalog SQL** | Apache Hive Metastore | 4.0.0 |
| **Stockage Data Lake** | MinIO (S3-compatible) | latest |
| **Base de données** | PostgreSQL | 13 |
| **Recherche plein-texte** | Elasticsearch | 8.11.0 |
| **BI / Dashboards** | Metabase | v0.56.6 |
| **Conteneurisation** | Docker Compose | 3.8 |
| **Parsing PDF** | PyMuPDF | — |
| **Parsing HTML** | BeautifulSoup4 / lxml | — |

---

## 4) Structure du projet

```text
university-data-platform_v2/
├── docker-compose.yml              # Orchestration des services
├── Dockerfile.airflow              # Image Airflow (OpenJDK 17 + Hudi)
├── Dockerfile.spark                # Image Spark (Hudi + MinIO JARs)
├── Dockerfile.hive                 # Image Hive (PostgreSQL JDBC + Hadoop-AWS)
├── requirements-airflow.txt        # Dépendances Airflow
├── README.md                       # Cette documentation
├── RUNBOOK.md                      # Guide opérationnel complet
│
├── conf/                           # Configuration Hadoop / Spark / Hive
│   ├── core-site.xml               # Configuration Hadoop core (MinIO S3A)
│   ├── hive-site.xml               # Configuration Hive Metastore
│   ├── hive-entrypoint.sh          # Script d'entrée Hive
│   └── spark-defaults.conf         # Configuration Spark par défaut
│
├── dags/                           # DAGs Apache Airflow
│   ├── hiba_pipeline.py            # Pipeline principal (ingestion → ETL → indexation)
│   └── common/
│       └── spark_utils.py          # Utilitaires Spark partagés
│
├── src/                            # Code source Python
│   ├── __init__.py
│   │
│   ├── ingestion/                  # Scripts d'ingestion
│   │   ├── api/
│   │   │   └── hiba_openalex.py    # Scraper OpenAlex API (auteurs)
│   │   ├── web/
│   │   │   └── hiba_uh2c.py        # Scraper UH2C (FSJESM, FSBM, ENSCASA, ENCGCASA)
│   │   └── docs/
│   │       └── hiba_hcp.py         # Scraper HCP (documents officiels, crawling BFS)
│   │
│   ├── storage/
│   │   └── minio/
│   │       └── hiba_client.py      # Client MinIO (upload JSON/binaire)
│   │
│   ├── transformations/            # Couche ETL Spark
│   │   ├── config/
│   │   │   └── hudi_config.py      # Configuration Hudi (tables)
│   │   ├── readers/
│   │   │   └── minio_reader.py     # Lecteur MinIO (découverte prefixes, JSON)
│   │   ├── writers/
│   │   │   └── hudi_writer.py      # Writer Hudi (upsert)
│   │   ├── transformers/
│   │   │   ├── faculty_transformer.py
│   │   │   ├── publications_transformer.py
│   │   │   ├── news_transformer.py
│   │   │   └── documents_transformer.py
│   │   ├── spark/                  # Jobs ETL (spark-submit)
│   │   │   ├── faculty_profiles_etl.py
│   │   │   ├── research_publications_etl.py
│   │   │   ├── university_news_etl.py
│   │   │   ├── documents_registry_etl.py
│   │   │   ├── course_catalog_etl.py
│   │   │   ├── hiba_html_parser.py
│   │   │   ├── hiba_json_parser.py
│   │   │   ├── hiba_logs_parser.py
│   │   │   ├── hiba_metadata_parser.py
│   │   │   ├── hiba_image_parser.py
│   │   │   └── hiba_document_parser.py
│   │   └── utils/
│   │       └── logger.py           # Logger personnalisé
│   │
│   └── search/                     # Indexation Elasticsearch
│       ├── index_faculty_profiles.py
│       ├── index_research_publications.py
│       ├── index_university_news.py
│       └── index_documents_registry.py
│
└── data/                           # Données traitées (outputs)
    ├── processed_documents/        # Documents parsés (Parquet)
    ├── processed_html/             # Pages HTML parsées
    ├── processed_images/           # Métadonnées d'images
    ├── processed_json/             # Données JSON parsées
    ├── processed_logs/             # Logs parsés
    ├── processed_metadata/         # Métadonnées extraites
    ├── curated/unified_content/    # Données unifiées
    └── lakehouse/hudi/             # Tables Hudi
        ├── faculty_profiles/
        ├── research_publications/
        ├── university_news/
        └── documents_registry/
```

---

## 5) Installation & démarrage (Docker Compose)

### Pré-requis

- **Docker Desktop** (ou Docker Engine) avec **WSL2** activé
- **Docker Compose** v2+
- Minimum **8 Go RAM** recommandés

### Cloner le projet

```bash
git clone https://github.com/VOTRE_UTILISATEUR/university-data-platform_v2.git
cd university-data-platform_v2
```

### Lancer l'infrastructure

```bash
docker compose up -d --build
```

| Flag | Rôle |
|------|------|
| `up` | Crée et démarre les containers |
| `-d` | Détache les containers (mode arrière-plan) |
| `--build` | Force la reconstruction des images (Dockerfile.airflow, Dockerfile.spark, Dockerfile.hive) |

### Vérification rapide

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

---

## 6) Sources de données

### Sources externes

| Source | Type | Script | Données |
|--------|------|--------|---------|
| **OpenAlex** | API REST | `src/ingestion/api/hiba_openalex.py` | Auteurs académiques |
| **UH2C** | Web Scraping (HTML) + Crawling BFS | `src/ingestion/web/hiba_uh2c.py` | News & Faculty de 4 institutions |
| **HCP** | Crawling BFS (Documents) | `src/ingestion/docs/hiba_hcp.py` | Documents officiels (PDF, Word, Excel, CSV) |

### Institutions UH2C

| Abréviation | Nom complet | URL |
|-------------|-------------|-----|
| FSJESM | FSJES Mohammedia | fsjesm.ma |
| FSBM | FSBM | www.fsbm.ma |
| ENSCASA | ENS Casablanca | www.enscasa.ma |
| ENCGCASA | ENCG Casablanca | encgcasa.ma |

### MinIO Buckets (Data Lake)

| Bucket | Contenu |
|--------|---------|
| `raw-json` | Données structurées JSON (ingestion + métadonnées) |
| `raw-html` | Pages HTML brutes |
| `raw-web-html` | Pages HTML du web scraping |
| `raw-documents` | PDFs, Word, Excel, CSV, archives |
| `raw-images` | Images extraites |
| `raw-logs` | Logs d'ingestion |
| `hudi` | Données transformées (tables Hudi) |

### Organisation des données

```
s3a://raw-json/
├── source=openalex/year=YYYY/month=MM/day=DD/
├── source=fsjesm/year=YYYY/month=MM/day=DD/
├── source=fsbm/year=YYYY/month=MM/day=DD/
├── source=enscasa/year=YYYY/month=MM/day=DD/
├── source=encgcasa/year=YYYY/month=MM/day=DD/
├── source=hcp_docs/year=YYYY/month=MM/day=DD/
└── ...
```

---

## 7) Pipeline Airflow

### DAG : `hiba_pipeline`

Le pipeline principal orchestre **11 tâches** en séquence :

```
ingest_openalex ─┐
ingest_uh2c    ──┤──► faculty_profiles_etl ──► research_publications_etl
ingest_hcp     ──┘                                │
                                                   ▼
                            university_news_etl ──► documents_registry_etl
                                                          │
                                                          ▼
                              index_faculty_profiles ──► index_research_publications
                                                              │
                                                              ▼
                                    index_university_news ──► index_documents_registry
```

| Task | Type | Description |
|------|------|-------------|
| `ingest_openalex` | BashOperator | Extraction auteurs via API OpenAlex |
| `ingest_uh2c` | BashOperator | Scraping FSJESM, FSBM, ENSCASA, ENCGCASA |
| `ingest_hcp` | BashOperator | Crawling BFS du site hcp.ma |
| `faculty_profiles_etl` | SparkSubmitOperator | ETL profils enseignants → Hudi |
| `research_publications_etl` | SparkSubmitOperator | ETL publications recherche → Hudi |
| `university_news_etl` | SparkSubmitOperator | ETL actualités universitaires → Hudi |
| `documents_registry_etl` | SparkSubmitOperator | ETL registre de documents → Hudi |
| `index_faculty_profiles` | BashOperator | spark-submit → Elasticsearch |
| `index_research_publications` | BashOperator | spark-submit → Elasticsearch |
| `index_university_news` | BashOperator | spark-submit → Elasticsearch |
| `index_documents_registry` | BashOperator | spark-submit → Elasticsearch |

---

## 8) Spark ETL

### 4 pipelines ETL actifs

Chaque pipeline lit des données JSON brutes depuis MinIO (`s3a://raw-json/`), applique des transformations (normalisation, validation, nettoyage), puis écrit les résultats dans une table Hudi synchronisée avec Hive Metastore.

| Pipeline | Script | Table Hudi | Description |
|----------|--------|------------|-------------|
| Faculty Profiles | `src/transformations/spark/faculty_profiles_etl.py` | `faculty_profiles` | Profils enseignants (noms, départements, emails, recherches) |
| Research Publications | `src/transformations/spark/research_publications_etl.py` | `research_publications` | Publications scientifiques (titres, auteurs, DOI) |
| University News | `src/transformations/spark/university_news_etl.py` | `university_news` | Actualités universitaires (titres, catégories) |
| Documents Registry | `src/transformations/spark/documents_registry_etl.py` | `documents_registry` | Registre de documents (noms, types, tailles) |

### Commandes spark-submit

```bash
# Accéder au conteneur Spark Master
docker exec -it spark-master bash
cd /opt/spark/work-dir

# Faculty Profiles ETL
PYTHONPATH=/opt/spark/work-dir \
/opt/spark/bin/spark-submit \
  --conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
  src/transformations/spark/faculty_profiles_etl.py

# Research Publications ETL
PYTHONPATH=/opt/spark/work-dir \
/opt/spark/bin/spark-submit \
  --conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
  src/transformations/spark/research_publications_etl.py

# University News ETL
PYTHONPATH=/opt/spark/work-dir \
/opt/spark/bin/spark-submit \
  --conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
  src/transformations/spark/university_news_etl.py

# Documents Registry ETL
PYTHONPATH=/opt/spark/work-dir \
/opt/spark/bin/spark-submit \
  --conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
  src/transformations/spark/documents_registry_etl.py
```

---

## 9) Apache Hudi

### Tables configurées

| Table | Record Key | Precombine | Partition | Base Path |
|-------|------------|------------|-----------|-----------|
| `faculty_profiles` | `record_id` | `crawl_timestamp` | `source_system` | `s3a://hudi/faculty_profiles` |
| `research_publications` | `record_id` | `crawl_timestamp` | `source_system` | `s3a://hudi/research_publications` |
| `university_news` | `record_id` | `crawl_timestamp` | `source_system` | `s3a://hudi/university_news` |
| `documents_registry` | `record_id` | `crawl_timestamp` | `source_system` | `s3a://hudi/documents_registry` |
| `course_catalog` | `record_id` | `crawl_timestamp` | `source_system` | `s3a://hudi/course_catalog` |

- **Type** : `COPY_ON_WRITE`
- **Opération** : `upsert`
- **Synchronisation Hive** : activée (`hoodie.datasource.hive_sync.enable=true`)
- **Database** : `university_data_platform`



### Vérification Hudi

```bash
# Tables dans Hive
docker exec -it spark-master bash
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SHOW TABLES IN university_data_platform;"

# Compter les lignes
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT COUNT(*) FROM university_data_platform.faculty_profiles;"

/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT COUNT(*) FROM university_data_platform.research_publications;"

/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT COUNT(*) FROM university_data_platform.university_news;"

/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT COUNT(*) FROM university_data_platform.documents_registry;"

# Afficher des lignes
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT * FROM university_data_platform.faculty_profiles LIMIT 3;"
```

---

## 10) Hive Metastore

- **Base de données** : `university_data_platform`
- **Backend PostgreSQL** : `university-postgres` (port 5432)
- **Port Thrift** : 9083
- **Synchronisation** : automatique via Hudi Hive Sync

---

## 11) Elasticsearch

### 4 index

| Index | Script | Contenu |
|-------|--------|---------|
| `faculty_profiles` | `src/search/index_faculty_profiles.py` | Noms, départements, emails, institutions |
| `research_publications` | `src/search/index_research_publications.py` | Titres, auteurs, DOI, mots-clés |
| `university_news` | `src/search/index_university_news.py` | Titres, catégories, institutions |
| `documents_registry` | `src/search/index_documents_registry.py` | Noms, types, tailles, sources |

### Commandes d'indexation

```bash
# Depuis le conteneur spark-master
cd /opt/spark/work-dir/src/search

/opt/spark/bin/spark-submit index_faculty_profiles.py
/opt/spark/bin/spark-submit index_research_publications.py
/opt/spark/bin/spark-submit index_university_news.py
/opt/spark/bin/spark-submit index_documents_registry.py
```

### Vérification Elasticsearch

```powershell
# Lister les index
Invoke-RestMethod http://localhost:9200/_cat/indices?v

# Compter les documents
Invoke-RestMethod http://localhost:9200/faculty_profiles/_count
Invoke-RestMethod http://localhost:9200/research_publications/_count
Invoke-RestMethod http://localhost:9200/university_news/_count
Invoke-RestMethod http://localhost:9200/documents_registry/_count

# Recherche de test
Invoke-RestMethod "http://localhost:9200/faculty_profiles/_search?q=*&pretty"
Invoke-RestMethod "http://localhost:9200/university_news/_search?q=*&pretty"
```

---

## 12) Metabase

- **URL** : http://localhost:3000
- **Base de données** : PostgreSQL dédiée (`metabase-postgres`, port 5434)
- **Configuration initiale** : assistant de setup au premier lancement

### Connexion aux données

| Méthode | Host | Port | Base |
|---------|------|------|------|
| PostgreSQL (Hive Metastore) | `university-postgres` | 5432 | `metastore` |
| Spark Thrift (JDBC) | `spark-thrift` | 10000 | — |

---

## 13) Accès aux services (ports)

| Service | URL | Port |
|---------|-----|------|
| **MinIO Console** | http://localhost:9001 | 9001 |
| **MinIO S3 API** | http://localhost:9000 | 9000 |
| **Metabase** | http://localhost:3000 | 3000 |
| **Airflow Webserver** | http://localhost:8081 | 8081 |
| **Elasticsearch** | http://localhost:9200 | 9200 |
| **Elasticsearch (transport)** | http://localhost:9300 | 9300 |
| **Spark Master UI** | http://localhost:8080 | 8080 |
| **Spark Worker UI** | http://localhost:8082 | 8082 |
| **Spark Thrift (JDBC/ODBC)** | jdbc:hive2://localhost:10000 | 10000 |
| **Hive Metastore** | thrift://localhost:9083 | 9083 |
| **PostgreSQL (Hive)** | localhost:5432 | 5432 |
| **PostgreSQL (Airflow)** | localhost:5433 | 5433 |
| **PostgreSQL (Metabase)** | localhost:5434 | 5434 |

### Identifiants

| Service | Utilisateur | Mot de passe |
|---------|-------------|--------------|
| **MinIO** | `minioadmin` | `minioadmin` |
| **Airflow** | `admin` | `admin` |
| **PostgreSQL (Hive)** | `hive` | `hive` |
| **PostgreSQL (Metabase)** | `metabase` | `metabase` |
| **PostgreSQL (Airflow)** | `airflow` | `airflow` |

---

## 14) Commandes d'exécution

### Ingestion

```bash
# OpenAlex API (auteurs)
python -m src.ingestion.api.hiba_openalex

# UH2C (FSJESM, FSBM, ENSCASA, ENCGCASA - news + faculty)
python -m src.ingestion.web.hiba_uh2c

# HCP Documents (crawling BFS)
python -m src.ingestion.docs.hiba_hcp
```

### Transformation Spark

```bash
# Depuis le conteneur spark-master
docker exec -it spark-master bash
cd /opt/spark/work-dir

PYTHONPATH=/opt/spark/work-dir \
/opt/spark/bin/spark-submit \
  --conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
  src/transformations/spark/faculty_profiles_etl.py
```

### Indexation Elasticsearch

```bash
cd /opt/spark/work-dir/src/search
/opt/spark/bin/spark-submit index_faculty_profiles.py
```

---

## 15) Commandes de vérification

### Containers

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### Spark Master

```powershell
Invoke-RestMethod http://localhost:8080/json/ | ConvertTo-Json -Depth 2
```

### Elasticsearch

```powershell
Invoke-RestMethod http://localhost:9200
Invoke-RestMethod http://localhost:9200/_cat/indices?v
```

### Hive Metastore

```bash
docker exec -it spark-master bash
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;"
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN university_data_platform;"
```

### MinIO

```powershell
Invoke-RestMethod http://localhost:9001 -ErrorAction SilentlyContinue
```

### PostgreSQL

```bash
docker exec university-postgres psql -U hive -d metastore -c "\dt"
```

---

## 16) Résultats

### Tables Hudi actives

| Table Hudi | Description | Partitions (`source_system`) |
|------------|-------------|------------------------------|
| `faculty_profiles` | Profils enseignants (noms, départements, emails, recherches) | `raw-json` |
| `research_publications` | Publications scientifiques (titres, auteurs, DOI) | `encgcasa`, `enscasa`, `fsbm`, `fsjesm` |
| `university_news` | Actualités universitaires (titres, catégories) | `encgcasa`, `enscasa`, `fsbm`, `fsjesm` |
| `documents_registry` | Documents officiels (noms, types, tailles) | `encgcasa`, `enscasa`, `fsbm`, `fsjesm`, `hcp_docs` |

### Données intermédiaires (Parquet)

Les données parquet intermédiaires sont disponibles dans `data/` :

| Dossier | Contenu |
|---------|---------|
| `processed_documents` | Documents parsés |
| `processed_html` | Pages HTML parsées |
| `processed_images` | Métadonnées d'images |
| `processed_json` | Données JSON parsées |
| `processed_logs` | Logs parsés |
| `processed_metadata` | Métadonnées extraites |
| `curated/unified_content` | Données unifiées |

---

## 17) Documentation

Ce projet contient également un guide opérationnel complet :

- **[RUNBOOK.md](RUNBOOK.md)**

Le RUNBOOK contient :

- Installation détaillée (Docker Desktop, WSL2, cloning)
- Déploiement Docker (`docker compose up -d --build`)
- Vérification de chaque container (Spark, Hive, PostgreSQL, Elasticsearch, MinIO, Metabase)
- Commandes Spark ETL pour chaque table
- Vérification Hudi (tables, lignes, échantillons)
- Vérification Elasticsearch (index, documents, recherche)
- Dépannage (11 problèmes courants documentés avec solutions)
- Checklist finale de validation

---

## 18) Auteurs

Projet réalisé dans le cadre du challenge **University Data Platform V2**.

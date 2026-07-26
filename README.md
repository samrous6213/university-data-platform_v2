# University Data Platform v2
![Status](https://img.shields.io/badge/status-production--ready-brightgreen)
![Python](https://img.shields.io/badge/python-3.9-blue)
![Spark](https://img.shields.io/badge/Spark-3.5.1-orange)
![Docker](https://img.shields.io/badge/docker-compose-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.10.0-blue)
![Elasticsearch](https://img.shields.io/badge/ES-8.11.0-green)
![Hudi](https://img.shields.io/badge/Hudi-0.15.0-purple)
![MinIO](https://img.shields.io/badge/MinIO-S3-orange)

> **Plateforme d'ingestion, transformation & analyse de données académiques** pour les universités marocaines.
>
> Données : **OpenAlex API** (auteurs/publications), **UCA / FSSM / ENSA / ENCG Marrakech** (web scraping), **IMIST** (documents/PDF) → normalisation → stockage data lake + transformation (Spark/Hudi/Parquet) + catalog (Hive/Metastore) + indexation (Elasticsearch) + visualisation (Metabase) + recherche vectorielle (Qdrant).

---

## 1) Présentation générale

**University Data Platform v2** est une plateforme data **end-to-end** conçue pour automatiser :

- **L'ingestion** de données depuis des sources académiques hétérogènes :
  - **OpenAlex API** : chercheurs et publications académiques
  - **UCA / FSSM / ENSA / ENCG Marrakech** (web scraping) : profils faculty, news, cours
  - **IMIST** (documents/PDF) : thèses et documents académiques
- **La transformation** et la structuration :
  - via **Apache Spark 3.5.1**
  - avec **Apache Hudi 0.15** (format ACID sur S3)
  - et un **catalog SQL** via **Hive Metastore**
- **L'indexation pour la recherche** :
  - via **Elasticsearch 8.11** (recherche full-text)
  - via **Qdrant** (recherche vectorielle / RAG)
- **La restitution BI** :
  - via **Metabase** (dashboard de KPIs)
- **L'orchestration** :
  - via **Apache Airflow 2.10** (pipeline planifié quotidien)

---

## 2) Architecture

```
OpenAlex API  ──┐
UCA / FSSM /     ├──> MinIO (raw-json, raw-web-html, raw-images,
ENSA / ENCG    ──┤      raw-documents, raw-logs)
IMIST PDFs     ──┘              │
                               ▼
                     ┌─────────────────┐
                     │  Spark ETL      │
                     │  (5 jobs Hudi)  │
                     │  run_all_etl.py │
                     └───────┬─────────┘
                             │
                    ┌────────┴────────┐
                    ▼                  ▼
            ┌──────────────┐   ┌──────────────┐
            │  Hudi COW    │   │ Elasticsearch│
            │  hudi-curated│   │ 5 indexes    │
            │  (5 tables)  │   │              │
            └──────┬───────┘   └──────────────┘
                   │
                   ▼
            ┌──────────────┐
            │ Hive         │
            │ Metastore    │
            │ + Server     │
            └──────┬───────┘
                   │
            ┌──────────────┐   
            │  Metabase    │ 
            │  (BI/KPIs)   │   
            └──────────────┘   

                   ┌─────────────────┐
                   │    Airflow      │
                   │ Orchestrateur   │
                   │ @daily schedule │
                   └─────────────────┘
```

### Flux fonctionnel

1. **Ingestion** — 3 scrapers (OpenAlex API, UCA web, IMIST PDFs) écrivent dans **MinIO** (buckets `raw-json`, `raw-web-html`, `raw-images`, `raw-documents`, `raw-logs`)
2. **Transformation** — Spark lit depuis MinIO via S3A, transforme et écrit 5 tables **Hudi COPY_ON_WRITE** dans `s3a://hudi-curated/` avec synchronisation **Hive Metastore**
3. **Indexation** — Les 5 DataFrames sont indexés dans **Elasticsearch** (5 indexes) et disponibles pour **Qdrant**
4. **BI & Recherche** — **Metabase** interroge les tables via Hive JDBC ; **Elasticsearch** permet la recherche full-text ; **Qdrant** prépare la recherche vectorielle

---

## 3) Stack technologique

| Composant | Version | Rôle |
|-----------|---------|------|
| Python | 3.9+ | Langage (scrapers, ETL) |
| Apache Spark | 3.5.1 | Transformation distribuée |
| Apache Hudi | 0.15.0 | Format ACID sur S3 (COW) |
| Apache Hive | 3.1.3 | Catalog SQL (Metastore + HiveServer2) |
| Apache Airflow | 2.10.0 | Orchestration pipeline |
| MinIO | latest | Stockage objet compatible S3 |
| Elasticsearch | 8.11.0 | Recherche full-text |
| Qdrant | latest | Recherche vectorielle (RAG) |
| Metabase | 0.56.6 | BI / Dashboards |
| PostgreSQL | 13 | Métadonnées (Hive, Airflow) |
| Docker Compose | - | Conteneurisation |

---

## 4) Structure du projet

```text
src/
  ingestion/
    api/
      chaimae_openalex.py       # Scraper OpenAlex (API)
    web/
      chaimae_uca_faculty.py    # Scraper UCA/FSSM/ENSA/ENCG (HTML)
    docs/
      chaimae_imist.py          # Scraper IMIST (PDF/documents)

  transformations/
    config/
      spark_config.py            # Configuration Spark (S3A, Hive, Hudi)
      hudi_config.py             # 5 tables Hudi (record_key, partition, options)
    readers/
      minio_reader.py            # Lecture MinIO via S3A (discovery, JSON, cache)
    transformers/
      base_transformer.py        # Utilitaires (drop_nulls, dedup, normalize)
      faculty_transformer.py     # Transform faculty → 19 champs
      course_transformer.py      # Transform courses → 16 champs
      news_transformer.py        # Transform news → 14 champs
      publications_transformer.py # Transform publications → 15 champs
      documents_transformer.py   # Transform documents → 13 champs
    writers/
      hudi_writer.py             # Écriture Hudi COW (upsert, retry)
      es_writer.py               # Indexation Elasticsearch (bulk, mappings)
    utils/
      logger.py                  # StructuredLogger
      metadata.py                # record_id, content_hash, timestamps
      schema_validator.py        # Validation de schéma
    etl/
      faculty_profiles_etl.py    # ETL faculty
      course_catalog_etl.py      # ETL courses
      university_news_etl.py     # ETL news
      research_publications_etl.py # ETL publications
      documents_registry_etl.py  # ETL documents
    run_all_etl.py               # Entry point (exécute les 5 ETL)

  storage/minio/
    chaimae_client.py             # Client MinIO (upload, buckets)

  scripts/
    inspect_parquet.py            # Inspection Parquet/Hudi

  search/elasticsearch/           # Placeholder (index.py, query.py)
  lakehouse/hive/                 # Placeholder (metastore.py)
  lakehouse/hudi/                 # Placeholder (tables.py, upsert.py)

dags/
  chaimae_pipeline.py             # DAG Airflow : ingestion → transformation → indexation
  common/
    spark_utils.py                # Utilitaires Spark partagés

scripts/
  hive/
    Dockerfile.hive               # Image HiveServer2
    entrypoint-hive-server.sh     # Entrypoint HiveServer2
    sync_and_validate.py          # Sync Hudi → Hive + validation
    metastore/
      Dockerfile                  # Image Hive Metastore
      entrypoint.sh               # Entrypoint Metastore
      hive-site.xml               # Config Metastore

docker-compose.yml
Dockerfile.spark                  # Image Spark 3.5.1 + Hudi 0.15
requirements.txt
README.md
RUNBOOK.md
hassari.md                        # Documentation MVP
CHAIME.md                         # Documentation technique
presentation_guide.md             # Guide de présentation
```

---

## 5) Installation & démarrage (Docker Compose)

### Pré-requis
- Docker Desktop (ou Docker Engine + Compose)
- Git

### Lancer l'infrastructure
```bash
docker compose up -d
```

### Vérification rapide
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

Conteneurs attendus (13) :
- `university-minio`, `university-postgres`, `hive-metastore`, `university-hive-server`
- `spark-master`, `spark-worker`
- `university-elasticsearch`, `university-qdrant`
- `university-metabase`
- `airflow-postgres`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`

---

## 6) Sources de données & volumes

### Sources externes

1. **OpenAlex API** (API REST)
   - Auteurs (authors) et publications (works) d'institutions marocaines
   - Pagination via `offset` / `per_page`
   - Stockage : `raw-json/source=openalex/`

2. **UCA / FSSM / ENSA / ENCG Marrakech** (Web scraping HTML)
   - Profils faculty (enseignants-chercheurs)
   - Actualités (news) et catalogue de cours
   - Extraction JSON-LD et HTML structuré
   - Stockage : `raw-json/source=fssm/`, `source=ensa/`, `source=encg/`

3. **IMIST** (Documents PDF)
   - Exploration BFS de `www.imist.ma`
   - Téléchargement PDF, DOC, XLS, PPT
   - Extraction de métadonnées par mots-clés
   - Checkpoint file pour reprise (`imist_crawl_checkpoint.txt`)
   - Stockage : `raw-documents/`, `raw-json/source=imist_docs/`

### MinIO buckets (data lake)

| Bucket | Contenu |
|--------|---------|
| `raw-json` | Données structurées JSON (toutes sources) |
| `raw-web-html` | Pages HTML brutes (UCA scraping) |
| `raw-images` | Images extraites |
| `raw-documents` | PDFs et documents bruts (IMIST) |
| `raw-logs` | Logs d'ingestion |
| `hudi-curated` | Tables Hudi transformées (Parquet) |

### Organisation
- Objets organisés par source et date : `source=<name>/year=YYYY/month=MM/day=DD/`
- Métadonnées d'exécution : `crawl_timestamp`, `content_hash` (SHA-256)

---

## 7) Commandes d'exécution

### 7.1 Scrapers (ingestion)

#### OpenAlex API (auteurs + publications)
```bash
python -m src.ingestion.api.chaimae_openalex
```

#### UCA / FSSM / ENSA / ENCG Web (faculty + news + cours)
```bash
python -m src.ingestion.web.chaimae_uca_faculty
```

#### IMIST PDFs (documents)
```bash
python -m src.ingestion.docs.chaimae_imist
```

### 7.2 Transformation Spark (Hudi + Hive)

#### Lancer les 5 ETL séquentiellement
```powershell
docker exec -e PYTHONPATH=/opt/spark/work-dir spark-master /opt/spark/bin/spark-submit `
  --master local[1] --driver-memory 4g `
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir `
  /opt/spark/work-dir/src/transformations/run_all_etl.py
```

**ETL exécutés :**
1. `faculty_profiles_etl` → `s3a://hudi-curated/faculty_profiles` + ES index `faculty_profiles`
2. `course_catalog_etl` → `s3a://hudi-curated/course_catalog` + ES index `course_catalog`
3. `university_news_etl` → `s3a://hudi-curated/university_news` + ES index `university_news`
4. `research_publications_etl` → `s3a://hudi-curated/research_publications` + ES index `research_publications`
5. `documents_registry_etl` → `s3a://hudi-curated/documents_registry` + ES index `documents_registry`

### 7.3 Indexation Elasticsearch

L'indexation est intégrée dans chaque ETL (via `write_to_elasticsearch()`). Les 5 indexes Elasticsearch sont créés automatiquement avec mappings prédéfinis :
- `faculty_profiles` (nom, institution, département, suggest)
- `course_catalog` (course_name, institution)
- `university_news` (title, institution, category)
- `research_publications` (title, abstract, doi, authors)
- `documents_registry` (document_name, document_type, checksum)

### 7.4 Orchestration Airflow

Le DAG `chaimae_pipeline` exécute (chaîne) :
1. `openalex_to_minio` → ingestion OpenAlex
2. `uca_to_minio` → scraping UCA web
3. `imist_pdfs_to_minio` → extraction IMIST PDFs
4. `spark_etl_to_elasticsearch` → Spark ETL + indexation ES

**Déclenchement :**
```bash
docker compose exec airflow-webserver airflow dags trigger chaimae_pipeline
```

---

## 8) Accès aux services

| Service | URL | Port | Identifiants |
|---------|:---:|:----:|:------------:|
| **MinIO Console** | http://localhost:9001 | 9001 | `minioadmin` / `minioadmin` |
| **MinIO S3 API** | http://localhost:9000 | 9000 | `minioadmin` / `minioadmin` |
| **Spark Master UI** | http://localhost:8080 | 8080 | - |
| **Spark Worker UI** | http://localhost:8082 | 8082 | - |
| **Metabase** | http://localhost:3000 | 3000 | À configurer 1er lancement |
| **Airflow Webserver** | http://localhost:8081 | 8081 | `admin` / `admin` |
| **Elasticsearch** | http://localhost:9200 | 9200 | désactivé |
| **Qdrant REST** | http://localhost:6333 | 6333 | - |
| **HiveServer2** | localhost:10000 | 10000 | `hive` / `hive` |
| **Hive Metastore** | thrift://localhost:9083 | 9083 | - |
| **PostgreSQL (Hive)** | localhost:5432 | 5432 | `hive` / `hive` |
| **PostgreSQL (Airflow)** | localhost:5433 | 5433 | `airflow` / `airflow` |

---

## 9) Résultats des données

Statistiques issues de l'exécution du pipeline :

| Table Hudi / Index ES | Enregistrements | Partition |
|-----------------------|:---------------:|:---------:|
| `faculty_profiles` | ~180 | `source_system` |
| `course_catalog` | ~56 | `source_system` |
| `university_news` | ~15 | `source_system` |
| `research_publications` | ~20 | `publication_year` |
| `documents_registry` | ~175 | `document_type` |
| **Total** | **~446 documents indexés** | |

### KPIs Metabase (8)
1. **Total Professors** — nombre d'enseignants
2. **Total Courses** — nombre de cours
3. **Total News** — nombre d'actualités
4. **Total Publications** — nombre de publications
5. **Professors by Institution** — répartition par institution
6. **Courses by Institution** — répartition des cours
7. **Publications by Year** — chronologie des publications
8. **Documents by Type** — répartition par type de document

---
leryExecutor pour DAG parallèle

---

## 10) Preuves de test

Toutes les captures d'écran des tests sont disponibles dans le dossier `docs/screenshots/` (si présent).

- [ ] UI Airflow — Graph View du DAG `chaimae_pipeline`
- [ ] UI Airflow — Logs d'une tâche (openalex_to_minio)
- [ ] MinIO — Liste des buckets
- [ ] MinIO — Contenu de `raw-json/source=fssm/`
- [ ] Elasticsearch — Comptage documents indexés
- [ ] Elasticsearch — Résultat de recherche
- [ ] Metabase — Dashboard (8 KPIs)
- [ ] Docker — Tous les conteneurs en cours

---

## 11) Documentation complémentaire

- [RUNBOOK d'exploitation](RUNBOOK.md) — Guide opérationnel complet
- [Documentation technique](CHAIME.md) — Architecture et fonctionnement interne
- [Guide de présentation](presentation_guide.md) — Script de démonstration
- [Documentation MVP](hassari.md) — Présentation générale du projet

---

## Auteure

- **Chaimae** — Projet réalisé dans le cadre du challenge University Data Platform.

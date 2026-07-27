# University Data Platform 
![Status](https://img.shields.io/badge/status-production--ready-brightgreen)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Spark](https://img.shields.io/badge/Spark-3.5.1-orange)
![Docker](https://img.shields.io/badge/docker-compose-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.10.0-blue)

> **Plateforme d’ingestion & d’analyse de données académiques** pour les universités marocaines.
>
> Données : **UIZ Web** (scraping HTML), **Khan Academy** (source documentaire), **ORCID API** → normalisation → stockage data lake + analytics (Spark/Hudi/Parquet, Hive/Metastore, PostgreSQL, Elasticsearch) + visualisation **Metabase**.

---

## 1) Présentation générale 

**University Data Platform** est une plateforme data **end-to-end** conçue pour automatiser :

- **L’ingestion** de données depuis des sources académiques hétérogènes :
  - **UIZ Web** (scraping HTML) : news & profils de faculty
  - **Khan Academy** (documents) : source documentaire
  - **ORCID API** : publications académiques
- **La transformation** et la structuration :
  - via **Apache Spark**
  - avec une couche **curated** (Hudi/Parquet)
  - et un **catalog SQL** via **Hive Metastore**
- **L’indexation pour la recherche** :
  - via **Elasticsearch**
- **La restitution BI** :
  - via **Metabase** (dashboard)
- **L’orchestration** :
  - via **Apache Airflow** (pipeline manuel)

---

## 2) Architecture (description + diagramme textuel) 

![Architecture de la plateforme](docs/archi.svg)



### Flux fonctionnel (high-level)

1. **Ingestion** 
   - Scrapers (Python) récupèrent les données.
   - Les scrapers écrivent des **fichiers bruts** et des **JSON structurés** dans **MinIO**.
2. **Transformation** 
   - Spark produit les données **curated**.
   - Les données curated sont validées en **Parquet** et organisées en **Hudi**.
   - Les tables sont enregistrées dans **Hive**.
3. **Indexation** 
   - Les données finales sont indexées dans Elasticsearch.
   - Les index Elasticsearch utilisés sont `safaa_faculty_profiles`, `safaa_university_news`, `safaa_research_publications`.
4. **BI & Recherche** 
   - Metabase lit les tables structurées depuis PostgreSQL.
   - Elasticsearch permet recherche / exploration via documents indexés.

---

## 3) Stack technologique 

- **Langage / runtime** : Python
- **Orchestration** : **Apache Airflow**
- **Ingestion** : scrapers Python
- **Stockage Data Lake** : **MinIO (S3 compatible)**
- **Transformation** : **Apache Spark 3.5.1**
- **Curated format** : **Hudi / Parquet** (avec Spark + Hive)
- **Catalog SQL** : **Hive Metastore**
- **Données structurées** : **PostgreSQL**
- **Recherche** : **Elasticsearch 8.11**
- **BI** : **Metabase**
- **Conteneurisation** : **Docker Compose**

---

## 4) Structure du projet 

```text
configs/

dags/
  safaa_end_to_end_pipeline.py

data/
  curated_from_spark/
    faculty_profiles/
    research_publications/
    university_news/
  spark_input/
    safaa/
      faculty_profiles/
      research_publications/
      university_news/


src/
  ingestion/
    api/
      safaa_orcid.py
    docs/
      safaa_khan_academy.py
    web/
      safaa_uiz.py

  storage/
    minio/
      safaa_client.py

  transformations/
    spark/
      safaa_transform_faculty_profiles.py
      safaa_transform_university_news.py
      safaa_transform_research_publications.py

docker-compose.yml
requirements.txt
README.md
RUNBOOK.md
```

---

## 5) Installation & démarrage (Docker Compose)

### Pré-requis
- Docker Desktop (ou Docker Engine)
- Docker Compose

### Lancer l’infrastructure
```bash
docker compose up -d
```

### Vérification rapide
- Vérifier les conteneurs :
```bash
docker ps
```

---

## 6) Sources de données & volumes 

### Sources externes
1. **UIZ Web** (HTML)
   - news (actualités)
   - profils faculty (enseignants)
   - extraction images et contenus associés quand disponibles
2. **Khan Academy** (source documentaire)
   - exploration de pages
   - détection de fichiers téléchargeables quand disponibles
3. **ORCID API**
   - publications académiques
   - profils chercheurs

### MinIO buckets (data lake) 

Buckets attendus/centralisés pour le pipeline :
- `raw-json` - Données structurées JSON
- `raw-documents` - Documents
- `raw-web-html` - Pages HTML brutes
- `raw-images` - Images extraites
- `raw-logs` - Logs d'ingestion
- `curated` - Données transformées copiées/centralisées

> Les scrapers écrivent des données **brutes** et **structurées JSON** dans MinIO.
> Les sorties Spark/Hudi sont générées et validées dans l’environnement Spark, puis les outputs peuvent être copiés vers MinIO pour centralisation.

### Organisation (partitionnement)
- Objets organisés par date : `year=YYYY/month=MM/day=DD` (selon la logique d’ingestion)
- Données structurées en JSON
- Binaires en objets (HTML / images / documents)

---

## 7) Commandes d’exécution (scrapers, transformation, indexation) 

> Les scrapers et traitements sont implémentés comme modules Python.
> Les scripts sont déclenchés par Airflow via `BashOperator`.

### 7.1 Scrapers (ingestion)

#### 1) ORCID (publications)
```bash
python -m src.ingestion.api.safaa_orcid
```

#### 2) Khan Academy (source documentaire)
```bash
python -m src.ingestion.docs.safaa_khan_academy
```

#### 3) Scraper UIZ (faculty + news + médias)
```bash
python -m src.ingestion.web.safaa_uiz
```

---

### 7.2 Transformation Spark (curated + Hive)

#### Scripts Spark
- `safaa_transform_faculty_profiles.py`
- `safaa_transform_university_news.py`
- `safaa_transform_research_publications.py`

> La transformation produit :
> - `/opt/spark/work-dir/data/curated/safaa/faculty_profiles`
> - `/opt/spark/work-dir/data/curated/safaa/university_news`
> - `/opt/spark/work-dir/data/curated/safaa/research_publications`
>
> Les tables Hudi sont générées dans :
> - `/opt/spark/work-dir/data/hudi/safaa/faculty_profiles`
> - `/opt/spark/work-dir/data/hudi/safaa/university_news`
> - `/opt/spark/work-dir/data/hudi/safaa/research_publications`
>
> Les tables sont enregistrées dans Hive via :
> - `safaa_final_register_hive.py`

---

### 7.3 Indexation Elasticsearch

#### Script indexeur
- `safaa_index_elasticsearch.py`

> Index cibles :
> - `safaa_faculty_profiles`
> - `safaa_university_news`
> - `safaa_research_publications`
>
> Dans le DAG final, la tâche `index_elasticsearch` valide les indices et les counts.

---

## 8) Accès aux services (ports & endpoints) 

| Service | URL | Port |
|---|---:|---:|
| **MinIO Console** | http://localhost:9001 | 9001 |
| **MinIO S3** | http://localhost:9000 | 9000 |
| **Metabase** | http://localhost:3000 | 3000 |
| **Airflow Webserver** | http://localhost:8081 | 8081 |
| **Elasticsearch** | http://localhost:9200 | 9200 |
| **Spark Master UI** | http://localhost:8080 | 8080 |

### Identifiants (selon `docker-compose.yml`)
- **MinIO** : `minioadmin` / `minioadmin`
- **Airflow** : `admin` / `admin`
- **Metabase** : À configurer au premier lancement (création compte admin)
- **PostgreSQL** : `hive` / `hive` (port 5432)

---

## 9) Résultats des données 

Statistiques issues de l’exécution actuelle du pipeline :

- **faculty_profiles** : **79** professeurs 
- **university_news** : **151** actualités 
- **research_publications** : **969** publications
- **Elasticsearch** :
  - `safaa_faculty_profiles` contient **79** documents
  - `safaa_university_news` contient **151** documents
  - `safaa_research_publications` contient **969** documents

✅ **Le DAG Airflow `safaa_end_to_end_pipeline` s'exécute avec succès, toutes les tâches passent en vert.**
Le pipeline est conçu pour être reproductible : en cas d'échec, les tâches peuvent être relancées individuellement via l'UI Airflow.

### KPIs Metabase 

Ces KPIs permettent de visualiser l'état des données ingérées et transformées.

1. **Total Faculty Profiles**
2. **Total University News**
3. **Total Research Publications**
4. **Faculty by Institution**
5. **News by Institution**
6. **Publications by Year**
7. **Research Publications Overview**

---

## 10) Preuves de test 

Toutes les captures d'écran des tests sont disponibles dans le dossier `docs/screenshots/`.

- [ ] UI Airflow - Vue d'ensemble du DAG (toutes les tâches en vert)
- [ ] UI Airflow - Logs d'une tâche
- [ ] UI Airflow - Graph View
- [ ] MinIO - Liste des buckets
- [ ] MinIO - Contenu d'un bucket (raw-json)
- [ ] Elasticsearch - Nombre de documents indexés
- [ ] Elasticsearch - Résultat de recherche
- [ ] Metabase - Dashboard
- [ ] Docker - Tous les conteneurs en cours

✅ Toutes les captures ont été prises et sont disponibles dans le dossier.

---

## Notes de fonctionnement 

- Le DAG Airflow **`safaa_end_to_end_pipeline`** exécute (chaîne) :
  1) `check_services`
  2) `ingest_web_uiz`
  3) `ingest_api_orcid`
  4) `ingest_documents`
  5) `store_raw_minio`
  6) `transform_spark`
  7) `check_curated_outputs`
  8) `write_hudi`
  9) `register_hive`
  10) `index_elasticsearch`
  11) `load_metabase`
  12) `check_metabase`
- Les tables Spark/Hudi principales sont stockées dans le conteneur Spark :
  - `/opt/spark/work-dir/data/curated/safaa`
  - `/opt/spark/work-dir/data/hudi/safaa`

---

### Checklist rapide 
- Vérifier Elasticsearch : `curl -X GET "http://localhost:9200/_cat/indices?v"`
- Démarrer : `docker compose up -d`
- Ouvrir Airflow : http://localhost:8081
- Déclencher le DAG `safaa_end_to_end_pipeline`
- Vérifier MinIO (buckets) puis Metabase (KPIs) et Elasticsearch (indices)


---
## 📚 Documentation complémentaire

- [Runbook d'exploitation](RUNBOOK.md) - Guide opérationnel
- [Architecture détaillée](docs/archi.svg) - Schéma complet
- [Demo Script](docs/demo-script.md) - Script de présentation pour la soutenance

---

## 11) Auteurs

- Projet réalisé dans le cadre du challenge University Data Platform.
- Partie Safaa : ingestion multi-source, Spark/Hudi/Hive, Elasticsearch, Metabase et orchestration Airflow.
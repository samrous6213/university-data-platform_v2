# Demo Script - University Data Platform V2

## Soutenance (15 minutes)

---

### 1. Introduction (1 min)

**University Data Platform V2** est une plateforme data **end-to-end** conçue pour automatiser l'ingestion, la transformation et l'analyse de données académiques marocaines.

**Problème résolu** : Les données universitaires (profils enseignants, publications, actualités, documents) sont dispersées sur plusieurs sites web, des API et des portails documentaires, sans standardisation ni outil de recherche centralisé.

**Technologies principales** :
- **Python** — scrapers et scripts d'ingestion
- **Apache Spark 3.5.1** — transformation et ETL
- **Apache Hudi 0.15.0** — couche curated (lakehouse)
- **MinIO** — data lake S3-compatible
- **Elasticsearch 8.11** — recherche plein-texte
- **Metabase** — visualisation et dashboards
- **Docker Compose** — conteneurisation de l'ensemble

---

### 2. Architecture (2 min)

#### Flux de données

```
Sources (OpenAlex / UH2C / HCP)
        │
        ▼
   MinIO (Data Lake)
   raw-json │ raw-web-html │ raw-documents │ raw-images │ raw-logs
        │
        ▼
   Apache Spark (ETL)
   faculty_profiles_etl │ research_publications_etl
   university_news_etl  │ documents_registry_etl
        │
        ▼
   Apache Hudi (Curated Layer)
   s3a://hudi/ → Hive Metastore sync
        │
        ▼
   Elasticsearch (Recherche)
   4 index : faculty / publications / news / documents
        │
        ▼
   Metabase (Dashboards)
```

#### Services Docker

| Service | Rôle | Port |
|---------|------|------|
| MinIO | Data Lake (S3) | 9000 / 9001 |
| Spark Master | Moteur de transformation | 8080 |
| Spark Worker | Exécution des jobs | — |
| Hive Metastore | Catalogue SQL | 9083 |
| PostgreSQL | Backend Hive + Metabase + Airflow | 5432 / 5433 / 5434 |
| Elasticsearch | Recherche plein-texte | 9200 |
| Metabase | BI / Dashboards | 3000 |
| Airflow | Orchestration du pipeline | 8081 |

---

### 3. Sources de données (2 min)

#### 1. OpenAlex API (`src/ingestion/api/hiba_openalex.py`)

- **Type** : API REST publique
- **Données** : Auteurs académiques (noms, institutions, publications)
- **Stockage** : `s3a://raw-json/source=openalex/`

#### 2. UH2C Web Scraping (`src/ingestion/web/hiba_uh2c.py`)

- **Type** : Scraping HTML + crawling BFS
- **Institutions** : FSJESM, FSBM, ENSCASA, ENCGCASA
- **Données** :
  - **Actualités** (news) : titres, dates, catégories, images
  - **Faculty** : noms, emails, départements
  - **Assets** : images, documents, pages HTML complètes
- **Stockage** :
  - `s3a://raw-json/source=fsjesm/` (et autres institutions)
  - `s3a://raw-web-html/` (pages HTML brutes)
  - `s3a://raw-images/` (images extraites)

#### 3. HCP Documents (`src/ingestion/docs/hiba_hcp.py`)

- **Type** : Crawling BFS (Breadth-First Search)
- **Site** : hcp.ma (Haut-Commissariat au Plan)
- **Données** : Documents officiels (PDF, Word, Excel, CSV, archives)
- **Stockage** :
  - `s3a://raw-documents/source=hcp_docs/` (fichiers bruts)
  - `s3a://raw-json/source=hcp_docs/` (métadonnées)

---

### 4. MinIO (2 min)

#### Démonstration

1. **Ouvrir MinIO Console** : http://localhost:9001
   - Identifiants : `minioadmin` / `minioadmin`

2. **Montrer les buckets** :

| Bucket | Contenu |
|--------|---------|
| `raw-json` | Données structurées JSON + métadonnées |
| `raw-web-html` | Pages HTML du web scraping |
| `raw-documents` | PDFs, Word, Excel, CSV |
| `raw-images` | Images extraites |
| `raw-logs` | Logs d'ingestion |
| `hudi` | Données transformées (tables Hudi) |

3. **Montrer un fichier JSON** : naviguer dans `raw-json/source=openalex/` ou `raw-json/source=fsjesm/`

4. **Montrer un document** : naviguer dans `raw-documents/source=hcp_docs/`

#### Différence Raw vs Curated

- **Raw** (`raw-*`) : données brutes telles quelles, non transformées
- **Curated** (`hudi/`) : données nettoyées, normalisées, structurées dans des tables Hudi

---

### 5. Spark + Hudi (3 min)

#### Présentation des 4 ETL

Chaque pipeline ETL :
1. **Lit** les données JSON depuis MinIO (`s3a://raw-json/`)
2. **Transforme** : normalisation, validation, nettoyage, génération de `record_id` MD5
3. **Écrit** dans une table Apache Hudi avec synchronisation Hive Metastore

| Pipeline | Script | Table Hudi | Données |
|----------|--------|------------|---------|
| Faculty Profiles | `faculty_profiles_etl.py` | `faculty_profiles` | Profils enseignants |
| Research Publications | `research_publications_etl.py` | `research_publications` | Publications scientifiques |
| University News | `university_news_etl.py` | `university_news` | Actualités universitaires |
| Documents Registry | `documents_registry_etl.py` | `documents_registry` | Documents officiels |

#### Commande d'exécution (exemple)

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

#### Vérification Beeline

```bash
# Lister les tables
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SHOW TABLES IN university_data_platform;"

# Compter les lignes
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT COUNT(*) FROM university_data_platform.faculty_profiles;"

# Afficher un échantillon
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT * FROM university_data_platform.faculty_profiles LIMIT 3;"
```

#### Tables Hudi

| Table | Record Key | Partition | Base Path |
|-------|------------|-----------|-----------|
| `faculty_profiles` | `record_id` | `source_system` | `s3a://hudi/faculty_profiles` |
| `research_publications` | `record_id` | `source_system` | `s3a://hudi/research_publications` |
| `university_news` | `record_id` | `source_system` | `s3a://hudi/university_news` |
| `documents_registry` | `record_id` | `source_system` | `s3a://hudi/documents_registry` |

---

### 6. Elasticsearch (2 min)

#### Index créés

| Index | Script | Contenu |
|-------|--------|---------|
| `faculty_profiles` | `index_faculty_profiles.py` | Noms, départements, emails |
| `research_publications` | `index_research_publications.py` | Titres, auteurs, DOI |
| `university_news` | `index_university_news.py` | Titres, catégories |
| `documents_registry` | `index_documents_registry.py` | Noms, types, tailles |

#### Commande d'indexation

```bash
cd /opt/spark/work-dir/src/search
/opt/spark/bin/spark-submit index_faculty_profiles.py
```

#### Vérification

```powershell
# Lister les index
Invoke-RestMethod http://localhost:9200/_cat/indices?v

# Compter les documents
Invoke-RestMethod http://localhost:9200/faculty_profiles/_count
Invoke-RestMethod http://localhost:9200/university_news/_count

# Recherche de test
Invoke-RestMethod "http://localhost:9200/faculty_profiles/_search?q=*&pretty"
```

#### Démonstration

1. **Ouvrir** http://localhost:9200
2. **Montrer les index** : `_cat/indices?v`
3. **Rechercher** un professeur ou une actualité

---

### 7. Metabase (2 min)

#### Accès

- **URL** : http://localhost:3000
- **Configuration** : assistant de setup au premier lancement
- **Source** : PostgreSQL (`university-postgres:5432`, base `metastore`)

#### Démonstration

1. **Se connecter** à Metabase
2. **Montrer les données** : les 4 tables Hudi accessibles via Hive/PostgreSQL
3. **Afficher un dashboard** avec des visualisations :
   - Nombre total de profils enseignants
   - Répartition par institution
   - Nombre de publications
   - Actualités par catégorie

#### Sources de données Metabase

| Méthode | Host | Port | Base |
|---------|------|------|------|
| PostgreSQL (Hive Metastore) | `university-postgres` | 5432 | `metastore` |
| Spark Thrift (JDBC) | `spark-thrift` | 10000 | — |

---

### 8. Conclusion (1 min)

#### Résumé

- **Architecture** : plateforme end-to-end (ingestion → transformation → stockage → recherche → visualisation)
- **3 sources** : OpenAlex API, UH2C Web Scraping (4 institutions), HCP Documents
- **4 tables Hudi** actives dans `university_data_platform`
- **4 index Elasticsearch** pour la recherche plein-texte
- **Infrastructure** : 12+ services Docker, orchestration Airflow

#### Pistes d'amélioration

- Intégrer la table `course_catalog` dans le pipeline actif
- Ajouter des KPIs Metabase supplémentaires
- Mettre en place des alertes sur la qualité des données
- Ajouter des tests automatisés pour les ETL

---

### 9. Questions / Documentation

#### Documentation disponible

| Document | Contenu |
|----------|---------|
| **README.md** | Documentation technique complète : architecture, stack, structure, commandes, ports |
| **RUNBOOK.md** | Guide opérationnel : installation, déploiement, vérifications, troubleshooting |

#### Points clés du RUNBOOK

- Installation détaillée (Docker Desktop, WSL2)
- Déploiement (`docker compose up -d --build`)
- Vérification de chaque service
- Commandes Spark ETL
- Vérification Elasticsearch
- 11 problèmes courants documentés avec solutions

---

### Notes pour le presentateur

#### Ordre de démonstration recommandé

1. Montrer l'architecture (schéma)
2. Ouvrir MinIO → montrer les buckets et données brutes
3. Expliquer le flux Spark → Hudi
4. Vérifier les tables via Beeline
5. Montrer Elasticsearch → les index
6. Ouvrir Metabase → les dashboards

#### Fichiers utiles

```
src/ingestion/api/hiba_openalex.py     # Scraper OpenAlex
src/ingestion/web/hiba_uh2c.py         # Scraper UH2C
src/ingestion/docs/hiba_hcp.py         # Scraper HCP
src/transformations/spark/faculty_profiles_etl.py
src/transformations/spark/research_publications_etl.py
src/transformations/spark/university_news_etl.py
src/transformations/spark/documents_registry_etl.py
src/search/index_faculty_profiles.py
src/search/index_research_publications.py
src/search/index_university_news.py
src/search/index_documents_registry.py
dags/hiba_pipeline.py
```

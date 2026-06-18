# University Data Platform (Challenge académique – 3 semaines)

> **Langues :** documentation principale en **français**, avec quelques sections bilingues (FR/EN) pour faciliter la soutenance.

## 1) Présentation & objectifs

L’**University Data Platform** est un projet “end-to-end” de collecte, ingestion et préparation de données académiques provenant de plusieurs sources (API, web, documents), stockées dans un **data lake** (MinIO), puis traitées par un pipeline de transformation (Semaine 2) basé sur **Spark**, **Hudi**, **Hive**, **Metabase** et **Elasticsearch**.

### Objectifs pédagogiques
- Mettre en place une **chaîne ingestion → stockage → (préparation) → indexation/analytics**.
- Concevoir un **schéma de métadonnées commun** et une **stratégie d’audit (logs + hashes)**.
- Documenter une architecture complète et reproductible (via Docker Compose).

### Périmètre Semaine 1 (Done)
- Ingestion **API** : **Crossref** (publications)
- Ingestion **Web** : 4 sites universitaires (professeurs + actualités)
- Ingestion **Documents** : **IMIST** (un PDF)
- Stockage en **MinIO** avec partitionnement par date **year/month/day**
- Normalisation et **métadonnées communes** (record_id, content_hash, timestamps…)

---

## 2) Architecture globale

### Vue d’ensemble

```text
API / Web / Documents
        ↓
      Ingestion
        ↓
       MinIO
   (Raw / Metadata)
        ↓
      Spark
 (ETL / Normalisation)
        ↓
       Hudi
 (Curated)
        ↓
      Hive
   (tables/SQL)
        ↓
   Metabase
 (BI / Dashboards)

        ↓
Elasticsearch
 (recherche & indexation)

        ↓
    Airflow
 (orchestration)
```

### Rôles par brique
- **MinIO** : data lake compatible S3 pour stocker brut + métadonnées + logs.
- **Spark** : transformations ETL (Semaine 2).
- **Hudi** : stockage “curated” versionné/upsert pour tables analytiques.
- **Hive** : exposition SQL / tables pour downstream.
- **Metabase** : exploration et dashboards.
- **Elasticsearch** : recherche (indexation des contenus structurés).
- **Airflow** : orchestration des pipelines (ingestion + ETL).

---

## 3) Sources de données ingérées (Semaine 1)

### 3.1 API — Crossref (Publications)
- Endpoint : `https://api.crossref.org/works`
- Stratégie : pagination (offset/rows) jusqu’au **limit** demandé.
- Publication : transforme Crossref en un format “ready” pour table **research_publications**.

### 3.2 Web — 4 sites universitaires
- **Professeurs** : EST Sale, EMI, ENS Rabat, FSJES Agdal
- **Actualités / News** : FSJES Agdal, EMI, ENS Rabat, EST Sale

> Les scrapeurs sont conçus pour extraire des éléments hétérogènes (tables HTML, données JavaScript embarquées, structures de pages variables), puis produire des enregistrements normalisés avec métadonnées communes.

### 3.3 Documents — IMIST (PDF)
- Source : téléchargement d’un document PDF IMIST.
- Extraction :
  - checksum SHA-256
  - métadonnées PDF (title/author/subject…)
  - URLs extraites depuis le contenu du PDF

---

## 4) Statistiques & métriques (Semaine 1)

- **Professeurs ingérés** : **468**
- **Actualités ingérées** : **142**
- **Publications (Crossref)** : **500+**
- **Documents** : **1 PDF**

### Métriques de qualité (approche)
- **Déduplication** :
  - professeurs : déduplication par `(first_name, last_name, email)`
  - actualités : déduplication par `(title, source)`
- **Traçabilité** :
  - `record_id` basé sur un hash déterministe du contenu (payload métier nettoyé)
  - `content_hash` SHA-256
- **Audit & logs** :
  - enregistrement de logs d’extraction (succès/erreur + timestamps)

---

## 5) Modèle de métadonnées communes (MinIO)

Tous les enregistrements “raw-json” sont enrichis avec un jeu de champs communs (issu des scrapeurs/transformations de Semaine 1).

### Champs standard
- `record_id` : identifiant déterministe basé sur le contenu métier
- `source_system` : identifiant de la source/agent d’ingestion
- `source_url` : URL source
- `content_hash` : SHA-256 du payload métier nettoyé
- `crawl_timestamp` : timestamp d’ingestion/crawling
- `business_timestamp` : timestamp “business” (même logique)
- `is_deleted` : bool (support futur “soft delete”)
- `language` : `fr` ou `en` selon la source
- `normalized_text` : champ réservé (utilisé plus tard pour indexation / NLP)

> En complément, chaque dataset apporte ses champs métier (ex: `authors`, `department`, `title`, `publication_date`, …).

---

## 6) Structure de stockage dans MinIO (Semaine 1)

### 6.1 Buckets
- Bucket principal : **`data-lake`**

### 6.2 Partitionnement temporel
- Tous les datasets sont partitionnés selon :
  - `year=YYYY`
  - `month=MM`
  - `day=DD`

### 6.3 Dossiers / chemins (exemples)

#### Publicات / Research publications (Crossref)
- `raw-json/research_publications/year=YYYY/month=MM/day=DD/crossref_publications_<timestamp>.json`

Le fichier contient :
- `metadata` (paramètres extraction)
- `raw_data` (réponse Crossref complète)
- `hudi_ready_data` (données transformées + métadonnées communes)

#### Profils professeurs (Web)
- `raw-json/faculty_profiles/year=YYYY/month=MM/day=DD/faculty_profiles_<timestamp>.json`

#### Actualités (Web)
- `raw-json/university_news/year=YYYY/month=MM/day=DD/university_news_<timestamp>.json`

#### Documents (PDF IMIST)
- PDF binaire :
  - `raw-documents/<source>/year=YYYY/month=MM/day=DD/<file>.pdf`
- Métadonnées JSON :
  - `raw-json/documents/<source>/year=YYYY/month=MM/day=DD/metadata_<timestamp>.json`
- Liens extraits :
  - `raw-json/documents/<source>/links/year=YYYY/month=MM/day=DD/links_<timestamp>.json`

---

## 7) Installation & exécution (Docker)

### 7.1 Lancer l’infrastructure
```bash
docker compose up -d
```

### 7.2 Accès aux services
- MinIO (Console) : `http://localhost:9001`
  - User/Password : `minioadmin` / `minioadmin`
- Metabase : `http://localhost:3000`
- Airflow : `http://localhost:8081` (admin/admin)
- Elasticsearch : `http://localhost:9200`
- Spark UI : `http://localhost:8090` (selon mapping du compose)

### 7.3 Exécution des ingestions Semaine 1

#### Crossref (API)
```bash
python -m src.ingestion.api.crossref_scraper
```

#### Web (Professeurs)
```bash
python -m src.ingestion.web.faculty_scraper
```

#### Web (Actualités)
```bash
python -m src.ingestion.web.news_scraper
```

#### Documents (IMIST / PDF)
```bash
python -m src.ingestion.docs.imist_scraper
```

> Remarque : les scripts utilisent MinIO via `src/storage/minio/sara_client.py`.
> Le endpoint s’adapte automatiquement entre exécution Docker vs exécution locale.

---

## 8) Validation de la Semaine 1 (Checklist)

- [x] Ingestion **Crossref** + transformation “Hudi-ready”
- [x] Ingestion **Web** professeurs (4 institutions)
- [x] Ingestion **Web** actualités (4 institutions)
- [x] Ingestion **Document PDF** + metadata + extracted links
- [x] Stockage dans **MinIO** avec partitionnement `year/month/day`
- [x] Métadonnées communes cohérentes (record_id/content_hash/timestamps)
- [x] Déduplication et logging d’extraction
- [x] Complétude des datasets (par ex: professeurs=468, actualités=142, publications=500+, documents=1)

---

## 9) Prochaines étapes (Semaine 2)

### Pipeline Semaine 2 : Spark ETL + Hudi + Hive + Metabase + Elasticsearch
- Lire les fichiers “raw-json” et “raw-documents” depuis MinIO
- Conformer les schémas (par dataset) pour produire des tables **curated**
- Upsert/versioning via **Hudi**
- Écrire les tables dans **Hive** (SQL)
- Mettre en place des dashboards Metabase
- Indexer dans **Elasticsearch** pour la recherche sémantique / filtering

### Résultats attendus
- Tables Hive prêtes pour analyses
- Dashboards Metabase (profils, news, publications)
- Recherche Elasticsearch (professeurs/actualités/publications)

---

## 10) Repères rapides (liens vers le code)

- MinIO client : `src/storage/minio/sara_client.py`
- Crossref : `src/ingestion/api/crossref_scraper.py`
- Web (faculty) : `src/ingestion/web/faculty_scraper.py`
- Web (news) : `src/ingestion/web/news_scraper.py`
- Documents (IMIST) : `src/ingestion/docs/imist_scraper.py`



# University Data Platform (Moroccan Universities Data Ingestion)

> **Langues :** documentation principale en **français**.

## Description
Plateforme d’ingestion de données pour les universités marocaines : collecte depuis **API**, **scraping web** et **documents**, normalisation et stockage dans un **data lake MinIO**. Le projet alimente ensuite un pipeline de transformation (Semaine 2) basé sur **Spark**, **Hudi**, **Hive**, **Metabase** et **Elasticsearch**.

## Structure du projet (src/)
```text
src/
  storage/minio/
    sara_client.py                 # client MinIO (upload JSON/binaire + création buckets)
  ingestion/
    api/
      crossref.py                  # ingestion Crossref (publications)
    web/
      um5.py                       # scraper combiné UM5 (facultés + actualités + images + documents)
    docs/
      toubkal.py                   # scraping IMIST/Toubkal (thèses + PDFs)
```

## Installation
### Pré-requis
- Docker & Docker Compose

### Lancer l’infrastructure
```bash
docker compose up -d
```

### Accès aux services
- MinIO console : http://localhost:9001 (user/pass : `minioadmin`/`minioadmin`)
- Metabase : http://localhost:3000
- Airflow : http://localhost:8081 (admin/admin)
- Elasticsearch : http://localhost:9200
- Spark UI : http://localhost:8090

## Comment exécuter chaque scraper (ingestion)
> Les scrapers écrivent dans MinIO via `src/storage/minio/sara_client.py`.

### 1) Crossref (publications)
```bash
python -m src.ingestion.api.crossref
```

### 2) IMIST / Toubkal (PDF thèses)
```bash
python -m src.ingestion.docs.toubkal
```

### 3) Scraper combiné UM5 (Professeurs + News + Images + Documents)
```bash
python -m src.ingestion.web.um5
```

## Données sources & ce qu’elles collectent
### Crossref (API)
- Publications académiques : informations de publication (données Crossref) via pagination `offset/rows`.

### Web — Professeurs (EST/EMI/ENS/FSJES)
- Profils enseignants : `first_name`, `last_name`, `email` (si disponible), `department`, `institution`.

### Web — Actualités/News/Avis/Événements/Appels d’offres
- Titres, URLs, dates (quand disponibles), catégories, source et institution.
- Images associées (si présentes) + déduplication côté scraper.

### Documents — IMIST / Toubkal
- Thèses : exploration des items puis téléchargement des **PDFs** depuis les pages “/full”.
- Stockage du binaire PDF + métadonnées d’extraction.

## MinIO : buckets utilisés
Les scrapers écrivent dans :
- `raw-json` : données structurées (publications, faculty, news) + métadonnées d’extraction
- `raw-web-html` : HTML brut des pages web
- `raw-images` : images extraites (avec métadonnées par image)
- `raw-documents` : documents binaires (ex: PDF)

## Partitionnement & organisation MinIO
- Objets organisés par date : `year=YYYY/month=MM/day=DD`.
- Les données structurées sont sauvegardées en **JSON** ; les binaires en objets (**HTML/images/PDF**).


# Demo Script - University Data Platform
## Soutenance - 15 minutes

---

### 1. Introduction (1 min)
> "Bonjour, je vous présente notre plateforme data universitaire. Notre objectif est de démontrer un pipeline complet d'ingestion, transformation et visualisation de données pour les universités marocaines."

---

### 2. Architecture (1 min)
- Montrer le diagramme d'architecture (disponible dans `docs/archi.svg`)
- Expliquer rapidement les composants :
  - **Airflow** pour l'orchestration
  - **MinIO** pour le stockage
  - **Spark** pour la transformation
  - **Elasticsearch** pour la recherche
  - **Metabase** pour la visualisation

---

### 3. Airflow - Orchestration (3 min)
1. Ouvrir http://localhost:8081
2. Se connecter : `admin` / `admin`
3. Montrer le DAG `sara_university_pipeline`
4. Expliquer les **5 tâches** :
   1. `scrape_um5` — scraping web UM5
   2. `scrape_toubkal` — scraping PDF IMIST
   3. `scrape_crossref` — API Crossref
   4. `transform_spark` — transformation Spark
   5. `index_elasticsearch` — indexation Elasticsearch
5. Montrer les logs d'une tâche (ex: `scrape_um5`)
6. Montrer l'historique des runs (toutes les tâches en vert)

**📸 Screenshots :**
- `docs/screenshots/00_airflow_overview.png`
- `docs/screenshots/01_airflow_dag_overview.png`
- `docs/screenshots/02_airflow_logs_um5.png`
- `docs/screenshots/03_airflow_graph_view.png`

---

### 4. MinIO - Stockage (2 min)
1. Ouvrir http://localhost:9001
2. Se connecter : `minioadmin` / `minioadmin`
3. Montrer les buckets :
   - `raw-json` (données structurées)
   - `raw-web-html` (pages HTML)
   - `raw-images` (images extraites)
   - `raw-documents` (PDFs)
   - `curated` (données transformées)
4. Ouvrir un fichier JSON pour montrer la structure des données

**📸 Screenshots :**
- `docs/screenshots/04_minio_buckets.png`
- `docs/screenshots/05_minio_raw_json_content.png`

---

### 5. Elasticsearch - Recherche (2 min)
1. Exécuter la commande pour compter les documents :
   ```bash
   curl -X GET "http://localhost:9200/university_data/_count?pretty"
   ```
   → Montrer le nombre de documents **(606)**

2. Effectuer une recherche simple :
   ```bash
   curl -X GET "http://localhost:9200/university_data/_search?q=institution:UM5&pretty"
   ```
   → Montrer un résultat de recherche avec les champs (`institution`, `title`, `department`, etc.)

**📸 Screenshots :**
- `docs/screenshots/06_elasticsearch_count.png`
- `docs/screenshots/07_elasticsearch_search_results.png`

---

### 6. Metabase - Dashboard (3 min)
1. Ouvrir http://localhost:3000
2. Se connecter avec les identifiants configurés
3. Montrer le dashboard avec les **7 KPIs** :
   - Total Professors
   - Total News
   - Professors by Institution
   - News by Institution
   - Top 10 Departments
   - News by Category
   - Recent News
4. Montrer un **filtre** en action (ex: filtrer par institution)
5. Montrer un **graphique interactif**

**📸 Screenshot :**
- `docs/screenshots/08_metabase_dashboard.png`

---

### 7. Conclusion (2 min)

**Récapitulatif des données récoltées :**
- ✅ 4 universités scrappées
- ✅ 468 professeurs
- ✅ 115 actualités
- ✅ 500 articles Crossref
- ✅ **606 documents indexés dans Elasticsearch**

**Points forts :**
- Pipeline robuste et reproductible
- Stack moderne (Airflow, Spark, Hudi, Elasticsearch)
- Architecture modulaire et scalable

**Pistes d'amélioration :**
- Enrichissement des données (IA, NLP)
- Plus de sources de données
- Optimisation des performances

---

### 8. Q&A (2 min)
- Répondre aux questions du jury
- Montrer la documentation si nécessaire (`README.md`, `RUNBOOK.md`, `docs/archi.svg`)

---

### ⚠️ Notes importantes
- ⏱ **Ne pas dépasser 15 minutes**
- 🔄 Si une tâche est en cours, expliquer que le pipeline est conçu pour être **reproductible**
- 🐛 En cas d'erreur, montrer les logs et expliquer comment récupérer


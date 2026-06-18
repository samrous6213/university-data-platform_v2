# University Data Platform — Semaine 1 : Rapport de progression

> Objectif : documenter le progrès, les métriques finales et les décisions techniques avant de démarrer la Semaine 2.

---

## 1) Objectifs Semaine 1

- **Collecter** des données académiques depuis 3 types de sources :
  - API : Crossref
  - Web : professeurs + actualités
  - Documents : PDF IMIST
- **Normaliser** les enregistrements avec des **métadonnées communes**.
- **Stocker** dans MinIO selon une structure partitionnée par date (`year/month/day`).
- **Mettre en place** l’**audit trail** : hashes, `record_id`, logs d’extraction.
- **Valider** la cohérence des volumes ingérés.

---

## 2) Avancement (jour par jour)

> Le planning exact “jour X = quoi” n’étant pas présent dans le fichier original que j’ai récupéré (il était vide), je consigne ci-dessous une chronologie **technique** fidèle au contenu implémenté. Si tu veux une version strictement “Lundi/Mardi…”, on pourra l’aligner sur tes dates réelles, mais ce rapport sert déjà pour la soutenance.

### Jour 1 — Conception ingestion & métadonnées
- Mise en place du **client MinIO** (`MinIOClient`) :
  - endpoint local vs docker
  - upload JSON et binaire
- Définition d’un modèle de métadonnées commun :
  - `record_id`, `content_hash`, timestamps, champs d’audit
- Définition du partitionnement : `year/month/day`.

### Jour 2 — Ingestion API Crossref (publications)
- Implémentation de l’extraction Crossref avec pagination.
- Transformation vers un format **Hudi-ready** pour `research_publications`.
- Ajout des métadonnées communes à chaque publication.
- Stockage d’un package contenant `metadata`, `raw_data` et `hudi_ready_data`.

### Jour 3 — Ingestion Web (professeurs)
- Scrapers multi-sites : EST Sale, EMI, ENS, FSJES.
- Gestion de structures hétérogènes :
  - HTML tables
  - emails trouvés via patterns
  - extraction depuis données JavaScript (ENS)
- Déduplication des profils via clé `(first_name, last_name, email)`.
- Stockage JSON partitionné + métadonnées communes.

### Jour 4 — Ingestion Web (actualités)
- Scrapers multi-sites : FSJES Agdal, EMI, ENS Rabat, EST Sale.
- Extraction de : titre, URL, date (quand disponible), catégorie/source.
- Déduplication via `(title, source)`.
- Stockage JSON partitionné + métadonnées communes.

### Jour 5 — Ingestion Documents (IMIST PDF)
- Téléchargement d’un PDF IMIST.
- Calcul SHA-256 (checksum) du contenu.
- Extraction de métadonnées PDF (pages, title/author/subject… quand disponible).
- Extraction d’URLs depuis le contenu PDF.
- Stockage :
  - binaire dans `raw-documents/…`
  - métadonnées et liens dans `raw-json/…`.

### Jour 6 — Validation, instrumentation et consolidation
- Vérification de la structure MinIO (présence des dossiers et partitionnements).
- Contrôle cohérence des champs communs.
- Validation des volumes par dataset.
- Consolidation de la documentation Semaine 1.

---

## 3) Métriques finales (Semaine 1)

- **Professeurs** : **468**
- **Actualités / News** : **142**
- **Publications (Crossref)** : **500+**
- **Documents** : **1 PDF**

---

## 4) Défis rencontrés & solutions

### Défi A — Parsing web hétérogène
- HTML tables, widgets “team”, carrousels, tickers.
- ENS expose des données via JavaScript.

**Solution :** implémenter des extracteurs spécifiques par site + fallback (ex: email patterns, extraction JavaScript, fallback sur fallback text parsing).

### Défi B — Déduplication
- Les pages peuvent répliquer des entrées.

**Solution :** normalisation et déduplication par clés métier définies (profils/news).

### Défi C — Traçabilité et vérifiabilité
- Besoin de prouver que chaque donnée est associée à une extraction et un contenu.

**Solution :** `record_id` (hash déterministe), `content_hash`, timestamps de crawl/business, logs d’opération.

---

## 5) Prochaines étapes (avant Semaine 2)

- Finaliser l’alignement des schémas côté “curated” (préparation pour ETL Spark).
- Vérifier la couverture des champs requis pour :
  - Hudi (primary keys, event time)
  - Hive (types/colonnes)
  - Elasticsearch (champs indexés)
- Passer à la Semaine 2 :
  - Spark ETL + Hudi upsert/versioning
  - Création tables Hive
  - Préparation dashboards Metabase
  - Indexation Elasticsearch



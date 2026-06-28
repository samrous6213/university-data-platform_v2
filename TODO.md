# TODO - Ingestion MinIO Raw architecture

## Étape 1 — Analyse (terminée)
- [x] Comprendre `RawPath` (structure clés `source=<src>/year=/month=/day=`)
- [x] Comprendre les 3 ingestions existantes (OpenAlex/UCA/IMIST)

## Étape 2 — OpenAlex (`src/ingestion/api/chaimae_openalex.py`)
- [ ] Récupérer **chercheurs** (authors) et **publications** (works/publications) via OpenAlex
- [ ] Stocker chaque réponse JSON **brute** dans `raw-json` avec clé `RawPath.json_key(source=openalex, ...)`
- [ ] Calculer SHA-256 pour chaque contenu brut JSON
- [ ] Stocker métadonnées en `raw-json` (ex: `*_meta.json`) : extraction_date, url_source, sha256, status_code, counts
- [ ] Stocker les logs d’exécution dans `raw-logs`

## Étape 3 — UCA (`src/ingestion/web/chaimae_uca.py`)
- [ ] Calculer SHA-256 + stocker un fichier `*_meta.json` en `raw-json` pour chaque page HTML brute
- [ ] Calculer SHA-256 + stocker métadonnées PDF en `raw-json` (déjà logs ok, à harmoniser)
- [ ] Stocker métadonnées JSON en `raw-json` (déjà checksum dans logs, à harmoniser)
- [ ] Conserver le crawler et la logique existante

## Étape 4 — IMIST (`src/ingestion/docs/chaimae_imist.py`)
- [ ] Corriger totalement l’architecture MinIO : utiliser les buckets `raw-web-html`, `raw-documents`, `raw-json`, `raw-logs` via `RawPath`
- [ ] Stocker HTML brut en `raw-web-html` + `*_meta.json` en `raw-json` (sha256 + url_source + extraction_date + metadata)
- [ ] Stocker PDF brut en `raw-documents` + métadonnées PDF en `raw-json` (sha256, num_pages, etc.)
- [ ] Stocker JSON brut accessible en `raw-json` + `*_meta.json`
- [ ] Stocker logs en `raw-logs`

## Étape 5 — Tests & validation
- [ ] `pytest -q`
- [ ] Lancer manuellement les 3 ingestions via `python -m ...`
- [ ] Vérifier dans MinIO les buckets et la convention des clés


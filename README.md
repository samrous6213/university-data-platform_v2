# 🏛️ University Data Platform — Projet Groupe

## Description

Plateforme d'ingestion et d'analyse de données académiques pour les universités marocaines. Chaque membre de l'équipe a travaillé sur des sources de données différentes pour couvrir un large spectre de sources académiques.

## Architecture globale

- **Orchestration** : Apache Airflow
- **Stockage** : MinIO (data lake S3-compatible)
- **Transformation** : Apache Spark + Hudi
- **Recherche** : Elasticsearch
- **Visualisation** : Metabase

---

## 👥 Répartition du travail par membre

| Membre | Branche | Sources Web | Sources API | Sources Docs | Fichiers principaux |
|--------|---------|-------------|-------------|--------------|---------------------|
| Sara Amrous | `sara` | UM5 | Crossref | Toubkal | `src/ingestion/web/um5.py`, `src/ingestion/api/crossref.py`, `src/ingestion/docs/toubkal.py` |
| Chaimae | `chaimae` | UCA | OpenAlex | IMIST | `src/ingestion/web/uca.py`, `src/ingestion/api/openalex.py`, `src/ingestion/docs/imist.py` |
| Ayoub | `ayoub` | USMBA | ORCID | Data.gov.ma | `src/ingestion/web/usmba.py`, `src/ingestion/api/orcid.py`, `src/ingestion/docs/datagovma.py` |
| Hiba | `hiba` | UH2C | OpenAlex | HCP | `src/ingestion/web/uh2c.py`, `src/ingestion/api/openalex.py`, `src/ingestion/docs/hcp.py` |
| Nezha | `nezha` | USMS | Crossref | MIT OCW | `src/ingestion/web/usms.py`, `src/ingestion/api/crossref.py`, `src/ingestion/docs/mit_ocw.py` |
| Safaa | `safaa` | UIZ | ORCID | Khan Academy | `src/ingestion/web/uiz.py`, `src/ingestion/api/orcid.py`, `src/ingestion/docs/khan_academy.py` |
| Fahd | `fahd` | ONOUSC | OpenAlex | Wikipedia Mathematics | `src/ingestion/web/onousc.py`, `src/ingestion/api/openalex.py`, `src/ingestion/docs/wikipedia_math.py` |

---

## 🔗 Accès aux branches individuelles

| Membre | Branche | Lien |
|--------|---------|------|
| Sara Amrous| `sara` | [Voir la branche sara](https://github.com/samrous6213/university-data-platform_v2/tree/sara) |
| Chaimae | `chaimae` | [Voir la branche chaimae](https://github.com/samrous6213/university-data-platform_v2/tree/chaimae) |
| Ayoub | `ayoub` | [Voir la branche ayoub](https://github.com/samrous6213/university-data-platform_v2/tree/ayoub) |
| Hiba | `hiba` | [Voir la branche hiba](https://github.com/samrous6213/university-data-platform_v2/tree/hiba) |
| Nezha | `nezha` | [Voir la branche nezha](https://github.com/samrous6213/university-data-platform_v2/tree/nezha) |
| Safaa | `safaa` | [Voir la branche safaa](https://github.com/samrous6213/university-data-platform_v2/tree/safaa) |
| Fahd | `fahd` | [Voir la branche fahd](https://github.com/samrous6213/university-data-platform_v2/tree/fahd) |

---

## ✅ Statut des sources

| Source | Membre | Branche | Statut |
|--------|--------|---------|--------|
| UM5 Web | Sara Amrous| `sara` | ✅ Fonctionnel |
| Toubkal | Sara Amrous| `sara` | ✅ Fonctionnel |
| Crossref API | Sara Amrous| `sara` | ✅ Fonctionnel |
| UCA Web | Chaimae | `chaimae` | ✅ Fonctionnel |
| OpenAlex API | Chaimae, Hiba, Fahd | `chaimae`, `hiba`, `fahd` | ✅ Fonctionnel |
| IMIST | Chaimae | `chaimae` | ✅ Fonctionnel |
| USMBA Web | Ayoub | `ayoub` | ✅ Fonctionnel |
| ORCID API | Ayoub, Safaa | `ayoub`, `safaa` | ✅ Fonctionnel |
| Data.gov.ma | Ayoub | `ayoub` | ✅ Fonctionnel |
| UH2C Web | Hiba | `hiba` | ✅ Fonctionnel |
| HCP | Hiba | `hiba` | ✅ Fonctionnel |
| USMS Web | Nezha | `nezha` | ✅ Fonctionnel |
| MIT OCW | Nezha | `nezha` | ✅ Fonctionnel |
| UIZ Web | Safaa | `safaa` | ✅ Fonctionnel |
| Khan Academy | Safaa | `safaa` | ✅ Fonctionnel |
| ONOUSC Web | Fahd | `fahd` | ✅ Fonctionnel |
| Wikipedia Mathematics | Fahd | `fahd` | ✅ Fonctionnel |

---

## 🚀 Comment exécuter le projet

1. **Cloner le dépôt :**
   ```bash
   git clone https://github.com/samrous6213/university-data-platform_v2.git
   ```

2. **Choisir la branche du membre concerné :**
   ```bash
   git checkout sara  # ou chaimae, ayoub, etc.
   ```

3. **Démarrer les services :**
   ```bash
   docker-compose up -d
   ```

4. **Accéder à l'interface Airflow :**
   - URL : http://localhost:8081
   - Identifiants : `admin` / `admin`

---

## 📚 Documentation complémentaire

Chaque membre a sa propre documentation dans sa branche :

| Membre | README | RUNBOOK | Demo Script |
|--------|--------|---------|-------------|
| Sara Amrous| [README](https://github.com/samrous6213/university-data-platform_v2/blob/sara/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/sara/RUNBOOK.md) | [Demo Script](https://github.com/samrous6213/university-data-platform_v2/blob/sara/docs/demo-script.md) |
| Chaimae | [README](https://github.com/samrous6213/university-data-platform_v2/blob/chaimae/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/chaimae/RUNBOOK.md) | [Demo Script](https://github.com/samrous6213/university-data-platform_v2/blob/chaimae/docs/demo-script.md) |
| Ayoub | [README](https://github.com/samrous6213/university-data-platform_v2/blob/ayoub/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/ayoub/RUNBOOK.md) | [Demo Script](https://github.com/samrous6213/university-data-platform_v2/blob/ayoub/docs/demo-script.md) |
| Hiba | [README](https://github.com/samrous6213/university-data-platform_v2/blob/hiba/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/hiba/RUNBOOK.md) | [Demo Script](https://github.com/samrous6213/university-data-platform_v2/blob/hiba/docs/demo-script.md) |
| Nezha | [README](https://github.com/samrous6213/university-data-platform_v2/blob/nezha/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/nezha/RUNBOOK.md) | [Demo Script](https://github.com/samrous6213/university-data-platform_v2/blob/nezha/docs/demo-script.md) |
| Safaa | [README](https://github.com/samrous6213/university-data-platform_v2/blob/safaa/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/safaa/RUNBOOK.md) | [Demo Script](https://github.com/samrous6213/university-data-platform_v2/blob/safaa/docs/demo-script.md) |
| Fahd | [README](https://github.com/samrous6213/university-data-platform_v2/blob/fahd/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/fahd/RUNBOOK.md) | [Demo Script](https://github.com/samrous6213/university-data-platform_v2/blob/fahd/docs/demo-script.md) |

---

## 👥 Auteurs

Projet réalisé dans le cadre du challenge **University Data Platform** par :

| Membre | Sources |
|--------|---------|
| **Sara Amrous(P1)** | UM5 Web, Toubkal, Crossref API |
| **Chaimae (P2)** | UCA Web, IMIST, OpenAlex API |
| **Ayoub (P3)** | USMBA Web, Data.gov.ma, ORCID API |
| **Hiba (P4)** | UH2C Web, HCP, OpenAlex API |
| **Nezha (P5)** | USMS Web, MIT OCW, Crossref API |
| **Safaa (P6)** | UIZ Web, Khan Academy, ORCID API |
| **Fahd (P7)** | ONOUSC Web, Wikipedia Mathematics, OpenAlex API |


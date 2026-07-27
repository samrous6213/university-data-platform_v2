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

| Membre           | Branche   | Sources Web | Sources API | Sources Docs          |
|------------------|-----------|-------------|-------------|-----------------------|
| Sara Amrous      | `sara`    | UM5         | Crossref    | Toubkal               | 
| Chaimae hassari  | `chaimae` | UCA         | OpenAlex    | IMIST                 | 
| Ayoub El Gazzouzi | `ayoub`   | USMBA       | ORCID       | Data.gov.ma           | 
| Hiba Hnaine            | `hiba`    | UH2C        | OpenAlex    | HCP                   | 
| Nezha Ait EL had | `nezha`   | USMS        | Crossref    | MIT OCW               | 
| Safaa Toukil          | `safaa`   | UIZ         | ORCID       | Khan Academy          |
| Fahd Souida            | `fahd`    | ONOUSC      | OpenAlex    | Wikipedia Mathematics |

---

## 🔗 Accès aux branches individuelles

| Membre | Branche | Lien |
|--------|---------|------|
| Sara Amrous| `sara` | [Voir la branche sara](https://github.com/samrous6213/university-data-platform_v2/tree/sara) |
| Chaimae hassari| `chaimae` | [Voir la branche chaimae](https://github.com/samrous6213/university-data-platform_v2/tree/chaimae) |
| Ayoub El Gazzouzi| `ayoub` | [Voir la branche ayoub](https://github.com/samrous6213/university-data-platform_v2/tree/ayoub) |
| Hiba Hnaine| `hiba` | [Voir la branche hiba](https://github.com/samrous6213/university-data-platform_v2/tree/hiba) |
| Nezha Ait EL had| `nezha` | [Voir la branche nezha](https://github.com/samrous6213/university-data-platform_v2/tree/nezha) |
| Safaa Toukil| `safaa` | [Voir la branche safaa](https://github.com/samrous6213/university-data-platform_v2/tree/safaa) |
| Fahd Souida | `fahd` | [Voir la branche fahd](https://github.com/samrous6213/university-data-platform_v2/tree/fahd) |

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

| Membre | README | RUNBOOK | 
|--------|--------|---------|
| Sara Amrous| [README](https://github.com/samrous6213/university-data-platform_v2/blob/sara/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/sara/RUNBOOK.md) |
| Chaimae hassari | [README](https://github.com/samrous6213/university-data-platform_v2/blob/chaimae/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/chaimae/RUNBOOK.md) | 
| Ayoub El Gazzouzi| [README](https://github.com/samrous6213/university-data-platform_v2/blob/ayoub/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/ayoub/RUNBOOK.md) | 
| Hiba Hnaine| [README](https://github.com/samrous6213/university-data-platform_v2/blob/hiba/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/hiba/RUNBOOK.md) | 
| Nezha Ait EL had| [README](https://github.com/samrous6213/university-data-platform_v2/blob/nezha/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/nezha/Runbook.md) |
| Safaa Toukil | [README](https://github.com/samrous6213/university-data-platform_v2/blob/safaa/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/safaa/RUNBOOK.md) | 
| Fahd Souida| [README](https://github.com/samrous6213/university-data-platform_v2/blob/fahd/README.md) | [RUNBOOK](https://github.com/samrous6213/university-data-platform_v2/blob/fahd/RUNBOOK.md) | 

---

## 👥 Auteurs

Projet réalisé dans le cadre du challenge **University Data Platform** par :

| Membre | Sources |
|--------|---------|
| **Sara Amrous(P1)** | UM5 Web, Toubkal, Crossref API |
| **Chaimae hassari (P2)** | UCA Web, IMIST, OpenAlex API |
| **Ayoub El Gazzouzi(P3)** | USMBA Web, Data.gov.ma, ORCID API |
| **Hiba Hnaine (P4)** | UH2C Web, HCP, OpenAlex API |
| **Nezha Ait EL had (P5)** | USMS Web, MIT OCW, Crossref API |
| **Safaa Toukil (P6)** | UIZ Web, Khan Academy, ORCID API |
| **Fahd Souida (P7)** | ONOUSC Web, Wikipedia Mathematics, OpenAlex API |


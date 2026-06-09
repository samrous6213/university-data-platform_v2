# University Data Platform V2

## Team Members (7 people)

| Person | Source | DAG File | Hudi Table |
|--------|--------|----------|-------------|
| P1 | API - OpenAlex | `dag_api_openalex.py` | faculty_profiles |
| P2 | Web - UM5 | `dag_web_um5.py` | course_catalog |
| P3 | PDF - MIT OCW | `dag_doc_mit.py` | course_catalog |
| P4 | API - Crossref | `dag_api_crossref.py` | faculty_profiles |
| P5 | Web - Data.gov.ma | `dag_web_datagov.py` | faculty_profiles |
| P6 | Web - UCA | `dag_web_uca.py` | course_catalog |
| P7 | Wiki - Mathematics | `dag_wiki_math.py` | course_catalog |

## Setup Instructions

1. Clone this repo
2. Each person copies `dags/person_0_template.py` to their own DAG file
3. Edit the `SOURCE_NAME` and extraction logic
4. Commit and push to GitHub

## Folder Structure
university-data-platform_v2/
├── dags/ # Airflow DAGs (one per person)
│ ├── common/ # Shared code (don't modify without team agreement)
│ ├── person_1_*.py
│ └── ...
├── plugins/ # Custom Airflow plugins
├── spark_jobs/ # Spark transformation scripts
└── notebooks/ # Development notebooks
## Git Workflow

1. Always pull before working: `git pull origin main`
2. Create your own branch: `git checkout -b person-X-source`
3. Work on your DAG only
4. Push and create a Pull Request: `git push origin person-X-source`

## Quick Commands

```bash
# Check your work
git status
git log --oneline -5

# Commit your changes
git add dags/your_dag_file.py
git commit -m "P1: Added OpenAlex API ingestion DAG"
git push origin your-branch-name

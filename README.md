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

<details>
<summary>Click to expand folder structure</summary>

```text
university-data-platform_v2/
.
├── dags/                           # ALL Airflow DAGs live here
│   ├── common/                     # Shared code (you all import this)
│   │   ├── __init__.py
│   │   ├── spark_utils.py          # Standard Spark session builder
│   │   └── minio_client.py         # Connection helper
│   ├── person_0_template.py        # Template for team members
│   ├── sara_openalex.py            # Sara's DAG (API source)
│   ├── person_2_um5.py             # Person 2 DAG (Web source)
│   ├── person_3_mit.py             # Person 3 DAG (PDF source)
│   ├── person_4_crossref.py        # Person 4 DAG (API source)
│   ├── person_5_datagov.py         # Person 5 DAG (Web source)
│   ├── person_6_uca.py             # Person 6 DAG (Web source)
│   └── person_7_wiki.py            # Person 7 DAG (Wiki source)
│
├── plugins/                        # Custom Airflow plugins/operators
│   └── hudi_operators.py           # Custom operator to write to Hudi
│
├── spark_jobs/                     # Standalone .py files for complex transforms
│   └── transform_hudi.py           # Called by Airflow via SparkOperator
│
├── notebooks/                      # Dev exploration (keep out of DAGs)
├── airflow_home/                   # Airflow configuration (gitignored)
│
├── docker-compose.yml              # Airflow + MinIO + Postgres (Metastore)
├── requirements.txt                # Python dependencies
├── .gitignore                      # Git ignore file
└── README.md                       # This file
```
</details>

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

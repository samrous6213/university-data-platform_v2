r"""
Étape 8 : Indexation des tables curated (Postgres "analytics") -> Elasticsearch.

Lit chaque table déjà exportée par transformations/spark/export_to_postgres.py
et l'indexe dans Elasticsearch pour la recherche full-text (un index par table).

Emplacement prévu : src/search/elasticsearch/index.py

Peut être lancé directement dans le conteneur spark-master :
    docker exec spark-master python3 /workspace/src/search/elasticsearch/index.py
"""

import os
import psycopg2
import psycopg2.extras
from elasticsearch import Elasticsearch, helpers

# --- Connexion Postgres (base "analytics") ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "hive")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "hive")

# --- Connexion Elasticsearch ---
ES_HOST = os.getenv("ES_HOST", "http://university-elasticsearch:9200")

# Tables Postgres à indexer -> nom de l'index Elasticsearch correspondant
TABLES = {
    "faculty_profiles": "faculty_profiles",
    "research_publications": "research_publications",
    "university_news": "university_news",
}


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_HOST)


def fetch_rows_as_dicts(conn, table_name: str):
    """Lit toutes les lignes d'une table Postgres et les retourne en liste de dicts."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f'SELECT * FROM "{table_name}";')
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def index_table(es: Elasticsearch, conn, table_name: str, index_name: str) -> None:
    print(f"→ Lecture de la table Postgres '{table_name}'")
    rows = fetch_rows_as_dicts(conn, table_name)
    print(f"  {len(rows)} lignes récupérées")

    if not rows:
        print(f"  ⚠️ Aucune ligne trouvée pour '{table_name}', index ignoré.")
        return

    # Recrée l'index proprement à chaque exécution (idempotent)
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    es.indices.create(index=index_name)

    # Utilise record_id comme identifiant de document si présent, sinon
    # laisse Elasticsearch générer un _id automatique.
    def generate_actions():
        for row in rows:
            doc_id = row.get("record_id")
            action = {
                "_index": index_name,
                "_source": row,
            }
            if doc_id:
                action["_id"] = doc_id
            yield action

    success, errors = helpers.bulk(es, generate_actions(), stats_only=False, raise_on_error=False)
    print(f"  ✅ {success} documents indexés dans '{index_name}'")
    if errors:
        print(f"  ⚠️ {len(errors)} erreurs lors de l'indexation, exemple : {errors[0]}")


def run_all() -> None:
    es = get_es_client()
    conn = get_postgres_connection()

    try:
        for table_name, index_name in TABLES.items():
            try:
                index_table(es, conn, table_name, index_name)
            except Exception as e:
                print(f"❌ Échec sur la table '{table_name}' : {e}")
                continue
    finally:
        conn.close()

    print("\n✅ Étape 8 (indexation Elasticsearch) terminée.")


if __name__ == "__main__":
    run_all()
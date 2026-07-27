import json
import os
import datetime
import decimal
import urllib.request
import urllib.error

from pyspark.sql import SparkSession


ES_URL = os.getenv("ES_URL", "http://university-elasticsearch:9200")

TABLES = [
    {
        "table_name": "faculty_profiles",
        "index_name": "safaa_faculty_profiles",
        "path": "/opt/spark/work-dir/data/curated/safaa/faculty_profiles",
        "id_column": "record_id",
    },
    {
        "table_name": "university_news",
        "index_name": "safaa_university_news",
        "path": "/opt/spark/work-dir/data/curated/safaa/university_news",
        "id_column": "record_id",
    },
    {
        "table_name": "research_publications",
        "index_name": "safaa_research_publications",
        "path": "/opt/spark/work-dir/data/curated/safaa/research_publications",
        "id_column": "record_id",
    },
]


def to_json_safe(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def clean_document(row_dict):
    clean = {}
    for key, value in row_dict.items():
        clean[key] = to_json_safe(value)

    clean["indexed_at"] = datetime.datetime.utcnow().isoformat()
    return clean


def es_request(method, endpoint, body=None, content_type="application/json"):
    url = ES_URL + endpoint

    data = None
    headers = {}

    if body is not None:
        if isinstance(body, str):
            data = body.encode("utf-8")
        else:
            data = json.dumps(body).encode("utf-8")

        headers["Content-Type"] = content_type

    request = urllib.request.Request(
        url=url,
        data=data,
        method=method,
        headers=headers,
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            response_body = response.read().decode("utf-8")
            if response_body:
                return json.loads(response_body)
            return {}
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        if e.code == 404 and method == "DELETE":
            return {"acknowledged": True, "message": "index did not exist"}
        raise RuntimeError(f"Elasticsearch HTTP error {e.code}: {error_body}")
    except Exception as e:
        raise RuntimeError(f"Elasticsearch request failed: {e}")


def check_elasticsearch():
    print("=" * 80)
    print("CHECK ELASTICSEARCH CONNECTION")
    print("=" * 80)

    response = es_request("GET", "/")
    print("Elasticsearch connected successfully")
    print("Cluster:", response.get("cluster_name"))
    print("Version:", response.get("version", {}).get("number"))


def create_index(index_name):
    print(f"Creating index: {index_name}")

    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "dynamic": True,
            "properties": {
                "record_id": {"type": "keyword"},
                "source_system": {"type": "keyword"},
                "source_url": {"type": "keyword"},
                "institution": {"type": "keyword"},
                "category": {"type": "keyword"},
                "language": {"type": "keyword"},
                "publication_year": {"type": "keyword"},
                "curated_table": {"type": "keyword"},
                "indexed_at": {"type": "date"},
                "title": {"type": "text"},
                "summary": {"type": "text"},
                "full_name": {"type": "text"},
                "author_name": {"type": "text"},
                "department": {"type": "text"},
                "journal": {"type": "text"}
            }
        }
    }

    es_request("PUT", f"/{index_name}", mapping)


def delete_index_if_exists(index_name):
    print(f"Deleting old index if exists: {index_name}")
    es_request("DELETE", f"/{index_name}")


def bulk_index_documents(index_name, documents, id_column):
    if not documents:
        return 0

    lines = []

    for doc in documents:
        doc_id = doc.get(id_column)

        if doc_id:
            action = {"index": {"_index": index_name, "_id": str(doc_id)}}
        else:
            action = {"index": {"_index": index_name}}

        lines.append(json.dumps(action, ensure_ascii=False))
        lines.append(json.dumps(doc, ensure_ascii=False))

    bulk_body = "\n".join(lines) + "\n"

    response = es_request(
        "POST",
        "/_bulk",
        bulk_body,
        content_type="application/x-ndjson",
    )

    if response.get("errors"):
        raise RuntimeError(f"Bulk indexing errors: {response}")

    return len(documents)


def index_table(spark, table_config):
    table_name = table_config["table_name"]
    index_name = table_config["index_name"]
    path = table_config["path"]
    id_column = table_config["id_column"]

    print("=" * 80)
    print(f"INDEXING TABLE: {table_name}")
    print("=" * 80)
    print("Input path:", path)
    print("Elasticsearch index:", index_name)

    df = spark.read.parquet(path)

    total_rows = df.count()
    print(f"Rows to index: {total_rows}")

    delete_index_if_exists(index_name)
    create_index(index_name)

    batch = []
    batch_size = 300
    indexed_count = 0

    for row in df.toLocalIterator():
        doc = clean_document(row.asDict(recursive=True))
        batch.append(doc)

        if len(batch) >= batch_size:
            indexed_count += bulk_index_documents(index_name, batch, id_column)
            print(f"Indexed so far in {index_name}: {indexed_count}")
            batch = []

    if batch:
        indexed_count += bulk_index_documents(index_name, batch, id_column)

    es_request("POST", f"/{index_name}/_refresh")

    count_response = es_request("GET", f"/{index_name}/_count")
    es_count = count_response.get("count", 0)

    print(f"Spark rows: {total_rows}")
    print(f"Elasticsearch documents: {es_count}")

    if es_count != total_rows:
        raise RuntimeError(
            f"COUNT MISMATCH for {index_name}: Spark={total_rows}, Elasticsearch={es_count}"
        )

    print(f"STATUS: PASSED - {index_name}")
    return es_count


def main():
    print("=" * 80)
    print("SAFAA ELASTICSEARCH INDEXING")
    print("=" * 80)

    check_elasticsearch()

    spark = (
        SparkSession.builder
        .appName("Safaa Elasticsearch Indexing")
        .getOrCreate()
    )

    results = {}

    for table_config in TABLES:
        count = index_table(spark, table_config)
        results[table_config["index_name"]] = count

    spark.stop()

    print("=" * 80)
    print("ELASTICSEARCH INDEXING COMPLETED SUCCESSFULLY")
    print("=" * 80)

    for index_name, count in results.items():
        print(f"{index_name}: {count} documents")


if __name__ == "__main__":
    main()
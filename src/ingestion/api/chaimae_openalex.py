import requests
from datetime import datetime
from src.storage.minio.chaimae_client import MinIOClient

# Définition de la source pour le partitionnement Hudi/Data Lake
SOURCE_NAME = "openalex"

def extract_openalex(limit=20):
    url = "https://api.openalex.org/authors"
    params = {
        "per-page": limit
    }

    print(f"OpenAlex extraction ({limit})")

    response = requests.get(
        url,
        params=params,
        timeout=30
    )
    response.raise_for_status()
    return response.json(), response.status_code


def run(limit=20):
    client = MinIOClient()
    now = datetime.now()
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    status = 500
    records = 0

    # Génération des variables de partitionnement (ex: 2026, 06, 19)
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')

    try:
        data, status = extract_openalex(limit)
        records = len(data.get("results", []))

        # 1. Stockage de la donnée brute dans le bucket dédié aux JSON
        object_path = f"source={SOURCE_NAME}/year={year}/month={month}/day={day}/openalex_{timestamp}.json"
        
        client.upload_json(
            bucket_name="raw-json",
            object_name=object_path,
            data=data
        )

        print("OpenAlex completed")

    except requests.exceptions.Timeout:
        print("Erreur : timeout lors de l'appel à l'API OpenAlex")

    except requests.exceptions.HTTPError as e:
        print(f"Erreur HTTP : {e}")
        status = e.response.status_code if e.response is not None else 500

    except requests.exceptions.RequestException as e:
        print(f"Erreur réseau : {e}")

    except Exception as e:
        print(f"Erreur inattendue : {e}")

    finally:
        log = {
            "source": SOURCE_NAME,
            "status": status,
            "records": records,
            "timestamp": now.isoformat()
        }

        # 2. Stockage du log de traitement dans le bucket dédié aux logs
        log_path = f"source={SOURCE_NAME}/year={year}/month={month}/day={day}/log_{timestamp}.json"
        
        client.upload_json(
            bucket_name="raw-logs",
            object_name=log_path,
            data=log
        )


if __name__ == "__main__":
    run()
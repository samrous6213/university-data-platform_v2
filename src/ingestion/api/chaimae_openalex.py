import requests
from datetime import datetime

from src.storage.minio.chaimae_client import MinIOClient


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

    try:
        data, status = extract_openalex(limit)
        records = len(data.get("results", []))

        client.upload_json(
            bucket_name="data-lake",
            object_name=(
                f"raw/api/openalex/"
                f"openalex_{timestamp}.json"
            ),
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
            "source": "openalex",
            "status": status,
            "records": records,
            "timestamp": now.isoformat()
        }

        client.upload_json(
            bucket_name="data-lake",
            object_name=(
                f"raw/logs/openalex/"
                f"log_{timestamp}.json"
            ),
            data=log
        )


if __name__ == "__main__":
    run()
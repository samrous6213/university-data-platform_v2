import requests
from datetime import datetime

from src.storage.minio.chaimae_client import MinIOClient


def extract_openalex(limit=20):

    url = "https://api.openalex.org/authors"

    params = {
        "per-page": limit
    }

    print(
        f"OpenAlex extraction ({limit})"
    )

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    return response.json()


def run():

    client = MinIOClient()

    data = extract_openalex(20)

    filename = (
        f"raw/api/openalex/"
        f"openalex_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )

    client.upload_json(
        bucket_name="data-lake",
        object_name=filename,
        data=data
    )

    log = {
        "source": "openalex",
        "status": 200,
        "records": len(
            data.get(
                "results",
                []
            )
        ),
        "timestamp": datetime.now().isoformat()
    }

    client.upload_json(
        bucket_name="data-lake",
        object_name=(
            f"raw/logs/openalex/"
            f"log_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        ),
        data=log
    )

    print("OpenAlex completed")


if __name__ == "__main__":
    run()
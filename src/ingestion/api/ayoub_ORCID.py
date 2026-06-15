import requests
from datetime import datetime

from src.storage.minio.ayoub_client import MinIOClient


def extract_orcid(orcid_id="0000-0002-1825-0097"):

    print("STEP 1 - Début extraction")

    url = f"https://pub.orcid.org/v3.0/{orcid_id}"

    headers = {
        "Accept": "application/json",
        "User-Agent": "UniversityDataPlatform/1.0"
    }

    print(f"ORCID extraction ({orcid_id})")
    print("STEP 2 - Avant requête ORCID")

    response = requests.get(
        url,
        headers=headers,
        timeout=30
    )

    print("STEP 3 - Réponse reçue")

    response.raise_for_status()

    print(f"STEP 4 - Status code : {response.status_code}")

    return response.json(), response.status_code


def run(orcid_id="0000-0002-1825-0097"):

    print("STEP A - Création client MinIO")

    client = MinIOClient()

    now = datetime.now()

    timestamp = now.strftime('%Y%m%d_%H%M%S')

    status = 500
    records = 0

    try:

        print("STEP B - Avant extract_orcid")

        data, status = extract_orcid(orcid_id)

        print("STEP C - Données ORCID récupérées")

        records = 1

        print("STEP D - Upload JSON principal")

        client.upload_json(
            bucket_name="data-lake",
            object_name=(
                f"raw/api/orcid/"
                f"orcid_{timestamp}.json"
            ),
            data=data
        )

        print("STEP E - Upload terminé")

        print("ORCID extraction completed")

    except requests.exceptions.Timeout:

        print("Erreur : timeout ORCID")

    except requests.exceptions.HTTPError as e:

        print(f"Erreur HTTP : {e}")

        status = (
            e.response.status_code
            if e.response is not None
            else 500
        )

    except requests.exceptions.RequestException as e:

        print(f"Erreur réseau : {e}")

    except Exception as e:

        print(f"Erreur inattendue : {e}")

    finally:

        print("STEP F - Création log")

        log = {
            "source": "orcid",
            "orcid_id": orcid_id,
            "status": status,
            "records": records,
            "timestamp": now.isoformat()
        }

        try:

            print("STEP G - Upload log")

            client.upload_json(
                bucket_name="data-lake",
                object_name=(
                    f"raw/logs/orcid/"
                    f"log_{timestamp}.json"
                ),
                data=log
            )

            print("STEP H - Log upload terminé")

        except Exception as e:

            print(f"Erreur upload log : {e}")


if __name__ == "__main__":
    run()
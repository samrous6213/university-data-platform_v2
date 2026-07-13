from bs4 import BeautifulSoup
from src.storage.minio.hiba_client import MinIOClient

KEYWORDS = [
    "faculté",
    "fst",
    "fsac",
    "enseignant",
    "professeur",
    "département",
    "formation",
    "licence",
    "master",
    "doctorat",
]

client = MinIOClient()

objects = client.client.list_objects(
    "data-lake",
    prefix="raw/html/",
    recursive=True
)

for obj in objects:

    try:
        response = client.client.get_object(
            "data-lake",
            obj.object_name
        )

        html = response.read().decode(
            "utf-8",
            errors="ignore"
        )

        soup = BeautifulSoup(
            html,
            "html.parser"
        )

        text = soup.get_text(
            separator=" ",
            strip=True
        ).lower()

        matches = [
            k for k in KEYWORDS
            if k.lower() in text
        ]

        if matches:
            print("\n" + "=" * 80)
            print(obj.object_name)
            print("FOUND:", matches)

    except Exception as e:
        print(
            f"Error {obj.object_name}: {e}"
        )
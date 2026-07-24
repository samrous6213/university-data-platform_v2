"""
Ingestion Web Scraping -> MinIO (zone raw).
Source : Site Web USMBA (Pages statiques brutes et extraction des documents associés).
"""

import logging
import sys
import uuid
import hashlib
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Utilisation de ton client MinIO
from src.storage.minio.ayoub_client import MinIOClient

CONNECTOR_VERSION = "1.1.0"
SOURCE_NAME = "web_usmba"

RAW_HTML_BUCKET = "raw-html"         # <-- NOUVEAU BUCKET POUR LE HTML
RAW_DOCUMENTS_BUCKET = "raw-documents" # <-- POUR LES PDF/DOCX
LOG_BUCKET = "raw-logs"

# Les 3 facultés de l'USMBA
PAGES_TO_SCRAPE = [
    {"name": "fsdm_sciences", "url": "http://www.fsdm.usmba.ac.ma/"},
    {"name": "fst_techniques", "url": "http://www.fst-usmba.ac.ma/"},
    {"name": "est_technologie", "url": "http://www.est-usmba.ac.ma/"}
]

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"ingestion_usmba_{datetime.now():%Y%m%d}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(SOURCE_NAME)

def build_session(max_retries: int = 5) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def ingest_webpage_and_docs(client: MinIOClient, session: requests.Session, page_info: dict) -> dict:
    page_name = page_info["name"]
    url = page_info["url"]
    logger.info(f"Début aspiration page et recherche de documents : {page_name} ({url})")
    
    now = datetime.now()
    year_str, month_str, day_str = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    timestamp_str = now.isoformat()
    ingestion_id = str(uuid.uuid4())
    error_message = None
    docs_downloaded = 0

    try:
        response = session.get(url, timeout=30, verify=False) 
        response.raise_for_status()
        html_content = response.content 

        # 1. Sauvegarde du fichier HTML brut
       # 1. Sauvegarde du fichier HTML brut dans raw-html
        html_object_name = (
            f"source={SOURCE_NAME}/entity={page_name}/"
            f"year={year_str}/month={month_str}/day={day_str}/page.html"
        )
        client.upload_binary(
            bucket_name=RAW_HTML_BUCKET, # <-- ON UTILISE LE BUCKET HTML ICI
            object_name=html_object_name,
            data=html_content,
            content_type="text/html"
        )
        logger.info(f"Page HTML stockée : s3://{RAW_HTML_BUCKET}/{html_object_name}")

        # 2. Utilisation de BeautifulSoup uniquement pour trouver les liens des documents
        soup = BeautifulSoup(html_content, "html.parser")
        anchors = soup.find_all("a", href=True)
        
        for link in anchors:
            href = link.get("href")
            # Si le lien pointe vers un document pertinent
            if href.lower().endswith((".pdf", ".doc", ".docx")):
                doc_url = urljoin(url, href) # Transforme un lien relatif en lien absolu
                try:
                    logger.info(f"Téléchargement du document trouvé : {doc_url}")
                    doc_resp = session.get(doc_url, timeout=20, verify=False)
                    doc_resp.raise_for_status()
                    
                    doc_filename = doc_url.split("/")[-1] or f"document_{uuid.uuid4().hex[:8]}.pdf"
                    doc_object_name = (
                        f"source={SOURCE_NAME}/entity={page_name}/"
                        f"year={year_str}/month={month_str}/day={day_str}/{doc_filename}"
                    )
                    
                    # Upload du document dans MinIO au format binaire
                    client.upload_binary(
                        bucket_name=RAW_DOCUMENTS_BUCKET,
                        object_name=doc_object_name,
                        data=doc_resp.content,
                        content_type="application/pdf" if doc_filename.lower().endswith(".pdf") else "application/octet-stream"
                    )
                    docs_downloaded += 1
                except Exception as e:
                    logger.warning(f"Impossible de télécharger le document {doc_url}: {e}")

    except Exception as e:
        error_message = str(e)
        logger.error(f"Erreur globale lors du scraping de {url}: {error_message}")

    finally:
        # 3. Mise à jour des logs pour inclure le nombre de documents récupérés
        log_payload = {
            "ingestion_id": ingestion_id,
            "source": SOURCE_NAME,
            "page_name": page_name,
            "timestamp": timestamp_str,
            "status": "success" if not error_message else "failed",
            "documents_retrieved": docs_downloaded,
            "error": error_message
        }
        log_object_name = (
            f"source={SOURCE_NAME}/entity={page_name}/"
            f"year={year_str}/month={month_str}/day={day_str}/run_{ingestion_id}.json"
        )
        client.upload_json(bucket_name=LOG_BUCKET, object_name=log_object_name, data=log_payload)

    return {"page_name": page_name, "error": error_message, "documents_retrieved": docs_downloaded}

def main():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    client = MinIOClient()
    session = build_session()
    
    for page in PAGES_TO_SCRAPE:
        ingest_webpage_and_docs(client, session, page)

if __name__ == "__main__":
    main()
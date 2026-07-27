# src/ingestion/docs/mit_ocw_pdf_scraper.py

import hashlib
import logging
import os
import re
import sys
import time
import requests
import urllib3
import tempfile
import uuid

from bs4 import BeautifulSoup
from collections import deque
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse

from dotenv import load_dotenv
load_dotenv()

# Ajouter src/ au PYTHONPATH
sys.path.insert(0, 'D:/university-data-platform_v2')

# ==============================================================
# IMPORTER MinIOClient DEPUIS LE BON ENDROIT
# ==============================================================
from src.storage.minio.nezha_client import MinIOClient

# ==============================================================

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
SOURCE_NAME = "mit_ocw"

# 🌐 URLs de départ pour découvrir tous les cours
SEED_URLS = [
    "https://ocw.mit.edu/courses/mathematics/",
    "https://ocw.mit.edu/courses/electrical-engineering-and-computer-science/",
    "https://ocw.mit.edu/courses/physics/",
    "https://ocw.mit.edu/courses/economics/",
    "https://ocw.mit.edu/courses/chemistry/",
    "https://ocw.mit.edu/courses/biology/",
    "https://ocw.mit.edu/courses/mechanical-engineering/",
    "https://ocw.mit.edu/courses/civil-and-environmental-engineering/",
]

MAX_DOCUMENTS = 5
MAX_PAGES = 30  # Sécurité pour éviter une boucle infinie
SLEEP_BETWEEN = 0.5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 2

CHECKPOINT_FILE = "mit_ocw_crawl_checkpoint.txt"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Connection": "keep-alive",
}

DOCUMENT_EXTS = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".ppt", ".pptx")
DOCUMENT_CONTENT_TYPES = (
    "application/pdf",
    "application/msword",
    "vnd.openxml",
    "application/vnd.ms-excel",
    "text/csv",
)

# ==================== HELPERS ====================

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    parsed = parsed._replace(fragment="")
    if parsed.path and parsed.path != "/" and parsed.path.endswith("/"):
        parsed = parsed._replace(path=parsed.path.rstrip("/"))
    return urlunparse(parsed)

def _extract_year_from_url(url: str) -> str:
    for pattern in (r'/(20[1-2][0-9])/', r'[-_](20[1-2][0-9])[-_]'):
        m = re.search(pattern, url)
        if m:
            return m.group(1)
    return str(datetime.now().year)

def _clean_filename(url: str, doc_id: str, content_type: str = "") -> str:
    base_name = os.path.basename(urlparse(url).path)
    if base_name and base_name.lower().endswith(DOCUMENT_EXTS):
        return base_name
    
    ext_map = {
        "pdf": ".pdf",
        "msword": ".doc",
        "vnd.openxmlformats-officedocument.wordprocessingml": ".docx",
        "vnd.openxmlformats-officedocument.spreadsheetml": ".xlsx",
        "vnd.ms-excel": ".xls",
        "csv": ".csv",
    }
    ext = ".pdf"
    for key, value in ext_map.items():
        if key in content_type:
            ext = value
            break
    
    return f"document_{doc_id}{ext}"

def _is_document(url: str, content_type: str) -> bool:
    if "text/html" in content_type:
        return False
    if any(t in content_type for t in DOCUMENT_CONTENT_TYPES):
        return True
    return url.lower().endswith(DOCUMENT_EXTS)

def _load_checkpoint() -> set:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            visited = {line.strip() for line in f if line.strip()}
        logger.info(f"📂 Checkpoint chargé : {len(visited)} URLs déjà visitées")
        return visited
    return set()

def _append_checkpoint(url: str) -> None:
    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
        f.write(url + "\n")

def _get_with_retries(session: requests.Session, url: str):
    last_exc = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            return session.get(
                url, stream=True, timeout=REQUEST_TIMEOUT,
                verify=False, allow_redirects=True,
            )
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise last_exc

# ==================== FONCTION UPLOAD CORRIGÉE ====================
def upload_binary_to_minio(bucket_name: str, object_name: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
    """
    Upload un fichier binaire vers MinIO en utilisant MinIOClient
    """
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        client.upload_binary(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
            content_type=content_type
        )
        logger.debug(f"✅ Fichier uploadé: {object_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur upload vers MinIO: {e}")
        return False

def upload_json_to_minio(bucket_name: str, object_name: str, data: dict) -> bool:
    """
    Upload des métadonnées JSON vers MinIO en utilisant MinIOClient
    """
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        client.upload_json(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data
        )
        logger.debug(f"✅ JSON uploadé: {object_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur upload JSON vers MinIO: {e}")
        return False

def get_course_links(soup, base_url):
    """Extraire les liens des cours depuis une page de catégorie"""
    course_links = []
    
    # Chercher les liens vers les cours
    for link in soup.find_all('a', href=True):
        href = link.get('href')
        if href and '/courses/' in href:
            # Nettoyer l'URL
            full_url = urljoin(base_url, href)
            # Vérifier que c'est un cours (pas une page de catégorie)
            if full_url.count('/') >= 4 and 'resources' not in full_url:
                if full_url not in course_links:
                    course_links.append(full_url)
    
    return course_links

# ==================== MAIN EXTRACTION ====================

def extract_mit_ocw_documents() -> None:
    # Initialiser le client MinIO pour vérifier la connexion
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        logger.info("✅ Connexion MinIO établie")
    except Exception as e:
        logger.error(f"❌ Impossible de se connecter à MinIO: {e}")
        logger.warning("⚠️ Le scraper va continuer mais les uploads échoueront")
    
    visited_urls = _load_checkpoint()
    queued_urls = set()
    queue = deque()

    # Ajouter les URLs de départ
    for seed in SEED_URLS:
        if seed not in visited_urls:
            queue.append(seed)
            queued_urls.add(seed)

    documents_count = 0
    pages_checked = 0
    courses_found = set()

    logger.info(f"🚀 MIT OCW document harvest - {len(SEED_URLS)} catégories de départ")
    logger.info(f"🎯 Objectif: collecter {MAX_DOCUMENTS} documents maximum")

    with requests.Session() as session:
        session.headers.update(HEADERS)

        # Boucle principale
        while queue and pages_checked < MAX_PAGES:
            url = _normalize_url(queue.popleft())

            if url in visited_urls:
                continue
            
            visited_urls.add(url)
            _append_checkpoint(url)
            pages_checked += 1

            logger.info(f"[{pages_checked} checked / {documents_count} saved] Analyzing: {url}")
            time.sleep(SLEEP_BETWEEN)

            try:
                with _get_with_retries(session, url) as res:
                    if res.status_code != 200:
                        logger.warning(f"[HTTP {res.status_code}] Skipping: {url}")
                        continue

                    content_type = res.headers.get("Content-Type", "").lower()

                    # 📄 CAS 1 : Document (PDF, etc.)
                    if _is_document(url, content_type):
                        try:
                            buffer = bytearray()
                            for chunk in res.iter_content(chunk_size=16384):
                                if chunk:
                                    buffer.extend(chunk)
                            doc_bytes = bytes(buffer)

                            if not doc_bytes:
                                logger.warning(f"[DOC] Empty content: {url}")
                                continue

                            now = datetime.now()
                            year = _extract_year_from_url(url)
                            doc_id = hashlib.md5(url.encode("utf-8")).hexdigest()
                            filename = _clean_filename(url, doc_id, content_type)
                            checksum = _sha256(doc_bytes)

                            object_path = (
                                f"source={SOURCE_NAME}/"
                                f"year={year}/"
                                f"month={now.month:02d}/"
                                f"day={now.day:02d}/"
                                f"{filename}"
                            )

                            # Upload du document vers MinIO
                            upload_success = upload_binary_to_minio(
                                bucket_name="raw-docs",
                                object_name=object_path,
                                data=doc_bytes,
                                content_type=content_type or "application/octet-stream",
                            )

                            if upload_success:
                                # Créer et uploader les métadonnées
                                metadata = {
                                    "record_id": doc_id,
                                    "source_system": SOURCE_NAME,
                                    "source_url": url,
                                    "content_hash": checksum,
                                    "crawl_timestamp": now.isoformat(),
                                    "file_name": filename,
                                    "file_size_bytes": len(doc_bytes),
                                    "content_type": content_type,
                                    "raw_storage_path": f"s3://raw-docs/{object_path}",
                                }
                                
                                metadata_path = (
                                    f"source={SOURCE_NAME}/"
                                    f"year={year}/"
                                    f"month={now.month:02d}/"
                                    f"day={now.day:02d}/"
                                    f"{filename}_metadata.json"
                                )
                                
                                upload_json_to_minio(
                                    bucket_name="raw-json",
                                    object_name=metadata_path,
                                    data=metadata,
                                )

                                documents_count += 1
                                logger.info(f"[SAVED #{documents_count}] {filename} ({len(doc_bytes)} bytes)")
                            else:
                                logger.error(f"[FAILED] Upload failed for: {filename}")

                            # Vérifier si on a atteint la limite
                            if documents_count >= MAX_DOCUMENTS:
                                logger.info(f"🎯 Limite de {MAX_DOCUMENTS} documents atteinte, arrêt du crawl")
                                break

                        except requests.exceptions.Timeout:
                            logger.warning(f"[DOC] Timeout: {url}")
                        except Exception as e:
                            logger.error(f"[DOC] Error: {e}")

                    # 🌐 CAS 2 : Page HTML
                    elif "text/html" in content_type:
                        try:
                            html_text = res.text
                        except Exception as e:
                            logger.warning(f"[HTML] Could not read: {url}: {e}")
                            continue

                        soup = BeautifulSoup(html_text, "html.parser")

                        # Extraire les liens des cours
                        course_links = get_course_links(soup, url)
                        for course_url in course_links:
                            if course_url not in courses_found:
                                courses_found.add(course_url)
                                logger.info(f"📚 Nouveau cours trouvé: {course_url}")
                            
                            if course_url not in visited_urls and course_url not in queued_urls:
                                queued_urls.add(course_url)
                                queue.append(course_url)

                        # Liens <a> et <iframe> pour découvrir des pages
                        candidate_links = [tag.get("href") for tag in soup.find_all("a", href=True)]
                        candidate_links += [tag.get("src") for tag in soup.find_all(["iframe", "embed", "source"], src=True)]

                        for href in candidate_links:
                            if not href:
                                continue
                            full_url = _normalize_url(urljoin(url, href))

                            if "ocw.mit.edu" not in urlparse(full_url).netloc:
                                continue

                            if full_url in visited_urls or full_url in queued_urls:
                                continue

                            queued_urls.add(full_url)

                            if full_url.lower().endswith(DOCUMENT_EXTS):
                                queue.appendleft(full_url)
                            else:
                                queue.append(full_url)

                    else:
                        logger.info(f"[SKIP] Unhandled content-type '{content_type}': {url}")

            except requests.exceptions.Timeout:
                logger.warning(f"[PAGE] Timeout: {url}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"[PAGE] Request error: {e}")
            except Exception as e:
                logger.error(f"[PAGE] Unexpected error: {e}")

    logger.info(
        f"✅ Mission complete -- "
        f"{documents_count} documents stored in MinIO, "
        f"{len(courses_found)} cours trouvés, "
        f"{pages_checked} pages checked"
    )

if __name__ == "__main__":
    extract_mit_ocw_documents()

import hashlib
import logging
import os
import re
import sys
import time
import requests
import urllib3

from bs4 import BeautifulSoup
from collections import deque
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse

from dotenv import load_dotenv
load_dotenv()

from src.storage.minio.hiba_client import MinIOClient

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Forcer UTF-8 sur la sortie pour éviter les crashs liés aux emojis sous Windows
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

TARGET_URL    = "https://www.hcp.ma/"
SOURCE_NAME   = "hcp_docs"

# Pour un scraping COMPLET : pas de limite artificielle sur le nombre de documents.
# On garde un garde-fou sur les pages pour éviter une boucle infinie en cas de site
# pathologique (redirections en boucle, paramètres infinis, etc.), mais à une valeur
# largement supérieure à la taille estimée du site.
MAX_DOCUMENTS = float("inf")
MAX_PAGES     = 20000
SLEEP_BETWEEN = 0.3
REQUEST_TIMEOUT = 25
MAX_RETRIES   = 2

# Fichier de checkpoint pour pouvoir reprendre un crawl interrompu
CHECKPOINT_FILE = "hcp_crawl_checkpoint.txt"

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

# Extensions élargies pour couvrir CSV, PowerPoint, archives, etc.
DOCUMENT_EXTS = (
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".csv", ".ppt", ".pptx", ".odt", ".ods", ".odp",
    ".zip", ".rar",".json", ".xml"
)

# Content-types élargis (CSV, Excel legacy, zip, etc.)
DOCUMENT_CONTENT_TYPES = (
    "application/pdf",
    "application/msword",
    "vnd.openxml",
    "application/octet-stream",
    "application/vnd.ms-excel",
    "application/vnd.oasis.opendocument",
    "application/zip",
    "application/x-zip-compressed",
    "text/csv",
    "application/json",
    "application/xml",
    "text/xml",
)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

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

    # Pas d'extension dans l'URL -> on déduit depuis le content-type
    ext_map = {
        "pdf": ".pdf",
        "msword": ".doc",
        "vnd.openxmlformats-officedocument.wordprocessingml": ".docx",
        "vnd.openxmlformats-officedocument.spreadsheetml": ".xlsx",
        "vnd.openxmlformats-officedocument.presentationml": ".pptx",
        "vnd.ms-excel": ".xls",
        "csv": ".csv",
        "zip": ".zip",
    }
    ext = ".pdf"  # défaut
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
    """Charge les URLs déjà visitées lors d'un run précédent, si disponible."""
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
    """GET avec quelques tentatives en cas d'erreur réseau transitoire."""
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


# ─────────────────────────────────────────────
# Main extraction
# ─────────────────────────────────────────────

def extract_hcp_documents() -> None:

    client       = MinIOClient()
    visited_urls = _load_checkpoint()
    queued_urls  = {TARGET_URL} | visited_urls
    queue        = deque([TARGET_URL]) if TARGET_URL not in visited_urls else deque()
    documents_count = 0
    pages_checked   = 0

    logger.info("Starting HCP FULL document harvest (no document cap)")

    with requests.Session() as session:
        session.headers.update(HEADERS)

        while queue and documents_count < MAX_DOCUMENTS and pages_checked < MAX_PAGES:

            url = _normalize_url(queue.popleft())

            if url in visited_urls:
                continue
            visited_urls.add(url)
            _append_checkpoint(url)
            pages_checked += 1

            logger.info(
                f"[{pages_checked} checked / {documents_count} saved] "
                f"Analyzing: {url}"
            )
            time.sleep(SLEEP_BETWEEN)

            try:
                with _get_with_retries(session, url) as res:

                    if res.status_code != 200:
                        logger.warning(f"[HTTP {res.status_code}] Skipping: {url}")
                        continue

                    content_type = res.headers.get("Content-Type", "").lower()

                    # 📄 CAS 1 : Document (PDF, Word, Excel, CSV, etc.)
                    if _is_document(url, content_type):
                        try:
                            buffer = bytearray()
                            for chunk in res.iter_content(chunk_size=16384):
                                if chunk:
                                    buffer.extend(chunk)
                            doc_bytes = bytes(buffer)

                            if not doc_bytes:
                                logger.warning(f"[DOC] Empty content, skipping: {url}")
                                continue

                            now      = datetime.now()
                            year     = _extract_year_from_url(url)
                            doc_id   = hashlib.md5(url.encode("utf-8")).hexdigest()
                            filename = _clean_filename(url, doc_id, content_type)
                            checksum = _sha256(doc_bytes)

                            object_path = (
                                f"source={SOURCE_NAME}/"
                                f"year={year}/"
                                f"month={now.month:02d}/"
                                f"day={now.day:02d}/"
                                f"{filename}"
                            )

                            # Fichier brut -> raw-documents
                            client.upload_binary(
                                bucket_name="raw-documents",
                                object_name=object_path,
                                data=doc_bytes,
                                content_type=content_type or "application/octet-stream",
                            )

                            # Métadonnées -> raw-json
                            metadata = {
                                "record_id": doc_id,
                                "source_system": SOURCE_NAME,
                                "source_url": url,
                                "content_hash": checksum,
                                "crawl_timestamp": now.isoformat(),
                                "file_name": filename,
                                "file_size_bytes": len(doc_bytes),
                                "content_type": content_type,
                                "raw_storage_path": f"s3://raw-documents/{object_path}",
                            }
                            client.upload_json(
                                bucket_name="raw-json",
                                object_name=(
                                    f"source={SOURCE_NAME}/"
                                    f"year={year}/"
                                    f"month={now.month:02d}/"
                                    f"day={now.day:02d}/"
                                    f"{filename}_metadata.json"
                                ),
                                data=metadata,
                            )

                            documents_count += 1
                            logger.info(f"[SAVED #{documents_count}] {filename} ({len(doc_bytes)} bytes)")

                        except requests.exceptions.Timeout:
                            logger.warning(f"[DOC] Timeout while streaming: {url}")
                        except Exception as e:
                            logger.error(f"[DOC] Error processing {url}: {e}")

                    # 🌐 CAS 2 : Page HTML -> extraction de liens
                    elif "text/html" in content_type:
                        try:
                            html_text = res.text
                        except Exception as e:
                            logger.warning(f"[HTML] Could not read text for {url}: {e}")
                            continue

                        soup = BeautifulSoup(html_text, "html.parser")

                        # Liens classiques <a href>
                        candidate_links = [tag.get("href") for tag in soup.find_all("a", href=True)]
                        # Liens potentiels dans <iframe>, <embed>, <source> (visionneuses de docs)
                        candidate_links += [tag.get("src") for tag in soup.find_all(["iframe", "embed", "source"], src=True)]

                        for href in candidate_links:
                            if not href:
                                continue
                            full_url = _normalize_url(urljoin(url, href))

                            if "hcp.ma" not in urlparse(full_url).netloc:
                                continue

                            if full_url in visited_urls or full_url in queued_urls:
                                continue

                            queued_urls.add(full_url)

                            # File de priorité : documents en premier (BFS orienté docs)
                            if full_url.lower().endswith(DOCUMENT_EXTS):
                                queue.appendleft(full_url)
                            else:
                                queue.append(full_url)

                    else:
                        logger.info(f"[SKIP] Unhandled content-type '{content_type}': {url}")

            except requests.exceptions.Timeout:
                logger.warning(f"[PAGE] Timeout: {url}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"[PAGE] Request error on {url}: {e}")
            except Exception as e:
                logger.error(f"[PAGE] Unexpected error on {url}: {e}")

    logger.info(
        f"Mission complete -- {documents_count} documents stored in MinIO "
        f"({pages_checked} pages checked)"
    )

    if documents_count == 0:
        logger.warning(
            "No documents were found. Check TARGET_URL, network access, "
            "or whether the site loads document listings via JavaScript "
            "(in which case requests/BeautifulSoup won't be enough)."
        )


if __name__ == "__main__":
    extract_hcp_documents()
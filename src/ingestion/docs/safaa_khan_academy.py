import hashlib
import html
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

from src.storage.minio.safaa_client import MinIOClient

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

# الرابط اللي عطاه prof
TARGET_URL = "https://www.khanacademy.org/math"
SOURCE_NAME = "khan_academy_docs"

# نفس structure ديال Chaimae
MAX_DOCUMENTS = 1000
MAX_PAGES = 900
SLEEP_BETWEEN = 0.3
REQUEST_TIMEOUT = 25
MAX_RETRIES = 2

CHECKPOINT_FILE = "khan_academy_crawl_checkpoint.txt"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7",
    "Connection": "keep-alive",
}

DOCUMENT_EXTS = (
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".csv", ".ppt", ".pptx", ".odt", ".ods", ".odp",
    ".zip", ".rar",
)

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
)


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
        "vnd.openxmlformats-officedocument.presentationml": ".pptx",
        "vnd.ms-excel": ".xls",
        "csv": ".csv",
        "zip": ".zip",
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


def _is_allowed_khan_url(url: str) -> bool:
    netloc = urlparse(url).netloc.lower()

    return (
        netloc.endswith("khanacademy.org")
        or netloc.endswith("kastatic.org")
    )


def _load_checkpoint() -> set:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            visited = {line.strip() for line in f if line.strip()}

        logger.info(f"Checkpoint loaded: {len(visited)} URLs already visited")
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
                url,
                stream=True,
                timeout=REQUEST_TIMEOUT,
                verify=False,
                allow_redirects=True,
            )

        except requests.exceptions.RequestException as e:
            last_exc = e

            if attempt < MAX_RETRIES:
                time.sleep(1.5 * (attempt + 1))
                continue

            raise last_exc


def _extract_links_from_html_text(url: str, html_text: str) -> list:
    """
    Solution 2:
    BeautifulSoup بوحدو ما كافيش مع Khan Academy.
    كنخرجو links من:
    - <a href>
    - iframe/embed/source src
    - raw HTML / JavaScript strings
    """

    links = set()

    soup = BeautifulSoup(html_text, "html.parser")

    # 1) liens classiques
    for tag in soup.find_all("a", href=True):
        links.add(tag.get("href"))

    # 2) src ديال iframe/embed/source/script/link
    for tag in soup.find_all(["iframe", "embed", "source", "script", "link"], src=True):
        links.add(tag.get("src"))

    for tag in soup.find_all(["script", "link"], href=True):
        links.add(tag.get("href"))

    # 3) URLs كاملة داخل JavaScript / raw HTML
    raw_text = html.unescape(html_text)

    raw_text = (
        raw_text
        .replace("\\u002F", "/")
        .replace("\\/", "/")
        .replace("&amp;", "&")
    )

    absolute_url_pattern = r'https?://[^\s"\'<>\\)]+'
    for found_url in re.findall(absolute_url_pattern, raw_text):
        links.add(found_url)

    # 4) relative Khan Academy paths داخل JavaScript
    relative_path_pattern = r'(?:"|\')((?:/math|/computing|/science|/downloads|/resources|/khan-for-educators)[^"\']+)(?:"|\')'
    for path in re.findall(relative_path_pattern, raw_text):
        links.add(path)

    normalized_links = []

    for href in links:
        if not href:
            continue

        if href.startswith(("mailto:", "tel:", "javascript:")):
            continue

        full_url = _normalize_url(urljoin(url, href))

        if not _is_allowed_khan_url(full_url):
            continue

        normalized_links.append(full_url)

    return list(dict.fromkeys(normalized_links))


def extract_khan_academy_documents() -> None:

    client = MinIOClient()

    visited_urls = _load_checkpoint()
    queued_urls = {TARGET_URL} | visited_urls
    queue = deque([TARGET_URL]) if TARGET_URL not in visited_urls else deque()

    documents_count = 0
    pages_checked = 0

    logger.info("Starting Khan Academy FULL document harvest")

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

                    # CAS 1 : Document
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

                            client.upload_binary(
                                bucket_name="raw-documents",
                                object_name=object_path,
                                data=doc_bytes,
                                content_type=content_type or "application/octet-stream",
                            )

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

                            logger.info(
                                f"[SAVED #{documents_count}] "
                                f"{filename} ({len(doc_bytes)} bytes)"
                            )

                        except requests.exceptions.Timeout:
                            logger.warning(f"[DOC] Timeout while streaming: {url}")

                        except Exception as e:
                            logger.error(f"[DOC] Error processing {url}: {e}")

                    # CAS 2 : Page HTML -> extraction de liens
                    elif "text/html" in content_type:
                        try:
                            html_text = res.text

                        except Exception as e:
                            logger.warning(f"[HTML] Could not read text for {url}: {e}")
                            continue

                        candidate_links = _extract_links_from_html_text(url, html_text)

                        logger.info(f"[HTML] Found {len(candidate_links)} candidate links")

                        for full_url in candidate_links:

                            if full_url in visited_urls or full_url in queued_urls:
                                continue

                            queued_urls.add(full_url)

                            if full_url.lower().endswith(DOCUMENT_EXTS):
                                queue.appendleft(full_url)
                            else:
                                queue.append(full_url)

                    else:
                        logger.info(
                            f"[SKIP] Unhandled content-type '{content_type}': {url}"
                        )

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
            "No documents were found. This means the Khan Academy math pages "
            "did not expose downloadable PDF/CSV/XLSX/DOCX/PPTX files to requests/BeautifulSoup."
        )


if __name__ == "__main__":
    extract_khan_academy_documents()
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from collections import deque
from datetime import datetime

from src.storage.minio.fahd_client import MinIOClient

BASE_URL = "https://www.onousc.ma"
MAX_PAGES = 100


def _normalize_url(url: str) -> str:
    """Normalize URLs to improve crawl discovery and avoid duplicates."""
    parsed = urlparse(url)

    # Drop fragments (#...)
    parsed = parsed._replace(fragment="")

    # Remove trailing slash for non-root paths
    if parsed.path and parsed.path != "/" and parsed.path.endswith("/"):
        parsed = parsed._replace(path=parsed.path.rstrip("/"))

    # Remove default index paths
    if parsed.path in ("/index", "/index.html"):
        parsed = parsed._replace(path="/")

    return urlunparse(parsed)


def _is_allowed_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.netloc or "").lower()
    return host.endswith("onousc.ma")


def _html_object_name(url: str) -> str:
    """Convert a URL to a safe MinIO object name for HTML files."""
    return (
        url.replace("https://", "")
           .replace("http://", "")
           .replace("/", "_")
    )


def _crawl_pdf(session: requests.Session, full_url: str, client: MinIOClient, logs: list, page_url: str):
    """Download and upload a single PDF to MinIO."""
    try:
        pdf_resp = session.get(
            full_url,
            timeout=20,
            verify=False,
            allow_redirects=True,
        )
        pdf_resp.raise_for_status()

        pdf_name = full_url.split("/")[-1] or "document.pdf"

        client.upload_binary(
            bucket_name="data-lake",
            object_name=f"raw/pdfs/{pdf_name}",
            data=pdf_resp.content,
            content_type="application/pdf",
        )

    except Exception as e:
        logs.append({
            "url": page_url,
            "status": "PDF_ERROR",
            "pdf_url": full_url,
            "message": str(e),
            "timestamp": datetime.now().isoformat(),
        })


def crawl_onousc():
    client = MinIOClient()
    visited = set()
    queue = deque([_normalize_url(BASE_URL)])
    logs = []

    now = datetime.now()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; ONOUSC crawler/1.0; +https://www.onousc.ma)",
    })

    while queue and len(visited) < MAX_PAGES:

        url = queue.popleft()

        if url in visited:
            continue

        visited.add(url)

        try:          
            response = session.get(
                url,
                timeout=20,
                verify=False,
                allow_redirects=True,
            )
            print(f"[{len(visited)}] Crawled: {url}")
            response.raise_for_status()

            # Upload HTML page
            client.upload_binary(
                bucket_name="data-lake",
                object_name=f"raw/html/{_html_object_name(url)}.html",
                data=response.text.encode("utf-8"),
                content_type="text/html",
            )

            soup = BeautifulSoup(response.text, "html.parser")
            anchors = soup.find_all("a", href=True)

            pdf_links = 0
            queued_links = 0
            href_samples = [str(a.get("href")) for a in anchors[:20]]

            for link in anchors:
                try:
                    href = link.get("href")
                    if not href:
                        continue

                    full_url = _normalize_url(urljoin(url, href))

                    if full_url.lower().endswith(".pdf"):
                        pdf_links += 1
                        _crawl_pdf(session, full_url, client, logs, url)

                    elif _is_allowed_url(full_url):
                        if full_url not in visited and full_url not in queue:
                            queue.append(full_url)
                            queued_links += 1

                except Exception as e:
                    logs.append({
                        "url": url,
                        "status": "LINK_PARSE_ERROR",
                        "message": str(e),
                        "timestamp": datetime.now().isoformat(),
                    })

            logs.append({
                "url": url,
                "status": response.status_code,
                "timestamp": datetime.now().isoformat(),
                "anchors_found": len(anchors),
                "pdf_links_found": pdf_links,
                "queued_links_added": queued_links,
                "href_samples": href_samples,
            })

        except requests.exceptions.Timeout:
            logs.append({
                "url": url,
                "status": "TIMEOUT",
                "message": "Request timed out after 20s",
                "timestamp": datetime.now().isoformat(),
            })

        except requests.exceptions.HTTPError as e:
            logs.append({
                "url": url,
                "status": e.response.status_code if e.response is not None else "HTTP_ERROR",
                "message": str(e),
                "timestamp": datetime.now().isoformat(),
            })

        except Exception as e:
            logs.append({
                "url": url,
                "status": "ERROR",
                "message": str(e),
                "timestamp": datetime.now().isoformat(),
            })

    # Upload crawl log
    client.upload_json(
        bucket_name="data-lake",
        object_name=f"raw/logs/onousc/crawl_{now.strftime('%Y%m%d_%H%M%S')}.json",
        data=logs,
    )

    print(f"{len(visited)} pages crawled")


if __name__ == "__main__":
    crawl_onousc()
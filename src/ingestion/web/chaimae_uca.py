import requests

from bs4 import BeautifulSoup

from urllib.parse import urljoin, urlparse, urlunparse

from collections import deque

from datetime import datetime

from src.storage.minio.chaimae_client import MinIOClient


BASE_URL = "https://www.uca.ma"

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
    # Allow both uca.ma and www.uca.ma
    host = (parsed.netloc or "").lower()
    return host.endswith("uca.ma")


def crawl_uca():

    client = MinIOClient()

    visited = set()

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; UCA crawler/1.0; +https://www.uca.ma)",
        }
    )

    queue = deque([_normalize_url(BASE_URL)])

    logs = []


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


            response.raise_for_status()

            html_name = (
                url.replace(
                    "https://",
                    ""
                )
                .replace(
                    "/",
                    "_"
                )
            )

            client.upload_binary(
                bucket_name="data-lake",
                object_name=(
                    f"raw/html/"
                    f"{html_name}.html"
                ),
                data=response.text.encode(
                    "utf-8"
                ),
                content_type="text/html"
            )

            soup = BeautifulSoup(response.text, "html.parser")

            anchors = soup.find_all("a", href=True)
            pdf_links = 0
            queued_links = 0

            href_samples = []

            for a in anchors[:20]:
                try:
                    href_samples.append(str(a.get("href")))
                except Exception:
                    pass

            for link in anchors:
                try:
                    href = link.get("href")
                    if not href:
                        continue

                    full_url = urljoin(url, href)
                    full_url = _normalize_url(full_url)

                    if full_url.lower().endswith(".pdf"):
                        pdf_links += 1
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
                            logs.append(
                                {
                                    "url": url,
                                    "status": "PDF_ERROR",
                                    "pdf_url": full_url,
                                    "message": str(e),
                                    "timestamp": datetime.now().isoformat(),
                                }
                            )

                    elif _is_allowed_url(full_url):
                        if full_url not in visited and full_url not in queue:
                            queue.append(full_url)
                            queued_links += 1

                except Exception as e:
                    logs.append(
                        {
                            "url": url,
                            "status": "LINK_PARSE_ERROR",
                            "message": str(e),
                            "timestamp": datetime.now().isoformat(),
                        }
                    )

            logs.append(
                {
                    "url": url,
                    "status": 200,
                    "timestamp": datetime.now().isoformat(),
                    "anchors_found": len(anchors),
                    "pdf_links_found": pdf_links,
                    "queued_links_added": queued_links,
                    "href_samples": href_samples,
                }
            )


        
        except Exception as e:


            logs.append({
                "url": url,
                "status": "ERROR",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            })

    client.upload_json(
        bucket_name="data-lake",
        object_name=(
            f"raw/logs/uca/"
            f"crawl_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        ),
        data=logs
    )

    print(
        f"{len(visited)} pages crawled"
    )


if __name__ == "__main__":
    crawl_uca()
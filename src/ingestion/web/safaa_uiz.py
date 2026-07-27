# src/ingestion/web/safaa_uiz.py
"""
UIZ WEB SCRAPER - SAFAA

Same general structure as Sara's scraper:
- SITES_CONFIG
- safe_request with retry logic
- save raw HTML in raw-web-html
- save images in raw-images
- save documents in raw-documents
- save structured news/faculty in raw-json
- save stats/logs in raw-logs

Adapted to selected UIZ faculty/school websites.
No course_catalog here.
"""

import os
import re
import json
import hashlib
import time
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup
from src.storage.minio.safaa_client import MinIOClient

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ==============================================================
# CONFIGURATION
# ==============================================================

IMAGE_CACHE_FILE = "uiz_image_cache.json"


def load_image_cache():
    try:
        if os.path.exists(IMAGE_CACHE_FILE):
            with open(IMAGE_CACHE_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
    except Exception:
        pass
    return set()


def save_image_cache(cache_set):
    try:
        with open(IMAGE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(list(cache_set), f)
    except Exception:
        pass


_image_cache = load_image_cache()


SITES_CONFIG = {
    "estl_laayoune": {
        "name": "EST Laayoune",
        "homepage": "https://w2.estl.ac.ma/",
        "news_urls": [],
        "faculty_urls": [],
        "scrape_news": True,
        "scrape_faculty": True
    },

    "fpt_taroudant": {
        "name": "FPT Taroudant",
        "homepage": "https://www.fpt.ac.ma/",
        "news_urls": [],
        "faculty_urls": [],
        "scrape_news": True,
        "scrape_faculty": True
    },

    "encg_agadir": {
        "name": "ENCG Agadir",
        "homepage": "https://encga.uiz.ac.ma/",
        "news_urls": [
            "https://encga.uiz.ac.ma/?cat=4"
        ],
        "faculty_urls": [
            "https://encga.uiz.ac.ma/?page_id=3898",
            "https://encga.uiz.ac.ma/?page_id=3900",
            "https://encga.uiz.ac.ma/?page_id=3902",
            "https://encga.uiz.ac.ma/?page_id=3904"
        ],
        "scrape_news": True,
        "scrape_faculty": True
    },

    "esta_agadir": {
        "name": "EST Agadir",
        "homepage": "https://www.esta.ac.ma/",
        "news_urls": [
            "https://www.esta.ac.ma/"
        ],
        "faculty_urls": [
            "https://www.esta.ac.ma/?page_id=328",
            "https://www.esta.ac.ma/?page_id=135",
            "https://www.esta.ac.ma/?page_id=326"
        ],
        "scrape_news": True,
        "scrape_faculty": True
    },

    "fsjes_agadir": {
        "name": "FSJES Agadir",
        "homepage": "https://fsjes-agadir.uiz.ac.ma/",
        "news_urls": [],
        "faculty_urls": [],
        "scrape_news": True,
        "scrape_faculty": True
    },

    "fmpa_agadir": {
        "name": "FMPA Agadir",
        "homepage": "https://fmpa.uiz.ac.ma/",
        "news_urls": [],
        "faculty_urls": [],
        "scrape_news": True,
        "scrape_faculty": True
    },

    "fpo_ouarzazate": {
        "name": "FPO Ouarzazate",
        "homepage": "https://fpo.uiz.ac.ma/",
        "news_urls": [],
        "faculty_urls": [],
        "scrape_news": True,
        "scrape_faculty": True
    },

    "fegg_guelmim": {
        "name": "FEGG Guelmim",
        "homepage": "https://fegg.uiz.ac.ma/",
        "news_urls": [],
        "faculty_urls": [],
        "scrape_news": True,
        "scrape_faculty": True
    }
}

SCRAPER_CONFIG = {
    "timeout": 45,
    "retry_attempts": 3,
    "retry_delay": 2,
    "request_delay": 1.0,
    "max_news_pages": 10,
    "verify_ssl": False,
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


DOC_EXTENSIONS = (
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".ppt", ".pptx", ".csv", ".json", ".zip", ".rar", ".txt"
)


# ==============================================================
# UTILS
# ==============================================================

def generate_record_id(source_system: str, source_url: str, data: dict) -> str:
    content_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
    hash_obj = hashlib.sha256(content_str.encode("utf-8"))
    return f"{source_system}_{hash_obj.hexdigest()[:16]}"


def generate_content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()


def normalize_url(url: str) -> str:
    if not url:
        return ""
    clean_url, _ = urldefrag(url)
    return clean_url.rstrip("/")


def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def get_date_partition() -> dict:
    now = datetime.now()
    return {
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
        "timestamp": now.strftime("%Y%m%d_%H%M%S"),
        "iso": now.isoformat()
    }


def create_common_fields(source_system: str, source_url: str, data: dict) -> dict:
    clean_data = {
        k: v for k, v in data.items()
        if k not in ["record_id", "source_system", "source_url"]
    }

    content_json = json.dumps(clean_data, sort_keys=True, ensure_ascii=False)
    json_ld = data.get("json_ld") if "json_ld" in data else None

    result = {
        "record_id": generate_record_id(source_system, source_url, clean_data),
        "source_system": source_system,
        "source_url": source_url,
        "content_hash": hashlib.sha256(content_json.encode("utf-8")).hexdigest(),
        "crawl_timestamp": datetime.now().isoformat(),
        "business_timestamp": datetime.now().isoformat(),
        "is_deleted": False,
        "language": "fr",
        "normalized_text": "",
        **data
    }

    if json_ld:
        result["json_ld"] = json_ld

    return result


def create_session() -> requests.Session:
    session = requests.Session()
    session.verify = SCRAPER_CONFIG["verify_ssl"]
    session.headers.update({
        "User-Agent": SCRAPER_CONFIG["user_agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    return session


def safe_request(session: requests.Session, url: str):
    if not url or not url.startswith(("http://", "https://")):
        logger.warning(f"URL invalide: {url}")
        return None

    max_retries = SCRAPER_CONFIG["retry_attempts"]
    retry_delay = SCRAPER_CONFIG["retry_delay"]
    timeout = SCRAPER_CONFIG["timeout"]

    for attempt in range(max_retries):
        try:
            response = session.get(
                url,
                timeout=timeout,
                verify=False,
                allow_redirects=True
            )
            response.raise_for_status()
            return response

        except requests.exceptions.SSLError:
            logger.warning(f"SSL Error tentative {attempt + 1}/{max_retries}: {url}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            return None

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            logger.warning(f"Erreur connexion tentative {attempt + 1}/{max_retries}: {url}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            return None

        except Exception as e:
            logger.error(f"Erreur inattendue: {url} - {e}")
            return None

    return None


def is_valid_url(url: str) -> bool:
    if not url:
        return False
    pattern = r"^https?://[^\s/$.?#].[^\s]*$"
    return bool(re.match(pattern, url))


def validate_email(email: str) -> bool:
    if not email:
        return False
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return bool(re.match(pattern, email))


def same_domain(url: str, homepage: str) -> bool:
    try:
        url_domain = urlparse(url).netloc.lower()
        home_domain = urlparse(homepage).netloc.lower()
        return url_domain == home_domain or url_domain.endswith("." + home_domain)
    except Exception:
        return False


def split_name(full_name: str) -> tuple:
    full_name = clean_text(full_name)
    full_name = re.sub(r"^(Pr|Prof\.?|Dr|Mme|M\.|Mr|Mlle)\s+", "", full_name, flags=re.I).strip()

    parts = full_name.split()

    if len(parts) >= 2:
        if parts[0].isupper():
            last_name = parts[0]
            first_name = " ".join(parts[1:])
        else:
            first_name = parts[0]
            last_name = " ".join(parts[1:])
    elif len(parts) == 1:
        first_name = parts[0]
        last_name = ""
    else:
        first_name = ""
        last_name = ""

    return first_name, last_name


def is_probable_person_name(text: str) -> bool:
    text = clean_text(text)

    if not text or len(text) < 5:
        return False

    lowered = text.lower()

    forbidden = [
        "home", "accueil", "actualités", "actualites", "formation",
        "contact", "copyright", "email", "téléphone", "telephone",
        "département", "departement", "enseignants", "chef de département",
        "vocation", "retour", "plus", "read more", "menu"
    ]

    if any(word in lowered for word in forbidden):
        return False

    if re.search(r"\d", text):
        return False

    words = text.split()

    if len(words) < 2 or len(words) > 5:
        return False

    alpha_words = [w for w in words if re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", w)]

    return len(alpha_words) >= 2


# ==============================================================
# MINIO SAVE FUNCTIONS
# ==============================================================

def save_raw_html(source_name: str, url: str, html_content: str, page_type: str) -> None:
    try:
        client = MinIOClient()
        partition = get_date_partition()
        timestamp = partition["timestamp"]

        url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()[:8]
        file_name = f"{source_name}_{page_type}_{url_hash}_{timestamp}.html"

        object_path = (
            f"source={source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"{file_name}"
        )

        client.upload_binary(
            bucket_name="raw-web-html",
            object_name=object_path,
            data=html_content.encode("utf-8", errors="ignore"),
            content_type="text/html"
        )

        soup = BeautifulSoup(html_content, "html.parser")
        page_title = soup.find("title")
        page_title = page_title.get_text(strip=True) if page_title else ""

        json_ld_data = []
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            if script.string:
                try:
                    json_ld_data.append(json.loads(script.string))
                except Exception:
                    pass

        metadata = {
            "source_url": url,
            "source_name": source_name,
            "page_type": page_type,
            "page_title": page_title,
            "timestamp": partition["iso"],
            "file_name": file_name,
            "content_hash": generate_content_hash(html_content),
            "size_bytes": len(html_content.encode("utf-8", errors="ignore")),
            "raw_storage_path": f"s3://raw-web-html/{object_path}",
            "json_ld": json_ld_data if json_ld_data else None
        }

        metadata_path = (
            f"source={source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"metadata_{page_type}_{timestamp}.json"
        )

        client.upload_json(
            bucket_name="raw-web-html",
            object_name=metadata_path,
            data=metadata
        )

    except Exception as e:
        logger.error(f"Erreur sauvegarde HTML: {e}")


def save_image(image_url: str, source_name: str, image_name: str = None) -> bool:
    global _image_cache

    if not is_valid_url(image_url):
        return False

    try:
        client = MinIOClient()
        partition = get_date_partition()

        session = create_session()
        response = safe_request(session, image_url)

        if not response:
            return False

        content_hash = hashlib.md5(response.content).hexdigest()
        cache_key = f"{source_name}_{content_hash}"

        if cache_key in _image_cache:
            return True

        content_type = response.headers.get("content-type", "image/jpeg").lower()

        ext = ".jpg"
        if "png" in content_type:
            ext = ".png"
        elif "gif" in content_type:
            ext = ".gif"
        elif "webp" in content_type:
            ext = ".webp"
        elif "svg" in content_type:
            ext = ".svg"

        if image_name:
            base_name = os.path.splitext(image_name)[0]
            file_name = f"{base_name}_{content_hash[:8]}{ext}"
        else:
            file_name = f"image_{content_hash}{ext}"

        object_path = (
            f"source={source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"{file_name}"
        )

        client.upload_binary(
            bucket_name="raw-images",
            object_name=object_path,
            data=response.content,
            content_type=content_type
        )

        metadata = {
            "source_url": image_url,
            "source_name": source_name,
            "timestamp": partition["iso"],
            "file_name": file_name,
            "content_hash": content_hash,
            "sha256_hash": hashlib.sha256(response.content).hexdigest(),
            "size_bytes": len(response.content),
            "content_type": content_type,
            "original_url": image_url,
            "raw_storage_path": f"s3://raw-images/{object_path}"
        }

        metadata_path = (
            f"source={source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"image_metadata_{partition['timestamp']}_{content_hash[:8]}.json"
        )

        client.upload_json(
            bucket_name="raw-images",
            object_name=metadata_path,
            data=metadata
        )

        _image_cache.add(cache_key)
        save_image_cache(_image_cache)

        return True

    except Exception as e:
        logger.debug(f"Erreur sauvegarde image {image_url}: {e}")
        return False


def save_document(document_url: str, source_name: str, document_name: str = None) -> bool:
    if not is_valid_url(document_url):
        return False

    try:
        client = MinIOClient()
        partition = get_date_partition()
        timestamp = partition["timestamp"]

        session = create_session()
        response = safe_request(session, document_url)

        if not response:
            return False

        content_type = response.headers.get("content-type", "").lower()
        url_lower = document_url.lower()

        extension_map = {
            "pdf": ".pdf",
            "doc": ".doc",
            "docx": ".docx",
            "xls": ".xls",
            "xlsx": ".xlsx",
            "ppt": ".ppt",
            "pptx": ".pptx",
            "csv": ".csv",
            "json": ".json",
            "zip": ".zip",
            "rar": ".rar",
            "txt": ".txt"
        }

        ext = ".pdf"
        for key, value in extension_map.items():
            if key in url_lower or key in content_type:
                ext = value
                break

        if document_name:
            file_name = document_name if document_name.endswith(ext) else f"{document_name}{ext}"
        else:
            base_name = document_url.split("/")[-1].split("?")[0]
            if base_name and "." in base_name:
                file_name = base_name
            else:
                file_name = f"doc_{hashlib.md5(document_url.encode('utf-8')).hexdigest()[:8]}_{timestamp}{ext}"

        object_path = (
            f"source={source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"documents/{file_name}"
        )

        client.upload_binary(
            bucket_name="raw-documents",
            object_name=object_path,
            data=response.content,
            content_type=content_type or "application/octet-stream"
        )

        metadata = {
            "source_url": document_url,
            "source_name": source_name,
            "timestamp": partition["iso"],
            "file_name": file_name,
            "content_hash": hashlib.sha256(response.content).hexdigest(),
            "size_bytes": len(response.content),
            "content_type": content_type,
            "extension": ext,
            "raw_storage_path": f"s3://raw-documents/{object_path}"
        }

        metadata_path = (
            f"source={source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"documents/metadata_{timestamp}_{hashlib.md5(document_url.encode('utf-8')).hexdigest()[:8]}.json"
        )

        client.upload_json(
            bucket_name="raw-documents",
            object_name=metadata_path,
            data=metadata
        )

        return True

    except Exception as e:
        logger.error(f"Erreur sauvegarde document {document_url}: {e}")
        return False


def save_structured_data(source_name: str, data_type: str, data_list: list) -> int:
    if not data_list:
        return 0

    try:
        client = MinIOClient()
        partition = get_date_partition()
        timestamp = partition["timestamp"]

        unique_data = []
        seen = set()
        valid_count = 0
        invalid_count = 0

        for item in data_list:
            cleaned_item = {k: v for k, v in item.items() if v is not None}

            if data_type == "faculty":
                if (
                    not cleaned_item.get("first_name")
                    and not cleaned_item.get("last_name")
                    and not cleaned_item.get("full_name")
                ):
                    invalid_count += 1
                    continue

                valid_count += 1
                key = (
                    f"{cleaned_item.get('first_name', '')}_"
                    f"{cleaned_item.get('last_name', '')}_"
                    f"{cleaned_item.get('full_name', '')}_"
                    f"{cleaned_item.get('email', '')}_"
                    f"{cleaned_item.get('institution', '')}"
                )

            else:
                if not cleaned_item.get("title"):
                    invalid_count += 1
                    continue

                valid_count += 1
                key = f"{cleaned_item.get('title', '')}_{cleaned_item.get('source', '')}"

            if key not in seen:
                seen.add(key)
                item_with_metadata = create_common_fields(
                    source_system=f"{data_type}_scraper",
                    source_url=cleaned_item.get("url", cleaned_item.get("source_url", "")),
                    data=cleaned_item
                )
                unique_data.append(item_with_metadata)

        if not unique_data:
            logger.warning(f"Aucune donnée valide pour {source_name} - {data_type}")
            return 0

        data_payload = {
            "source": source_name,
            "table_type": f"university_{data_type}",
            "scrape_timestamp": partition["iso"],
            "validation_stats": {
                "total": len(data_list),
                "valid": valid_count,
                "invalid": invalid_count,
                "unique": len(unique_data)
            },
            f"total_{data_type}": len(unique_data),
            f"{data_type}_items": unique_data
        }

        object_path = (
            f"source={source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"{data_type}_{timestamp}.json"
        )

        client.upload_json(
            bucket_name="raw-json",
            object_name=object_path,
            data=data_payload
        )

        logger.info(f"{len(unique_data)} {data_type} sauvegardés pour {source_name}")
        return len(unique_data)

    except Exception as e:
        logger.error(f"Erreur sauvegarde {data_type} pour {source_name}: {e}")
        return 0


def save_consolidated_data(all_news: list, all_faculty: list) -> None:
    try:
        client = MinIOClient()
        partition = get_date_partition()
        timestamp = partition["timestamp"]

        if all_news:
            unique_news = []
            seen = set()

            for news in all_news:
                key = f"{news.get('title', '')}_{news.get('source', '')}"

                if key not in seen:
                    seen.add(key)
                    news_with_metadata = create_common_fields(
                        source_system="news_web_scraper",
                        source_url=news.get("url", ""),
                        data=news
                    )
                    unique_news.append(news_with_metadata)

            news_data = {
                "source": "uiz_selected_institutions",
                "table_type": "university_news",
                "scrape_timestamp": partition["iso"],
                "total_news": len(unique_news),
                "news_items": unique_news
            }

            object_path = (
                f"uiz_selected_institutions/"
                f"year={partition['year']}/"
                f"month={partition['month']}/"
                f"day={partition['day']}/"
                f"university_news_{timestamp}.json"
            )

            client.upload_json(
                bucket_name="raw-json",
                object_name=object_path,
                data=news_data
            )

            logger.info(f"{len(unique_news)} actualités consolidées sauvegardées")

        if all_faculty:
            unique_faculty = []
            seen = set()

            for faculty in all_faculty:
                cleaned = {k: v for k, v in faculty.items() if v is not None}
                key = (
                    f"{cleaned.get('first_name', '')}_"
                    f"{cleaned.get('last_name', '')}_"
                    f"{cleaned.get('full_name', '')}_"
                    f"{cleaned.get('email', '')}_"
                    f"{cleaned.get('institution', '')}"
                )

                if key not in seen:
                    seen.add(key)
                    faculty_with_metadata = create_common_fields(
                        source_system="faculty_web_scraper",
                        source_url=cleaned.get("source_url", ""),
                        data=cleaned
                    )
                    unique_faculty.append(faculty_with_metadata)

            faculty_data = {
                "source": "uiz_selected_institutions",
                "table_type": "faculty_profiles",
                "scrape_timestamp": partition["iso"],
                "total_faculty": len(unique_faculty),
                "faculty_members": unique_faculty
            }

            object_path = (
                f"faculty_profiles/"
                f"year={partition['year']}/"
                f"month={partition['month']}/"
                f"day={partition['day']}/"
                f"faculty_profiles_{timestamp}.json"
            )

            client.upload_json(
                bucket_name="raw-json",
                object_name=object_path,
                data=faculty_data
            )

            logger.info(f"{len(unique_faculty)} profils enseignants consolidés sauvegardés")

    except Exception as e:
        logger.error(f"Erreur sauvegarde consolidée: {e}")


def save_stats(source_name: str, stats: dict) -> None:
    try:
        client = MinIOClient()
        partition = get_date_partition()
        timestamp = partition["timestamp"]

        stats_report = {
            "source": source_name,
            "timestamp": partition["iso"],
            "stats": stats,
            "summary": {
                "total_pages": stats.get("pages_visited", 0),
                "total_news": stats.get("news_found", 0),
                "total_faculty": stats.get("faculty_found", 0),
                "total_images": stats.get("images_saved", 0),
                "total_documents": stats.get("documents_saved", 0),
                "total_errors": stats.get("errors", 0),
                "success_rate": (
                    f"{(1 - stats.get('errors', 0) / max(stats.get('pages_visited', 1), 1)) * 100:.2f}%"
                )
            }
        }

        object_path = (
            f"source={source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"stats_{timestamp}.json"
        )

        client.upload_json(
            bucket_name="raw-logs",
            object_name=object_path,
            data=stats_report
        )

    except Exception as e:
        logger.error(f"Erreur sauvegarde stats: {e}")


# ==============================================================
# DISCOVERY
# ==============================================================

def discover_urls_from_homepage(homepage_url: str, session: requests.Session) -> dict:
    discovered = {
        "news_urls": [],
        "faculty_urls": []
    }

    try:
        response = safe_request(session, homepage_url)
        if not response:
            return discovered

        soup = BeautifulSoup(response.text, "html.parser")

        news_keywords = [
            "actualités", "actualites", "actualité", "actualite",
            "actus", "news", "annonces", "avis", "événements",
            "evenements", "communiqué", "communique"
        ]

        faculty_keywords = [
            "enseignants", "enseignant", "professeurs",
            "professoral", "corps professoral", "faculty",
            "annuaire", "staff", "équipe pédagogique",
            "equipe pedagogique"
        ]

        for link in soup.find_all("a", href=True):
            text = clean_text(link.get_text()).lower()
            href = link.get("href", "")

            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            full_url = normalize_url(urljoin(homepage_url, href))

            if not same_domain(full_url, homepage_url):
                continue

            lowered = f"{text} {href.lower()}"

            if any(keyword in lowered for keyword in news_keywords):
                discovered["news_urls"].append(full_url)

            if any(keyword in lowered for keyword in faculty_keywords):
                discovered["faculty_urls"].append(full_url)

        for key in discovered:
            discovered[key] = list(dict.fromkeys(discovered[key]))[:SCRAPER_CONFIG["max_news_pages"]]

    except Exception as e:
        logger.error(f"Erreur découverte URLs: {e}")

    return discovered


# ==============================================================
# SCRAPERS
# ==============================================================

def detect_documents_and_images(container, base_url: str, source_name: str) -> tuple:
    document_urls = []
    image_urls = []

    for link in container.find_all("a", href=True):
        href = link.get("href", "")
        full_url = normalize_url(urljoin(base_url, href))

        if full_url.lower().endswith(DOC_EXTENSIONS):
            document_urls.append(full_url)
            save_document(full_url, source_name)

    image_count = 0

    for img in container.find_all("img"):
        if image_count >= 2:
            break

        img_src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
        if not img_src:
            continue

        full_img_url = normalize_url(urljoin(base_url, img_src))

        if is_valid_url(full_img_url):
            image_urls.append(full_img_url)
            save_image(
                full_img_url,
                source_name,
                f"{source_name}_image_{hashlib.md5(full_img_url.encode('utf-8')).hexdigest()[:8]}"
            )
            image_count += 1

    return list(dict.fromkeys(document_urls)), list(dict.fromkeys(image_urls))


def extract_date_from_text(text: str) -> str:
    if not text:
        return ""

    patterns = [
        r"(\d{4})-(\d{2})-(\d{2})",
        r"(\d{2})/(\d{2})/(\d{4})",
        r"(\d{2})-(\d{2})-(\d{4})"
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            groups = match.groups()

            if len(groups[0]) == 4:
                year, month, day = groups
            else:
                day, month, year = groups

            return f"{year}-{month}-{day}"

    return ""


def classify_news_category(title: str, url: str) -> str:
    text = f"{title} {url}".lower()

    if any(k in text for k in ["concours", "candidature", "appel", "admissibles", "admis"]):
        return "Appels / Concours"

    if any(k in text for k in ["avis", "communiqué", "communique", "annonce"]):
        return "Avis / Annonces"

    if any(k in text for k in ["événement", "evenement", "event", "colloque", "conférence", "conference"]):
        return "Événements"

    if any(k in text for k in ["examen", "résultat", "resultat", "calendrier", "planning"]):
        return "Examens / Résultats"

    return "Actualités"


def scrape_generic_news(source_name: str, site_config: dict, urls: list, session: requests.Session) -> list:
    logger.info(f"  Scraping actualités for {site_config['name']}")
    news_list = []

    for url in urls[:SCRAPER_CONFIG["max_news_pages"]]:
        response = safe_request(session, url)
        if not response:
            continue

        save_raw_html(source_name, url, response.text, "news")
        soup = BeautifulSoup(response.text, "html.parser")

        articles = soup.find_all("article")

        if not articles:
            articles = soup.find_all(
                "div",
                class_=re.compile(r"(post|article|actualit|news|annonce|avis|event|entry|views-row|item)", re.I)
            )

        if not articles:
            articles = soup.find_all("li")

        for article in articles:
            link = article.find("a", href=True)
            if not link:
                continue

            title = clean_text(link.get_text())
            href = link.get("href", "")
            article_url = normalize_url(urljoin(url, href))

            if len(title) < 8:
                heading = article.find(["h1", "h2", "h3", "h4"])
                if heading:
                    title = clean_text(heading.get_text())

            if not title or len(title) < 8:
                continue

            if not same_domain(article_url, site_config["homepage"]):
                continue

            article_text = clean_text(article.get_text(" "))
            publication_date = ""

            time_elem = article.find("time")
            if time_elem:
                publication_date = time_elem.get("datetime") or clean_text(time_elem.get_text())

            if not publication_date:
                publication_date = extract_date_from_text(article_text)

            document_urls, image_urls = detect_documents_and_images(article, url, source_name)

            news_list.append({
                "title": title,
                "url": article_url,
                "publication_date": publication_date,
                "image_url": image_urls[0] if image_urls else "",
                "category": classify_news_category(title, article_url),
                "source": site_config["name"],
                "institution": site_config["name"],
                "documents": document_urls,
                "source_url": url
            })

        time.sleep(SCRAPER_CONFIG["request_delay"])

    unique_news = []
    seen = set()

    for news in news_list:
        key = f"{news.get('title', '')}_{news.get('url', '')}"

        if key not in seen:
            seen.add(key)
            unique_news.append(news)

    logger.info(f"      {len(unique_news)} actualités trouvées")
    return unique_news


def extract_names_after_heading(soup: BeautifulSoup, url: str, site_config: dict, source_name: str) -> list:
    faculty_list = []

    page_text = soup.get_text("\n")
    lines = [clean_text(line) for line in page_text.split("\n") if clean_text(line)]

    capture = False
    department = ""

    for line in lines:
        upper_line = line.upper()

        if "DÉPARTEMENT" in upper_line or "DEPARTEMENT" in upper_line:
            if len(line) < 80:
                department = line

        if "ENSEIGNANTS DU DÉPARTEMENT" in upper_line or "ENSEIGNANTS DU DEPARTEMENT" in upper_line:
            capture = True
            continue

        if capture:
            stop_markers = [
                "Ecole Nationale", "Contact info", "Copyright",
                "Retour en haut", "Home", "Actualités",
                "Main Menu", "Formation", "Recherche"
            ]

            if any(marker.lower() in line.lower() for marker in stop_markers):
                capture = False
                continue

            if is_probable_person_name(line):
                first_name, last_name = split_name(line)
                faculty_list.append({
                    "full_name": line,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": "",
                    "department": department,
                    "source_url": url,
                    "institution": site_config["name"],
                    "source": source_name
                })

    return faculty_list


def scrape_generic_faculty(source_name: str, site_config: dict, urls: list, session: requests.Session) -> list:
    logger.info(f"  Scraping faculty for {site_config['name']}")
    faculty_list = []

    email_pattern = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")

    for url in urls[:SCRAPER_CONFIG["max_news_pages"]]:
        response = safe_request(session, url)
        if not response:
            continue

        save_raw_html(source_name, url, response.text, "faculty")
        soup = BeautifulSoup(response.text, "html.parser")

        # Method 1: mailto links
        for mail_link in soup.find_all("a", href=re.compile(r"mailto:", re.I)):
            email = mail_link.get("href", "").replace("mailto:", "").strip()
            email = email.split("?")[0].strip()

            parent = mail_link.find_parent(["tr", "li", "div", "p"])
            parent_text = clean_text(parent.get_text(" ")) if parent else clean_text(mail_link.get_text())

            name_text = parent_text.replace(email, "").strip()
            first_name, last_name = split_name(name_text)
            full_name = clean_text(f"{first_name} {last_name}")

            if full_name or email:
                faculty_list.append({
                    "full_name": full_name,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email if validate_email(email) else "",
                    "department": "",
                    "source_url": url,
                    "institution": site_config["name"],
                    "source": source_name
                })

        # Method 2: tables with emails
        for table in soup.find_all("table"):
            rows = table.find_all("tr")

            for row in rows:
                row_text = clean_text(row.get_text(" "))
                email_match = email_pattern.search(row_text)

                if email_match:
                    email = email_match.group()
                    name_text = row_text[:email_match.start()].strip()

                    first_name, last_name = split_name(name_text)
                    full_name = clean_text(f"{first_name} {last_name}")

                    if full_name:
                        faculty_list.append({
                            "full_name": full_name,
                            "first_name": first_name,
                            "last_name": last_name,
                            "email": email if validate_email(email) else "",
                            "department": "",
                            "source_url": url,
                            "institution": site_config["name"],
                            "source": source_name
                        })

        # Method 3: tables without emails
        for table in soup.find_all("table"):
            rows = table.find_all("tr")

            for row in rows:
                cells = row.find_all(["td", "th"])
                cell_texts = [clean_text(cell.get_text(" ")) for cell in cells if clean_text(cell.get_text(" "))]

                if not cell_texts:
                    continue

                possible_name = cell_texts[0]

                if is_probable_person_name(possible_name):
                    first_name, last_name = split_name(possible_name)
                    specialty = cell_texts[1] if len(cell_texts) > 1 else ""

                    faculty_list.append({
                        "full_name": possible_name,
                        "first_name": first_name,
                        "last_name": last_name,
                        "email": "",
                        "department": specialty,
                        "source_url": url,
                        "institution": site_config["name"],
                        "source": source_name
                    })

        # Method 4: profile blocks
        profile_blocks = soup.find_all(
            ["div", "li", "article"],
            class_=re.compile(r"(teacher|enseignant|prof|faculty|staff|team|member|person|profile|enseignants)", re.I)
        )

        for block in profile_blocks:
            text = clean_text(block.get_text(" "))
            if len(text) < 5:
                continue

            email_match = email_pattern.search(text)
            email = email_match.group() if email_match else ""

            heading = block.find(["h2", "h3", "h4", "strong"])
            name_text = clean_text(heading.get_text()) if heading else ""

            if not name_text and email_match:
                name_text = text[:email_match.start()].strip()

            if not name_text:
                continue

            if not is_probable_person_name(name_text):
                continue

            first_name, last_name = split_name(name_text)
            full_name = clean_text(f"{first_name} {last_name}")

            if full_name:
                faculty_list.append({
                    "full_name": full_name,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email if validate_email(email) else "",
                    "department": "",
                    "source_url": url,
                    "institution": site_config["name"],
                    "source": source_name
                })

        # Method 5: ENCG-like department lists
        faculty_list.extend(
            extract_names_after_heading(
                soup=soup,
                url=url,
                site_config=site_config,
                source_name=source_name
            )
        )

        time.sleep(SCRAPER_CONFIG["request_delay"])

    unique_faculty = []
    seen = set()

    for faculty in faculty_list:
        key = (
            f"{faculty.get('full_name', '')}_"
            f"{faculty.get('email', '')}_"
            f"{faculty.get('institution', '')}"
        )

        if key not in seen and faculty.get("full_name"):
            seen.add(key)
            unique_faculty.append(faculty)

    logger.info(f"      {len(unique_faculty)} professeurs trouvés")
    return unique_faculty


# ==============================================================
# MAIN
# ==============================================================

def run():
    session = create_session()
    partition = get_date_partition()

    logger.info("=" * 70)
    logger.info("UIZ WEB SCRAPER - SAFAA")
    logger.info("=" * 70)
    logger.info(f"Date: {partition['year']}-{partition['month']}-{partition['day']}")
    logger.info(f"Heure: {partition['timestamp']}")
    logger.info("=" * 70)

    all_news = []
    all_faculty = []
    all_stats = {}

    for site_key, site_config in SITES_CONFIG.items():
        logger.info("\n" + "=" * 60)
        logger.info(f"{site_config['name']} ({site_key})")
        logger.info("=" * 60)

        stats = {
            "pages_visited": 0,
            "news_found": 0,
            "faculty_found": 0,
            "images_saved": 0,
            "documents_saved": 0,
            "errors": 0
        }

        source_news = []
        source_faculty = []

        # 1. Homepage
        logger.info("\n  Page d'accueil...")
        homepage_response = safe_request(session, site_config["homepage"])

        if homepage_response:
            save_raw_html(site_key, site_config["homepage"], homepage_response.text, "homepage")
            stats["pages_visited"] += 1
            logger.info("      Page d'accueil sauvegardée")

            discovered = discover_urls_from_homepage(site_config["homepage"], session)

            if discovered.get("news_urls"):
                site_config["news_urls"] = list(
                    dict.fromkeys(site_config.get("news_urls", []) + discovered["news_urls"])
                )[:SCRAPER_CONFIG["max_news_pages"]]

            if discovered.get("faculty_urls"):
                site_config["faculty_urls"] = list(
                    dict.fromkeys(site_config.get("faculty_urls", []) + discovered["faculty_urls"])
                )[:SCRAPER_CONFIG["max_news_pages"]]

        else:
            logger.error("      Impossible de lire la page d'accueil")
            stats["errors"] += 1

        # 2. News
        if site_config.get("scrape_news", True):
            logger.info("\n  Scraping actualités...")

            try:
                news_urls = site_config.get("news_urls", [])

                if not news_urls:
                    news_urls = [site_config["homepage"]]

                source_news = scrape_generic_news(site_key, site_config, news_urls, session)

                if source_news:
                    save_structured_data(site_key, "news", source_news)
                    all_news.extend(source_news)
                    stats["news_found"] = len(source_news)

            except Exception as e:
                logger.error(f"      Erreur scraping news: {e}")
                stats["errors"] += 1

        # 3. Faculty
        if site_config.get("scrape_faculty", True):
            logger.info("\n  Scraping faculty...")

            try:
                faculty_urls = site_config.get("faculty_urls", [])

                if faculty_urls:
                    source_faculty = scrape_generic_faculty(site_key, site_config, faculty_urls, session)
                else:
                    logger.warning("      Aucun lien faculty trouvé depuis homepage")

                if source_faculty:
                    save_structured_data(site_key, "faculty", source_faculty)
                    all_faculty.extend(source_faculty)
                    stats["faculty_found"] = len(source_faculty)

            except Exception as e:
                logger.error(f"      Erreur scraping faculty: {e}")
                stats["errors"] += 1

        # 4. Stats
        save_stats(site_key, stats)
        all_stats[site_key] = stats

        logger.info(f"\n  {site_config['name']} terminé:")
        logger.info(f"     - News: {len(source_news)}")
        logger.info(f"     - Faculty: {len(source_faculty)}")
        logger.info(f"     - Erreurs: {stats['errors']}")

    # 5. Consolidation
    logger.info("\n" + "=" * 70)
    logger.info("SAUVEGARDE CONSOLIDÉE")
    logger.info("=" * 70)

    save_consolidated_data(all_news, all_faculty)

    # 6. Final summary
    logger.info("\n" + "=" * 70)
    logger.info("RÉSUMÉ FINAL - UIZ WEB SCRAPER")
    logger.info("=" * 70)

    total_news = len(all_news)
    total_faculty = len(all_faculty)
    total_errors = sum(s.get("errors", 0) for s in all_stats.values())

    logger.info(f"Total sites: {len(all_stats)}")
    logger.info(f"Total actualités: {total_news}")
    logger.info(f"Total enseignants: {total_faculty}")
    logger.info(f"Total erreurs: {total_errors}")

    logger.info("\nDétail par source:")
    for site_key, stats in all_stats.items():
        name = SITES_CONFIG[site_key]["name"]
        logger.info(
            f"  {name}: "
            f"{stats['news_found']} news, "
            f"{stats['faculty_found']} faculty"
        )

    logger.info("\nStructure MinIO:")
    logger.info("  raw-web-html/     -> HTML brut des pages")
    logger.info("  raw-images/       -> Images extraites")
    logger.info("  raw-documents/    -> Documents PDF/DOC/XLS/CSV")
    logger.info("  raw-json/         -> Données structurées")
    logger.info("     source=*/news_*.json")
    logger.info("     source=*/faculty_*.json")
    logger.info("     uiz_selected_institutions/university_news_*.json")
    logger.info("     faculty_profiles/faculty_profiles_*.json")
    logger.info("  raw-logs/         -> Logs et statistiques")
    logger.info("=" * 70)

    logger.info(f"\nCache des images: {len(_image_cache)} entrées")
    logger.info(f"Fichier cache: {IMAGE_CACHE_FILE}")

    logger.info("\nUIZ WEB SCRAPING TERMINÉ")


if __name__ == "__main__":
    run()
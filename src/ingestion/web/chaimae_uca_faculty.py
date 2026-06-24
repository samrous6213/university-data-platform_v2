# src/ingestion/web/scraper_marrakech_enhanced.py
"""
SCRAPER UNIVERSITAIRE - MARRRAKECH (amélioré)
Fusion du scraping ciblé (news, faculty) et du crawling BFS (exploration de pages, téléchargement d'assets)
Conserve la structure exacte du scraper UM5 original.
"""

import os
import re
import json
import hashlib
import time
import logging
from collections import deque
from datetime import datetime
from urllib.parse import urljoin, urlparse, urldefrag, urlunparse

import requests
from bs4 import BeautifulSoup
from src.storage.minio.chaimae_client import MinIOClient
# ==============================================================
# CONFIGURATION
# ==============================================================

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Cache des images (global, partagé avec toutes les fonctions)
IMAGE_CACHE_FILE = "image_cache_marrakech.json"

def load_image_cache():
    try:
        if os.path.exists(IMAGE_CACHE_FILE):
            with open(IMAGE_CACHE_FILE, 'r') as f:
                return set(json.load(f))
    except:
        pass
    return set()

def save_image_cache(cache_set):
    try:
        with open(IMAGE_CACHE_FILE, 'w') as f:
            json.dump(list(cache_set), f)
    except:
        pass

_image_cache = load_image_cache()

# Configuration des sites (même structure que l'original)
SITES_CONFIG = {
    "fssm": {
        "name": "FSSM Marrakech",
        "homepage": "https://www.uca.ma/fssm/fr",
        "news_url": None,          # Sera découverte automatiquement
        "faculty_url": None,       # Sera découverte automatiquement
        "scrape_news": True,
        "scrape_faculty": True,
        "crawl_mode": True,        # NOUVEAU : activer le crawling BFS pour ce site
        "max_pages": 500,          # Nombre max de pages HTML à explorer
    },
    "ensa": {
        "name": "ENSA Marrakech",
        "homepage": "https://ensa-marrakech.uca.ma",
        "news_url": None,
        "faculty_url": None,
        "scrape_news": True,
        "scrape_faculty": True,
        "crawl_mode": True,
        "max_pages": 300,
    },
    "encg": {
        "name": "ENCG Marrakech",
        "homepage": "https://www.uca.ma/encg/fr",
        "news_url": None,
        "faculty_url": None,
        "scrape_news": True,
        "scrape_faculty": True,
        "crawl_mode": True,
        "max_pages": 300,
    }
}

SCRAPER_CONFIG = {
    "timeout": 45,
    "retry_attempts": 3,
    "retry_delay": 2,
    "request_delay": 1.0,
    "max_news_pages": 10,
    "verify_ssl": False,
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ==============================================================
# FONCTIONS UTILITAIRES (reprises du code original, enrichies)
# ==============================================================

def generate_record_id(source_system: str, source_url: str, data: dict) -> str:
    content_str = json.dumps({k: v for k, v in data.items() if k not in ['record_id', 'source_system', 'source_url']}, sort_keys=True)
    hash_obj = hashlib.sha256(content_str.encode())
    return f"{source_system}_{hash_obj.hexdigest()[:16]}"

def generate_content_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()

def normalize_url(url: str) -> str:
    if not url:
        return ""
    clean_url, _ = urldefrag(url)
    return clean_url.rstrip("/")

def _normalize_url_crawl(url: str) -> str:
    """Normalisation utilisée pour le crawling BFS (supprime fragment et slash final)."""
    parsed = urlparse(url)
    parsed = parsed._replace(fragment="")
    if parsed.path and parsed.path != "/" and parsed.path.endswith("/"):
        parsed = parsed._replace(path=parsed.path.rstrip("/"))
    return urlunparse(parsed)

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
    clean_data = {k: v for k, v in data.items() if k not in ['record_id', 'source_system', 'source_url']}
    content_json = json.dumps(clean_data, sort_keys=True)
    json_ld = data.get('json_ld') if 'json_ld' in data else None
    result = {
        "record_id": generate_record_id(source_system, source_url, clean_data),
        "source_system": source_system,
        "source_url": source_url,
        "content_hash": hashlib.sha256(content_json.encode()).hexdigest(),
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
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    return session

def safe_request(session: requests.Session, url: str) -> requests.Response:
    if not url or not url.startswith(('http://', 'https://')):
        logger.warning(f"URL invalide: {url}")
        return None
    max_retries = SCRAPER_CONFIG["retry_attempts"]
    retry_delay = SCRAPER_CONFIG["retry_delay"]
    timeout = SCRAPER_CONFIG["timeout"]
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=timeout, verify=False, allow_redirects=True)
            response.raise_for_status()
            return response
        except Exception as e:
            logger.warning(f"Erreur (tentative {attempt+1}/{max_retries}): {url} - {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            return None
    return None

def is_valid_url(url: str) -> bool:
    if not url:
        return False
    pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    return bool(re.match(pattern, url))

def validate_email(email: str) -> bool:
    if not email:
        return False
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return bool(re.match(pattern, email))

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def _extract_year_from_url(url: str) -> str:
    for pattern in (r'/(20[1-2][0-9])/', r'[-_](20[1-2][0-9])[-_]'):
        m = re.search(pattern, url)
        if m:
            return m.group(1)
    return str(datetime.now().year)

def _safe_filename(url: str, default_ext: str = "") -> str:
    filename = os.path.basename(urlparse(url).path)
    if not filename:
        filename = _sha256(url.encode("utf-8"))[:15] + default_ext
    return filename

def _partition(faculty: str, url: str, now: datetime) -> str:
    year = _extract_year_from_url(url)
    return (
        f"source={faculty}/"
        f"year={year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}"
    )

# ==============================================================
# FONCTIONS DE SAUVEGARDE MINIO (reprises et enrichies)
# ==============================================================

def save_raw_html(source_name: str, url: str, html_content: str, page_type: str) -> None:
    """Version originale améliorée avec partition par année extraite de l'URL."""
    try:
        client = MinIOClient(endpoint="localhost:9000")
        now = datetime.now()
        partition = _partition(source_name, url, now)
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        file_name = f"{source_name}_{page_type}_{url_hash}_{now.strftime('%Y%m%d_%H%M%S')}.html"
        object_path = f"{partition}/{file_name}"
        client.upload_binary(
            bucket_name="raw-web-html",
            object_name=object_path,
            data=html_content.encode('utf-8'),
            content_type="text/html"
        )
        # Métadonnées
        soup = BeautifulSoup(html_content, 'html.parser')
        page_title = soup.find('title')
        page_title = page_title.get_text(strip=True) if page_title else ""
        json_ld_data = []
        for script in soup.find_all("script", type="application/ld+json"):
            if script.string:
                try:
                    json_ld_data.append(json.loads(script.string))
                except:
                    pass
        metadata = {
            "source_url": url,
            "source_name": source_name,
            "page_type": page_type,
            "page_title": page_title,
            "timestamp": now.isoformat(),
            "file_name": file_name,
            "content_hash": generate_content_hash(html_content),
            "size_bytes": len(html_content),
            "json_ld": json_ld_data if json_ld_data else None
        }
        metadata_path = f"{partition}/metadata_{page_type}_{now.strftime('%Y%m%d_%H%M%S')}.json"
        client.upload_json(
            bucket_name="raw-web-html",
            object_name=metadata_path,
            data=metadata
        )
        logger.debug(f"HTML sauvegardé: {object_path}")
    except Exception as e:
        logger.error(f"Erreur sauvegarde HTML: {e}")

def save_image(image_url: str, source_name: str, image_name: str = None) -> bool:
    """Version enrichie avec cache persistant et gestion d'erreurs."""
    global _image_cache
    if not is_valid_url(image_url):
        return False
    try:
        client = MinIOClient(endpoint="localhost:9000")
        now = datetime.now()
        session = create_session()
        response = safe_request(session, image_url)
        if not response:
            return False
        content_hash = hashlib.md5(response.content).hexdigest()
        cache_key = f"{source_name}_{content_hash}"
        if cache_key in _image_cache:
            logger.debug(f"Image déjà sauvegardée (cache): {image_url}")
            return True
        content_type = response.headers.get('content-type', 'image/jpeg')
        ext = '.jpg'
        if 'png' in content_type:
            ext = '.png'
        elif 'gif' in content_type:
            ext = '.gif'
        elif 'webp' in content_type:
            ext = '.webp'
        elif 'svg' in content_type:
            ext = '.svg'
        elif 'jpeg' in content_type or 'jpg' in content_type:
            ext = '.jpg'
        if image_name:
            base_name = os.path.splitext(image_name)[0]
            file_name = f"{base_name}_{content_hash[:8]}{ext}"
        else:
            file_name = f"image_{content_hash}{ext}"
        partition = _partition(source_name, image_url, now)
        object_path = f"{partition}/{file_name}"
        client.upload_binary(
            bucket_name="raw-images",
            object_name=object_path,
            data=response.content,
            content_type=content_type
        )
        # Métadonnées
        metadata = {
            "source_url": image_url,
            "source_name": source_name,
            "timestamp": now.isoformat(),
            "file_name": file_name,
            "content_hash": content_hash,
            "sha256_hash": hashlib.sha256(response.content).hexdigest(),
            "size_bytes": len(response.content),
            "content_type": content_type,
            "original_url": image_url
        }
        metadata_path = f"{partition}/image_metadata_{now.strftime('%Y%m%d_%H%M%S')}_{content_hash[:8]}.json"
        client.upload_json(
            bucket_name="raw-images",
            object_name=metadata_path,
            data=metadata
        )
        _image_cache.add(cache_key)
        save_image_cache(_image_cache)
        logger.debug(f"Image sauvegardée: {object_path} (hash: {content_hash[:8]})")
        return True
    except Exception as e:
        logger.debug(f"Erreur sauvegarde image {image_url}: {e}")
        return False

def save_document(document_url: str, source_name: str, document_name: str = None) -> bool:
    """Version enrichie pour documents."""
    if not is_valid_url(document_url):
        return False
    try:
        client = MinIOClient(endpoint="localhost:9000")
        now = datetime.now()
        session = create_session()
        response = safe_request(session, document_url)
        if not response:
            return False
        content_type = response.headers.get('content-type', '').lower()
        url_lower = document_url.lower()
        extension_map = {
            'pdf': '.pdf', 'doc': '.doc', 'docx': '.docx',
            'xls': '.xls', 'xlsx': '.xlsx', 'ppt': '.ppt',
            'pptx': '.pptx', 'csv': '.csv', 'json': '.json',
            'zip': '.zip', 'rar': '.rar', 'txt': '.txt'
        }
        ext = '.pdf'
        for key, value in extension_map.items():
            if key in url_lower or key in content_type:
                ext = value
                break
        if document_name:
            file_name = document_name if document_name.endswith(ext) else f"{document_name}{ext}"
        else:
            file_name = _safe_filename(document_url, ext)
        partition = _partition(source_name, document_url, now)
        object_path = f"{partition}/documents/{file_name}"
        client.upload_binary(
            bucket_name="raw-documents",
            object_name=object_path,
            data=response.content,
            content_type=content_type or 'application/octet-stream'
        )
        metadata = {
            "source_url": document_url,
            "source_name": source_name,
            "timestamp": now.isoformat(),
            "file_name": file_name,
            "content_hash": hashlib.sha256(response.content).hexdigest(),
            "size_bytes": len(response.content),
            "content_type": content_type,
            "extension": ext
        }
        metadata_path = f"{partition}/documents/metadata_{now.strftime('%Y%m%d_%H%M%S')}_{hashlib.md5(document_url.encode()).hexdigest()[:8]}.json"
        client.upload_json(
            bucket_name="raw-documents",
            object_name=metadata_path,
            data=metadata
        )
        logger.debug(f"Document sauvegardé: {object_path}")
        return True
    except Exception as e:
        logger.error(f"Erreur sauvegarde document {document_url}: {e}")
        return False

def save_jsonld(source_name: str, url: str, json_data: dict, log_index: int, script_index: int, now: datetime) -> None:
    """Sauvegarde un bloc JSON-LD dans raw-json."""
    try:
        client = MinIOClient(endpoint="localhost:9000")
        partition = _partition(source_name, url, now)
        object_name = f"{partition}/jsonld_{log_index}_{script_index}.json"
        client.upload_json(
            bucket_name="raw-json",
            object_name=object_name,
            data=json_data
        )
    except Exception as e:
        logger.warning(f"Erreur upload JSON-LD: {e}")

def save_logs(source_name: str, logs: list) -> None:
    """Sauvegarde les logs de crawling dans raw-logs."""
    try:
        client = MinIOClient(endpoint="localhost:9000")
        now = datetime.now()
        partition = _partition(source_name, "https://placeholder", now)  # on utilise l'année courante
        object_name = f"{partition}/crawl_{now.strftime('%Y%m%d_%H%M%S')}.json"
        client.upload_json(
            bucket_name="raw-logs",
            object_name=object_name,
            data=logs
        )
    except Exception as e:
        logger.error(f"Erreur sauvegarde logs: {e}")

def save_structured_data(source_name: str, data_type: str, data_list: list) -> int:
    """Version inchangée de l'original."""
    if not data_list:
        return 0
    try:
        client = MinIOClient(endpoint="localhost:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]
        unique_data = []
        seen = set()
        valid_count = 0
        invalid_count = 0
        for item in data_list:
            cleaned_item = {k: v for k, v in item.items() if v is not None}
            if data_type == "faculty":
                if not cleaned_item.get("first_name") and not cleaned_item.get("last_name"):
                    invalid_count += 1
                    continue
                valid_count += 1
                key = f"{cleaned_item.get('first_name', '')}_{cleaned_item.get('last_name', '')}_{cleaned_item.get('email', '')}"
            else:  # news
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
        object_path = f"source={source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/{data_type}_{timestamp}.json"
        client.upload_json(
            bucket_name="raw-json",
            object_name=object_path,
            data=data_payload
        )
        logger.info(f"✅ {len(unique_data)} {data_type} sauvegardés pour {source_name}")
        return len(unique_data)
    except Exception as e:
        logger.error(f"Erreur sauvegarde {data_type} pour {source_name}: {e}")
        return 0

def save_consolidated_data(all_news: list, all_faculty: list) -> None:
    """Version inchangée."""
    try:
        client = MinIOClient(endpoint="localhost:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]
        if all_news:
            unique_news = []
            seen = set()
            for news in all_news:
                key = f"{news['title']}_{news['source']}"
                if key not in seen:
                    seen.add(key)
                    news_with_metadata = create_common_fields(
                        source_system="news_web_scraper",
                        source_url=news.get("url", ""),
                        data=news
                    )
                    unique_news.append(news_with_metadata)
            news_data = {
                "source": "all_institutions_marrakech",
                "table_type": "university_news",
                "scrape_timestamp": partition["iso"],
                "total_news": len(unique_news),
                "news_items": unique_news
            }
            object_path = f"all_institutions_marrakech/year={partition['year']}/month={partition['month']}/day={partition['day']}/university_news_{timestamp}.json"
            client.upload_json(bucket_name="raw-json", object_name=object_path, data=news_data)
            logger.info(f"✅ {len(unique_news)} actualités consolidées sauvegardées")
        if all_faculty:
            unique_faculty = []
            seen = set()
            for faculty in all_faculty:
                cleaned = {k: v for k, v in faculty.items() if v is not None}
                key = f"{cleaned.get('first_name', '')}_{cleaned.get('last_name', '')}_{cleaned.get('email', '')}"
                if key not in seen:
                    seen.add(key)
                    faculty_with_metadata = create_common_fields(
                        source_system="faculty_web_scraper",
                        source_url=cleaned.get("source_url", ""),
                        data=cleaned
                    )
                    unique_faculty.append(faculty_with_metadata)
            faculty_data = {
                "source": "all_institutions_marrakech",
                "table_type": "faculty_profiles",
                "scrape_timestamp": partition["iso"],
                "total_faculty": len(unique_faculty),
                "faculty_members": unique_faculty
            }
            object_path = f"faculty_profiles_marrakech/year={partition['year']}/month={partition['month']}/day={partition['day']}/faculty_profiles_{timestamp}.json"
            client.upload_json(bucket_name="raw-json", object_name=object_path, data=faculty_data)
            logger.info(f"✅ {len(unique_faculty)} profils enseignants consolidés sauvegardés")
    except Exception as e:
        logger.error(f"Erreur sauvegarde consolidée: {e}")

def save_stats(source_name: str, stats: dict) -> None:
    """Version inchangée."""
    try:
        client = MinIOClient(endpoint="localhost:9000")
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
                "total_errors": stats.get("errors", 0),
                "success_rate": f"{(1 - stats.get('errors', 0) / max(stats.get('pages_visited', 1), 1)) * 100:.2f}%"
            }
        }
        object_path = f"source={source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/stats_{timestamp}.json"
        client.upload_json(
            bucket_name="raw-logs",
            object_name=object_path,
            data=stats_report
        )
        logger.debug(f"Statistiques sauvegardées pour {source_name}")
    except Exception as e:
        logger.error(f"Erreur sauvegarde stats: {e}")

# ==============================================================
# DÉCOUVERTE D'URLS (inchangée)
# ==============================================================

def discover_urls_from_homepage(homepage_url: str, session: requests.Session) -> dict:
    discovered = {"news_url": None, "faculty_url": None}
    try:
        response = safe_request(session, homepage_url)
        if not response:
            return discovered
        soup = BeautifulSoup(response.text, 'html.parser')
        news_keywords = ['actualités', 'actus', 'news', 'actualite', 'annonce']
        faculty_keywords = ['professoral', 'enseignants', 'faculty', 'corps professoral', 'annuaire']
        for link in soup.find_all('a', href=True):
            text = link.get_text().strip().lower()
            href = link['href']
            if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                continue
            full_url = urljoin(homepage_url, href)
            full_url = normalize_url(full_url)
            if not discovered["news_url"]:
                for keyword in news_keywords:
                    if keyword in text or keyword in href.lower():
                        discovered["news_url"] = full_url
                        logger.info(f"🔍 URL actualités trouvée: {full_url}")
                        break
            if not discovered["faculty_url"]:
                for keyword in faculty_keywords:
                    if keyword in text or keyword in href.lower():
                        discovered["faculty_url"] = full_url
                        logger.info(f"🔍 URL faculté trouvée: {full_url}")
                        break
            if discovered["news_url"] and discovered["faculty_url"]:
                break
    except Exception as e:
        logger.error(f"Erreur découverte URLs: {e}")
    return discovered

# ==============================================================
# SCRAPERS SPÉCIFIQUES (adaptés pour FSSM, ENSA, ENCG)
# ==============================================================

# --- FSSM ---
def scrape_fssm_news(url: str, session: requests.Session) -> list:
    logger.info(f"  Scraping FSSM actualités from: {url}")
    news_list = []
    try:
        response = safe_request(session, url)
        if not response:
            return []
        save_raw_html("fssm", url, response.text, "news")
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('article', class_=re.compile(r'node-?article|post|entry'))
        if not articles:
            articles = soup.find_all('div', class_='views-row')
        for article in articles:
            title_elem = article.find('h2') or article.find('h3') or article.find('a', class_=re.compile(r'title'))
            if title_elem:
                link = title_elem if title_elem.name == 'a' else title_elem.find('a')
                if link:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    url_article = urljoin(url, href)
                else:
                    title = title_elem.get_text(strip=True)
                    url_article = ""
            else:
                continue
            if not title:
                continue
            date_elem = article.find('time') or article.find('span', class_=re.compile(r'date|created|posted'))
            date_text = date_elem.get_text(strip=True) if date_elem else ""
            img_elem = article.find('img')
            image_url = ""
            if img_elem and img_elem.get('src'):
                img_src = img_elem.get('src')
                image_url = urljoin(url, img_src)
                if is_valid_url(image_url):
                    save_image(image_url, "fssm", f"fssm_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}")
            doc_urls = []
            for link in article.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                if full_url.lower().endswith(('.pdf','.doc','.docx','.xls','.xlsx','.ppt','.pptx','.zip')):
                    doc_urls.append(full_url)
                    save_document(full_url, "fssm")
            news_list.append({
                "title": title,
                "url": url_article,
                "publication_date": date_text,
                "image_url": image_url if is_valid_url(image_url) else "",
                "category": "Actualités",
                "source": "FSSM Marrakech",
                "institution": "FSSM",
                "documents": doc_urls
            })
        logger.info(f"      ✅ {len(news_list)} actualités trouvées")
    except Exception as e:
        logger.error(f"      Erreur: {e}")
    return news_list

def scrape_fssm_faculty(url: str, session: requests.Session) -> list:
    logger.info(f"  Scraping FSSM faculty from: {url}")
    faculty_list = []
    try:
        response = safe_request(session, url)
        if not response:
            return []
        save_raw_html("fssm", url, response.text, "faculty")
        soup = BeautifulSoup(response.text, 'html.parser')
        email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
        rows = soup.find_all('div', class_=re.compile(r'views-row|field--name-field-enseignant'))
        if not rows:
            rows = soup.find_all('tr')
        for row in rows:
            text = row.get_text()
            email_match = email_pattern.search(text)
            email = email_match.group() if email_match else ""
            name_elem = row.find(class_=re.compile(r'nom|name|full-name')) or row.find('strong')
            if name_elem:
                name = name_elem.get_text(strip=True)
            else:
                if email:
                    parts = text.split(email)
                    if parts:
                        name = parts[0].strip()
                        name = re.sub(r'[|,;:]', ' ', name).strip()
                else:
                    continue
            if not name:
                continue
            name_parts = name.split()
            if len(name_parts) >= 2:
                first_name = name_parts[0]
                last_name = ' '.join(name_parts[1:])
            else:
                first_name = name
                last_name = ""
            faculty_list.append({
                "last_name": last_name,
                "first_name": first_name,
                "email": email,
                "department": "Général",
                "source_url": url,
                "institution": "Faculté des Sciences Semlalia - Marrakech"
            })
        logger.info(f"      ✅ {len(faculty_list)} enseignants trouvés")
    except Exception as e:
        logger.error(f"      Erreur: {e}")
    return faculty_list

# --- ENSA ---
def scrape_ensa_news(url: str, session: requests.Session) -> list:
    logger.info(f"  Scraping ENSA actualités from: {url}")
    news_list = []
    try:
        response = safe_request(session, url)
        if not response:
            return []
        save_raw_html("ensa", url, response.text, "news")
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('article', class_=re.compile(r'post|entry'))
        if not articles:
            articles = soup.find_all('div', class_=re.compile(r'post|entry'))
        for article in articles:
            title_elem = article.find('h2') or article.find('h3') or article.find('a', class_=re.compile(r'title'))
            if title_elem:
                link = title_elem if title_elem.name == 'a' else title_elem.find('a')
                if link:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    url_article = urljoin(url, href)
                else:
                    title = title_elem.get_text(strip=True)
                    url_article = ""
            else:
                continue
            if not title:
                continue
            date_elem = article.find('time') or article.find('span', class_=re.compile(r'date|posted'))
            date_text = date_elem.get_text(strip=True) if date_elem else ""
            img_elem = article.find('img')
            image_url = ""
            if img_elem and img_elem.get('src'):
                img_src = img_elem.get('src')
                image_url = urljoin(url, img_src)
                if is_valid_url(image_url):
                    save_image(image_url, "ensa", f"ensa_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}")
            doc_urls = []
            for link in article.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                if full_url.lower().endswith(('.pdf','.doc','.docx','.xls','.xlsx','.ppt','.pptx','.zip')):
                    doc_urls.append(full_url)
                    save_document(full_url, "ensa")
            news_list.append({
                "title": title,
                "url": url_article,
                "publication_date": date_text,
                "image_url": image_url if is_valid_url(image_url) else "",
                "category": "Actualités",
                "source": "ENSA Marrakech",
                "institution": "ENSA Marrakech",
                "documents": doc_urls
            })
        logger.info(f"      ✅ {len(news_list)} actualités trouvées")
    except Exception as e:
        logger.error(f"      Erreur: {e}")
    return news_list

def scrape_ensa_faculty(url: str, session: requests.Session) -> list:
    logger.info(f"  Scraping ENSA faculty from: {url}")
    faculty_list = []
    try:
        response = safe_request(session, url)
        if not response:
            return []
        save_raw_html("ensa", url, response.text, "faculty")
        soup = BeautifulSoup(response.text, 'html.parser')
        email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
        for elem in soup.find_all(text=email_pattern):
            parent = elem.parent
            if parent:
                text = parent.get_text()
                email_match = email_pattern.search(text)
                if email_match:
                    email = email_match.group()
                    name_text = text[:email_match.start()].strip()
                    name_text = re.sub(r'[|,;:]', ' ', name_text).strip()
                    if name_text:
                        name_parts = name_text.split()
                        if len(name_parts) >= 2:
                            first_name = name_parts[0]
                            last_name = ' '.join(name_parts[1:])
                        else:
                            first_name = name_text
                            last_name = ""
                        faculty_list.append({
                            "last_name": last_name,
                            "first_name": first_name,
                            "email": email,
                            "department": "Général",
                            "source_url": url,
                            "institution": "ENSA Marrakech"
                        })
        if not faculty_list:
            for p in soup.find_all(['p', 'div', 'li']):
                text = p.get_text()
                if 'prof' in text.lower() or 'enseignant' in text.lower():
                    email_match = email_pattern.search(text)
                    if email_match:
                        email = email_match.group()
                        name = text[:email_match.start()].strip()
                        name = re.sub(r'[|,;:]', ' ', name).strip()
                        if name:
                            parts = name.split()
                            if len(parts) >= 2:
                                first = parts[0]
                                last = ' '.join(parts[1:])
                            else:
                                first = name
                                last = ""
                            faculty_list.append({
                                "last_name": last,
                                "first_name": first,
                                "email": email,
                                "department": "Général",
                                "source_url": url,
                                "institution": "ENSA Marrakech"
                            })
        logger.info(f"      ✅ {len(faculty_list)} enseignants trouvés")
    except Exception as e:
        logger.error(f"      Erreur: {e}")
    return faculty_list

# --- ENCG ---
def scrape_encg_news(url: str, session: requests.Session) -> list:
    logger.info(f"  Scraping ENCG actualités from: {url}")
    news_list = []
    try:
        response = safe_request(session, url)
        if not response:
            return []
        save_raw_html("encg", url, response.text, "news")
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('article', class_=re.compile(r'node-?article|post|entry'))
        if not articles:
            articles = soup.find_all('div', class_='views-row')
        for article in articles:
            title_elem = article.find('h2') or article.find('h3') or article.find('a', class_=re.compile(r'title'))
            if title_elem:
                link = title_elem if title_elem.name == 'a' else title_elem.find('a')
                if link:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    url_article = urljoin(url, href)
                else:
                    title = title_elem.get_text(strip=True)
                    url_article = ""
            else:
                continue
            if not title:
                continue
            date_elem = article.find('time') or article.find('span', class_=re.compile(r'date|created'))
            date_text = date_elem.get_text(strip=True) if date_elem else ""
            img_elem = article.find('img')
            image_url = ""
            if img_elem and img_elem.get('src'):
                img_src = img_elem.get('src')
                image_url = urljoin(url, img_src)
                if is_valid_url(image_url):
                    save_image(image_url, "encg", f"encg_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}")
            doc_urls = []
            for link in article.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                if full_url.lower().endswith(('.pdf','.doc','.docx','.xls','.xlsx','.ppt','.pptx','.zip')):
                    doc_urls.append(full_url)
                    save_document(full_url, "encg")
            news_list.append({
                "title": title,
                "url": url_article,
                "publication_date": date_text,
                "image_url": image_url if is_valid_url(image_url) else "",
                "category": "Actualités",
                "source": "ENCG Marrakech",
                "institution": "ENCG",
                "documents": doc_urls
            })
        logger.info(f"      ✅ {len(news_list)} actualités trouvées")
    except Exception as e:
        logger.error(f"      Erreur: {e}")
    return news_list

def scrape_encg_faculty(url: str, session: requests.Session) -> list:
    logger.info(f"  Scraping ENCG faculty from: {url}")
    faculty_list = []
    try:
        response = safe_request(session, url)
        if not response:
            return []
        save_raw_html("encg", url, response.text, "faculty")
        soup = BeautifulSoup(response.text, 'html.parser')
        email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
        rows = soup.find_all('tr')
        if not rows:
            rows = soup.find_all('div', class_=re.compile(r'views-row|field|item'))
        for row in rows:
            text = row.get_text()
            email_match = email_pattern.search(text)
            email = email_match.group() if email_match else ""
            name_elem = row.find(class_=re.compile(r'nom|name|enseignant')) or row.find('strong')
            if name_elem:
                name = name_elem.get_text(strip=True)
            else:
                if email:
                    parts = text.split(email)
                    if parts:
                        name = parts[0].strip()
                        name = re.sub(r'[|,;:]', ' ', name).strip()
                else:
                    continue
            if not name:
                continue
            name_parts = name.split()
            if len(name_parts) >= 2:
                first_name = name_parts[0]
                last_name = ' '.join(name_parts[1:])
            else:
                first_name = name
                last_name = ""
            faculty_list.append({
                "last_name": last_name,
                "first_name": first_name,
                "email": email,
                "department": "Général",
                "source_url": url,
                "institution": "ENCG Marrakech"
            })
        logger.info(f"      ✅ {len(faculty_list)} enseignants trouvés")
    except Exception as e:
        logger.error(f"      Erreur: {e}")
    return faculty_list

# ==============================================================
# CRAWLER BFS (ajouté depuis le deuxième code)
# ==============================================================

def crawl_faculty_bfs(faculty_name: str, base_url: str, max_pages: int = 500) -> None:
    """
    Explore en largeur (BFS) un site, sauvegarde HTML, images, documents, JSON‑LD.
    Utilise les mêmes fonctions de sauvegarde que le scraper.
    """
    logger.info(f"🚀 Démarrage du crawl BFS pour: {faculty_name} (Max {max_pages} pages)")

    client = MinIOClient(endpoint="localhost:9000")
    now = datetime.now()

    visited_pages = set()
    downloaded_assets = set()

    queue = deque([_normalize_url_crawl(base_url)])
    logs = []
    domain = urlparse(base_url).netloc

    with requests.Session() as session:
        session.headers.update({
            "User-Agent": SCRAPER_CONFIG["user_agent"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        })

        while queue and len(visited_pages) < max_pages:
            url = _normalize_url_crawl(queue.popleft())
            if url in visited_pages:
                continue

            visited_pages.add(url)
            logger.info(f"[{len(visited_pages)}/{max_pages}] Traitement page: {url}")

            time.sleep(SCRAPER_CONFIG["request_delay"])

            try:
                response = session.get(url, timeout=20, verify=False, allow_redirects=True)
                response.raise_for_status()

                if "text/html" not in response.headers.get("Content-Type", "").lower():
                    continue

                # Sauvegarde HTML + métadonnées
                save_raw_html(faculty_name, url, response.text, "crawl_bfs")

                soup = BeautifulSoup(response.text, "html.parser")

                # JSON-LD
                scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
                for script_idx, script in enumerate(scripts):
                    if not script.string:
                        continue
                    try:
                        json_data = json.loads(script.string)
                        save_jsonld(faculty_name, url, json_data, len(logs), script_idx, now)
                    except Exception:
                        logger.warning(f"Impossible de parser JSON-LD sur {url}")

                # Téléchargement des assets (images, documents)
                for tag in soup.find_all(["a", "img"]):
                    link = tag.get("href") or tag.get("src")
                    if not link:
                        continue

                    full_url = _normalize_url_crawl(urljoin(url, link))
                    lower_url = full_url.lower()

                    if lower_url.endswith(('.pdf','.doc','.docx','.xls','.xlsx','.ppt','.pptx','.zip','.rar')):
                        if full_url not in downloaded_assets and full_url not in visited_pages:
                            downloaded_assets.add(full_url)
                            try:
                                with session.get(full_url, timeout=30, verify=False, stream=True) as file_res:
                                    if "text/html" not in file_res.headers.get("Content-Type", "").lower() and file_res.status_code == 200:
                                        save_document(full_url, faculty_name)
                                    else:
                                        logger.warning(f"Faux document ignoré (page HTML) : {full_url}")
                            except Exception as e:
                                logger.warning(f"Erreur téléchargement doc {full_url}: {e}")

                    elif lower_url.endswith(('.png','.jpg','.jpeg','.gif','.webp','.svg')):
                        if full_url not in downloaded_assets and full_url not in visited_pages:
                            downloaded_assets.add(full_url)
                            try:
                                img_res = session.get(full_url, timeout=20, verify=False)
                                if img_res.status_code == 200:
                                    save_image(full_url, faculty_name)
                            except Exception as e:
                                logger.warning(f"Erreur téléchargement image {full_url}: {e}")

                    elif domain in urlparse(full_url).netloc:
                        if full_url not in visited_pages and full_url not in queue:
                            queue.append(full_url)

                logs.append({
                    "url": url,
                    "status": response.status_code,
                    "timestamp": now.isoformat(),
                })

            except requests.exceptions.Timeout:
                logs.append({"url": url, "status": "TIMEOUT", "message": "Timeout after 20s", "timestamp": now.isoformat()})
                logger.warning(f"Timeout: {url}")
            except requests.exceptions.HTTPError as e:
                logs.append({"url": url, "status": e.response.status_code if e.response else "HTTP_ERROR", "message": str(e), "timestamp": now.isoformat()})
                logger.warning(f"Erreur HTTP sur {url} : {e}")
            except Exception as e:
                logs.append({"url": url, "status": "ERROR", "message": str(e), "timestamp": now.isoformat()})
                logger.error(f"Erreur globale sur {url} : {e}")

    save_logs(faculty_name, logs)
    logger.info(f"✅ BFS {faculty_name} terminé: {len(visited_pages)} pages HTML crawlées.")

# ==============================================================
# MAIN (run) - fusion des deux approches
# ==============================================================

def run():
    session = create_session()
    partition = get_date_partition()
    logger.info("="*70)
    logger.info("🚀 SCRAPER UNIVERSITAIRE - MARRRAKECH (amélioré)")
    logger.info("="*70)
    logger.info(f"📅 Date: {partition['year']}-{partition['month']}-{partition['day']}")
    logger.info(f"⏰ Heure: {partition['timestamp']}")
    logger.info("="*70)

    all_news = []
    all_faculty = []
    all_stats = {}

    for site_key, site_config in SITES_CONFIG.items():
        logger.info(f"\n{'='*60}")
        logger.info(f"📚 {site_config['name']} ({site_key})")
        logger.info(f"{'='*60}")

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

        # ---- 1. Page d'accueil et découverte ----
        logger.info("\n  🏠 Page d'accueil...")
        homepage_response = safe_request(session, site_config["homepage"])
        if homepage_response:
            save_raw_html(site_key, site_config["homepage"], homepage_response.text, "homepage")
            stats["pages_visited"] += 1
            logger.info("      ✅ Page d'accueil sauvegardée")
            logger.info("  🔍 Découverte des URLs...")
            discovered = discover_urls_from_homepage(site_config["homepage"], session)
            if discovered["news_url"] and not site_config.get("news_url"):
                site_config["news_url"] = discovered["news_url"]
                logger.info(f"      ✅ URL actualités: {discovered['news_url']}")
            if discovered["faculty_url"] and not site_config.get("faculty_url"):
                site_config["faculty_url"] = discovered["faculty_url"]
                logger.info(f"      ✅ URL faculté: {discovered['faculty_url']}")

        # ---- 2. Scraping ciblé (news & faculty) ----
        if site_config.get("scrape_news", True) and site_config.get("news_url"):
            logger.info("\n  📰 Scraping actualités...")
            try:
                if site_key == "fssm":
                    news = scrape_fssm_news(site_config["news_url"], session)
                elif site_key == "ensa":
                    news = scrape_ensa_news(site_config["news_url"], session)
                elif site_key == "encg":
                    news = scrape_encg_news(site_config["news_url"], session)
                else:
                    news = []
                if news:
                    source_news.extend(news)
                    saved = save_structured_data(site_key, "news", source_news)
                    all_news.extend(source_news)
                    stats["news_found"] = len(source_news)
            except Exception as e:
                logger.error(f"      ❌ Erreur scraping news: {e}")
                stats["errors"] += 1

        if site_config.get("scrape_faculty", True) and site_config.get("faculty_url"):
            logger.info("\n  👨‍🏫 Scraping faculty...")
            try:
                if site_key == "fssm":
                    faculty = scrape_fssm_faculty(site_config["faculty_url"], session)
                elif site_key == "ensa":
                    faculty = scrape_ensa_faculty(site_config["faculty_url"], session)
                elif site_key == "encg":
                    faculty = scrape_encg_faculty(site_config["faculty_url"], session)
                else:
                    faculty = []
                if faculty:
                    source_faculty.extend(faculty)
                    saved = save_structured_data(site_key, "faculty", source_faculty)
                    all_faculty.extend(source_faculty)
                    stats["faculty_found"] = len(source_faculty)
            except Exception as e:
                logger.error(f"      ❌ Erreur scraping faculty: {e}")
                stats["errors"] += 1

        # ---- 3. Crawling BFS (optionnel) ----
        if site_config.get("crawl_mode", False):
            logger.info("\n  🌐 Lancement du crawling BFS...")
            try:
                max_pages = site_config.get("max_pages", 500)
                crawl_faculty_bfs(site_key, site_config["homepage"], max_pages)
                stats["pages_visited"] += max_pages  # approximation
            except Exception as e:
                logger.error(f"      ❌ Erreur crawling BFS: {e}")
                stats["errors"] += 1

        # ---- 4. Statistiques ----
        save_stats(site_key, stats)
        all_stats[site_key] = stats
        logger.info(f"\n  ✅ {site_config['name']} terminé:")
        logger.info(f"     - News: {len(source_news)}")
        logger.info(f"     - Faculty: {len(source_faculty)}")
        logger.info(f"     - Erreurs: {stats['errors']}")

    # ---- Consolidation ----
    logger.info("\n" + "="*70)
    logger.info("💾 SAUVEGARDE CONSOLIDÉE")
    logger.info("="*70)
    save_consolidated_data(all_news, all_faculty)

    # ---- Résumé ----
    logger.info("\n" + "="*70)
    logger.info("📊 RÉSUMÉ FINAL - MARRRAKECH")
    logger.info("="*70)
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
        logger.info(f"   {name}: {stats['news_found']} news, {stats['faculty_found']} faculty, BFS activé: {SITES_CONFIG[site_key].get('crawl_mode', False)}")
    logger.info("\n Structure MinIO :")
    logger.info("   raw-web-html/     → HTML brut (scraping + BFS)")
    logger.info("   raw-images/       → Images extraites")
    logger.info("   raw-documents/    → Documents")
    logger.info("   raw-json/         → Données structurées + JSON-LD")
    logger.info("   raw-logs/         → Logs et statistiques")
    logger.info("="*70)
    logger.info(f"\n Cache des images: {len(_image_cache)} entrées")
    logger.info(f" Fichier cache: {IMAGE_CACHE_FILE}")
    logger.info("\n SCRAPING MARRRAKECH TERMINÉ!")

if __name__ == "__main__":
    run()
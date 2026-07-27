# src/ingestion/web/usms_scraper.py (version finale : crawl unique + homepage ENSAK corrigée + fix soup.title)

import os
import sys

sys.path.insert(0, 'D:/university-data-platform_v2')

import re
import json
import hashlib
import time
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse, urldefrag
from collections import deque

import requests
from bs4 import BeautifulSoup

from src.storage.minio.nezha_client import MinIOClient

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Cache des images
IMAGE_CACHE_FILE = "usms_image_cache.json"

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

URL_CACHE_FILE = "usms_url_cache.json"

def load_url_cache():
    try:
        if os.path.exists(URL_CACHE_FILE):
            with open(URL_CACHE_FILE, 'r') as f:
                return set(json.load(f))
    except:
        pass
    return set()

def save_url_cache(cache_set):
    try:
        with open(URL_CACHE_FILE, 'w') as f:
            json.dump(list(cache_set), f)
    except:
        pass

_url_cache = load_url_cache()

# ==============================================================
# CONFIGURATION DES SITES
# ==============================================================

SITES_CONFIG = {
    "flsh": {
        "name": "Faculté des Lettres et Sciences Humaines – Beni Mellal",
        "homepage": "https://www.flshbm.ma",
        "departments": {
            "Département de Géographie": "https://www.flshbm.ma/page/departement-de-geographie",
            "Département de Langue et de Littérature Anglaises": "https://www.flshbm.ma/page/departement-de-langue-et-de-litterature-anglaises",
            "Département d'Histoire et de Patrimoine": "https://www.flshbm.ma/page/departement-d-histoire-et-de-patrimoine",
            "Département de Langue et de Littérature françaises": "https://www.flshbm.ma/page/departement-de-langue-et-de-litterature-francaises",
            "Département de Langue et de Littérature arabes": "https://www.flshbm.ma/page/departement-de-langue-et-de-litterature-arabes",
            "Département de Sociologie": "https://www.flshbm.ma/page/departement-de-sociologie",
            "Département des Etudes Islamiques": "https://www.flshbm.ma/page/departement-des-etudes-islamiques-flsh-beni-mellal-equipe-et-structure"
        }
    },
    "fst": {
        "name": "Faculté des Sciences et Techniques – Beni Mellal",
        "homepage": "https://fstbm.ac.ma",
        "departments": {
            "Département Physique": "https://fstbm.ac.ma/departement/physique",
            "Département Informatique": "https://fstbm.ac.ma/departement/informatique",
            "Département Mathématique": "https://fstbm.ac.ma/departement/mathematique",
            "Département Langue et Communication": "https://fstbm.ac.ma/departement/langue-communication",
            "Département Génie Mécanique": "https://fstbm.ac.ma/departement/genie-mecanique",
            "Département Science de la Vie": "https://fstbm.ac.ma/departement/science-vie",
            "Département Chimie et Environnement": "https://fstbm.ac.ma/departement/chimie-environnement",
            "Département Génie Electrique": "https://fstbm.ac.ma/departement/genie-electrique",
            "Département Science de la Terre": "https://fstbm.ac.ma/departement/science-terre"
        }
    },
    "ensak": {
        "name": "ENSA Khouribga",
        "homepage": "http://ensak.usms.ac.ma/ensak/",
        "departments": {
            "Mathématiques et Informatique": "http://ensak.usms.ac.ma/ensak/maths-informatique/",
            "Génie Electrique": "http://ensak.usms.ac.ma/ensak/genie-electrique/",
            "Réseaux et Télécommunications": "http://ensak.usms.ac.ma/ensak/genie-reseaux-telecoms/",
            "Génie des Procédés": "http://ensak.usms.ac.ma/ensak/genie-des-procedes/"
        }
    },
    "estkh": {
        "name": "EST Khenifra",
        "homepage": "https://estkh.usms.ac.ma",
        "departments": {
            "Département de Biotechnologie et Analyses": "https://estkh.usms.ac.ma/departements/biotechnologie",
            "Département de Langues étrangères et Soft Skills": "https://estkh.usms.ac.ma/departements/langues",
            "Département de Génie de l'Environnement": "https://estkh.usms.ac.ma/departements/environnement",
            "Département de Génie Energétique et Procédés": "https://estkh.usms.ac.ma/departements/energetique",
            "Département de Génie Informatique et Mathématique": "https://estkh.usms.ac.ma/departements/informatique",
            "Département de Sciences Economiques, Sociales et Dynamique Territoriale": "https://estkh.usms.ac.ma/departements/sciences_eco"
        }
    }
}

# ==============================================================
# CONFIGURATION DES ACTUALITÉS
# ==============================================================

NEWS_SOURCES_CONFIG = {
    "flsh": {
        "listing_urls": [f"https://www.flshbm.ma/articles?page={i}" for i in range(1, 4)],
        "article_link_pattern": "/article/",
        "exclude_patterns": [],
    },
    "fst": {
        "listing_urls": ["https://fstbm.ac.ma/actualites"] + [
            f"https://fstbm.ac.ma/actualites/{i}" for i in range(2, 4)
        ],
        "article_link_pattern": "/actualite/",
        "exclude_patterns": [],
    },
    "ensak": {
        "listing_urls": ["http://ensak.usms.ac.ma/ensak/category/news/"] + [
            f"http://ensak.usms.ac.ma/ensak/category/news/page/{i}/" for i in range(2, 4)
        ],
        "article_link_pattern": None,
        "exclude_patterns": ["/category/", "/page/", "/tag/", "/wp-"],
    },
    "estkh": {
        "listing_urls": [],
        "article_link_pattern": None,
        "exclude_patterns": [],
    },
}

MONTHS_FR = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6,
    "juillet": 7, "août": 8, "aout": 8, "septembre": 9, "octobre": 10,
    "novembre": 11, "décembre": 12, "decembre": 12,
}
MONTHS_AR = {
    "يناير": 1, "فبراير": 2, "مارس": 3, "أبريل": 4, "ابريل": 4, "ماي": 5, "يونيو": 6,
    "يوليوز": 7, "يوليو": 7, "غشت": 8, "أغسطس": 8, "شتنبر": 9, "سبتمبر": 9,
    "أكتوبر": 10, "نونبر": 11, "نوفمبر": 11, "دجنبر": 12, "ديسمبر": 12,
}

SCRAPER_CONFIG = {
    "timeout": 45,
    "retry_attempts": 3,
    "retry_delay": 2,
    "request_delay": 0.5,
    "max_pages_per_site": 15,
    "verify_ssl": False,
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ==============================================================
# FONCTIONS UTILITAIRES
# ==============================================================

def create_common_fields(source_system: str, source_url: str, data: dict) -> dict:
    clean_data = {k: v for k, v in data.items() if k not in ['record_id', 'source_system', 'source_url']}
    content_json = json.dumps(clean_data, sort_keys=True)

    result = {
        "record_id": hashlib.md5(content_json.encode()).hexdigest()[:16],
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
    return result

def get_date_partition() -> dict:
    now = datetime.now()
    return {
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
        "timestamp": now.strftime("%Y%m%d_%H%M%S"),
        "iso": now.isoformat()
    }

def create_session() -> requests.Session:
    session = requests.Session()
    session.verify = SCRAPER_CONFIG["verify_ssl"]
    session.headers.update({
        "User-Agent": SCRAPER_CONFIG["user_agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    })
    return session

def safe_request(session: requests.Session, url: str) -> requests.Response:
    if not url or not url.startswith(('http://', 'https://')):
        return None

    for attempt in range(SCRAPER_CONFIG["retry_attempts"]):
        try:
            response = session.get(url, timeout=SCRAPER_CONFIG["timeout"], verify=False)
            response.raise_for_status()
            return response
        except Exception as e:
            if attempt < SCRAPER_CONFIG["retry_attempts"] - 1:
                time.sleep(SCRAPER_CONFIG["retry_delay"] * (attempt + 1))
                continue
            logger.error(f"Erreur requête {url}: {e}")
            return None
    return None

def is_internal_link(url: str, base_domain: str) -> bool:
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        domain = domain.replace('www.', '')
        base = base_domain.replace('www.', '')
        return domain == base or domain.endswith(f'.{base}')
    except:
        return False

def extract_links(soup: BeautifulSoup, base_url: str, base_domain: str) -> list:
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        full_url = urljoin(base_url, href)
        full_url = urldefrag(full_url)[0]

        if is_internal_link(full_url, base_domain):
            if not any(ext in full_url.lower() for ext in ['.pdf', '.jpg', '.png', '.gif', '.css', '.js']):
                links.append(full_url)

    return list(dict.fromkeys(links))

def save_html(source_name: str, url: str, html_content: str, page_type: str):
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()

        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        file_name = f"{source_name}_{page_type}_{url_hash}_{partition['timestamp']}.html"

        object_path = f"source=usms/html/{source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/{file_name}"

        client.upload_binary(
            bucket_name="raw-web-html",
            object_name=object_path,
            data=html_content.encode('utf-8'),
            content_type="text/html"
        )
        logger.debug(f"HTML sauvegardé: {object_path}")
    except Exception as e:
        logger.error(f"Erreur sauvegarde HTML: {e}")

def save_image(image_url: str, source_name: str, category: str = None) -> str:
    """
    Télécharge et sauvegarde une image dans le bucket raw-images.
    `category` distingue le type de page d'origine ("news" pour les
    actualités ; None = image générique de page).
    Retourne le chemin objet MinIO (sans le nom du bucket), ou None si
    échec ou image déjà en cache.
    """
    global _image_cache

    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()

        response = requests.get(image_url, timeout=30, verify=False)
        if response.status_code != 200:
            return None

        content_hash = hashlib.md5(response.content).hexdigest()
        cache_key = f"{category or 'general'}_{source_name}_{content_hash}"

        if cache_key in _image_cache:
            return None

        content_type = response.headers.get('content-type', 'image/jpeg')
        ext = '.jpg'
        if 'png' in content_type:
            ext = '.png'
        elif 'webp' in content_type:
            ext = '.webp'

        file_name = f"image_{content_hash[:8]}{ext}"

        if category:
            object_path = (
                f"source=usms/images/{category}/{source_name}/"
                f"year={partition['year']}/month={partition['month']}/day={partition['day']}/{file_name}"
            )
        else:
            object_path = (
                f"source=usms/images/{source_name}/"
                f"year={partition['year']}/month={partition['month']}/day={partition['day']}/{file_name}"
            )

        client.upload_binary(
            bucket_name="raw-images",
            object_name=object_path,
            data=response.content,
            content_type=content_type
        )

        _image_cache.add(cache_key)
        save_image_cache(_image_cache)

        return object_path

    except Exception as e:
        logger.debug(f"Erreur sauvegarde image: {e}")
        return None

# ==============================================================
# UTILITAIRES POUR LES ACTUALITÉS
# ==============================================================

def parse_flexible_date(text: str):
    """Essaie plusieurs formats de date : ISO, dd-mm-yyyy, '3 juillet 2026', '3 يوليوز 2026'."""
    if not text:
        return None
    text = text.strip()

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        pass

    m = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", text)
    if m:
        d, mo, y = map(int, m.groups())
        try:
            return datetime(y, mo, d)
        except Exception:
            pass

    m = re.search(r"(\d{1,2})\s+([^\s\d]+)\s+(\d{4})", text)
    if m:
        d, month_word, y = m.groups()
        month_num = MONTHS_FR.get(month_word.lower()) or MONTHS_AR.get(month_word)
        if month_num:
            try:
                return datetime(int(y), month_num, int(d))
            except Exception:
                pass

    return None


def extract_meta(soup: BeautifulSoup, prop: str):
    tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
    return tag.get("content", "").strip() if tag and tag.get("content") else None


def get_article_links(soup: BeautifulSoup, base_url: str, config: dict) -> list:
    """Extrait les liens d'articles d'une page de listing selon la config du site."""
    links = set()
    pattern = config.get("article_link_pattern")
    excludes = config.get("exclude_patterns", [])

    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a["href"])
        href = urldefrag(href)[0]

        if pattern and pattern not in href:
            continue
        if any(ex in href for ex in excludes):
            continue
        if href == base_url or href.rstrip("/") == base_url.rstrip("/"):
            continue

        links.add(href)

    return list(links)


def extract_article_details(soup: BeautifulSoup, url: str) -> dict:
    """Extrait titre, contenu, date, catégorie et image d'une page d'article."""
    title = extract_meta(soup, "og:title")
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else None
    if title:
        title = re.sub(r"\s*\|.*$", "", title).strip()

    description = extract_meta(soup, "og:description")
    image_url = extract_meta(soup, "og:image")

    published_dt = parse_flexible_date(extract_meta(soup, "article:published_time"))
    if not published_dt:
        page_text = soup.get_text(" ", strip=True)
        published_dt = parse_flexible_date(page_text[:2000])

    content_container = (
        soup.find("article")
        or soup.find("div", class_=re.compile(r"(content|article|entry|post)", re.I))
    )
    paragraphs = content_container.find_all("p") if content_container else soup.find_all("p")
    content = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
    if not content:
        content = description or ""

    if not image_url:
        img = soup.find("img")
        if img and img.get("src"):
            image_url = urljoin(url, img["src"])

    category = None
    cat_link = soup.find("a", href=re.compile(r"/category/"))
    if cat_link:
        category = cat_link.get_text(strip=True)

    return {
        "title": title,
        "content": content[:5000],
        "category": category,
        "image_url": image_url,
        "article_url": url,
        "publish_date": published_dt.isoformat() if published_dt else None,
    }

# ==============================================================
# CRAWLER UNIQUE (pages génériques + détection/extraction des news au passage)
# ==============================================================

def crawl_website(session: requests.Session, source_name: str, homepage: str, base_domain: str, news_config: dict = None):
    """
    Crawl unique du site entier. Détecte et extrait les actualités au
    passage (pas de deuxième fetch séparé).
    Retourne (crawl_result: dict, news_items: list).
    """
    logger.info(f"  🌐 Crawling {source_name} - {homepage}")

    visited_urls = set()
    urls_to_visit = deque([homepage])
    known_article_urls = set()

    news_config = news_config or {}
    listing_url_set = set(news_config.get("listing_urls", []))
    article_pattern = news_config.get("article_link_pattern")
    news_excludes = news_config.get("exclude_patterns", [])

    for listing_url in listing_url_set:
        if listing_url not in urls_to_visit:
            urls_to_visit.append(listing_url)

    pages_crawled = 0
    images_found = 0
    all_pages_data = []
    news_items = []

    while urls_to_visit and pages_crawled < SCRAPER_CONFIG["max_pages_per_site"]:
        current_url = urls_to_visit.popleft()

        if current_url in visited_urls:
            continue
        visited_urls.add(current_url)

        response = safe_request(session, current_url)
        if not response:
            continue

        pages_crawled += 1
        logger.info(f"      📄 Page {pages_crawled}: {current_url[:80]}...")

        soup = BeautifulSoup(response.text, 'html.parser')

        # Si page de listing news -> on en extrait les liens d'articles
        if current_url in listing_url_set:
            listing_cfg = {"article_link_pattern": article_pattern, "exclude_patterns": news_excludes}
            for link in get_article_links(soup, current_url, listing_cfg):
                known_article_urls.add(link)
                if link not in visited_urls and link not in urls_to_visit:
                    urls_to_visit.appendleft(link)

        is_news_article = bool(
            (article_pattern and article_pattern in current_url)
            or current_url in known_article_urls
        )
        page_type = "news_article" if is_news_article else "webpage"
        save_html(source_name, current_url, response.text, page_type)

        image_category = "news" if is_news_article else None
        for img in soup.find_all('img'):
            src = img.get('src')
            if src:
                if not src.startswith('http'):
                    src = urljoin(current_url, src)
                save_image(src, source_name, category=image_category)
                images_found += 1

        if is_news_article:
            details = extract_article_details(soup, current_url)
            if details["title"] and len(details["title"]) >= 5:
                image_storage_path = ""
                if details["image_url"]:
                    saved_path = save_image(details["image_url"], source_name, category="news")
                    if saved_path:
                        image_storage_path = f"s3://raw-images/{saved_path}"

                news_item = {
                    "title": details["title"],
                    "content": details["content"],
                    "category": details["category"] or "general",
                    "image_url": details["image_url"] or "",
                    "image_storage_path": image_storage_path,
                    "article_url": details["article_url"],
                    "publish_date": details["publish_date"] or datetime.now().isoformat(),
                    "institution": source_name,
                }
                news_with_meta = create_common_fields(
                    f"USMS-{source_name.upper()}-NEWS", current_url, news_item
                )
                news_items.append(news_with_meta)
                logger.info(f"      📰 Actualité extraite: {details['title'][:60]}")

        # --- FIX : soup.title peut exister avec .string = None (balise vide ou avec sous-tags) ---
        try:
            page_title = soup.title.get_text(strip=True) if soup.title else ""
        except Exception:
            page_title = ""

        page_data = {
            "url": current_url,
            "title": page_title,
            "images_count": len(soup.find_all('img')),
            "links_count": len(soup.find_all('a')),
            "text_length": len(soup.get_text()),
            "crawled_at": datetime.now().isoformat()
        }
        all_pages_data.append(page_data)

        new_links = extract_links(soup, current_url, base_domain)
        for link in new_links:
            if link not in visited_urls and link not in urls_to_visit:
                urls_to_visit.append(link)

        _url_cache.add(current_url)
        save_url_cache(_url_cache)

        time.sleep(SCRAPER_CONFIG["request_delay"])

    logger.info(
        f"      ✅ {pages_crawled} pages crawlées, {images_found} images, "
        f"{len(news_items)} actualités extraites"
    )

    crawl_result = {
        "source": source_name,
        "homepage": homepage,
        "pages_crawled": pages_crawled,
        "images_found": images_found,
        "news_found": len(news_items),
        "pages": all_pages_data
    }
    return crawl_result, news_items

# ==============================================================
# SCRAPERS FACULTY
# ==============================================================

def scrape_flsh_faculty(session):
    logger.info("  Scraping FLSH faculty...")
    faculty_list = []

    departments = SITES_CONFIG["flsh"]["departments"]

    for dept_name, url in departments.items():
        logger.info(f"    📚 {dept_name[:50]}")
        response = safe_request(session, url)
        if not response:
            continue

        save_html("flsh", url, response.text, "faculty")
        soup = BeautifulSoup(response.text, 'html.parser')

        for img in soup.find_all('img'):
            src = img.get('src')
            if src:
                if not src.startswith('http'):
                    src = urljoin(url, src)
                save_image(src, "flsh")

        professor_names = []

        pedagogical_section = None
        for elem in soup.find_all(['h2', 'h3', 'h4', 'p', 'strong']):
            if 'équipe pédagogique' in elem.get_text(strip=True).lower():
                pedagogical_section = elem
                break

        if pedagogical_section:
            current = pedagogical_section.find_next_sibling()
            while current and current.name not in ['h2', 'h3', 'h4']:
                if current.name == 'ul':
                    for li in current.find_all('li'):
                        name = li.get_text(strip=True)
                        if name:
                            professor_names.append(name)
                elif current.name == 'p':
                    name = current.get_text(strip=True)
                    if name and len(name) > 2:
                        professor_names.append(name)
                for strong in current.find_all(['strong', 'span']):
                    name = strong.get_text(strip=True)
                    if name and len(name) > 2:
                        professor_names.append(name)
                current = current.find_next_sibling()

        if not professor_names:
            page_text = soup.get_text(separator='\n', strip=True)
            lines = page_text.split('\n')
            start_capture = False
            for line in lines:
                if 'équipe pédagogique' in line.lower():
                    start_capture = True
                    continue
                if start_capture:
                    if any(keyword in line.lower() for keyword in ['structure', 'contact', 'administration']):
                        break
                    name = line.strip()
                    if name and len(name) > 2 and not any(keyword in name.lower() for keyword in ['équipe', 'pedagogique']):
                        professor_names.append(name)

        if not professor_names:
            lines = soup.get_text(separator='\n', strip=True).split('\n')
            start = False
            for line in lines:
                if any(x in line.lower() for x in ['équipe des enseignants', 'equipe des enseignants']):
                    start = True
                    continue
                if start and (line.lower().startswith('chef') or line.lower().startswith('coordinateur')):
                    break
                if start and line and len(line) > 2:
                    name = re.sub(r'^(Pr\.|Prof\.|M\.|Mme|Mlle|Dr\.)\s+', '', line.strip())
                    name = re.sub(r'\s*\([^)]*\)', '', name).strip()
                    if name and len(name) > 2 and not any(x in name.lower() for x in ['équipe', 'chef']):
                        if re.match(r'^[A-Za-zÀ-ÖØ-öø-ÿ\s\-.]+$', name):
                            professor_names.append(name)

        cleaned_names = []
        for name in professor_names:
            name = re.sub(r'^(Pr\.|Prof\.|M\.|Mme|Mlle|Dr\.|Pr|Mme\.)\s+', '', name)
            name = re.sub(r'\s*\([^)]*\)', '', name)
            name = re.sub(r'\s*[-–]\s*Enseignant.*$', '', name)
            name = ' '.join(name.split())

            if name and len(name) > 2 and re.match(r'^[A-Za-zÀ-ÖØ-öø-ÿ\s\-\.]+$', name):
                cleaned_names.append(name)

        professor_names = list(dict.fromkeys(cleaned_names))

        for name in professor_names:
            faculty = {
                "name": name,
                "title": "Professeur",
                "email": "",
                "department": dept_name,
                "faculty": "Faculté des Lettres et Sciences Humaines – Beni Mellal",
                "university": "Université Sultan Moulay Slimane",
                "city": "Beni Mellal",
                "country": "Morocco"
            }
            faculty_with_meta = create_common_fields("USMS-FLSH", url, faculty)
            faculty_list.append(faculty_with_meta)

        logger.info(f"      → {len(professor_names)} professeurs trouvés")
        time.sleep(SCRAPER_CONFIG["request_delay"])

    return faculty_list


def scrape_fst_faculty(session):
    logger.info("  Scraping FST faculty...")
    faculty_list = []

    departments = SITES_CONFIG["fst"]["departments"]

    for dept_name, url in departments.items():
        logger.info(f"    📚 {dept_name[:50]}")
        response = safe_request(session, url)
        if not response:
            continue

        save_html("fst", url, response.text, "faculty")
        soup = BeautifulSoup(response.text, 'html.parser')

        for img in soup.find_all('img'):
            src = img.get('src')
            if src:
                if not src.startswith('http'):
                    src = urljoin(url, src)
                save_image(src, "fst")

        tables = soup.find_all('table')

        if tables:
            table = max(tables, key=lambda t: len(t.find_all('tr')))
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 2:
                    name = cells[0].get_text(strip=True)
                    email = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    name = re.sub(r'^(Pr\.|Prof\.|M\.|Mme|Dr\.)\s+', '', name).strip()
                    email = email.strip() if '@' in email else ""

                    if name and len(name) > 3 and not any(x in name.lower() for x in ['chef', 'département']):
                        faculty = {
                            "name": name,
                            "title": "Professeur",
                            "email": email,
                            "department": dept_name,
                            "faculty": "Faculté des Sciences et Techniques – Beni Mellal",
                            "university": "Université Sultan Moulay Slimane",
                            "city": "Beni Mellal",
                            "country": "Morocco"
                        }
                        faculty_with_meta = create_common_fields("USMS-FST", url, faculty)
                        faculty_list.append(faculty_with_meta)

            logger.info(f"      → {len([f for f in faculty_list if f['department'] == dept_name])} professeurs")

        time.sleep(SCRAPER_CONFIG["request_delay"])

    return faculty_list


def scrape_ensak_faculty(session):
    logger.info("  Scraping ENSA Khouribga faculty...")
    faculty_list = []

    departments = SITES_CONFIG["ensak"]["departments"]

    for dept_name, url in departments.items():
        logger.info(f"    📚 {dept_name[:50]}")
        response = safe_request(session, url)
        if not response:
            continue

        save_html("ensak", url, response.text, "faculty")
        soup = BeautifulSoup(response.text, 'html.parser')

        for img in soup.find_all('img'):
            src = img.get('src')
            if src:
                if not src.startswith('http'):
                    src = urljoin(url, src)
                save_image(src, "ensak")

        page_text = soup.get_text()
        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', page_text)

        for email in emails:
            name = email.split('@')[0].replace('.', ' ').replace('_', ' ').title()
            exclude = ['technicien', 'labo', 'filière', 'master', 'ingénieur', 'chef', 'adjoint']
            if not any(word in name.lower() for word in exclude):
                faculty = {
                    "name": name,
                    "title": "Professeur",
                    "email": email,
                    "department": dept_name,
                    "faculty": "ENSA Khouribga",
                    "university": "Université Sultan Moulay Slimane",
                    "city": "Khouribga",
                    "country": "Morocco"
                }
                faculty_with_meta = create_common_fields("USMS-ENSAK", url, faculty)
                faculty_list.append(faculty_with_meta)

        logger.info(f"      → {len([f for f in faculty_list if f['department'] == dept_name])} professeurs")
        time.sleep(SCRAPER_CONFIG["request_delay"])

    return faculty_list


def scrape_estkh_faculty(session):
    logger.info("  Scraping EST Khenifra faculty...")
    faculty_list = []

    departments = SITES_CONFIG["estkh"]["departments"]

    for dept_name, url in departments.items():
        logger.info(f"    📚 {dept_name[:50]}")
        response = safe_request(session, url)
        if not response:
            continue

        save_html("estkh", url, response.text, "faculty")
        soup = BeautifulSoup(response.text, 'html.parser')

        for img in soup.find_all('img'):
            src = img.get('src')
            if src:
                if not src.startswith('http'):
                    src = urljoin(url, src)
                save_image(src, "estkh")

        professor_names = []

        for header in soup.find_all(['h2', 'h3', 'h4', 'strong']):
            if 'membres du département' in header.get_text(strip=True).lower():
                current = header.find_next_sibling()
                while current and current.name not in ['h2', 'h3', 'h4']:
                    if current.name == 'ul':
                        for item in current.find_all('li'):
                            name = item.get_text(strip=True)
                            if name and len(name) > 3:
                                professor_names.append(name)
                    current = current.find_next_sibling()
                break

        for name in dict.fromkeys(professor_names):
            faculty = {
                "name": name,
                "title": "Professeur",
                "email": "",
                "department": dept_name,
                "faculty": "EST Khenifra",
                "university": "Université Sultan Moulay Slimane",
                "city": "Khenifra",
                "country": "Morocco"
            }
            faculty_with_meta = create_common_fields("USMS-ESTKH", url, faculty)
            faculty_list.append(faculty_with_meta)

        logger.info(f"      → {len(professor_names)} professeurs")
        time.sleep(SCRAPER_CONFIG["request_delay"])

    return faculty_list

# ==============================================================
# SAUVEGARDE MINIO
# ==============================================================

def save_structured_data(source_name: str, data_type: str, data_list: list) -> int:
    if not data_list:
        return 0

    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]

        data_payload = {
            "source": source_name,
            "table_type": f"university_{data_type}",
            "scrape_timestamp": partition["iso"],
            f"total_{data_type}": len(data_list),
            f"{data_type}_items": data_list
        }

        object_path = f"source=usms/{data_type}/year={partition['year']}/month={partition['month']}/day={partition['day']}/{data_type}_{timestamp}.json"
        client.upload_json(
            bucket_name="raw-json",
            object_name=object_path,
            data=data_payload
        )

        logger.info(f"✅ {len(data_list)} {data_type} sauvegardés pour {source_name}")
        return len(data_list)

    except Exception as e:
        logger.error(f"Erreur sauvegarde {data_type}: {e}")
        return 0

def save_crawl_report(crawl_data: dict) -> None:
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]

        object_path = f"source=usms/crawl_reports/year={partition['year']}/month={partition['month']}/day={partition['day']}/crawl_report_{timestamp}.json"
        client.upload_json(
            bucket_name="raw-json",
            object_name=object_path,
            data=crawl_data
        )
        logger.info(f"✅ Rapport de crawl sauvegardé")
    except Exception as e:
        logger.error(f"Erreur sauvegarde rapport: {e}")

def save_consolidated_data(all_faculty: list) -> None:
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]

        faculty_data = {
            "source": "all_institutions",
            "table_type": "faculty_profiles",
            "scrape_timestamp": partition["iso"],
            "total_faculty": len(all_faculty),
            "faculty_members": all_faculty
        }

        object_path = f"faculty_profiles/year={partition['year']}/month={partition['month']}/day={partition['day']}/faculty_profiles_{timestamp}.json"
        client.upload_json(
            bucket_name="raw-json",
            object_name=object_path,
            data=faculty_data
        )
        logger.info(f"✅ {len(all_faculty)} profils consolidés sauvegardés")

    except Exception as e:
        logger.error(f"Erreur sauvegarde consolidée: {e}")

def save_consolidated_news(all_news: list) -> None:
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]

        news_data = {
            "source": "all_institutions",
            "table_type": "university_news",
            "scrape_timestamp": partition["iso"],
            "total_news": len(all_news),
            "news_items": all_news,
        }

        object_path = (
            f"university_news/year={partition['year']}/month={partition['month']}/"
            f"day={partition['day']}/university_news_{timestamp}.json"
        )
        client.upload_json(bucket_name="raw-json", object_name=object_path, data=news_data)
        logger.info(f"✅ {len(all_news)} actualités consolidées sauvegardées")

    except Exception as e:
        logger.error(f"Erreur sauvegarde news: {e}")

# ==============================================================
# MAIN
# ==============================================================

def run():
    session = create_session()
    partition = get_date_partition()

    logger.info("="*70)
    logger.info("🚀 USMS - SCRAPER COMPLET (Pages + Images + Professeurs + Actualités)")
    logger.info("="*70)
    logger.info(f"📅 Date: {partition['year']}-{partition['month']}-{partition['day']}")
    logger.info("="*70)

    all_faculty = []
    all_crawl_data = []
    all_news = []

    # 1. CRAWL UNIQUE PAR SITE (pages + news + images, en un seul passage)
    logger.info("\n" + "="*70)
    logger.info("🌐 CRAWLING DES SITES ENTIERS (avec détection des actualités)")
    logger.info("="*70)

    for source_name, config in SITES_CONFIG.items():
        logger.info(f"\n📂 {source_name.upper()} - {config['name']}")

        homepage = config['homepage']
        base_domain = urlparse(homepage).netloc
        news_config = NEWS_SOURCES_CONFIG.get(source_name)

        crawl_result, site_news = crawl_website(session, source_name, homepage, base_domain, news_config)
        all_crawl_data.append(crawl_result)
        all_news.extend(site_news)
        save_crawl_report(crawl_result)

    # 2. SCRAPER LES PROFESSEURS PAR DÉPARTEMENT
    logger.info("\n" + "="*70)
    logger.info("👨‍🏫 SCRAPING DES PROFESSEURS")
    logger.info("="*70)

    logger.info(f"\n📚 FLSH")
    flsh_faculty = scrape_flsh_faculty(session)
    save_structured_data("flsh", "faculty", flsh_faculty)
    all_faculty.extend(flsh_faculty)

    logger.info(f"\n📚 FST")
    fst_faculty = scrape_fst_faculty(session)
    save_structured_data("fst", "faculty", fst_faculty)
    all_faculty.extend(fst_faculty)

    logger.info(f"\n📚 ENSA Khouribga")
    ensak_faculty = scrape_ensak_faculty(session)
    save_structured_data("ensak", "faculty", ensak_faculty)
    all_faculty.extend(ensak_faculty)

    logger.info(f"\n📚 EST Khenifra")
    estkh_faculty = scrape_estkh_faculty(session)
    save_structured_data("estkh", "faculty", estkh_faculty)
    all_faculty.extend(estkh_faculty)

    # 3. SAUVEGARDE CONSOLIDÉE FACULTY
    logger.info("\n" + "="*70)
    logger.info("💾 SAUVEGARDE CONSOLIDÉE FACULTY")
    logger.info("="*70)
    save_consolidated_data(all_faculty)

    # 4. SAUVEGARDE CONSOLIDÉE NEWS
    logger.info("\n" + "="*70)
    logger.info("📰 SAUVEGARDE CONSOLIDÉE DES ACTUALITÉS")
    logger.info("="*70)
    save_consolidated_news(all_news)

    # 5. RÉSUMÉ FINAL
    logger.info("\n" + "="*70)
    logger.info("📊 RÉSUMÉ FINAL")
    logger.info("="*70)

    total_pages = sum(c['pages_crawled'] for c in all_crawl_data)
    total_images = sum(c['images_found'] for c in all_crawl_data)
    news_with_images = sum(1 for n in all_news if n.get("image_storage_path"))

    logger.info(f"📄 Pages crawlées: {total_pages}")
    logger.info(f"🖼️  Images trouvées (toutes catégories): {total_images}")
    logger.info(f"👨‍🏫 Professeurs trouvés: {len(all_faculty)}")
    logger.info(f"📰 Actualités trouvées: {len(all_news)}")
    logger.info(f"🖼️  Images d'actualités (raw-images/.../news/...): {news_with_images}")

    logger.info("\n📂 Détail par site:")
    for c in all_crawl_data:
        logger.info(
            f"   • {c['source'].upper()}: {c['pages_crawled']} pages, "
            f"{c['images_found']} images, {c.get('news_found', 0)} actualités"
        )

    logger.info("="*70)

if __name__ == "__main__":
    run()

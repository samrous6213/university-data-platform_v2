# src/ingestion/web/um5_scraper.py
"""
SCRAPER UNIVERSITAIRE - SEMAINE 1
Un seul fichier pour scraper les profs et actualités de UM5 + 4 écoles
Ingestion -> MinIO (raw) avec métadonnées complètes
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
from src.storage.minio.sara_client import MinIOClient

# ==============================================================
# CONFIGURATION
# ==============================================================

# Désactiver les warnings SSL
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Cache des images pour éviter les doublons
IMAGE_CACHE_FILE = "image_cache.json"

def load_image_cache():
    """Charge le cache des images depuis le disque."""
    try:
        if os.path.exists(IMAGE_CACHE_FILE):
            with open(IMAGE_CACHE_FILE, 'r') as f:
                return set(json.load(f))
    except:
        pass
    return set()

def save_image_cache(cache_set):
    """Sauvegarde le cache des images sur disque."""
    try:
        with open(IMAGE_CACHE_FILE, 'w') as f:
            json.dump(list(cache_set), f)
    except:
        pass

# Charger le cache au démarrage
_image_cache = load_image_cache()

# Configuration des sites
SITES_CONFIG = {
    "fsjes_agdal": {
        "name": "FSJES Agdal",
        "homepage": "https://fsjes-agdal.um5.ac.ma",
        "news_url": "https://fsjes-agdal.um5.ac.ma/fr/taxonomy/term/26",
        "avis_url": "https://fsjes-agdal.um5.ac.ma/fr/home-1",
        "faculty_url": "https://fsjes-agdal.um5.ac.ma/fr/corps-professoral",
        "scrape_news": True,
        "scrape_faculty": True
    },
    "emi": {
        "name": "EMI",
        "homepage": "https://www.emi.ac.ma",
        "news_url": "https://www.emi.ac.ma/actualites/",
        "faculty_urls": {
            "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-civil/": "Genie Civil",
            "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-electrique/": "Genie Electrique",
            "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-industriel/": "Genie Industriel",
            "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-informatique/": "Genie Informatique",
            "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-mecanique/": "Genie Mecanique",
            "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-mineral/": "Genie Mineral",
            "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-modelisation-et-informatique-scientifique/": "Modelisation et Informatique Scientifique",
            "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-des-procedes/": "Genie des Procedes"
        },
        "scrape_news": True,
        "scrape_faculty": True
    },
    "ens_rabat": {
        "name": "ENS Rabat",
        "homepage": "https://ens.um5.ac.ma",
        "news_url": "https://ens.um5.ac.ma/",
        "faculty_url": "https://ens.um5.ac.ma/annuaire-des-enseignants",
        "scrape_news": True,
        "scrape_faculty": True
    },
    "est_sale": {
        "name": "EST Salé",
        "homepage": "https://est.um5.ac.ma",
        "news_url": "https://est.um5.ac.ma/",
        "faculty_url": "https://est.um5.ac.ma/corps-professoral/",
        "scrape_news": True,
        "scrape_faculty": True
    }
}

# Configuration globale du scraper
SCRAPER_CONFIG = {
    "timeout": 45,
    "retry_attempts": 3,
    "retry_delay": 2,
    "request_delay": 1.0,
    "max_news_pages": 10,
    "verify_ssl": False,
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ==============================================================
# FONCTIONS UTILITAIRES
# ==============================================================

def generate_record_id(source_system: str, source_url: str, data: dict) -> str:
    """Génère un record_id unique pour traçabilité."""
    content_str = json.dumps(data, sort_keys=True)
    hash_obj = hashlib.sha256(content_str.encode())
    return f"{source_system}_{hash_obj.hexdigest()[:16]}"

def generate_content_hash(content: str) -> str:
    """Génère un hash SHA256 du contenu."""
    return hashlib.sha256(content.encode()).hexdigest()

def normalize_url(url: str) -> str:
    """Normalise une URL: enlève les fragments et le slash final."""
    if not url:
        return ""
    clean_url, _ = urldefrag(url)
    return clean_url.rstrip("/")

def get_date_partition() -> dict:
    """Retourne les composants de partitionnement date."""
    now = datetime.now()
    return {
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
        "timestamp": now.strftime("%Y%m%d_%H%M%S"),
        "iso": now.isoformat()
    }

def create_common_fields(source_system: str, source_url: str, data: dict) -> dict:
    """Ajoute les champs communs avec hash de contenu et support JSON-LD."""
    clean_data = {k: v for k, v in data.items() if k not in ['record_id', 'source_system', 'source_url']}
    content_json = json.dumps(clean_data, sort_keys=True)
    
    # Extraire le JSON-LD des métadonnées si présent
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
    
    # Ajouter JSON-LD si présent
    if json_ld:
        result["json_ld"] = json_ld
    
    return result

def create_session() -> requests.Session:
    """Crée une session HTTP avec headers."""
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
    """Effectue une requête avec retry logic."""
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
            
        except requests.exceptions.SSLError as e:
            logger.warning(f"SSL Error (tentative {attempt+1}/{max_retries}): {url}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                session.headers.update({
                    "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/12{attempt+1}.0.0.0 Safari/537.36"
                })
                continue
            return None
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f"Erreur connexion (tentative {attempt+1}/{max_retries}): {url}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            return None
            
        except Exception as e:
            logger.error(f"Erreur inattendue: {url} - {e}")
            return None
    
    return None

def is_valid_url(url: str) -> bool:
    """Vérifie si une URL est valide."""
    if not url:
        return False
    pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    return bool(re.match(pattern, url))

def validate_email(email: str) -> bool:
    """Valide une adresse email."""
    if not email:
        return False
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return bool(re.match(pattern, email))

# ==============================================================
# FONCTIONS DE SAUVEGARDE MINIO
# ==============================================================

def save_raw_html(source_name: str, url: str, html_content: str, page_type: str) -> None:
    """Sauvegarde le HTML brut dans MinIO avec métadonnées et JSON-LD."""
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]
        
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        file_name = f"{source_name}_{page_type}_{url_hash}_{timestamp}.html"
        
        object_path = f"source={source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/{file_name}"
        
        client.upload_binary(
            bucket_name="raw-web-html",
            object_name=object_path,
            data=html_content.encode('utf-8'),
            content_type="text/html"
        )
        
        # Extraction des métadonnées et JSON-LD
        soup = BeautifulSoup(html_content, 'html.parser')
        page_title = soup.find('title')
        page_title = page_title.get_text(strip=True) if page_title else ""
        
        # Extraction JSON-LD
        json_ld_data = []
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
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
            "timestamp": partition["iso"],
            "file_name": file_name,
            "content_hash": generate_content_hash(html_content),
            "size_bytes": len(html_content),
            "json_ld": json_ld_data if json_ld_data else None
        }
        
        metadata_path = f"source={source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/metadata_{page_type}_{timestamp}.json"
        client.upload_json(
            bucket_name="raw-web-html",
            object_name=metadata_path,
            data=metadata
        )
        
        logger.debug(f"HTML sauvegardé: {object_path}")
        
    except Exception as e:
        logger.error(f"Erreur sauvegarde HTML: {e}")

def save_image(image_url: str, source_name: str, image_name: str = None) -> bool:
    """
    Sauvegarde une image dans MinIO avec déduplication par hash et cache persistant.
    
    Args:
        image_url: URL de l'image
        source_name: Nom de la source
        image_name: Nom personnalisé pour l'image (optionnel)
    
    Returns:
        bool: True si sauvegarde réussie
    """
    global _image_cache
    
    if not is_valid_url(image_url):
        return False
    
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()
        
        session = create_session()
        response = safe_request(session, image_url)
        if not response:
            return False
        
        # Calculer le hash du contenu de l'image
        content_hash = hashlib.md5(response.content).hexdigest()
        
        # Vérifier dans le cache persistant
        cache_key = f"{source_name}_{content_hash}"
        if cache_key in _image_cache:
            logger.debug(f"Image déjà sauvegardée (cache): {image_url}")
            return True
        
        # Déterminer l'extension
        content_type = response.headers.get('content-type', 'image/jpeg')
        ext = '.jpg'  # par défaut
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
        
        # Générer un nom de fichier unique basé sur le hash
        if image_name:
            # Si un nom est fourni, on l'utilise comme base
            base_name = os.path.splitext(image_name)[0]
            file_name = f"{base_name}_{content_hash[:8]}{ext}"
        else:
            # Sinon, utiliser uniquement le hash
            file_name = f"image_{content_hash}{ext}"
        
        object_path = f"source={source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/{file_name}"
        
        # Sauvegarder l'image
        client.upload_binary(
            bucket_name="raw-images",
            object_name=object_path,
            data=response.content,
            content_type=content_type
        )
        
        # Métadonnées enrichies avec les hashes
        metadata = {
            "source_url": image_url,
            "source_name": source_name,
            "timestamp": partition["iso"],
            "file_name": file_name,
            "content_hash": content_hash,
            "sha256_hash": hashlib.sha256(response.content).hexdigest(),
            "size_bytes": len(response.content),
            "content_type": content_type,
            "original_url": image_url  # Pour traçabilité
        }
        
        metadata_path = f"source={source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/image_metadata_{partition['timestamp']}_{content_hash[:8]}.json"
        client.upload_json(
            bucket_name="raw-images",
            object_name=metadata_path,
            data=metadata
        )
        
        # Ajouter au cache persistant
        _image_cache.add(cache_key)
        save_image_cache(_image_cache)  # Sauvegarder immédiatement
        
        logger.debug(f"Image sauvegardée: {object_path} (hash: {content_hash[:8]})")
        return True
        
    except Exception as e:
        logger.debug(f"Erreur sauvegarde image {image_url}: {e}")
        return False

def save_document(document_url: str, source_name: str, document_name: str = None) -> bool:
    """
    Sauvegarde un document (PDF, DOC, DOCX, etc.) dans MinIO.
    
    Args:
        document_url: URL du document
        source_name: Nom de la source
        document_name: Nom personnalisé (optionnel)
    
    Returns:
        bool: True si sauvegarde réussie
    """
    if not is_valid_url(document_url):
        return False
    
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]
        
        session = create_session()
        response = safe_request(session, document_url)
        if not response:
            return False
        
        # Déterminer l'extension et le type MIME
        content_type = response.headers.get('content-type', '').lower()
        url_lower = document_url.lower()
        
        # Mapping des extensions
        extension_map = {
            'pdf': '.pdf',
            'doc': '.doc',
            'docx': '.docx',
            'xls': '.xls',
            'xlsx': '.xlsx',
            'ppt': '.ppt',
            'pptx': '.pptx',
            'csv': '.csv',
            'json': '.json',
            'zip': '.zip',
            'rar': '.rar',
            'txt': '.txt'
        }
        
        # Détection de l'extension
        ext = '.pdf'  # par défaut
        for key, value in extension_map.items():
            if key in url_lower or key in content_type:
                ext = value
                break
        
        # Nom du fichier
        if document_name:
            file_name = document_name if document_name.endswith(ext) else f"{document_name}{ext}"
        else:
            # Extraire le nom du fichier depuis l'URL
            base_name = document_url.split("/")[-1].split("?")[0]
            if base_name and '.' in base_name:
                file_name = base_name
            else:
                file_name = f"doc_{hashlib.md5(document_url.encode()).hexdigest()[:8]}_{timestamp}{ext}"
        
        # Sauvegarde
        object_path = f"source={source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/documents/{file_name}"
        
        client.upload_binary(
            bucket_name="raw-documents",
            object_name=object_path,
            data=response.content,
            content_type=content_type or 'application/octet-stream'
        )
        
        # Métadonnées du document
        metadata = {
            "source_url": document_url,
            "source_name": source_name,
            "timestamp": partition["iso"],
            "file_name": file_name,
            "content_hash": hashlib.sha256(response.content).hexdigest(),
            "size_bytes": len(response.content),
            "content_type": content_type,
            "extension": ext
        }
        
        metadata_path = f"source={source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/documents/metadata_{timestamp}_{hashlib.md5(document_url.encode()).hexdigest()[:8]}.json"
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

def save_structured_data(source_name: str, data_type: str, data_list: list) -> int:
    """Sauvegarde les données structurées dans MinIO avec validation."""
    if not data_list:
        return 0
    
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]
        
        # Validation et déduplication
        unique_data = []
        seen = set()
        valid_count = 0
        invalid_count = 0
        
        for item in data_list:
            cleaned_item = {k: v for k, v in item.items() if v is not None}
            
            # Validation basique
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
    """Sauvegarde les données consolidées."""
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]
        
        # News consolidées
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
                "source": "all_institutions",
                "table_type": "university_news",
                "scrape_timestamp": partition["iso"],
                "total_news": len(unique_news),
                "news_items": unique_news
            }
            
            object_path = f"all_institutions/year={partition['year']}/month={partition['month']}/day={partition['day']}/university_news_{timestamp}.json"
            client.upload_json(bucket_name="raw-json", object_name=object_path, data=news_data)
            logger.info(f"✅ {len(unique_news)} actualités consolidées sauvegardées")
        
        # Faculty consolidées
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
                "source": "all_institutions",
                "table_type": "faculty_profiles",
                "scrape_timestamp": partition["iso"],
                "total_faculty": len(unique_faculty),
                "faculty_members": unique_faculty
            }
            
            object_path = f"faculty_profiles/year={partition['year']}/month={partition['month']}/day={partition['day']}/faculty_profiles_{timestamp}.json"
            client.upload_json(bucket_name="raw-json", object_name=object_path, data=faculty_data)
            logger.info(f"✅ {len(unique_faculty)} profils enseignants consolidés sauvegardés")
            
    except Exception as e:
        logger.error(f"Erreur sauvegarde consolidée: {e}")

def save_stats(source_name: str, stats: dict) -> None:
    """Sauvegarde les statistiques du scraping."""
    try:
        client = MinIOClient(endpoint="university-minio:9000")
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
# FONCTIONS DE DÉCOUVERTE DES URLs
# ==============================================================

def discover_urls_from_homepage(homepage_url: str, session: requests.Session) -> dict:
    """Découvre les URLs depuis la page d'accueil."""
    discovered = {"news_url": None, "faculty_url": None}
    
    try:
        response = safe_request(session, homepage_url)
        if not response:
            return discovered
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Mots-clés
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
# SCRAPERS FACULTY
# ==============================================================

def scrape_fsjes_faculty(url: str, session: requests.Session) -> list:
    """Scrape les professeurs de la FSJES Agdal."""
    logger.info(f"  Scraping FSJES faculty from: {url}")
    faculty_list = []
    
    try:
        response = safe_request(session, url)
        if not response:
            return []
        
        save_raw_html("fsjes_agdal", url, response.text, "faculty")
        soup = BeautifulSoup(response.text, 'html.parser')
        email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
        
        # Méthode 1: gsc-team widgets
        team_widgets = soup.find_all('div', class_='widget gsc-team team-vertical-small')
        current_department = "General"
        
        for team_widget in team_widgets:
            # Département
            parent_col = team_widget.find_parent('div', class_=re.compile(r'gsc-column'))
            if parent_col:
                heading = parent_col.find_previous('div', class_='gsc-heading')
                if heading:
                    title_span = heading.find('span', class_='title')
                    if title_span:
                        dept_text = title_span.get_text(strip=True)
                        if 'DEPARTEMENT' in dept_text.upper():
                            current_department = dept_text.replace('DEPARTEMENT DE', '').replace('DEPARTMENT OF', '').strip()
            
            team_name_elem = team_widget.find('h3', class_='team-name')
            if not team_name_elem:
                continue
            
            full_name = team_name_elem.get_text(strip=True)
            full_name = re.sub(r'^(Pr|Prof\.?)\s+', '', full_name, flags=re.IGNORECASE)
            full_name = full_name.strip()
            
            if not full_name:
                continue
            
            # Email
            email = ""
            team_info = team_widget.find('div', class_='team-info')
            if team_info:
                info_text = team_info.get_text()
                email_match = email_pattern.search(info_text)
                if email_match:
                    email = email_match.group()
            
            if not email:
                for link in team_widget.find_all('a'):
                    link_text = link.get_text()
                    email_match = email_pattern.search(link_text)
                    if email_match:
                        email = email_match.group()
                        break
            
            if not email:
                widget_text = team_widget.get_text()
                email_match = email_pattern.search(widget_text)
                if email_match:
                    email = email_match.group()
            
            # Position
            position = team_widget.find('div', class_='team-position')
            if position:
                dept_from_position = position.get_text(strip=True)
                if dept_from_position and len(dept_from_position) > 2:
                    current_department = dept_from_position
            
            # Split name
            name_parts = full_name.split()
            if len(name_parts) >= 2:
                if name_parts[0].isupper() and len(name_parts) > 1:
                    last_name = name_parts[0]
                    first_name = ' '.join(name_parts[1:])
                else:
                    first_name = name_parts[0]
                    last_name = ' '.join(name_parts[1:])
            elif len(name_parts) == 1:
                first_name = name_parts[0]
                last_name = ""
            else:
                first_name = full_name
                last_name = ""
            
            faculty_list.append({
                "last_name": last_name.strip(),
                "first_name": first_name.strip(),
                "email": email,
                "department": current_department,
                "source_url": url,
                "institution": "Faculte des Sciences Juridiques, Economiques et Sociales (FSJES) Agdal"
            })
        
        # Méthode 2: Fallback
        if len(faculty_list) == 0:
            logger.info("      Méthode 2 fallback...")
            page_text = soup.get_text()
            lines = page_text.split('\n')
            current_dept = "General"
            
            for line in lines:
                line = line.strip()
                
                if 'DEPARTEMENT' in line.upper():
                    dept_match = re.search(r'DEPARTEMENT\s+DE\s+(.+?)(?:\s*$)', line, re.IGNORECASE)
                    if dept_match:
                        current_dept = dept_match.group(1).strip()
                    continue
                
                prof_match = re.match(r'^(Pr|Prof\.?|PES|PH|PA)\s+(.+)$', line, re.IGNORECASE)
                if prof_match:
                    full_name = prof_match.group(2).strip()
                    
                    email = ""
                    for i in range(1, 6):
                        if i < len(lines):
                            email_match = email_pattern.search(lines[i])
                            if email_match:
                                email = email_match.group()
                                break
                    
                    name_parts = full_name.split()
                    if len(name_parts) >= 2:
                        if name_parts[0].isupper() and len(name_parts) > 1:
                            last_name = name_parts[0]
                            first_name = ' '.join(name_parts[1:])
                        else:
                            first_name = name_parts[0]
                            last_name = ' '.join(name_parts[1:])
                    else:
                        first_name = full_name
                        last_name = ""
                    
                    faculty_list.append({
                        "last_name": last_name,
                        "first_name": first_name,
                        "email": email,
                        "department": current_dept,
                        "source_url": url,
                        "institution": "Faculte des Sciences Juridiques, Economiques et Sociales (FSJES) Agdal"
                    })
        
        # Déduplication
        unique_faculty = []
        seen = set()
        for faculty in faculty_list:
            key = f"{faculty['first_name']}_{faculty['last_name']}"
            if key not in seen and (faculty['first_name'] or faculty['last_name']):
                seen.add(key)
                unique_faculty.append(faculty)
        
        emails_found = sum(1 for f in unique_faculty if f['email'])
        logger.info(f"      ✅ {len(unique_faculty)} professeurs trouvés ({emails_found} avec email)")
        return unique_faculty
        
    except Exception as e:
        logger.error(f"      Erreur: {e}")
        return []

def scrape_emi_faculty(url: str, session: requests.Session, dept_name: str) -> list:
    """Scrape les professeurs d'un département EMI."""
    logger.info(f"  Scraping EMI department: {dept_name}")
    faculty_list = []
    
    try:
        response = safe_request(session, url)
        if not response:
            return []
        
        save_raw_html("emi", url, response.text, f"faculty_{dept_name.replace(' ', '_')}")
        soup = BeautifulSoup(response.text, 'html.parser')
        email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
        
        # Méthode 1: mailto links
        email_links = soup.find_all('a', href=re.compile(r'mailto:'))
        for link in email_links:
            email = link.get('href', '').replace('mailto:', '').strip()
            parent = link.find_parent(['td', 'li', 'div', 'p'])
            if parent:
                text = parent.get_text()
                names = re.findall(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)', text)
                if names:
                    name = names[0]
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
                        "department": dept_name,
                        "source_url": url,
                        "institution": "Ecole Mohammadia d'Ingenieurs (EMI)"
                    })
        
        # Méthode 2: Tables
        if not faculty_list:
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        for cell in cells:
                            email_match = email_pattern.search(cell.get_text())
                            if email_match:
                                email = email_match.group()
                                name_text = cells[0].get_text().strip()
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
                                    "department": dept_name,
                                    "source_url": url,
                                    "institution": "Ecole Mohammadia d'Ingenieurs (EMI)"
                                })
                                break
        
        # Méthode 3: Recherche directe
        if not faculty_list:
            page_text = soup.get_text()
            lines = page_text.split('\n')
            for line in lines:
                if '@' in line:
                    email_match = email_pattern.search(line)
                    if email_match:
                        email = email_match.group()
                        name_text = line[:email_match.start()].strip()
                        if name_text:
                            name_text = re.sub(r'[|,;:]', ' ', name_text).strip()
                            name_parts = name_text.split()
                            if len(name_parts) >= 2:
                                first_name = name_parts[0]
                                last_name = ' '.join(name_parts[1:])
                            elif len(name_parts) == 1:
                                first_name = name_parts[0]
                                last_name = ""
                            else:
                                first_name = ""
                                last_name = ""
                            if first_name:
                                faculty_list.append({
                                    "last_name": last_name,
                                    "first_name": first_name,
                                    "email": email,
                                    "department": dept_name,
                                    "source_url": url,
                                    "institution": "Ecole Mohammadia d'Ingenieurs (EMI)"
                                })
        
        logger.info(f"      ✅ {len(faculty_list)} professeurs trouvés dans {dept_name}")
        return faculty_list
        
    except Exception as e:
        logger.error(f"      Erreur: {e}")
        return []

def scrape_ens_faculty(url: str, session: requests.Session) -> list:
    """Scrape les professeurs de l'ENS Rabat (JavaScript)."""
    logger.info(f"  Scraping ENS faculty from: {url}")
    
    try:
        response = safe_request(session, url)
        if not response:
            return []
        
        save_raw_html("ens_rabat", url, response.text, "faculty")
        
        # Extraire les données JavaScript
        pattern = r'const originalData = (\[.*?\]);'
        match = re.search(pattern, response.text, re.DOTALL)
        
        if not match:
            logger.warning("      Données JavaScript non trouvées")
            return []
        
        js_array_str = match.group(1)
        js_array_str = js_array_str.replace("'", '"')
        js_array_str = js_array_str.replace('[at]', '@')
        js_array_str = re.sub(r',\s*}', '}', js_array_str)
        js_array_str = re.sub(r',\s*]', ']', js_array_str)
        
        try:
            faculty_data = json.loads(js_array_str)
        except json.JSONDecodeError:
            entries = re.findall(r"\{[^}]+\}", js_array_str)
            faculty_data = []
            for entry in entries:
                try:
                    clean_entry = entry.replace("'", '"')
                    clean_entry = re.sub(r',\s*}', '}', clean_entry)
                    faculty_data.append(json.loads(clean_entry))
                except:
                    pass
        
        formatted_faculty = []
        for faculty in faculty_data:
            formatted = {
                "last_name": faculty.get("Nom ", "").strip(),
                "first_name": faculty.get("Prénom", "").strip(),
                "department": faculty.get("Département", "").strip(),
                "email": faculty.get("Email Institutionnel", "").strip(),
                "source_url": url,
                "institution": "Ecole Normale Superieure (ENS) Rabat"
            }
            if formatted["last_name"] or formatted["first_name"]:
                formatted_faculty.append(formatted)
        
        logger.info(f"      ✅ {len(formatted_faculty)} professeurs trouvés")
        return formatted_faculty
        
    except Exception as e:
        logger.error(f"      Erreur: {e}")
        return []

def scrape_est_faculty(url: str, session: requests.Session) -> list:
    """Scrape les professeurs de l'EST Salé (table HTML)."""
    logger.info(f"  Scraping EST faculty from: {url}")
    faculty_list = []
    
    try:
        response = safe_request(session, url)
        if not response:
            return []
        
        save_raw_html("est_sale", url, response.text, "faculty")
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='two-per-line')
        
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if cells:
                    name = cells[0].get_text(strip=True)
                    if name and len(name) > 2:
                        name = re.sub(r'^(Pr|Prof\.?)\s+', '', name, flags=re.IGNORECASE)
                        name = name.strip()
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
                            "email": "",
                            "department": "General",
                            "source_url": url,
                            "institution": "Ecole Superieure de Technologie (EST) Sale"
                        })
        
        logger.info(f"      ✅ {len(faculty_list)} professeurs trouvés")
        return faculty_list
        
    except Exception as e:
        logger.error(f"      Erreur: {e}")
        return []

# ==============================================================
# SCRAPERS NEWS
# ==============================================================

def scrape_fsjes_news(url: str, session: requests.Session) -> list:
    """Scrape les actualités FSJES avec pagination."""
    logger.info(f"  Scraping FSJES actualités from: {url}")
    all_news = []
    page = 0
    max_pages = SCRAPER_CONFIG["max_news_pages"]
    
    while page < max_pages:
        try:
            page_url = url if page == 0 else f"{url}?page={page}"
            response = safe_request(session, page_url)
            if not response:
                break
            
            save_raw_html("fsjes_agdal", page_url, response.text, "news")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            articles = soup.find_all('article', class_='node--type-article')
            if not articles:
                articles = soup.find_all('div', class_='item')
            
            if not articles:
                break
            
            page_news = []
            for article in articles:
                news_item = extract_fsjes_news_article(article, session)
                if news_item:
                    page_news.append(news_item)
            
            if not page_news:
                break
            
            all_news.extend(page_news)
            logger.info(f"    Page {page + 1}: {len(page_news)} articles")
            
            next_link = soup.find('a', rel='next')
            if not next_link:
                break
            
            page += 1
            time.sleep(SCRAPER_CONFIG["request_delay"])
            
        except Exception as e:
            logger.error(f"    Erreur page {page + 1}: {e}")
            break
    
    return all_news

def extract_fsjes_news_article(article, session: requests.Session) -> dict:
    """Extrait les informations d'un article FSJES avec documents."""
    try:
        title_elem = article.find('h3', class_='post-title')
        if not title_elem:
            link = article.find('a')
            if link:
                title = link.get_text(strip=True)
                href = link.get('href', '')
                url_article = f"https://fsjes-agdal.um5.ac.ma{href}" if href else ""
            else:
                return None
        else:
            link = title_elem.find('a')
            if link:
                title = link.get_text(strip=True)
                href = link.get('href', '')
                url_article = f"https://fsjes-agdal.um5.ac.ma{href}" if href else ""
            else:
                title = title_elem.get_text(strip=True)
                url_article = ""
        
        if not title:
            return None
        
        date_text = ""
        date_elem = article.find('span', class_='post-created')
        if date_elem:
            date_text = date_elem.get_text(strip=True)
            date_match = re.search(r'(\d{2})/(\d{2})/(\d{4})', date_text)
            if date_match:
                day, month, year = date_match.groups()
                date_text = f"{year}-{month}-{day}"
        
        image_url = ""
        img_elem = article.find('img')
        if img_elem and img_elem.get('src'):
            img_src = img_elem.get('src')
            if img_src.startswith('/'):
                image_url = f"https://fsjes-agdal.um5.ac.ma{img_src}"
            else:
                image_url = img_src
            if image_url and is_valid_url(image_url):
                save_image(image_url, "fsjes_agdal", f"fsjes_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}")
        
        # Détection des documents dans l'article
        document_urls = []
        for link in article.find_all('a', href=True):
            href = link['href']
            if href:
                full_url = urljoin("https://fsjes-agdal.um5.ac.ma", href)
                doc_extensions = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.csv', '.zip')
                if full_url.lower().endswith(doc_extensions):
                    document_urls.append(full_url)
                    save_document(full_url, "fsjes_agdal")
        
        category = "Actualités"
        cat_elem = article.find('span', class_='post-categories')
        if cat_elem:
            cat_text = cat_elem.get_text(strip=True)
            if cat_text:
                category = cat_text
        
        return {
            "title": title,
            "url": url_article,
            "publication_date": date_text,
            "image_url": image_url if is_valid_url(image_url) else "",
            "category": category,
            "source": "FSJES Agdal",
            "institution": "FSJES Agdal",
            "documents": document_urls
        }
    except Exception as e:
        return None

def scrape_fsjes_avis(url: str, session: requests.Session) -> list:
    """Scrape les avis FSJES."""
    logger.info(f"  Scraping FSJES avis from: {url}")
    news_list = []
    
    try:
        response = safe_request(session, url)
        if not response:
            return []
        
        save_raw_html("fsjes_agdal", url, response.text, "avis")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        sections = [
            ("Licence", "avis-licence"),
            ("Master", "avis-master"),
            ("Doctorat", "avis-doctorat"),
            ("Soutenances", "avis-soutenances"),
        ]
        
        for section_name, section_class in sections:
            section = soup.find('div', class_=re.compile(section_class.replace('-', '_')))
            if not section:
                section = soup.find('div', id=re.compile(section_class))
            
            if not section:
                continue
            
            items = section.find_all('li', class_='view-list-item')
            if not items:
                items = section.find_all('div', class_='views-row')
            
            for item in items:
                date_text = ""
                date_elem = item.find('div', class_='views-field-created')
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    date_match = re.search(r'(\d{2})-(\d{2})-(\d{4})', date_text)
                    if date_match:
                        day, month, year = date_match.groups()
                        date_text = f"{year}-{month}-{day}"
                
                title_elem = item.find('div', class_='views-field-title')
                if title_elem:
                    link = title_elem.find('a')
                    if link:
                        title = link.get_text(strip=True)
                        href = link.get('href', '')
                        url_article = f"https://fsjes-agdal.um5.ac.ma{href}" if href else ""
                        news_list.append({
                            "title": title,
                            "url": url_article,
                            "publication_date": date_text,
                            "image_url": "",
                            "category": f"Avis {section_name}",
                            "source": "FSJES Agdal",
                            "institution": "FSJES Agdal",
                            "documents": []
                        })
        
        logger.info(f"      ✅ {len(news_list)} avis trouvés")
        return news_list
        
    except Exception as e:
        logger.error(f"      Erreur: {e}")
        return []

def scrape_emi_news(url: str, session: requests.Session) -> list:
    """Scrape les actualités EMI."""
    logger.info(f"  Scraping EMI actualités from: {url}")
    news_list = []
    
    try:
        response = safe_request(session, url)
        if not response:
            return []
        
        save_raw_html("emi", url, response.text, "news")
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('li', class_='wp-block-post')
        if not articles:
            articles = soup.find_all('article', class_='post')
        
        for article in articles:
            title = ""
            url_article = ""
            
            title_elem = article.find('h2', class_='wp-block-post-title')
            if not title_elem:
                title_elem = article.find('h2') or article.find('h3')
            
            if title_elem:
                link = title_elem.find('a')
                if link:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    url_article = href if href.startswith('http') else f"https://www.emi.ac.ma{href}"
                else:
                    title = title_elem.get_text(strip=True)
            
            if not title:
                link = article.find('a')
                if link:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    url_article = href if href.startswith('http') else f"https://www.emi.ac.ma{href}"
            
            if not title:
                continue
            
            date_text = ""
            date_elem = article.find('time') or article.find('span', class_=re.compile(r'date|posted'))
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', date_text)
                if not date_match:
                    date_match = re.search(r'(\d{2})/(\d{2})/(\d{4})', date_text)
                if date_match:
                    groups = date_match.groups()
                    if len(groups) == 3:
                        if len(groups[0]) == 4:
                            year, month, day = groups
                        else:
                            day, month, year = groups
                        date_text = f"{year}-{month}-{day}"
            
            image_url = ""
            img_elem = article.find('img')
            if img_elem and img_elem.get('src'):
                img_src = img_elem.get('src')
                if img_src.startswith('/'):
                    image_url = f"https://www.emi.ac.ma{img_src}"
                else:
                    image_url = img_src
                if image_url and is_valid_url(image_url):
                    save_image(image_url, "emi", f"emi_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}")
            
            # Détection des documents dans l'article
            document_urls = []
            for link in article.find_all('a', href=True):
                href = link['href']
                if href:
                    full_url = urljoin("https://www.emi.ac.ma", href)
                    doc_extensions = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.csv', '.zip')
                    if full_url.lower().endswith(doc_extensions):
                        document_urls.append(full_url)
                        save_document(full_url, "emi")
            
            category = "Actualités"
            cat_elem = article.find('span', class_=re.compile(r'category|cat'))
            if cat_elem:
                cat_text = cat_elem.get_text(strip=True)
                if cat_text:
                    category = cat_text
            
            news_list.append({
                "title": title,
                "url": url_article,
                "publication_date": date_text,
                "image_url": image_url if is_valid_url(image_url) else "",
                "category": category,
                "source": "EMI",
                "institution": "EMI",
                "documents": document_urls
            })
        
        logger.info(f"      ✅ {len(news_list)} actualités trouvées")
        return news_list
        
    except Exception as e:
        logger.error(f"      Erreur: {e}")
        return []

def scrape_ens_news(url: str, session: requests.Session) -> list:
    """Scrape les actualités ENS Rabat."""
    logger.info(f"  Scraping ENS actualités from: {url}")
    news_list = []
    
    try:
        response = safe_request(session, url)
        if not response:
            return []
        
        save_raw_html("ens_rabat", url, response.text, "news")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Carrousel
        carousel_items = soup.find_all('div', class_='item', recursive=True)
        for item in carousel_items:
            link = item.find('a')
            if link:
                title = link.get_text(strip=True)
                href = link.get('href', '')
                url_article = f"https://ens.um5.ac.ma{href}" if href else ""
                
                image_url = ""
                img = item.find('img')
                if img and img.get('src'):
                    img_src = img.get('src')
                    if img_src.startswith('/'):
                        image_url = f"https://ens.um5.ac.ma{img_src}"
                    else:
                        image_url = img_src
                    if image_url and is_valid_url(image_url):
                        save_image(image_url, "ens_rabat", f"ens_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}")
                
                date_text = ""
                date_elem = item.find('span', class_=re.compile(r'date|created|changed'))
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                
                # Détection des documents
                document_urls = []
                for doc_link in item.find_all('a', href=True):
                    href = doc_link['href']
                    if href:
                        full_url = urljoin("https://ens.um5.ac.ma", href)
                        doc_extensions = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.csv', '.zip')
                        if full_url.lower().endswith(doc_extensions):
                            document_urls.append(full_url)
                            save_document(full_url, "ens_rabat")
                
                if title and len(title) > 10:
                    news_list.append({
                        "title": title,
                        "url": url_article,
                        "publication_date": date_text,
                        "image_url": image_url if is_valid_url(image_url) else "",
                        "category": "Actualités",
                        "source": "ENS Rabat",
                        "institution": "ENS Rabat",
                        "documents": document_urls
                    })
        
        # Ticker
        ticker_items = soup.find_all('ul', class_='news-ticker-h')
        if ticker_items:
            ticker = ticker_items[0]
            items = ticker.find_all('li')
            for item in items:
                link = item.find('a')
                if link:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    if href.startswith('/sites/default/files/'):
                        url_article = f"https://ens.um5.ac.ma{href}"
                    elif href:
                        url_article = f"https://ens.um5.ac.ma{href}" if href.startswith('/') else href
                    else:
                        url_article = ""
                    
                    if title and len(title) > 5:
                        news_list.append({
                            "title": title,
                            "url": url_article,
                            "publication_date": "",
                            "image_url": "",
                            "category": "Avis",
                            "source": "ENS Rabat",
                            "institution": "ENS Rabat",
                            "documents": []
                        })
        
        logger.info(f"      ✅ {len(news_list)} actualités trouvées")
        return news_list
        
    except Exception as e:
        logger.error(f"      Erreur: {e}")
        return []

def scrape_est_news(url: str, session: requests.Session) -> list:
    """Scrape les actualités EST Salé."""
    logger.info(f"  Scraping EST actualités from: {url}")
    news_list = []
    
    try:
        response = safe_request(session, url)
        if not response:
            return []
        
        save_raw_html("est_sale", url, response.text, "news")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        articles = soup.find_all('div', class_='thim-ekits-post__article')
        if not articles:
            articles = soup.find_all('article', class_='post')
        if not articles:
            articles = soup.find_all('div', class_=re.compile(r'post|entry|item'))
        
        for article in articles:
            title = ""
            url_article = ""
            
            title_elem = article.find('p', class_='thim-ekits-post__title')
            if title_elem:
                link = title_elem.find('a')
                if link:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    url_article = href if href.startswith('http') else f"https://est.um5.ac.ma{href}"
            
            if not title:
                title_elem = article.find('h3') or article.find('h2')
                if title_elem:
                    link = title_elem.find('a')
                    if link:
                        title = link.get_text(strip=True)
                        href = link.get('href', '')
                        url_article = href if href.startswith('http') else f"https://est.um5.ac.ma{href}"
            
            if not title:
                link = article.find('a')
                if link:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    url_article = href if href.startswith('http') else f"https://est.um5.ac.ma{href}"
            
            if not title:
                continue
            
            date_text = ""
            date_elem = article.find('span', class_='thim-ekits-post__date')
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                date_match = re.search(r'(\d{1,2})\s+(\w+)\.?\s+(\d{2,4})', date_text)
                if date_match:
                    day, month, year = date_match.groups()
                    months = {
                        'Jan': '01', 'Fév': '02', 'Mar': '03', 'Avr': '04', 'Mai': '05', 'Juin': '06',
                        'Jul': '07', 'Aoû': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Déc': '12'
                    }
                    month_num = months.get(month[:3], '01')
                    year_full = f"20{year}" if len(year) == 2 else year
                    date_text = f"{year_full}-{month_num}-{day.zfill(2)}"
            
            image_url = ""
            img_elem = article.find('img')
            if img_elem and img_elem.get('src'):
                img_src = img_elem.get('src')
                if img_src.startswith('/'):
                    image_url = f"https://est.um5.ac.ma{img_src}"
                else:
                    image_url = img_src
                if image_url and is_valid_url(image_url):
                    save_image(image_url, "est_sale", f"est_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}")
            
            # Détection des documents
            document_urls = []
            for link in article.find_all('a', href=True):
                href = link['href']
                if href:
                    full_url = urljoin("https://est.um5.ac.ma", href)
                    doc_extensions = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.csv', '.zip')
                    if full_url.lower().endswith(doc_extensions):
                        document_urls.append(full_url)
                        save_document(full_url, "est_sale")
            
            category = "Actualités"
            if 'evenement' in url_article.lower() or 'événement' in title.lower():
                category = "Événements"
            elif 'appel' in title.lower() or 'candidature' in title.lower() or 'concours' in title.lower():
                category = "Appels d'Offres"
            
            news_list.append({
                "title": title,
                "url": url_article,
                "publication_date": date_text,
                "image_url": image_url if is_valid_url(image_url) else "",
                "category": category,
                "source": "EST Sale",
                "institution": "EST Sale",
                "documents": document_urls
            })
        
        logger.info(f"      ✅ {len(news_list)} actualités trouvées")
        return news_list
        
    except Exception as e:
        logger.error(f"      Erreur: {e}")
        return []

# ==============================================================
# MAIN FUNCTION - SEMAINE 1
# ==============================================================

def run():
    """
    Exécute le scraper pour la semaine 1.
    - Scraping des sites configurés
    - Sauvegarde en MinIO
    - Statistiques et logs
    """
    session = create_session()
    partition = get_date_partition()
    
    logger.info("="*70)
    logger.info("🚀 SCRAPER UNIVERSITAIRE - SEMAINE 1")
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
        
        # === 1. PAGE D'ACCUEIL ===
        logger.info("\n  🏠 Page d'accueil...")
        homepage_response = safe_request(session, site_config["homepage"])
        if homepage_response:
            save_raw_html(site_key, site_config["homepage"], homepage_response.text, "homepage")
            stats["pages_visited"] += 1
            logger.info("      ✅ Page d'accueil sauvegardée")
            
            # Découverte des URLs
            logger.info("  🔍 Découverte des URLs...")
            discovered = discover_urls_from_homepage(site_config["homepage"], session)
            
            if discovered["news_url"] and not site_config.get("news_url"):
                site_config["news_url"] = discovered["news_url"]
                logger.info(f"      ✅ URL actualités: {discovered['news_url']}")
            
            if discovered["faculty_url"] and not site_config.get("faculty_url"):
                site_config["faculty_url"] = discovered["faculty_url"]
                logger.info(f"      ✅ URL faculté: {discovered['faculty_url']}")
        
        # === 2. NEWS ===
        if site_config.get("scrape_news", True) and site_config.get("news_url"):
            logger.info("\n  📰 Scraping actualités...")
            
            try:
                if site_key == "fsjes_agdal":
                    news = scrape_fsjes_news(site_config["news_url"], session)
                    source_news.extend(news)
                    
                    if site_config.get("avis_url"):
                        avis = scrape_fsjes_avis(site_config["avis_url"], session)
                        source_news.extend(avis)
                        
                elif site_key == "emi":
                    news = scrape_emi_news(site_config["news_url"], session)
                    source_news.extend(news)
                    
                elif site_key == "ens_rabat":
                    news = scrape_ens_news(site_config["news_url"], session)
                    source_news.extend(news)
                    
                elif site_key == "est_sale":
                    news = scrape_est_news(site_config["news_url"], session)
                    source_news.extend(news)
                
                if source_news:
                    saved = save_structured_data(site_key, "news", source_news)
                    all_news.extend(source_news)
                    stats["news_found"] = len(source_news)
                    
            except Exception as e:
                logger.error(f"      ❌ Erreur scraping news: {e}")
                stats["errors"] += 1
        
        # === 3. FACULTY ===
        if site_config.get("scrape_faculty", True):
            logger.info("\n  👨‍🏫 Scraping faculty...")
            
            try:
                if site_key == "fsjes_agdal" and site_config.get("faculty_url"):
                    faculty = scrape_fsjes_faculty(site_config["faculty_url"], session)
                    source_faculty.extend(faculty)
                    
                elif site_key == "emi" and site_config.get("faculty_urls"):
                    for url, dept_name in site_config["faculty_urls"].items():
                        dept_faculty = scrape_emi_faculty(url, session, dept_name)
                        source_faculty.extend(dept_faculty)
                        
                elif site_key == "ens_rabat" and site_config.get("faculty_url"):
                    faculty = scrape_ens_faculty(site_config["faculty_url"], session)
                    source_faculty.extend(faculty)
                    
                elif site_key == "est_sale" and site_config.get("faculty_url"):
                    faculty = scrape_est_faculty(site_config["faculty_url"], session)
                    source_faculty.extend(faculty)
                
                if source_faculty:
                    saved = save_structured_data(site_key, "faculty", source_faculty)
                    all_faculty.extend(source_faculty)
                    stats["faculty_found"] = len(source_faculty)
                    
            except Exception as e:
                logger.error(f"      ❌ Erreur scraping faculty: {e}")
                stats["errors"] += 1
        
        # === 4. STATISTIQUES ===
        save_stats(site_key, stats)
        all_stats[site_key] = stats
        
        logger.info(f"\n  ✅ {site_config['name']} terminé:")
        logger.info(f"     - News: {len(source_news)}")
        logger.info(f"     - Faculty: {len(source_faculty)}")
        logger.info(f"     - Erreurs: {stats['errors']}")
    
    # === 5. SAUVEGARDE CONSOLIDÉE ===
    logger.info("\n" + "="*70)
    logger.info("💾 SAUVEGARDE CONSOLIDÉE")
    logger.info("="*70)
    
    save_consolidated_data(all_news, all_faculty)
    
    # === 6. RÉSUMÉ FINAL ===
    logger.info("\n" + "="*70)
    logger.info("📊 RÉSUMÉ FINAL - SEMAINE 1")
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
        logger.info(f"  ✅ {name}: {stats['news_found']} news, {stats['faculty_found']} faculty")
    
    logger.info("\n📦 Structure MinIO (Semaine 1):")
    logger.info("  📁 raw-web-html/     → HTML brut des pages")
    logger.info("  📁 raw-images/       → Images extraites (dédupliquées par hash)")
    logger.info("  📁 raw-documents/    → Documents (PDF, DOC, etc.)")
    logger.info("  📁 raw-json/         → Données structurées")
    logger.info("    ├── source=*/news_*.json     ← News par source")
    logger.info("    ├── source=*/faculty_*.json  ← Faculty par source")
    logger.info("    ├── all_institutions/university_news_*.json   ← News consolidées")
    logger.info("    └── faculty_profiles/faculty_profiles_*.json  ← Faculty consolidé")
    logger.info("  📁 raw-logs/         → Logs et statistiques")
    logger.info("="*70)
    
    # Afficher les stats du cache
    logger.info(f"\n💾 Cache des images: {len(_image_cache)} entrées")
    logger.info(f"📁 Fichier cache: {IMAGE_CACHE_FILE}")
    
    logger.info("\n✅ SCRAPING SEMAINE 1 TERMINÉ!")

if __name__ == "__main__":
    run()


# had ali khdam 
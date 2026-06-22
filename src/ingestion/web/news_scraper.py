import re
import json
import hashlib
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from src.storage.minio.sara_client import MinIOClient


# ==============================================================
# FONCTIONS UTILITAIRES POUR LES MÉTADONNÉES
# ==============================================================
def generate_record_id(source_system: str, source_url: str, data: dict) -> str:
    """Génère un record_id unique pour traçabilité."""
    content_str = json.dumps(data, sort_keys=True)
    hash_obj = hashlib.sha256(content_str.encode())
    return f"{source_system}_{hash_obj.hexdigest()[:16]}"


def create_common_fields(source_system: str, source_url: str, data: dict) -> dict:
    """Ajoute les champs communs requis par le storage design."""
    clean_data = {k: v for k, v in data.items() if k not in ['record_id', 'source_system', 'source_url']}
    
    return {
        "record_id": generate_record_id(source_system, source_url, clean_data),
        "source_system": source_system,
        "source_url": source_url,
        "content_hash": hashlib.sha256(json.dumps(clean_data, sort_keys=True).encode()).hexdigest(),
        "crawl_timestamp": datetime.now().isoformat(),
        "business_timestamp": datetime.now().isoformat(),
        "is_deleted": False,
        "language": "fr",
        "normalized_text": "",
        **data
    }


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


def save_raw_html(source_name: str, url: str, html_content: str, page_type: str = "general") -> None:
    """
    Sauvegarde le HTML brut dans le bucket raw-web-html.
    
    Args:
        source_name: Nom de la source (ex: est_sale, fsjes_agdal)
        url: URL de la page
        html_content: Contenu HTML
        page_type: Type de page (faculty, news, home, avis, etc.)
    """
    client = MinIOClient(endpoint="localhost:9000")
    partition = get_date_partition()
    timestamp = partition["timestamp"]
    
    # Générer un nom de fichier basé sur l'URL
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    file_name = f"{source_name}_{page_type}_{url_hash}_{timestamp}.html"
    
    object_path = f"{source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/{file_name}"
    
    # Sauvegarder le HTML
    client.upload_binary(
        bucket_name="raw-web-html",
        object_name=object_path,
        data=html_content.encode('utf-8'),
        content_type="text/html"
    )
    
    # Sauvegarder les métadonnées du HTML avec le type de page
    metadata = {
        "source_url": url,
        "source_name": source_name,
        "page_type": page_type,
        "timestamp": partition["iso"],
        "file_name": file_name,
        "content_hash": hashlib.sha256(html_content.encode()).hexdigest(),
        "size_bytes": len(html_content)
    }
    
    metadata_path = f"{source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/metadata_{page_type}_{timestamp}.json"
    client.upload_json(
        bucket_name="raw-web-html",
        object_name=metadata_path,
        data=metadata
    )
    
    print(f"      HTML ({page_type}) saved: {object_path}")


def save_image(image_url: str, source_name: str, image_name: str = None) -> None:
    """
    Sauvegarde une image dans le bucket raw-images.
    
    Args:
        image_url: URL de l'image
        source_name: Nom de la source
        image_name: Nom personnalisé pour l'image (optionnel)
    """
    try:
        client = MinIOClient(endpoint="localhost:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]
        
        # Télécharger l'image
        response = requests.get(image_url, timeout=30, verify=False)
        response.raise_for_status()
        
        # Déterminer le nom du fichier
        if image_name:
            file_name = image_name
        else:
            # Extraire le nom du fichier depuis l'URL
            file_name = image_url.split("/")[-1]
            if not file_name or '.' not in file_name:
                file_name = f"image_{hashlib.md5(image_url.encode()).hexdigest()[:8]}_{timestamp}.jpg"
        
        # Déterminer le content-type
        content_type = response.headers.get('content-type', 'image/jpeg')
        
        object_path = f"{source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/{file_name}"
        
        client.upload_binary(
            bucket_name="raw-images",
            object_name=object_path,
            data=response.content,
            content_type=content_type
        )
        
        # Sauvegarder les métadonnées de l'image
        metadata = {
            "source_url": image_url,
            "source_name": source_name,
            "timestamp": partition["iso"],
            "file_name": file_name,
            "content_hash": hashlib.sha256(response.content).hexdigest(),
            "size_bytes": len(response.content),
            "content_type": content_type
        }
        
        metadata_path = f"{source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/image_metadata_{timestamp}.json"
        client.upload_json(
            bucket_name="raw-images",
            object_name=metadata_path,
            data=metadata
        )
        
        print(f"      Image saved: {object_path}")
        
    except Exception as e:
        print(f"      Warning: Could not save image {image_url}: {e}")


# ==============================================================
# SCRAPER 1: FSJES AGDAL - TOUTES LES ACTUALITÉS (PAGE TAXONOMY)
# ==============================================================
def scrape_fsjes_all_actualites(url: str, session: requests.Session = None, max_pages: int = 10) -> list:
    """
    Scrape toutes les actualités depuis la page taxonomy/term/26 avec pagination.
    """
    if session is None:
        session = requests.Session()
    
    print(f"  Scraping FSJES actualités from: {url}")
    all_news = []
    page = 0
    
    while page < max_pages:
        try:
            if page == 0:
                page_url = url
            else:
                page_url = f"{url}?page={page}"
            
            print(f"    Page {page + 1}: {page_url}")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }
            
            response = session.get(page_url, timeout=30, verify=False, headers=headers)
            response.raise_for_status()
            
            # Sauvegarder le HTML brut
            save_raw_html("fsjes_agdal", page_url, response.text, "news")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            articles = soup.find_all('article', class_='node--type-article')
            
            if not articles:
                articles = soup.find_all('div', class_='item')
            
            if not articles:
                print(f"    Plus d'articles trouvés à la page {page + 1}")
                break
            
            page_news = []
            for article in articles:
                news_item = extract_fsjes_news_from_article(article, session)
                if news_item:
                    page_news.append(news_item)
            
            if not page_news:
                print(f"    Aucun article extrait de la page {page + 1}")
                break
            
            all_news.extend(page_news)
            print(f"    Page {page + 1}: {len(page_news)} articles trouvés")
            
            next_link = soup.find('a', rel='next')
            if not next_link:
                print(f"    Dernière page atteinte")
                break
            
            page += 1
            
        except Exception as e:
            print(f"    Error scraping page {page + 1}: {e}")
            break
    
    return all_news


def extract_fsjes_news_from_article(article, session: requests.Session = None) -> dict:
    """Extrait les informations d'un article FSJES."""
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
            
            # Sauvegarder l'image
            if image_url:
                img_name = f"fsjes_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}.jpg"
                save_image(image_url, "fsjes_agdal", img_name)
        
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
            "image_url": image_url,
            "category": category,
            "source": "FSJES Agdal",
            "institution": "FSJES Agdal"
        }
        
    except Exception as e:
        return None


# ==============================================================
# SCRAPER 2: FSJES AGDAL - AVIS
# ==============================================================
def scrape_fsjes_avis(url_base: str, session: requests.Session = None) -> list:
    """Scrape les avis (annonces) depuis les sections Licence, Master, Doctorat, Soutenances."""
    if session is None:
        session = requests.Session()
    
    print(f"  Scraping FSJES avis from: {url_base}")
    news_list = []
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = session.get(url_base, timeout=30, verify=False, headers=headers)
        response.raise_for_status()
        
        # Sauvegarder le HTML brut
        save_raw_html("fsjes_agdal", url_base, response.text, "avis")
        
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
                            "institution": "FSJES Agdal"
                        })
        
        print(f"      Found {len(news_list)} avis from FSJES")
        return news_list
        
    except Exception as e:
        print(f"      Error scraping FSJES avis: {e}")
        return []


# ==============================================================
# SCRAPER 3: EMI - ACTUALITÉS
# ==============================================================
def scrape_emi_actualites(url: str, session: requests.Session = None) -> list:
    """Scrape les actualités depuis la page Actualités de l'EMI."""
    if session is None:
        session = requests.Session()
    
    print(f"  Scraping EMI actualités from: {url}")
    news_list = []
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = session.get(url, timeout=30, verify=False, headers=headers)
        response.raise_for_status()
        
        # Sauvegarder le HTML brut
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
                
                # Sauvegarder l'image
                if image_url:
                    img_name = f"emi_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}.jpg"
                    save_image(image_url, "emi", img_name)
            
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
                "image_url": image_url,
                "category": category,
                "source": "EMI",
                "institution": "EMI"
            })
        
        print(f"      Found {len(news_list)} actualités from EMI")
        return news_list
        
    except Exception as e:
        print(f"      Error scraping EMI: {e}")
        return []


# ==============================================================
# SCRAPER 4: ENS RABAT - ACTUALITÉS (CARROUSEL + TICKER)
# ==============================================================
def scrape_ens_actualites(url: str, session: requests.Session = None, max_retries: int = 3) -> list:
    """
    Scrape les actualités depuis la page d'accueil de l'ENS Rabat.
    Récupère les articles du carrousel "Actualités" et du ticker "Actualités et Avis".
    """
    if session is None:
        session = requests.Session()
    
    print(f"  Scraping ENS actualités from: {url}")
    news_list = []
    
    for attempt in range(max_retries):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0",
            }
            
            if attempt > 0:
                headers["User-Agent"] = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/12{attempt+1}.0.0.0 Safari/537.36"
            
            response = session.get(url, timeout=30, verify=False, headers=headers)
            response.raise_for_status()
            
            # Sauvegarder le HTML brut
            save_raw_html("ens_rabat", url, response.text, "news")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # === MÉTHODE 1: Carrousel "Actualités" ===
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
                        
                        if image_url:
                            img_name = f"ens_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}.jpg"
                            save_image(image_url, "ens_rabat", img_name)
                    
                    date_text = ""
                    date_elem = item.find('span', class_=re.compile(r'date|created|changed'))
                    if date_elem:
                        date_text = date_elem.get_text(strip=True)
                    
                    if title and len(title) > 10:
                        news_list.append({
                            "title": title,
                            "url": url_article,
                            "publication_date": date_text,
                            "image_url": image_url,
                            "category": "Actualités",
                            "source": "ENS Rabat",
                            "institution": "ENS Rabat"
                        })
            
            # === MÉTHODE 2: Ticker "Actualités et Avis" ===
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
                                "institution": "ENS Rabat"
                            })
            
            print(f"      Found {len(news_list)} actualités from ENS")
            return news_list
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print(f"      Tentative {attempt + 1}/{max_retries}: Accès refusé (403), nouvelle tentative...")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    print(f"      Échec après {max_retries} tentatives pour ENS")
                    return []
            else:
                print(f"      Error scraping ENS: {e}")
                return []
        except Exception as e:
            print(f"      Error scraping ENS: {e}")
            return []
    
    return []


# ==============================================================
# SCRAPER 5: EST SALÉ - ACTUALITÉS
# ==============================================================
def scrape_est_actualites(url: str, session: requests.Session = None) -> list:
    """
    Scrape les actualités depuis la page d'accueil de l'EST Salé.
    Récupère les articles des sections "Actualités", "Évènements" et "Appels d'Offres".
    """
    if session is None:
        session = requests.Session()
    
    print(f"  Scraping EST actualités from: {url}")
    news_list = []
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = session.get(url, timeout=30, verify=False, headers=headers)
        response.raise_for_status()
        
        # Sauvegarder le HTML brut
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
                
                if image_url:
                    img_name = f"est_news_{hashlib.md5(image_url.encode()).hexdigest()[:8]}.jpg"
                    save_image(image_url, "est_sale", img_name)
            
            category = "Actualités"
            if 'evenement' in url_article.lower() or 'événement' in title.lower():
                category = "Événements"
            elif 'appel' in title.lower() or 'candidature' in title.lower() or 'concours' in title.lower():
                category = "Appels d'Offres"
            
            news_list.append({
                "title": title,
                "url": url_article,
                "publication_date": date_text,
                "image_url": image_url,
                "category": category,
                "source": "EST Sale",
                "institution": "EST Sale"
            })
        
        print(f"      Found {len(news_list)} actualités from EST")
        return news_list
        
    except Exception as e:
        print(f"      Error scraping EST: {e}")
        return []


# ==============================================================
# MAIN FUNCTION - ALL NEWS SCRAPER
# ==============================================================
def run():
    """Scrape toutes les actualités de toutes les institutions avec buckets MinIO."""
    
    client = MinIOClient(endpoint="localhost:9000")
    partition = get_date_partition()
    timestamp = partition["timestamp"]
    
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Academic Scraper/1.0)"})
    
    print("="*60)
    print("UNIVERSITY NEWS SCRAPER (FSJES + EMI + ENS + EST)")
    print("="*60)
    print(f"Date: {partition['year']}-{partition['month']}-{partition['day']}")
    print("\n📦 Buckets utilisés:")
    print("  - raw-web-html: HTML brut des pages")
    print("  - raw-images: Images extraites")
    print("  - raw-json: Données structurées")
    print("="*60)
    
    all_news = []
    
    # 1. FSJES Agdal - Actualités + Avis
    print("\n[1/4] Scraping FSJES Agdal actualités + avis...")
    
    fsjes_actualites = scrape_fsjes_all_actualites(
        "https://fsjes-agdal.um5.ac.ma/fr/taxonomy/term/26",
        session,
        max_pages=10
    )
    all_news.extend(fsjes_actualites)
    print(f"  FSJES actualités: {len(fsjes_actualites)}")
    
    fsjes_avis = scrape_fsjes_avis(
        "https://fsjes-agdal.um5.ac.ma/fr/home-1",
        session
    )
    all_news.extend(fsjes_avis)
    print(f"  FSJES avis: {len(fsjes_avis)}")
    print(f"  Total FSJES: {len(fsjes_actualites) + len(fsjes_avis)}")
    
    # 2. EMI - Actualités
    print("\n[2/4] Scraping EMI actualités...")
    emi_news = scrape_emi_actualites(
        "https://www.emi.ac.ma/actualites/",
        session
    )
    all_news.extend(emi_news)
    print(f"  Total EMI: {len(emi_news)}")
    
    # 3. ENS Rabat - Actualités
    print("\n[3/4] Scraping ENS Rabat actualités...")
    ens_news = scrape_ens_actualites(
        "https://ens.um5.ac.ma/",
        session
    )
    all_news.extend(ens_news)
    print(f"  Total ENS: {len(ens_news)}")
    
    # 4. EST Sale - Actualités
    print("\n[4/4] Scraping EST Sale actualités...")
    est_news = scrape_est_actualites(
        "https://est.um5.ac.ma/",
        session
    )
    all_news.extend(est_news)
    print(f"  Total EST: {len(est_news)}")
    
    # Dédoublonner et ajouter les métadonnées
    unique_news = []
    seen_titles = set()
    
    for news in all_news:
        key = f"{news['title']}_{news['source']}"
        if key not in seen_titles and news['title']:
            seen_titles.add(key)
            
            news_with_metadata = create_common_fields(
                source_system="news_web_scraper",
                source_url=news.get("url", ""),
                data=news
            )
            unique_news.append(news_with_metadata)
    
    # Sauvegarder dans raw-json bucket
    if unique_news:
        news_data = {
            "source": "all_institutions",
            "table_type": "university_news",
            "scrape_timestamp": partition["iso"],
            "scrape_date": f"{partition['year']}-{partition['month']}-{partition['day']}",
            "total_news": len(unique_news),
            "news_items": unique_news
        }
        
        object_path = (
            f"university_news/"
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
        
        print(f"\n  ✅ Total news saved: {len(unique_news)} -> {object_path}")
        print(f"  📦 Bucket: raw-json")
        
        # Breakdown
        print(f"\n  📊 Breakdown by institution:")
        inst_counts = {}
        for news in unique_news:
            inst = news.get('source', 'Unknown')
            inst_counts[inst] = inst_counts.get(inst, 0) + 1
        
        for inst, count in sorted(inst_counts.items(), key=lambda x: -x[1]):
            print(f"    - {inst}: {count}")
        
        # Sample
        print(f"\n  📝 Sample news (first 3):")
        for news in unique_news[:3]:
            print(f"    - [{news.get('source')}] {news.get('title', '')[:70]}...")
            print(f"      Record ID: {news.get('record_id', '')[:20]}...")
            if news.get('publication_date'):
                print(f"      Date: {news.get('publication_date')}")
    
    print("\n" + "="*60)
    print("SCRAPING COMPLETE")
    print("="*60)
    print("\n📦 Résumé des buckets utilisés:")
    print("  - raw-web-html: HTML brut sauvegardé")
    print("  - raw-images: Images sauvegardées")
    print("  - raw-json: Données structurées sauvegardées")


if __name__ == "__main__":
    run()
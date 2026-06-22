# src/ingestion/docs/toubkal_selenium_scraper.py
"""
SCRAPER TOUBKAL IMIST AVEC SELENIUM - Version corrigée
"""

import hashlib
import logging
import sys
import time
import re
import requests
import urllib3
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup  # 🔥 IMPORT AJOUTÉ

from src.storage.minio.sara_client import MinIOClient

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SOURCE_NAME = "toubkal_theses"
BASE_URL = "https://toubkal.imist.ma"

# Institutions avec leurs URLs de recherche
KNOWN_INSTITUTIONS = [
    {"name": "Université Mohammed V - Rabat", "url": "https://toubkal.imist.ma/communities/6afdb78c-3214-46d5-a45d-efb9288c299e/search"},
    {"name": "Université Cadi Ayyad - Marrakech", "url": "https://toubkal.imist.ma/communities/1329a881-3bd2-47cf-b9ad-604001972d7e/search"},
]

class ToubkalSeleniumScraper:
    def __init__(self):
        self.client = MinIOClient()
        self.driver = None
        self.doc_count = 0
        self.errors = 0
        self.total_theses = 0
        
    def setup_driver(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        try:
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(60)
            logger.info("✅ Driver Chrome initialisé")
            return True
        except Exception as e:
            logger.error(f"❌ Erreur: {e}")
            return False
    
    def get_theses_from_institution(self, search_url, max_theses=20):
        logger.info(f"    📂 Récupération des thèses...")
        theses = []
        
        try:
            self.driver.get(search_url)
            time.sleep(3)
            
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/items/')]"))
                )
            except TimeoutException:
                logger.warning("      ⏰ Aucun résultat")
                return []
            
            links = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/items/')]")
            url_to_title = {}
            
            for link in links:
                href = link.get_attribute('href')
                text = link.text.strip()
                
                if href and '/items/' in href:
                    if not text or len(text) < 5:
                        try:
                            parent = link.find_element(By.XPATH, "..")
                            text = parent.text.strip()
                            if text:
                                text = text.split('\n')[0]
                        except:
                            pass
                    
                    if len(text) > 10:
                        url_to_title[href] = text
            
            for url, title in url_to_title.items():
                if url not in [t['url'] for t in theses]:
                    theses.append({'title': title, 'url': url})
                    if len(theses) >= max_theses:
                        break
            
            if not theses:
                seen = set()
                for link in links[:max_theses*2]:
                    href = link.get_attribute('href')
                    if href and '/items/' in href and href not in seen:
                        seen.add(href)
                        try:
                            text = link.find_element(By.XPATH, "..").text.strip().split('\n')[0]
                        except:
                            text = f"Thèse_{len(theses)+1}"
                        theses.append({'title': text, 'url': href})
                        if len(theses) >= max_theses:
                            break
            
            logger.info(f"    ✅ {len(theses)} thèses trouvées")
            return theses
            
        except Exception as e:
            logger.error(f"    ❌ Erreur: {e}")
            return []
    
    def get_full_item_page(self, item_url):
        try:
            full_url = item_url + "/full"
            logger.debug(f"      🌐 Chargement: {full_url}")
            self.driver.get(full_url)
            time.sleep(3)
            return self.driver.page_source
        except Exception as e:
            logger.debug(f"      ❌ Erreur: {e}")
            return None
    
    def extract_pdf_from_full_page(self, html_content):
        """Extrait l'URL du PDF depuis la page /full."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # MÉTHODE 1: Chercher les bitstreams
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/bitstreams/' in href and '/download' in href:
                    pdf_url = urljoin(BASE_URL, href)
                    logger.debug(f"      ✅ Bitstream trouvé: {pdf_url}")
                    return pdf_url
            
            # MÉTHODE 2: Chercher "Download"
            for link in soup.find_all('a', href=True):
                text = link.text.strip()
                href = link['href']
                if 'Download' in text and '/bitstreams/' in href:
                    pdf_url = urljoin(BASE_URL, href)
                    logger.debug(f"      ✅ Download trouvé: {pdf_url}")
                    return pdf_url
            
            # MÉTHODE 3: Chercher dans la section Files
            for div in soup.find_all('div'):
                if 'Files' in div.text or 'Original bundle' in div.text:
                    for link in div.find_all('a', href=True):
                        href = link['href']
                        if '/bitstreams/' in href:
                            pdf_url = urljoin(BASE_URL, href)
                            logger.debug(f"      ✅ Files section: {pdf_url}")
                            return pdf_url
            
            # MÉTHODE 4: Regex
            bitstream_pattern = r'href="([^"]*\/bitstreams\/[^"]*\/download[^"]*)"'
            matches = re.findall(bitstream_pattern, html_content)
            if matches:
                pdf_url = urljoin(BASE_URL, matches[0])
                logger.debug(f"      ✅ Regex trouvé: {pdf_url}")
                return pdf_url
            
            # MÉTHODE 5: Selenium
            try:
                bitstream_elem = self.driver.find_element(By.XPATH, "//a[contains(@href, '/bitstreams/')]")
                if bitstream_elem:
                    pdf_url = bitstream_elem.get_attribute('href')
                    logger.debug(f"      ✅ Selenium trouvé: {pdf_url}")
                    return pdf_url
            except:
                pass
            
            logger.debug(f"      ⚠️ Aucun PDF trouvé")
            return None
            
        except Exception as e:
            logger.debug(f"      ❌ Erreur extraction: {e}")
            return None
    
    def download_pdf(self, pdf_url, metadata):
        try:
            logger.info(f"      📥 Téléchargement...")
            
            session = requests.Session()
            session.verify = False
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })
            
            resp = session.get(pdf_url, timeout=60, stream=True)
            if resp.status_code != 200:
                logger.warning(f"        ⚠️ HTTP {resp.status_code}")
                return False
            
            content = resp.content
            
            if len(content) < 1000:
                logger.warning(f"        ⚠️ Fichier trop petit: {len(content)} bytes")
                return False
            
            if not content[:4] == b'%PDF':
                logger.warning(f"        ⚠️ Pas un PDF")
                return False
            
            doc_id = hashlib.md5(pdf_url.encode()).hexdigest()[:10]
            institution_name = metadata.get('institution', 'unknown').replace(' ', '_')
            thesis_title = metadata.get('thesis_title', 'unknown')[:30]
            thesis_title = re.sub(r'[^a-zA-Z0-9_\u0600-\u06FF]', '_', thesis_title)
            filename = f"thesis_{institution_name}_{thesis_title}_{doc_id}.pdf"
            
            now = datetime.now()
            object_path = (
                f"source={SOURCE_NAME}/"
                f"year={now.year}/"
                f"month={now.month:02d}/"
                f"day={now.day:02d}/"
                f"{filename}"
            )
            
            self.client.upload_binary(
                bucket_name="raw-documents",
                object_name=object_path,
                data=content,
                content_type="application/pdf"
            )
            
            metadata.update({
                "record_id": doc_id,
                "source_system": SOURCE_NAME,
                "source_url": pdf_url,
                "crawl_timestamp": now.isoformat(),
                "file_name": filename,
                "file_size_bytes": len(content),
                "content_hash": hashlib.sha256(content).hexdigest(),
            })
            
            self.client.upload_json(
                bucket_name="raw-json",
                object_name=(
                    f"source={SOURCE_NAME}/"
                    f"year={now.year}/"
                    f"month={now.month:02d}/"
                    f"day={now.day:02d}/"
                    f"{filename}_metadata.json"
                ),
                data=metadata
            )
            
            self.doc_count += 1
            logger.info(f"      ✅ PDF #{self.doc_count} sauvegardé")
            return True
            
        except Exception as e:
            logger.error(f"      ❌ Erreur: {e}")
            self.errors += 1
            return False
    
    def scrape_institution(self, institution, max_theses=20):
        logger.info(f"\n🏛️  {institution['name']}")
        
        theses = self.get_theses_from_institution(institution['url'], max_theses)
        if not theses:
            logger.warning(f"  ⚠️ Aucune thèse")
            return
        
        self.total_theses += len(theses)
        
        for idx, thesis in enumerate(theses):
            logger.info(f"    📄 [{idx+1}/{len(theses)}] {thesis['title'][:50]}...")
            
            full_page = self.get_full_item_page(thesis['url'])
            if not full_page:
                logger.debug(f"      ⚠️ Pas de page full")
                continue
            
            pdf_url = self.extract_pdf_from_full_page(full_page)
            
            if pdf_url:
                metadata = {
                    'institution': institution['name'],
                    'thesis_title': thesis['title'],
                    'thesis_url': thesis['url']
                }
                self.download_pdf(pdf_url, metadata)
            else:
                logger.debug(f"      ⚠️ Aucun PDF")
            
            time.sleep(1)
    
    def run(self, max_institutions=2, max_theses_per_institution=5):
        logger.info("="*70)
        logger.info("🚀 TOUBKAL IMIST - SCRAPER")
        logger.info("="*70)
        logger.info(f"🏛️  Max institutions: {max_institutions}")
        logger.info(f"📄 Max thèses: {max_theses_per_institution}")
        logger.info("="*70)
        
        if not self.setup_driver():
            return
        
        try:
            institutions = KNOWN_INSTITUTIONS[:max_institutions]
            for institution in institutions:
                self.scrape_institution(institution, max_theses_per_institution)
                time.sleep(2)
            
            logger.info("\n" + "="*70)
            logger.info("📊 RAPPORT FINAL")
            logger.info("="*70)
            logger.info(f"✅ PDF sauvegardés: {self.doc_count}")
            logger.info(f"❌ Erreurs: {self.errors}")
            logger.info("="*70)
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("🔒 Driver fermé")

if __name__ == "__main__":
    scraper = ToubkalSeleniumScraper()
    scraper.run(max_institutions=2, max_theses_per_institution=5)

# hada khdam mzyan final baqi nzid fih gha institution kter 
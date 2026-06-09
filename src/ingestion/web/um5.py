"""
Web scraping pour UM5
Utilisée par: Chaimae (à confirmer)
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime

def scrape_um5_faculties():
    """
    Scrape la liste des facultés UM5
    """
    url = "https://www.um5.ac.ma/facultes"
    
    print(f"🌐 Scraping UM5 - {url}")
    response = requests.get(url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # TODO: Adapter les sélecteurs selon la structure réelle du site
    faculties = []
    for item in soup.find_all('div', class_='views-row'):
        title_elem = item.find('h2')
        if title_elem:
            faculties.append({
                "name": title_elem.text.strip(),
                "url": url,
                "source": "um5",
                "scrape_timestamp": datetime.now().isoformat()
            })
    
    print(f"✅ UM5: {len(faculties)} facultés extraites")
    return faculties

def scrape_um5_news():
    """
    Scrape les actualités UM5
    """
    url = "https://www.um5.ac.ma/actualites"
    
    print(f"🌐 Scraping UM5 Actualités - {url}")
    response = requests.get(url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    news = []
    for item in soup.find_all('article'):
        title = item.find('h2')
        if title:
            news.append({
                "title": title.text.strip(),
                "url": url,
                "source": "um5_news",
                "scrape_timestamp": datetime.now().isoformat()
            })
    
    print(f"✅ UM5: {len(news)} actualités extraites")
    return news

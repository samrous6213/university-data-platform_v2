"""
Web scraping pour UCA
Utilisée par: Sara (à confirmer)
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime

def scrape_uca_faculties():
    """
    Scrape la liste des facultés UCA
    """
    url = "https://www.uca.ma/facultes"
    
    print(f"🌐 Scraping UCA - {url}")
    response = requests.get(url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # TODO: Adapter les sélecteurs pour UCA
    faculties = []
    for item in soup.find_all('div', class_='field-content'):
        link = item.find('a')
        if link:
            faculties.append({
                "name": link.text.strip(),
                "url": link.get('href', ''),
                "source": "uca",
                "scrape_timestamp": datetime.now().isoformat()
            })
    
    print(f"✅ UCA: {len(faculties)} facultés extraites")
    return faculties

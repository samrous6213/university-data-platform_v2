import requests
from bs4 import BeautifulSoup

url = "https://est.um5.ac.ma/"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

response = requests.get(url, headers=headers, verify=False)

# Sauvegarder le HTML
with open("page.html", "w", encoding="utf-8") as f:
    f.write(response.text)

print("HTML sauvegardé dans page.html")
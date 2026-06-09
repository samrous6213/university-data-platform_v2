"""
Extraction depuis Crossref API
Utilisée par: Sara (à confirmer)
"""
import requests
from datetime import datetime

def extract_crossref(query="university", rows=20):
    """
    Extrait les publications depuis Crossref
    """
    url = "https://api.crossref.org/works"
    params = {
        "query": query,
        "rows": rows,
        "mailto": "team@university-platform.ma"
    }
    
    print(f"📡 Extraction Crossref - Recherche: {query}")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    results = data.get('message', {}).get('items', [])
    print(f"✅ Crossref: {len(results)} publications extraites")
    
    return results

def transform_crossref_to_publication(pub_data):
    """
    Transforme les données Crossref au format publication
    """
    return {
        "record_id": pub_data.get('DOI', ''),
        "title": pub_data.get('title', ['Unknown'])[0],
        "authors": [a.get('family', '') for a in pub_data.get('author', [])],
        "year": pub_data.get('published-print', {}).get('date-parts', [[datetime.now().year]])[0][0],
        "source_system": "crossref",
        "source_url": f"https://doi.org/{pub_data.get('DOI', '')}",
        "crawl_timestamp": datetime.now().isoformat()
    }

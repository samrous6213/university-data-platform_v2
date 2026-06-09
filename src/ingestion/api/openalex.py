"""
Extraction depuis OpenAlex API
Utilisée par: Chaimae (à confirmer)
"""
import requests
import json
from datetime import datetime

def extract_openalex(limit=20):
    """
    Extrait les auteurs marocains depuis OpenAlex
    """
    url = "https://api.openalex.org/authors"
    params = {
        "filter": "institutions.country_code:MA",
        "per-page": limit,
        "mailto": "team@university-platform.ma"
    }
    
    print(f"📡 Extraction OpenAlex - Récupération de {limit} auteurs...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    results = data.get('results', [])
    print(f"✅ OpenAlex: {len(results)} auteurs extraits")
    
    return results

def transform_openalex_to_faculty(author_data):
    """
    Transforme les données OpenAlex au format faculty_profiles
    """
    return {
        "record_id": author_data.get('id', '').split('/')[-1],
        "name": author_data.get('display_name', 'Unknown'),
        "title": "Researcher",
        "department": author_data.get('last_known_institution', {}).get('display_name', 'Unknown'),
        "email": "",
        "research_interests": ", ".join([t.get('display_name', '') for t in author_data.get('topics', [])[:3]]),
        "source_system": "openalex",
        "source_url": author_data.get('id', ''),
        "crawl_timestamp": datetime.now().isoformat(),
        "year": datetime.now().year
    }

if __name__ == "__main__":
    # Test
    data = extract_openalex(5)
    print(json.dumps(data[0] if data else {}, indent=2))

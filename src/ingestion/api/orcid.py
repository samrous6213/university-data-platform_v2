"""
Extraction depuis ORCID API
Utilisée par: Ayoub (à confirmer)
"""
import requests
from datetime import datetime

def extract_orcid(orcid_id=None):
    """
    Extrait les données d'un chercheur depuis ORCID
    """
    if orcid_id:
        url = f"https://pub.orcid.org/v3.0/{orcid_id}/record"
    else:
        # Exemple d'ORCID public
        url = "https://pub.orcid.org/v3.0/0000-0001-5000-0007/record"
    
    headers = {"Accept": "application/json"}
    
    print(f"📡 Extraction ORCID...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    print(f"✅ ORCID: données extraites")
    return data

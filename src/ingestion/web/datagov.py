"""
Extraction depuis Data.gov.ma
Utilisée par: Ayoub (à confirmer)
"""
import requests
import csv
import io
from datetime import datetime

def extract_datagov_dataset(dataset_id):
    """
    Extrait un dataset depuis Data.gov.ma
    """
    # Exemple avec un dataset public
    url = f"https://www.data.gov.ma/api/1/datasets/{dataset_id}"
    
    print(f"📊 Extraction Data.gov.ma - {dataset_id}")
    response = requests.get(url)
    response.raise_for_status()
    
    data = response.json()
    print(f"✅ Data.gov.ma: dataset extrait")
    
    return data

def extract_datagov_csv(csv_url):
    """
    Extrait et parse un fichier CSV depuis Data.gov.ma
    """
    print(f"📊 Extraction CSV depuis Data.gov.ma")
    response = requests.get(csv_url)
    response.raise_for_status()
    
    csv_content = csv.DictReader(io.StringIO(response.text))
    rows = list(csv_content)
    
    print(f"✅ CSV: {len(rows)} lignes extraites")
    return rows

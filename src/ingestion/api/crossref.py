# src/ingestion/api/crossref.py

import requests
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import time

# Ajouter src/ au PYTHONPATH
sys.path.insert(0, 'D:/university-data-platform_v2')

# IMPORTER MinIOClient DEPUIS LE BON ENDROIT
from src.storage.minio.nezha_client import MinIOClient

# ==================== CONFIG ====================
SOURCE_NAME = "crossref"

# ==================== FONCTIONS ====================

def extract_crossref(query="moroccan university", rows=100):
    """Extract publications from Crossref"""
    url = "https://api.crossref.org/works"
    params = {
        "query": query,
        "rows": rows,
        "sort": "relevance",
        "mailto": "team@university-platform.ma"
    }
    
    print(f"📡 Crossref: Fetching '{query}' (max {rows} rows)")
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    results = data.get("message", {}).get("items", [])
    
    print(f"✅ Crossref: {len(results)} publications fetched")
    return results, response.status_code

def transform_crossref_to_publication(pub_data):
    """Transform publication to standard format"""
    authors = []
    for author in pub_data.get("author", []):
        given = author.get('given', '')
        family = author.get('family', '')
        fullname = f"{given} {family}".strip()
        if fullname:
            authors.append(fullname)
    
    year = None
    if pub_data.get("published-print"):
        year = pub_data["published-print"]["date-parts"][0][0]
    elif pub_data.get("published-online"):
        year = pub_data["published-online"]["date-parts"][0][0]
    
    title = pub_data.get("title", ["Unknown"])[0] if pub_data.get("title") else "Unknown"
    journal = pub_data.get("container-title", ["Unknown"])[0] if pub_data.get("container-title") else "Unknown"
    doi = pub_data.get("DOI", "")
    
    publication = {
        "source_system": "Crossref",
        "source_url": f"https://doi.org/{doi}" if doi else "",
        "extraction_timestamp": datetime.now().isoformat(),
        "doi": doi,
        "title": title,
        "authors": authors,
        "publication_year": year,
        "journal": journal,
        "abstract": pub_data.get("abstract", ""),
        "content_hash": ""
    }
    
    content_str = f"{doi}{title}{year}{journal}"
    publication['content_hash'] = hashlib.md5(content_str.encode()).hexdigest()
    
    return publication

def fetch_crossref_publications(query="moroccan university", rows=100):
    """Main function: extract + transform"""
    raw_publications, status = extract_crossref(query, rows)
    
    transformed_publications = []
    for pub in raw_publications:
        try:
            transformed = transform_crossref_to_publication(pub)
            transformed_publications.append(transformed)
        except Exception as e:
            print(f"⚠️ Error transforming publication: {e}")
            continue
    
    print(f"✅ Transformed {len(transformed_publications)} publications")
    return transformed_publications, status

def run(query="moroccan university", rows=100):
    """
    Fonction principale pour exécuter le pipeline Crossref
    """
    # Vérifier la connexion à MinIO
    try:
        client = MinIOClient(endpoint="university-minio:9000")
        print("✅ Connexion MinIO établie")
        minio_available = True
    except Exception as e:
        print(f"⚠️ MinIO non disponible: {e}")
        minio_available = False
    
    now = datetime.now()
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    status = 500
    records = 0

    # Génération des variables de partitionnement
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')

    try:
        # 1. Extraction des publications
        publications, status = fetch_crossref_publications(query, rows)
        records = len(publications)

        if publications and minio_available:
            # 2. Stockage de la donnée brute dans le bucket dédié aux API
            object_name = f"source={SOURCE_NAME}/year={year}/month={month}/day={day}/crossref_{timestamp}.json"
            
            client.upload_json(
                bucket_name="raw-json",
                object_name=object_name,
                data=publications
            )
            
            print(f"✅ Crossref completed - {records} publications saved to MinIO")
        else:
            # Fallback: sauvegarde locale
            filename = f"crossref_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(publications, f, indent=2, ensure_ascii=False)
            print(f"💾 Saved locally: {filename}")

    except requests.exceptions.Timeout:
        print("❌ Erreur : timeout lors de l'appel à l'API Crossref")
        status = 408

    except requests.exceptions.HTTPError as e:
        print(f"❌ Erreur HTTP : {e}")
        status = e.response.status_code if e.response is not None else 500

    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur réseau : {e}")
        status = 500

    except Exception as e:
        print(f"❌ Erreur inattendue : {e}")
        status = 500

    finally:
        # 3. Stockage du log de traitement dans le bucket dédié aux logs
        log = {
            "source": SOURCE_NAME,
            "query": query,
            "rows": rows,
            "records": records,
            "status": status,
            "timestamp": now.isoformat()
        }

        log_filename = f"crossref_log_{timestamp}.json"
        
        if minio_available:
            try:
                log_path = f"source={SOURCE_NAME}/year={year}/month={month}/day={day}/log_{timestamp}.json"
                client.upload_json(
                    bucket_name="raw-logs",
                    object_name=log_path,
                    data=log
                )
                print("📝 Log saved to MinIO")
            except Exception as e:
                print(f"⚠️ Error saving log: {e}")
                with open(log_filename, 'w', encoding='utf-8') as f:
                    json.dump(log, f, indent=2, ensure_ascii=False)
                print(f"💾 Log saved locally: {log_filename}")
        else:
            with open(log_filename, 'w', encoding='utf-8') as f:
                json.dump(log, f, indent=2, ensure_ascii=False)
            print(f"💾 Log saved locally: {log_filename}")
            
if __name__ == "__main__":
    import sys
    
    # Si l'utilisateur donne une query, l'utiliser, sinon par défaut
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        query = "moroccan university"
    
    print("=" * 50)
    print(f"🚀 Crossref Ingestion: '{query}'")
    print("=" * 50)
    
    run(query=query, rows=50)

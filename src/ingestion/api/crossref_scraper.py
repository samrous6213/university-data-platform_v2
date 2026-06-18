import re
import json
import hashlib
import requests
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
        "language": "en",
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


# ==============================================================
# FONCTIONS CROSSREF
# ==============================================================
def extract_crossref(limit=100, offset=0, query="", filter_params=None):
    """
    Extract data from Crossref API with pagination support.
    
    Args:
        limit: Number of records per request (max 1000 per Crossref API)
        offset: Starting offset for pagination
        query: Search query string (leave empty for affiliation-only search)
        filter_params: Dictionary of filter parameters 
            (e.g., {"from-pub-date": "2020", "type": "journal-article", "affiliation": "Universite Mohammed V"})
    """
    
    url = "https://api.crossref.org/works"
    
    params = {
        "rows": min(limit, 1000),
        "offset": offset,
        "sort": "relevance",
        "order": "desc"
    }
    
    # Add query if provided
    if query and query.strip():
        params["query"] = query
    
    # Add filters if provided
    if filter_params:
        filter_string = ",".join([f"{k}:{v}" for k, v in filter_params.items()])
        params["filter"] = filter_string
    
    print(f"  Crossref API call: limit={limit}, offset={offset}, query='{query}', filters={filter_params}")
    
    response = requests.get(
        url,
        params=params,
        timeout=30,
        headers={
            "User-Agent": "Academic Data Collector (mail@example.com)"  # Replace with your email
        }
    )
    
    response.raise_for_status()
    
    return response.json(), response.status_code


def extract_crossref_with_pagination(total_limit=500, query="", filter_params=None):
    """
    Extract data from Crossref API with automatic pagination.
    
    Args:
        total_limit: Total number of records to fetch (max 10000 due to API limits)
        query: Search query string (leave empty for affiliation-only)
        filter_params: Dictionary of filter parameters
    """
    all_items = []
    offset = 0
    max_per_request = 1000
    remaining = min(total_limit, 10000)
    
    print(f"\n  Fetching up to {remaining} records from Crossref...")
    if filter_params:
        print(f"  Filters: {filter_params}")
    
    while remaining > 0:
        current_limit = min(max_per_request, remaining)
        
        try:
            data, status = extract_crossref(
                limit=current_limit, 
                offset=offset, 
                query=query,
                filter_params=filter_params
            )
            
            items = data.get("message", {}).get("items", [])
            if not items:
                print(f"    No more items found at offset {offset}")
                break
                
            all_items.extend(items)
            print(f"    Fetched {len(items)} records (total: {len(all_items)})")
            
            offset += len(items)
            remaining -= len(items)
            
            total_results = data.get("message", {}).get("total-results", 0)
            if offset >= total_results:
                print(f"    Reached end of results ({total_results} total)")
                break
                
        except Exception as e:
            print(f"    Error fetching at offset {offset}: {e}")
            break
    
    complete_data = {
        "status": "success",
        "message-type": "work-list",
        "message-version": "1.0.0",
        "message": {
            "facets": {},
            "items": all_items,
            "items-per-page": len(all_items),
            "query": {
                "start-index": 0,
                "search-terms": query if query else "affiliation_filter"
            },
            "total-results": len(all_items)
        }
    }
    
    return complete_data, 200


def transform_crossref_to_hudi(crossref_data: dict) -> list:
    """
    Transforme les données Crossref en format Hudi pour la table research_publications.
    """
    publications = []
    
    items = crossref_data.get("message", {}).get("items", [])
    
    for item in items:
        # Extraire les auteurs avec leurs affiliations
        authors = []
        author_affiliations = []
        for author in item.get("author", []):
            name_parts = []
            if "given" in author:
                name_parts.append(author["given"])
            if "family" in author:
                name_parts.append(author["family"])
            full_name = " ".join(name_parts)
            if full_name:
                authors.append(full_name)
            
            # Extraire l'affiliation de l'auteur
            if "affiliation" in author:
                for aff in author.get("affiliation", []):
                    if "name" in aff:
                        author_affiliations.append(aff["name"])
        
        # Déterminer l'institution principale (chercher "Mohammed V" ou "UM5")
        institution = "Unknown"
        for aff in author_affiliations:
            if "Mohammed V" in aff or "UM5" in aff or "Mohammed V" in aff.upper():
                institution = "Universite Mohammed V Rabat"
                break
            elif "EMI" in aff or "Mohammadia" in aff:
                institution = "Ecole Mohammadia d'Ingenieurs"
                break
            elif "ENS" in aff or "Ecole Normale" in aff:
                institution = "Ecole Normale Superieure"
                break
            elif "FSJES" in aff or "Faculte des Sciences Juridiques" in aff:
                institution = "FSJES Agdal"
                break
            elif "EST" in aff or "Ecole Superieure de Technologie" in aff:
                institution = "EST Sale"
                break
        
        # Extraire le DOI
        doi = item.get("DOI", "")
        
        # Extraire la date de publication
        pub_year = ""
        pub_date = ""
        if "issued" in item and "date-parts" in item["issued"]:
            date_parts = item["issued"]["date-parts"]
            if date_parts and len(date_parts) > 0:
                if len(date_parts[0]) > 0:
                    pub_year = str(date_parts[0][0])
                    if len(date_parts[0]) > 1:
                        month = str(date_parts[0][1]).zfill(2)
                        day = str(date_parts[0][2]).zfill(2) if len(date_parts[0]) > 2 else "01"
                        pub_date = f"{pub_year}-{month}-{day}"
        
        # Extraire l'abstract (nettoyer les balises HTML)
        abstract = item.get("abstract", "")
        if abstract:
            # Nettoyer les balises HTML simples
            import re
            abstract = re.sub(r'<[^>]+>', '', abstract)
            abstract = abstract.strip()
        
        # Construction de l'enregistrement
        publication = {
            "publication_id": doi if doi else "",
            "title": item.get("title", ["No title"])[0] if item.get("title") else "No title",
            "authors": authors,
            "author_affiliations": author_affiliations,
            "year": pub_year,
            "publication_date": pub_date,
            "journal": item.get("container-title", ["Unknown"])[0] if item.get("container-title") else "Unknown",
            "doi": doi,
            "abstract": abstract[:1000] if abstract else "",  # Limiter à 1000 caractères
            "institution": institution,
            "source_url": f"https://doi.org/{doi}" if doi else "",
            "publisher": item.get("publisher", ""),
            "type": item.get("type", ""),
            "volume": item.get("volume", ""),
            "issue": item.get("issue", ""),
            "pages": item.get("page", ""),
            "references_count": item.get("references-count", 0),
            "source": "crossref",
            "scrape_timestamp": datetime.now().isoformat()
        }
        
        publications.append(publication)
    
    return publications


# ==============================================================
# MAIN FUNCTION - AVEC STOCKAGE STRUCTURÉ
# ==============================================================
def run(limit=500, query="", filter_params=None, source_name="crossref"):
    """
    Main function to extract Crossref data and store in MinIO with structured storage.
    
    Args:
        limit: Total number of records to fetch (default: 500)
        query: Search query string (default: "")
        filter_params: Dictionary of filter parameters
        source_name: Source identifier for storage
    """
    
    client = MinIOClient(endpoint="localhost:9000")
    partition = get_date_partition()
    timestamp = partition["timestamp"]
    status = 500
    records = 0
    
    print("="*60)
    print("CROSSREF RESEARCH PUBLICATIONS SCRAPER")
    print("="*60)
    print(f"Date: {partition['year']}-{partition['month']}-{partition['day']}")
    print(f"Query: {query if query else 'Affiliation-based'}")
    print(f"Max records: {limit}")
    if filter_params:
        print(f"Filters: {filter_params}")
    print()
    
    try:
        # Extract data with pagination
        data, status = extract_crossref_with_pagination(
            total_limit=limit,
            query=query,
            filter_params=filter_params
        )
        
        records = len(data.get("message", {}).get("items", []))
        
        # Transform to Hudi format
        publications = transform_crossref_to_hudi(data)
        
        # Ajouter les champs communs à chaque publication
        publications_with_metadata = []
        for pub in publications:
            pub_with_metadata = create_common_fields(
                source_system="crossref_api",
                source_url=pub.get("source_url", ""),
                data=pub
            )
            publications_with_metadata.append(pub_with_metadata)
        
        # Prepare metadata
        api_metadata = {
            "source": source_name,
            "query": query if query else "affiliation_filter",
            "filters": filter_params,
            "total_records": records,
            "requested_limit": limit,
            "extraction_timestamp": partition["iso"],
            "extraction_date": f"{partition['year']}-{partition['month']}-{partition['day']}",
            "api_version": "1.0.0",
            "records_with_institution": sum(1 for p in publications if p["institution"] != "Unknown")
        }
        
        # Create complete data package with transformed data
        complete_package = {
            "metadata": api_metadata,
            "raw_data": data,
            "hudi_ready_data": publications_with_metadata
        }
        
        # Store in raw-json avec partitionnement
        object_path = (
            f"raw-json/research_publications/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"crossref_publications_{timestamp}.json"
        )
        
        client.upload_json(
            bucket_name="data-lake",
            object_name=object_path,
            data=complete_package
        )
        
        print(f"\n  ✅ Success: {records} records saved to MinIO")
        print(f"  Location: {object_path}")
        print(f"  Publications with Moroccan institution: {api_metadata['records_with_institution']}")
        
        # Sample of transformed records
        if publications_with_metadata:
            print(f"\n  📝 Sample publications (first 3):")
            for i, pub in enumerate(publications_with_metadata[:3], 1):
                print(f"    {i}. {pub['title'][:80]}...")
                print(f"       Authors: {', '.join(pub['authors'][:3])}")
                print(f"       Journal: {pub['journal']} ({pub['year']})")
                print(f"       Institution: {pub['institution']}")
                print(f"       Record ID: {pub.get('record_id', '')[:20]}...")
                print()
        
    except requests.exceptions.Timeout:
        print("\n  ❌ Error: Timeout during Crossref API call")
        status = 408
        
    except requests.exceptions.HTTPError as e:
        print(f"\n  ❌ Error HTTP: {e}")
        status = e.response.status_code if e.response is not None else 500
        
    except requests.exceptions.RequestException as e:
        print(f"\n  ❌ Error Network: {e}")
        status = 500
        
    except Exception as e:
        print(f"\n  ❌ Unexpected error: {e}")
        status = 500
        
    finally:
        # Save log in raw-json/logs
        log = {
            "source": source_name,
            "operation": "extract",
            "status": status,
            "query": query if query else "affiliation_filter",
            "filters": filter_params,
            "records_requested": limit,
            "records_fetched": records,
            "timestamp": partition["iso"]
        }
        
        log_path = (
            f"raw-json/logs/crossref/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"log_{timestamp}.json"
        )
        
        client.upload_json(
            bucket_name="data-lake",
            object_name=log_path,
            data=log
        )
        
        print(f"\n  📋 Log saved: {log_path}")
        
        # Final summary
        print("\n" + "="*60)
        if status == 200:
            print("✅ CROSSREF EXTRACTION COMPLETED SUCCESSFULLY")
        else:
            print("❌ CROSSREF EXTRACTION COMPLETED WITH ERRORS")
        print("="*60)
        print(f"Status: {status}")
        print(f"Records fetched: {records}")
        print(f"Data stored in: raw-json/research_publications/year={partition['year']}/month={partition['month']}/day={partition['day']}/")
        print("="*60)


if __name__ == "__main__":
    
    # =============================================================
    # CONFIGURATION - Choisissez l'une des options ci-dessous
    # =============================================================
    
    # OPTION 1: Publications des universités marocaines (RECOMMANDÉ)
    print("\n" + "="*60)
    print("OPTION 1: Publications des universités marocaines")
    print("="*60)
    run(
        limit=500,
        query="",  # Pas de query, on utilise les filtres
        filter_params={
            "type": "journal-article",
            "from-pub-date": "2020",  # Publications des 5 dernières années
        }
    )
    
    # OPTION 2: Publications sur un sujet spécifique (ex: "data science")
    # run(
    #     limit=500,
    #     query="data science",
    #     filter_params={
    #         "type": "journal-article",
    #         "from-pub-date": "2022"
    #     }
    # )
    
    # OPTION 3: Publications UM5 spécifiques (avec affiliation dans la query)
    # run(
    #     limit=300,
    #     query="Universite Mohammed V Rabat",
    #     filter_params={
    #         "type": "journal-article",
    #         "from-pub-date": "2020"
    #     }
    # )
    
    # OPTION 4: Multi-institutions (plus large)
    # run(
    #     limit=1000,
    #     query="Morocco university research",
    #     filter_params={
    #         "type": "journal-article",
    #         "from-pub-date": "2020"
    #     }
    # )
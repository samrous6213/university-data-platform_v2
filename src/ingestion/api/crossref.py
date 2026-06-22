# src/ingestion/api/crossref_scraper.py
# Version avec les bonnes requêtes qui fonctionnent

import re
import json
import hashlib
import time
import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from src.storage.minio.sara_client import MinIOClient

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
# FONCTIONS CROSSREF API
# ==============================================================

class CrossrefAPIClient:
    """Client pour l'API Crossref avec gestion des erreurs et retry."""
    
    BASE_URL = "https://api.crossref.org/works"
    MAX_RETRIES = 3
    RETRY_DELAY = 2
    
    def __init__(self, email: str = "academic@collector.com"):
        self.email = email
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": f"Academic Data Collector (mailto:{email})",
            "Accept": "application/json"
        })
        self.stats = {
            "api_calls": 0,
            "records_fetched": 0,
            "errors": 0,
            "retries": 0
        }
    
    def _make_request(self, params: Dict) -> Optional[Dict]:
        """Effectue une requête avec retry logic."""
        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                self.stats["api_calls"] += 1
                return response.json()
                
            except requests.exceptions.Timeout:
                logger.warning(f"  ⏰ Timeout (tentative {attempt+1}/{self.MAX_RETRIES})")
                self.stats["retries"] += 1
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                    continue
                self.stats["errors"] += 1
                return None
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    wait_time = self.RETRY_DELAY * (attempt + 1) * 2
                    logger.warning(f"  ⏰ Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                logger.error(f"  ❌ HTTP Error: {e}")
                self.stats["errors"] += 1
                return None
                
            except Exception as e:
                logger.error(f"  ❌ Error: {e}")
                self.stats["errors"] += 1
                return None
        
        return None
    
    def extract(self, limit: int = 100, offset: int = 0, 
                query: str = "", filter_params: Optional[Dict] = None) -> tuple:
        """Extrait des données depuis Crossref API."""
        params = {
            "rows": min(limit, 1000),
            "offset": offset,
            "sort": "relevance",
            "order": "desc"
        }
        
        if query and query.strip():
            params["query"] = query
        
        if filter_params:
            valid_filters = [
                "type", "from-pub-date", "until-pub-date", 
                "from-created-date", "until-created-date",
                "from-index-date", "until-index-date",
                "has-license", "has-full-text", "has-abstract",
                "container-title", "publisher", "funder", "orcid"
            ]
            
            filtered = {k: v for k, v in filter_params.items() if k in valid_filters}
            if filtered:
                filter_string = ",".join([f"{k}:{v}" for k, v in filtered.items()])
                params["filter"] = filter_string
        
        logger.info(f"  📡 API Call: limit={limit}, offset={offset}")
        
        response = self._make_request(params)
        
        if response:
            return response, 200
        else:
            return {"message": {"items": []}}, 500
    
    def extract_with_pagination(self, total_limit: int = 500, 
                                query: str = "", 
                                filter_params: Optional[Dict] = None) -> tuple:
        """Extrait avec pagination automatique."""
        all_items = []
        offset = 0
        max_per_request = 1000
        remaining = min(total_limit, 10000)
        
        logger.info(f"\n  🔄 Fetching up to {remaining} records...")
        logger.info(f"  🔍 Query: {query[:100] if query else 'Toutes'}")
        
        while remaining > 0:
            current_limit = min(max_per_request, remaining)
            
            data, status = self.extract(
                limit=current_limit,
                offset=offset,
                query=query,
                filter_params=filter_params
            )
            
            if status != 200:
                logger.error(f"  ❌ API Error at offset {offset}")
                break
            
            items = data.get("message", {}).get("items", [])
            if not items:
                logger.info(f"    No more items at offset {offset}")
                break
            
            all_items.extend(items)
            self.stats["records_fetched"] += len(items)
            logger.info(f"    ✅ Fetched {len(items)} records (total: {len(all_items)})")
            
            offset += len(items)
            remaining -= len(items)
            
            total_results = data.get("message", {}).get("total-results", 0)
            if offset >= total_results:
                logger.info(f"    Reached end of results ({total_results} total)")
                break
            
            time.sleep(0.5)
        
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
                    "search-terms": query if query else "all"
                },
                "total-results": len(all_items)
            }
        }
        
        return complete_data, 200
    
    def get_stats(self) -> Dict:
        return self.stats.copy()


# ==============================================================
# FONCTIONS DE SAUVEGARDE
# ==============================================================

def save_raw_crossref_data(client: MinIOClient, data: Dict, 
                          source_name: str, partition: Dict) -> str:
    timestamp = partition["timestamp"]
    
    object_path = (
        f"source={source_name}/"
        f"year={partition['year']}/"
        f"month={partition['month']}/"
        f"day={partition['day']}/"
        f"raw_crossref_{timestamp}.json"
    )
    
    client.upload_json(
        bucket_name="raw-json",
        object_name=object_path,
        data=data
    )
    
    logger.info(f"  ✅ Raw data saved: {object_path}")
    return object_path


def save_extraction_metadata(client: MinIOClient, source_name: str,
                           stats: Dict, partition: Dict) -> str:
    timestamp = partition["timestamp"]
    
    metadata = {
        "source": source_name,
        "source_type": "crossref_api",
        "extraction_timestamp": partition["iso"],
        "extraction_date": f"{partition['year']}-{partition['month']}-{partition['day']}",
        "stats": stats,
        "api_version": "1.0.0"
    }
    
    object_path = (
        f"source={source_name}/"
        f"year={partition['year']}/"
        f"month={partition['month']}/"
        f"day={partition['day']}/"
        f"metadata_{timestamp}.json"
    )
    
    client.upload_json(
        bucket_name="raw-json",
        object_name=object_path,
        data=metadata
    )
    
    logger.info(f"  ✅ Metadata saved: {object_path}")
    return object_path


# ==============================================================
# MAIN FUNCTION
# ==============================================================

def run(limit: int = 500, query: str = "", 
        filter_params: Optional[Dict] = None,
        source_name: str = "crossref") -> None:
    
    client = MinIOClient(endpoint="localhost:9000")
    partition = get_date_partition()
    
    logger.info("="*70)
    logger.info("🚀 CROSSREF API INGESTION - SEMAINE 1")
    logger.info("="*70)
    logger.info(f"📅 Date: {partition['year']}-{partition['month']}-{partition['day']}")
    logger.info(f"📄 Max records: {limit}")
    logger.info(f"🔍 Query: {query if query else 'Toutes les publications'}")
    if filter_params:
        logger.info(f"🔧 Filters: {filter_params}")
    logger.info("="*70)
    
    crossref_client = CrossrefAPIClient()
    
    logger.info("\n📡 Extraction des données Crossref...")
    data, status = crossref_client.extract_with_pagination(
        total_limit=limit,
        query=query,
        filter_params=filter_params
    )
    
    records = len(data.get("message", {}).get("items", []))
    stats = crossref_client.get_stats()
    stats["records_fetched"] = records
    stats["http_status"] = status
    
    logger.info("\n💾 Sauvegarde des données...")
    raw_path = save_raw_crossref_data(client, data, source_name, partition)
    metadata_path = save_extraction_metadata(client, source_name, stats, partition)
    
    logger.info("\n" + "="*70)
    logger.info("📊 RAPPORT FINAL - INGESTION CROSSREF")
    logger.info("="*70)
    logger.info(f"✅ Status: {'SUCCESS' if status == 200 else 'ERROR'}")
    logger.info(f"📄 Records fetched: {records}")
    logger.info(f"📡 API calls: {stats['api_calls']}")
    logger.info(f"🔄 Retries: {stats['retries']}")
    logger.info(f"❌ Errors: {stats['errors']}")
    logger.info("\n📦 Structure MinIO:")
    logger.info(f"  📁 raw-json/")
    logger.info(f"    └── source={source_name}/")
    logger.info(f"        └── year={partition['year']}/")
    logger.info(f"            └── month={partition['month']}/")
    logger.info(f"                └── day={partition['day']}/")
    logger.info(f"                    ├── raw_crossref_{partition['timestamp']}.json  ← DONNÉES BRUTES")
    logger.info(f"                    └── metadata_{partition['timestamp']}.json      ← MÉTADONNÉES")
    logger.info("="*70)
    logger.info("\n✅ INGESTION TERMINÉE - PRÊT POUR LA SEMAINE 2")


# ==============================================================
# CONFIGURATION - VERSION AVEC REQUÊTES QUI FONCTIONNENT
# ==============================================================

if __name__ == "__main__":
    
    print("\n" + "="*70)
    print("🔬 CROSSREF INGESTION - SEMAINE 1")
    print("="*70)
    
    # =============================================================
    # OPTION 1: Université Mohammed V (RECOMMANDÉ) ⭐
    # ✅ Testé: 243,831 résultats
    # =============================================================
    print("\n📌 OPTION 1: Université Mohammed V")
    print("-"*70)
    run(
        limit=500,
        query='affiliation:"Universite Mohammed V"',
        filter_params={
            "type": "journal-article",
            "from-pub-date": "2020"
        },
        source_name="crossref_um5"
    )
    
    # =============================================================
    # OPTION 2: Cadi Ayyad University
    # ✅ Testé: 2,765,477 résultats
    # =============================================================
    # print("\n📌 OPTION 2: Cadi Ayyad University")
    # print("-"*70)
    # run(
    #     limit=500,
    #     query='affiliation:"Cadi Ayyad University"',
    #     filter_params={
    #         "type": "journal-article",
    #         "from-pub-date": "2020"
    #     },
    #     source_name="crossref_uca"
    # )
    
    # =============================================================
    # OPTION 3: Morocco university (large)
    # ✅ Testé: 2,773,984 résultats
    # =============================================================
    # print("\n📌 OPTION 3: Morocco university")
    # print("-"*70)
    # run(
    #     limit=1000,
    #     query="Morocco university",
    #     filter_params={
    #         "type": "journal-article",
    #         "from-pub-date": "2020"
    #     },
    #     source_name="crossref_morocco"
    # )
    
    # =============================================================
    # OPTION 4: Renewable energy Morocco
    # ✅ Testé: 928,686 résultats
    # =============================================================
    # print("\n📌 OPTION 4: Renewable energy Morocco")
    # print("-"*70)
    # run(
    #     limit=500,
    #     query="renewable energy Morocco",
    #     filter_params={
    #         "type": "journal-article",
    #         "from-pub-date": "2020"
    #     },
    #     source_name="crossref_energy"
    # )
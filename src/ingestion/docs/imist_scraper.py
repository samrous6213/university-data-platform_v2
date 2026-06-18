import re
import json
import hashlib
import PyPDF2
import requests
from io import BytesIO
from datetime import datetime
from src.storage.minio.sara_client import MinIOClient

# Désactiver les avertissements SSL si nécessaire
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
# FONCTIONS DOCUMENT
# ==============================================================
def calculate_checksum(content: bytes) -> str:
    """Calculate SHA-256 checksum of content."""
    return hashlib.sha256(content).hexdigest()


def extract_links_from_pdf(pdf_content: bytes) -> list:
    """Extract all URLs from PDF content."""
    try:
        # Convertir bytes en string (ignorer les erreurs de caractères)
        text = pdf_content.decode('utf-8', errors='ignore')
        
        # Pattern pour trouver les URLs
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        urls = re.findall(url_pattern, text)
        
        # Supprimer les doublons et URLs vides
        unique_urls = list(set([url.strip() for url in urls if url.strip()]))
        
        # Filtrer les URLs trop courtes ou invalides
        valid_urls = [url for url in unique_urls if len(url) > 10 and '.' in url]
        
        return valid_urls
    except Exception as e:
        print(f"Warning: Could not extract links from PDF: {e}")
        return []


def extract_pdf_metadata(pdf_content: bytes) -> dict:
    """Extract metadata from PDF file."""
    metadata = {
        "title": "",
        "author": "",
        "subject": "",
        "keywords": "",
        "creator": "",
        "producer": "",
        "num_pages": 0
    }
    
    try:
        pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
        metadata["num_pages"] = len(pdf_reader.pages)
        
        # Extract document info if available
        if pdf_reader.metadata:
            if pdf_reader.metadata.get("/Title"):
                metadata["title"] = pdf_reader.metadata.get("/Title")
            if pdf_reader.metadata.get("/Author"):
                metadata["author"] = pdf_reader.metadata.get("/Author")
            if pdf_reader.metadata.get("/Subject"):
                metadata["subject"] = pdf_reader.metadata.get("/Subject")
            if pdf_reader.metadata.get("/Keywords"):
                metadata["keywords"] = pdf_reader.metadata.get("/Keywords")
            if pdf_reader.metadata.get("/Creator"):
                metadata["creator"] = pdf_reader.metadata.get("/Creator")
            if pdf_reader.metadata.get("/Producer"):
                metadata["producer"] = pdf_reader.metadata.get("/Producer")
                
    except Exception as e:
        print(f"[INFO] Could not extract embedded PDF metadata: {e}")
    
    return metadata


def download_file(url: str, session: requests.Session) -> tuple:
    """Download file from URL, following redirects."""
    response = session.get(url, timeout=60, verify=False, stream=True)
    response.raise_for_status()
    content = response.content
    return content, response.status_code


# ==============================================================
# MAIN FUNCTION - AVEC STOCKAGE STRUCTURÉ
# ==============================================================
def run(pdf_url: str, source_name: str = "imist"):
    """Main function to download and store document from IMIST with structured storage."""
    
    client = MinIOClient(endpoint="localhost:9000")
    partition = get_date_partition()
    timestamp = partition["timestamp"]
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; IMIST downloader/1.0)",
    })
    
    status = 500
    if pdf_url.endswith('/download'):
        file_name = f"imist_document_{timestamp}.pdf"
    else:
        file_name = pdf_url.split("/")[-1]
        if not file_name.endswith('.pdf'):
            file_name = f"{file_name}.pdf"
    
    all_links = []
    
    try:
        # 1. Download document
        print(f"[1/6] Downloading from: {pdf_url}")
        file_content, status = download_file(pdf_url, session)
        print(f"      Download successful. Size: {len(file_content)} bytes")
        
        # 2. Calculate checksum
        print("[2/6] Calculating checksum...")
        checksum = calculate_checksum(file_content)
        print(f"      Checksum (SHA-256): {checksum[:16]}...")
        
        # 3. Extract metadata from PDF
        print("[3/6] Extracting PDF metadata...")
        metadata = extract_pdf_metadata(file_content)
        print(f"      Pages: {metadata['num_pages']}")
        if metadata['author']:
            print(f"      Author: {metadata['author']}")
        if metadata['title']:
            print(f"      Title: {metadata['title'][:50]}...")
        
        # 4. Extract links from PDF
        print("[4/6] Extracting links from PDF...")
        all_links = extract_links_from_pdf(file_content)
        print(f"      Found {len(all_links)} unique URLs in the PDF")
        if all_links:
            print(f"      First 3 links: {all_links[:3]}")
        
        # 5. Upload PDF to MinIO (raw-documents avec partitionnement)
        print("[5/6] Uploading PDF to MinIO...")
        file_object_name = (
            f"raw-documents/{source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"{file_name}"
        )
        client.upload_binary(
            bucket_name="data-lake",
            object_name=file_object_name,
            data=file_content,
            content_type="application/pdf",
        )
        print(f"      Uploaded to: {file_object_name}")
        
        # 6. Upload metadata, links, and logs (raw-json avec partitionnement)
        print("[6/6] Saving metadata, links, and logs...")
        
        # Préparer les données avec métadonnées communes
        document_data = {
            "source": source_name,
            "source_url": pdf_url,
            "file_name": file_name,
            "file_size": len(file_content),
            "checksum": checksum,
            "download_timestamp": partition["iso"],
            "download_date": f"{partition['year']}-{partition['month']}-{partition['day']}",
            "pdf_metadata": metadata,
            "extracted_links": all_links,
            "total_links_found": len(all_links),
            "storage_path": file_object_name
        }
        
        # Ajouter les champs communs
        document_with_metadata = create_common_fields(
            source_system=f"document_{source_name}",
            source_url=pdf_url,
            data=document_data
        )
        
        # Save metadata in raw-json
        metadata_object_name = (
            f"raw-json/documents/{source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"metadata_{timestamp}.json"
        )
        client.upload_json(
            bucket_name="data-lake",
            object_name=metadata_object_name,
            data=document_with_metadata
        )
        print(f"      Metadata saved to: {metadata_object_name}")
        
        # Save links separately in raw-json
        if all_links:
            links_object_name = (
                f"raw-json/documents/{source_name}/links/"
                f"year={partition['year']}/"
                f"month={partition['month']}/"
                f"day={partition['day']}/"
                f"links_{timestamp}.json"
            )
            links_data = {
                "source": source_name,
                "source_file": file_name,
                "total_links": len(all_links),
                "links": all_links,
                "extraction_timestamp": partition["iso"],
                "extraction_date": f"{partition['year']}-{partition['month']}-{partition['day']}"
            }
            client.upload_json(
                bucket_name="data-lake",
                object_name=links_object_name,
                data=links_data
            )
            print(f"      Links saved to: {links_object_name}")
        
        # Save log in raw-json/logs
        log = {
            "source": source_name,
            "operation": "download",
            "status": status,
            "source_url": pdf_url,
            "file_name": file_name,
            "file_size": len(file_content),
            "checksum": checksum,
            "num_pages": metadata["num_pages"],
            "links_extracted": len(all_links),
            "timestamp": partition["iso"],
            "download_date": f"{partition['year']}-{partition['month']}-{partition['day']}"
        }
        
        log_object_name = (
            f"raw-json/logs/documents/{source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"download_{timestamp}.json"
        )
        client.upload_json(
            bucket_name="data-lake",
            object_name=log_object_name,
            data=log
        )
        print(f"      Log saved to: {log_object_name}")
        
        # Final summary
        print("\n" + "="*60)
        print("SUCCESS! IMIST document processing completed.")
        print("="*60)
        print(f"Document: {file_name}")
        print(f"Pages: {metadata['num_pages']}")
        print(f"Size: {len(file_content)} bytes ({len(file_content)/1024:.2f} KB)")
        print(f"Checksum: {checksum[:32]}...")
        print(f"Links found in PDF: {len(all_links)}")
        print(f"Record ID: {document_with_metadata.get('record_id', '')[:20]}...")
        print(f"\nStored in MinIO (raw-documents + raw-json):")
        print(f"  - PDF: {file_object_name}")
        print(f"  - Metadata: {metadata_object_name}")
        if all_links:
            print(f"  - Links: {links_object_name}")
        print(f"  - Log: {log_object_name}")
        
    except requests.exceptions.RequestException as e:
        print(f"\n[ERROR] Network/Download failed: {e}")
        error_log = {
            "source": source_name,
            "operation": "download",
            "status": 500,
            "source_url": pdf_url,
            "error": str(e),
            "timestamp": partition["iso"]
        }
        error_log_name = (
            f"raw-json/logs/documents/{source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"error_{timestamp}.json"
        )
        try:
            client.upload_json(
                bucket_name="data-lake",
                object_name=error_log_name,
                data=error_log
            )
        except:
            print(f"      Could not save error log to MinIO")
            
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")
        error_log = {
            "source": source_name,
            "operation": "download",
            "status": 500,
            "source_url": pdf_url,
            "error": str(e),
            "timestamp": partition["iso"]
        }
        error_log_name = (
            f"raw-json/logs/documents/{source_name}/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"error_{timestamp}.json"
        )
        try:
            client.upload_json(
                bucket_name="data-lake",
                object_name=error_log_name,
                data=error_log
            )
        except:
            print(f"      Could not save error log to MinIO")


if __name__ == "__main__":
    # Ton lien toubkal IMIST
    DOCUMENT_URL = "https://toubkal.imist.ma/bitstreams/a69f14f2-5c96-4baf-93e0-44c454295989/download"
    
    run(DOCUMENT_URL, source_name="imist")
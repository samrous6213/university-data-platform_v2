import requests
import hashlib
import PyPDF2
import re
from io import BytesIO
from datetime import datetime

# Utilisation de ton client
from src.storage.minio.ayoub_client import MinIOClient

# Désactiver les avertissements SSL si nécessaire
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def calculate_checksum(content: bytes) -> str:
    """Calculate SHA-256 checksum of content."""
    return hashlib.sha256(content).hexdigest()


def extract_links_from_pdf(pdf_content: bytes) -> list:
    """Extract all URLs from PDF content."""
    try:
        text = pdf_content.decode('utf-8', errors='ignore')
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        urls = re.findall(url_pattern, text)
        unique_urls = list(set([url.strip() for url in urls if url.strip()]))
        valid_urls = [url for url in unique_urls if len(url) > 10 and '.' in url]
        return valid_urls
    except Exception as e:
        print(f"Warning: Could not extract links from PDF: {e}")
        return []


def extract_pdf_metadata(pdf_content: bytes) -> dict:
    """Extract metadata from PDF file."""
    metadata = {
        "title": "", "author": "", "subject": "",
        "keywords": "", "creator": "", "producer": "", "num_pages": 0
    }
    try:
        pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
        metadata["num_pages"] = len(pdf_reader.pages)
        if pdf_reader.metadata:
            if pdf_reader.metadata.get("/Title"): metadata["title"] = pdf_reader.metadata.get("/Title")
            if pdf_reader.metadata.get("/Author"): metadata["author"] = pdf_reader.metadata.get("/Author")
            if pdf_reader.metadata.get("/Subject"): metadata["subject"] = pdf_reader.metadata.get("/Subject")
            if pdf_reader.metadata.get("/Keywords"): metadata["keywords"] = pdf_reader.metadata.get("/Keywords")
            if pdf_reader.metadata.get("/Creator"): metadata["creator"] = pdf_reader.metadata.get("/Creator")
            if pdf_reader.metadata.get("/Producer"): metadata["producer"] = pdf_reader.metadata.get("/Producer")
    except Exception as e:
        print(f"[INFO] Could not extract embedded PDF metadata: {e}")
    return metadata


def download_file(url: str, session: requests.Session) -> tuple:
    """Download file from URL, following redirects."""
    response = session.get(url, timeout=60, verify=False, stream=True)
    response.raise_for_status()
    return response.content, response.status_code


def run(document_url: str, source_name: str = "datagov"):
    """Main function to download and store document from Data.gov.ma."""
    
    client = MinIOClient(endpoint="localhost:9000")
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; Datagov downloader/1.0)",
    })
    
    status = 500
    file_name = document_url.split("/")[-1]
    
    # Sécurité au cas où l'URL n'a pas d'extension claire
    if not (file_name.endswith('.pdf') or file_name.endswith('.csv') or file_name.endswith('.json') or file_name.endswith('.xls') or file_name.endswith('.xlsx')):
        file_name = f"dataset_datagov_{timestamp}.xls"
        
    is_pdf = file_name.lower().endswith('.pdf')
    all_links = []
    metadata = {"num_pages": 0, "title": file_name, "author": "Data.gov.ma"}
    
    try:
        # 1. Download document
        print(f"[1/6] Downloading from: {document_url}")
        file_content, status = download_file(document_url, session)
        print(f"      Download successful. Size: {len(file_content)} bytes")
        
        # 2. Calculate checksum
        print("[2/6] Calculating checksum...")
        checksum = calculate_checksum(file_content)
        print(f"      Checksum (SHA-256): {checksum[:16]}...")
        
        # 3 & 4. Extract metadata and links (Seulement si c'est un PDF)
        if is_pdf:
            print("[3/6] Extracting PDF metadata...")
            metadata = extract_pdf_metadata(file_content)
            print("[4/6] Extracting links from PDF...")
            all_links = extract_links_from_pdf(file_content)
        else:
            print("[3/6] Not a PDF. Skipping PDF metadata extraction...")
            print("[4/6] Not a PDF. Skipping link extraction...")
            
        # 5. Upload Document to MinIO
        print("[5/6] Uploading Document to MinIO...")
        file_object_name = f"raw/documents/{source_name}/{file_name}"
        
        # Déterminer le bon Content-Type
        content_type = "application/pdf" if is_pdf else "application/vnd.ms-excel"
        
        client.upload_binary(
            bucket_name="data-lake",
            object_name=file_object_name,
            data=file_content,
            content_type=content_type,
        )
        print(f"      Uploaded to: {file_object_name}")
        
        # 6. Upload metadata, links, and logs
        print("[6/6] Saving metadata, links, and logs...")
        
        metadata_record = {
            "source": source_name,
            "source_url": document_url,
            "file_name": file_name,
            "file_size": len(file_content),
            "checksum": checksum,
            "download_timestamp": now.isoformat(),
            "document_metadata": metadata,
            "extracted_links": all_links,
            "total_links_found": len(all_links),
            "storage_path": file_object_name
        }
        
        metadata_object_name = f"raw/metadata/{source_name}/metadata_{timestamp}.json"
        client.upload_json(
            bucket_name="data-lake",
            object_name=metadata_object_name,
            data=metadata_record
        )
        
        log = {
            "source": source_name,
            "operation": "download",
            "status": status,
            "source_url": document_url,
            "file_name": file_name,
            "file_size": len(file_content),
            "checksum": checksum,
            "timestamp": now.isoformat()
        }
        
        log_object_name = f"raw/logs/{source_name}/download_{timestamp}.json"
        client.upload_json(bucket_name="data-lake", object_name=log_object_name, data=log)
        
        print("\n" + "="*60)
        print("SUCCESS! Data.gov.ma document processing completed.")
        print("="*60)
        
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")


if __name__ == "__main__":
    # TON VRAI LIEN OFFICIEL !
    DOCUMENT_URL = "https://data.gov.ma/data/fr/dataset/d4589781-4f02-4fbf-9317-2088b315fa97/resource/df6bb4cc-b694-4520-9637-69700e52817f/download/etab-ensprimaire-public-men-2013-2014-2.xls"
    
    run(DOCUMENT_URL, source_name="datagov")
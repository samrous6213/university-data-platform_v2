import requests
import hashlib
import PyPDF2
import re
from io import BytesIO
from datetime import datetime
from src.storage.minio.sara_client import MinIOClient

# Désactiver les avertissements SSL si nécessaire
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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


def run(pdf_url: str, source_name: str = "imist"):
    """Main function to download and store document from IMIST."""
    
    client = MinIOClient(endpoint="localhost:9000")
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    
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
        
        # 5. Upload PDF to MinIO
        print("[5/6] Uploading PDF to MinIO...")
        file_object_name = f"raw/documents/{source_name}/{file_name}"
        client.upload_binary(
            bucket_name="data-lake",
            object_name=file_object_name,
            data=file_content,
            content_type="application/pdf",
        )
        print(f"      Uploaded to: {file_object_name}")
        
        # 6. Upload metadata, links, and logs
        print("[6/6] Saving metadata, links, and logs...")
        
        # Metadata record with links included
        metadata_record = {
            "source": source_name,
            "source_url": pdf_url,
            "file_name": file_name,
            "file_size": len(file_content),
            "checksum": checksum,
            "download_timestamp": now.isoformat(),
            "pdf_metadata": metadata,
            "extracted_links": all_links,
            "total_links_found": len(all_links),
            "storage_path": file_object_name
        }
        
        # Save metadata
        metadata_object_name = f"raw/metadata/{source_name}/metadata_{timestamp}.json"
        client.upload_json(
            bucket_name="data-lake",
            object_name=metadata_object_name,
            data=metadata_record
        )
        
        # Save links separately (optionnel mais utile)
        if all_links:
            links_object_name = f"raw/links/{source_name}/links_{timestamp}.json"
            links_data = {
                "source": source_name,
                "source_file": file_name,
                "total_links": len(all_links),
                "links": all_links,
                "extraction_timestamp": now.isoformat()
            }
            client.upload_json(
                bucket_name="data-lake",
                object_name=links_object_name,
                data=links_data
            )
            print(f"      Links saved to: {links_object_name}")
        
        # Save log
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
            "timestamp": now.isoformat()
        }
        
        log_object_name = f"raw/logs/{source_name}/download_{timestamp}.json"
        client.upload_json(
            bucket_name="data-lake",
            object_name=log_object_name,
            data=log
        )
        
        # Final summary
        print("\n" + "="*60)
        print("SUCCESS! IMIST document processing completed.")
        print("="*60)
        print(f"Document: {file_name}")
        print(f"Pages: {metadata['num_pages']}")
        print(f"Size: {len(file_content)} bytes ({len(file_content)/1024:.2f} KB)")
        print(f"Checksum: {checksum[:32]}...")
        print(f"Links found in PDF: {len(all_links)}")
        print(f"\nStored in MinIO:")
        print(f"  - PDF: {file_object_name}")
        print(f"  - Metadata: {metadata_object_name}")
        if all_links:
            print(f"  - Links: raw/links/{source_name}/links_{timestamp}.json")
        print(f"  - Log: {log_object_name}")
        
    except requests.exceptions.RequestException as e:
        print(f"\n[ERROR] Network/Download failed: {e}")
        error_log = {
            "source": source_name,
            "operation": "download",
            "status": 500,
            "source_url": pdf_url,
            "error": str(e),
            "timestamp": now.isoformat()
        }
        client.upload_json(
            bucket_name="data-lake",
            object_name=f"raw/logs/{source_name}/error_{timestamp}.json",
            data=error_log
        )
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")


if __name__ == "__main__":
    # Ton lien IMIST
    DOCUMENT_URL = "https://toubkal.imist.ma/bitstreams/a69f14f2-5c96-4baf-93e0-44c454295989/download"
    
    run(DOCUMENT_URL, source_name="imist")
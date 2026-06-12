import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
from src.storage.minio.sara_client import MinIOClient


def calculate_checksum(content: bytes) -> str:
    """Calculate SHA-256 checksum of content."""
    return hashlib.sha256(content).hexdigest()


def extract_structured_data(soup: BeautifulSoup, url: str) -> dict:
    """Extract structured metadata from HTML page."""
    structured_data = {
        "url": url,
        "title": "",
        "description": "",
        "keywords": "",
        "h1_headings": [],
        "h2_headings": [],
        "paragraphs_count": 0,
        "links_count": 0,
        "images_count": 0
    }
    
    # Extract title
    title_tag = soup.find("title")
    if title_tag:
        structured_data["title"] = title_tag.get_text(strip=True)
    
    # Extract meta description
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        structured_data["description"] = meta_desc.get("content", "")
    
    # Extract meta keywords
    meta_keywords = soup.find("meta", attrs={"name": "keywords"})
    if meta_keywords:
        structured_data["keywords"] = meta_keywords.get("content", "")
    
    # Extract H1 headings
    h1_tags = soup.find_all("h1")
    structured_data["h1_headings"] = [h1.get_text(strip=True) for h1 in h1_tags[:5]]
    
    # Extract H2 headings
    h2_tags = soup.find_all("h2")
    structured_data["h2_headings"] = [h2.get_text(strip=True) for h2 in h2_tags[:10]]
    
    # Count paragraphs
    structured_data["paragraphs_count"] = len(soup.find_all("p"))
    
    # Count links
    structured_data["links_count"] = len(soup.find_all("a", href=True))
    
    # Count images
    structured_data["images_count"] = len(soup.find_all("img"))
    
    return structured_data


def scrape_um5():
    client = MinIOClient()
    
    # Plus de pages à scraper
    pages = [
        "https://www.um5.ac.ma/um5/ecole-normale-superieure",
        "https://www.um5.ac.ma/um5/faculte-des-sciences-de-rabat",
        "https://www.um5.ac.ma/um5/faculte-des-lettres-et-des-sciences-humaines-de-rabat",
        "https://www.um5.ac.ma/um5/faculte-des-sciences-juridiques-economiques-et-sociales-agdal",
        "https://www.um5.ac.ma/um5/ecole-mohammadia-dingenieurs",
        "https://www.um5.ac.ma/um5/ecole-nationale-superieure-dinformatique-et-danalyse-des-systemes",
        "https://www.um5.ac.ma/um5/faculte-de-medecine-et-de-pharmacie",
        "https://www.um5.ac.ma/um5/institut-scientifique"
    ]
    
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    all_pdfs = []
    all_structured_data = []
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; UM5 scraper/1.0; +https://www.um5.ac.ma)",
    })
    
    for url in pages:
        try:
            response = session.get(url, timeout=30, verify=False)
            response.raise_for_status()
            
            content_bytes = response.text.encode("utf-8")
            checksum = calculate_checksum(content_bytes)
            
            # Sauvegarder le HTML brut
            page_name = url.split('/')[-1]
            html_object_name = f"raw/html/um5_{page_name}_{timestamp}.html"
            client.upload_binary(
                bucket_name="data-lake",
                object_name=html_object_name,
                data=content_bytes,
                content_type="text/html",
            )
            
            # Parser et extraire les données structurées
            soup = BeautifulSoup(response.text, "html.parser")
            structured_data = extract_structured_data(soup, url)
            structured_data["checksum"] = checksum
            structured_data["html_path"] = html_object_name
            all_structured_data.append(structured_data)
            
            print(f"Saved HTML: {url}")
            print(f"  Title: {structured_data['title'][:50]}...")
            print(f"  Checksum: {checksum[:16]}...")
            
            # Chercher et télécharger les PDFs
            pdf_links = []
            for link in soup.find_all("a", href=True):
                href = link.get("href")
                if href and ".pdf" in href.lower():
                    pdf_links.append(href)
            
            print(f"  Found {len(pdf_links)} PDF links")
            
            # Télécharger chaque PDF
            for pdf_url in pdf_links:
                try:
                    if pdf_url.startswith('/'):
                        full_pdf_url = "https://www.um5.ac.ma" + pdf_url
                    else:
                        full_pdf_url = pdf_url
                    
                    pdf_response = session.get(full_pdf_url, timeout=30, verify=False)
                    pdf_response.raise_for_status()
                    
                    pdf_content = pdf_response.content
                    pdf_checksum = calculate_checksum(pdf_content)
                    
                    pdf_name = full_pdf_url.split('/')[-1]
                    if not pdf_name.endswith('.pdf'):
                        pdf_name = pdf_name + ".pdf"
                    
                    pdf_object_name = f"raw/pdfs/um5_{pdf_name}_{timestamp}.pdf"
                    client.upload_binary(
                        bucket_name="data-lake",
                        object_name=pdf_object_name,
                        data=pdf_content,
                        content_type="application/pdf",
                    )
                    
                    all_pdfs.append({
                        "source_page": url,
                        "pdf_url": full_pdf_url,
                        "pdf_name": pdf_name,
                        "pdf_path": pdf_object_name,
                        "checksum": pdf_checksum,
                        "status": "downloaded"
                    })
                    
                    print(f"    Downloaded PDF: {pdf_name} (checksum: {pdf_checksum[:16]}...)")
                    
                except Exception as e:
                    print(f"    Failed to download PDF {pdf_url}: {e}")
                    all_pdfs.append({
                        "source_page": url,
                        "pdf_url": pdf_url,
                        "status": "failed",
                        "error": str(e)
                    })
                    
        except Exception as e:
            print(f"Error with {url}: {e}")
    
    # Sauvegarder les données structurées dans MinIO
    structured_data_object = f"raw/structured/um5_structured_data_{timestamp}.json"
    client.upload_json(
        bucket_name="data-lake",
        object_name=structured_data_object,
        data=all_structured_data
    )
    
    # Log complet
    log = {
        "source": "um5_web",
        "timestamp": now.isoformat(),
        "pages_scraped": len(pages),
        "pages_with_data": len(all_structured_data),
        "pdfs_found": len(all_pdfs),
        "pdfs_downloaded": len([p for p in all_pdfs if p.get("status") == "downloaded"]),
        "pdfs_failed": len([p for p in all_pdfs if p.get("status") == "failed"]),
        "pdf_details": all_pdfs,
        "structured_data_location": structured_data_object,
        "summary": {
            "total_links_found": sum(d.get("links_count", 0) for d in all_structured_data),
            "total_paragraphs": sum(d.get("paragraphs_count", 0) for d in all_structured_data),
            "total_images": sum(d.get("images_count", 0) for d in all_structured_data)
        }
    }
    
    client.upload_json(
        bucket_name="data-lake",
        object_name=f"raw/logs/um5/scrape_{timestamp}.json",
        data=log
    )
    
    print(f"\n{'='*50}")
    print(f"UM5 scraping completed")
    print(f"{'='*50}")
    print(f"Pages scraped: {len(pages)}")
    print(f"PDFs downloaded: {log['pdfs_downloaded']}")
    print(f"PDFs failed: {log['pdfs_failed']}")
    print(f"Total links found: {log['summary']['total_links_found']}")
    print(f"Total paragraphs: {log['summary']['total_paragraphs']}")
    print(f"Structured data saved to: {structured_data_object}")
    print(f"Log saved to: raw/logs/um5/scrape_{timestamp}.json")


if __name__ == "__main__":
    scrape_um5()
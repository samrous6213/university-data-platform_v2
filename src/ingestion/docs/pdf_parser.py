"""
Parseur de PDF
Utilisée par: Chaimae, Sara, Ayoub
"""
import requests
import pdfplumber
import io
from datetime import datetime

def parse_pdf_from_url(pdf_url):
    """
    Télécharge et parse un PDF depuis une URL
    """
    print(f"📄 Parsing PDF - {pdf_url[:50]}...")
    
    response = requests.get(pdf_url)
    response.raise_for_status()
    
    text_content = []
    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
        for i, page in enumerate(pdf.pages[:5]):  # Limiter aux 5 premières pages
            page_text = page.extract_text()
            if page_text:
                text_content.append({
                    "page": i + 1,
                    "text": page_text[:1000]  # Limiter la taille
                })
    
    print(f"✅ PDF: {len(text_content)} pages extraites")
    
    return {
        "source_url": pdf_url,
        "pages": text_content,
        "total_pages": len(pdf.pages),
        "parse_timestamp": datetime.now().isoformat()
    }

def parse_pdf_from_file(file_path):
    """
    Parse un PDF depuis un fichier local
    """
    print(f"📄 Parsing PDF local - {file_path}")
    
    text_content = []
    with pdfplumber.open(file_path) as pdf:
        for i, page in enumerate(pdf.pages[:5]):
            page_text = page.extract_text()
            if page_text:
                text_content.append({
                    "page": i + 1,
                    "text": page_text[:1000]
                })
    
    return {
        "source_file": file_path,
        "pages": text_content,
        "total_pages": len(pdf.pages),
        "parse_timestamp": datetime.now().isoformat()
    }

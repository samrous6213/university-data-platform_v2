import re
import json
import hashlib
import requests
from bs4 import BeautifulSoup
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
        "language": "fr",
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


def save_raw_html(source_name: str, url: str, html_content: str, page_type: str = "general") -> None:
    """
    Sauvegarde le HTML brut dans le bucket raw-web-html.
    
    Args:
        source_name: Nom de la source (ex: est_sale, fsjes_agdal)
        url: URL de la page
        html_content: Contenu HTML
        page_type: Type de page (faculty, news, home, avis, etc.)
    """
    client = MinIOClient(endpoint="localhost:9000")
    partition = get_date_partition()
    timestamp = partition["timestamp"]
    
    # Générer un nom de fichier basé sur l'URL
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    file_name = f"{source_name}_{page_type}_{url_hash}_{timestamp}.html"
    
    object_path = f"{source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/{file_name}"
    
    # Sauvegarder le HTML
    client.upload_binary(
        bucket_name="raw-web-html",
        object_name=object_path,
        data=html_content.encode('utf-8'),
        content_type="text/html"
    )
    
    # Sauvegarder les métadonnées du HTML avec le type de page
    metadata = {
        "source_url": url,
        "source_name": source_name,
        "page_type": page_type,
        "timestamp": partition["iso"],
        "file_name": file_name,
        "content_hash": hashlib.sha256(html_content.encode()).hexdigest(),
        "size_bytes": len(html_content)
    }
    
    metadata_path = f"{source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/metadata_{page_type}_{timestamp}.json"
    client.upload_json(
        bucket_name="raw-web-html",
        object_name=metadata_path,
        data=metadata
    )
    
    print(f"      HTML ({page_type}) saved: {object_path}")


def save_image(image_url: str, source_name: str, image_name: str = None) -> None:
    """
    Sauvegarde une image dans le bucket raw-images.
    
    Args:
        image_url: URL de l'image
        source_name: Nom de la source
        image_name: Nom personnalisé pour l'image (optionnel)
    """
    try:
        client = MinIOClient(endpoint="localhost:9000")
        partition = get_date_partition()
        timestamp = partition["timestamp"]
        
        # Télécharger l'image
        response = requests.get(image_url, timeout=30, verify=False)
        response.raise_for_status()
        
        # Déterminer le nom du fichier
        if image_name:
            file_name = image_name
        else:
            # Extraire le nom du fichier depuis l'URL
            file_name = image_url.split("/")[-1]
            if not file_name or '.' not in file_name:
                file_name = f"image_{hashlib.md5(image_url.encode()).hexdigest()[:8]}_{timestamp}.jpg"
        
        # Déterminer le content-type
        content_type = response.headers.get('content-type', 'image/jpeg')
        
        object_path = f"{source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/{file_name}"
        
        client.upload_binary(
            bucket_name="raw-images",
            object_name=object_path,
            data=response.content,
            content_type=content_type
        )
        
        # Sauvegarder les métadonnées de l'image
        metadata = {
            "source_url": image_url,
            "source_name": source_name,
            "timestamp": partition["iso"],
            "file_name": file_name,
            "content_hash": hashlib.sha256(response.content).hexdigest(),
            "size_bytes": len(response.content),
            "content_type": content_type
        }
        
        metadata_path = f"{source_name}/year={partition['year']}/month={partition['month']}/day={partition['day']}/image_metadata_{timestamp}.json"
        client.upload_json(
            bucket_name="raw-images",
            object_name=metadata_path,
            data=metadata
        )
        
        print(f"      Image saved: {object_path}")
        
    except Exception as e:
        print(f"      Warning: Could not save image {image_url}: {e}")


# ==============================================================
# SCRAPER 1: EST SALÉ - FACULTY FROM HTML TABLE
# ==============================================================
def scrape_est_faculty(url: str, session: requests.Session = None) -> list:
    """Extract faculty names from EST Sale page."""
    
    if session is None:
        session = requests.Session()
    
    print(f"  Scraping EST faculty from: {url}")
    faculty_list = []
    
    try:
        response = session.get(url, timeout=30, verify=False)
        response.raise_for_status()
        
        # Sauvegarder le HTML brut
        save_raw_html("est_sale", url, response.text, "faculty")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table with class 'two-per-line'
        table = soup.find('table', class_='two-per-line')
        
        if table:
            # Find all rows in tbody
            rows = table.find_all('tr')
            
            for row in rows:
                # Find the cell containing the name
                cells = row.find_all('td')
                if cells:
                    name = cells[0].get_text(strip=True)
                    if name and len(name) > 2:
                        # Enlever "Pr" ou "Prof" au début
                        name = re.sub(r'^(Pr|Prof\.?)\s+', '', name, flags=re.IGNORECASE)
                        name = name.strip()
                        
                        # Split name into first and last name
                        name_parts = name.split()
                        if len(name_parts) >= 2:
                            first_name = name_parts[0]
                            last_name = ' '.join(name_parts[1:])
                        else:
                            first_name = name
                            last_name = ""
                        
                        faculty_list.append({
                            "last_name": last_name,
                            "first_name": first_name,
                            "email": "",
                            "department": "General",
                            "source_url": url,
                            "institution": "Ecole Superieure de Technologie (EST) Sale"
                        })
            
            print(f"      Found {len(faculty_list)} faculty members")
            return faculty_list
        else:
            print("      Could not find table with class 'two-per-line'")
            return []
            
    except Exception as e:
        print(f"      Error: {e}")
        return []


# ==============================================================
# SCRAPER 2: EMI - FACULTY (AMÉLIORÉ) - CORRIGÉ (plus de doublons)
# ==============================================================
def scrape_emi_faculty_improved(url: str, session: requests.Session, dept_name: str) -> list:
    """Extract faculty from EMI department page with improved parsing."""
    print(f"  Scraping EMI department: {dept_name}")
    faculty_list = []
    try:
        response = session.get(url, timeout=30, verify=False)
        response.raise_for_status()
        
        # Sauvegarder le HTML brut - TOUT dans le dossier emi/ avec page_type
        save_raw_html("emi", url, response.text, f"faculty_{dept_name.replace(' ', '_')}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Method 1: Look for email links
        email_links = soup.find_all('a', href=re.compile(r'mailto:'))
        for link in email_links:
            email = link.get('href', '').replace('mailto:', '').strip()
            # Try to find the name in parent elements
            parent = link.find_parent(['td', 'li', 'div', 'p'])
            if parent:
                text = parent.get_text()
                # Extract name (looking for patterns like "First Last" or "LAST First")
                name_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
                names = re.findall(name_pattern, text)
                if names:
                    name = names[0]
                    name_parts = name.split()
                    if len(name_parts) >= 2:
                        first_name = name_parts[0]
                        last_name = ' '.join(name_parts[1:])
                    else:
                        first_name = name
                        last_name = ""
                    
                    faculty_list.append({
                        "last_name": last_name,
                        "first_name": first_name,
                        "email": email,
                        "department": dept_name,
                        "source_url": url,
                        "institution": "Ecole Mohammadia d'Ingenieurs (EMI)"
                    })
        
        # Method 2: Look for tables
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    # Check if any cell contains an email
                    for cell in cells:
                        email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', cell.get_text())
                        if email_match:
                            email = email_match.group()
                            # Name might be in previous cell or same cell
                            name_text = cells[0].get_text().strip()
                            name_parts = name_text.split()
                            if len(name_parts) >= 2:
                                first_name = name_parts[0]
                                last_name = ' '.join(name_parts[1:])
                            else:
                                first_name = name_text
                                last_name = ""
                            
                            faculty_list.append({
                                "last_name": last_name,
                                "first_name": first_name,
                                "email": email,
                                "department": dept_name,
                                "source_url": url,
                                "institution": "Ecole Mohammadia d'Ingenieurs (EMI)"
                            })
                            break
        
        # Method 3: Look for any text containing @ and possible name pattern
        if not faculty_list:
            page_text = soup.get_text()
            lines = page_text.split('\n')
            for line in lines:
                if '@' in line:
                    # Try to extract email and name
                    email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', line)
                    if email_match:
                        email = email_match.group()
                        # Extract name (looking for text before email)
                        name_text = line[:email_match.start()].strip()
                        if name_text:
                            # Clean up name
                            name_text = re.sub(r'[|,;:]', ' ', name_text).strip()
                            name_parts = name_text.split()
                            if len(name_parts) >= 2:
                                first_name = name_parts[0]
                                last_name = ' '.join(name_parts[1:])
                            elif len(name_parts) == 1:
                                first_name = name_parts[0]
                                last_name = ""
                            else:
                                first_name = ""
                                last_name = ""
                            
                            if first_name:
                                faculty_list.append({
                                    "last_name": last_name,
                                    "first_name": first_name,
                                    "email": email,
                                    "department": dept_name,
                                    "source_url": url,
                                    "institution": "Ecole Mohammadia d'Ingenieurs (EMI)"
                                })
        
        print(f"      Found {len(faculty_list)} faculty members in {dept_name}")
        return faculty_list
    except Exception as e:
        print(f"      Error scraping {dept_name}: {e}")
        return []


# ==============================================================
# SCRAPER 3: ENS - FACULTY FROM JAVASCRIPT DATA
# ==============================================================
def extract_faculty_from_javascript(html_content: str) -> list:
    """Extract faculty data from the originalData JavaScript variable."""
    
    # Pattern to find the originalData array in the JavaScript
    pattern = r'const originalData = (\[.*?\]);'
    match = re.search(pattern, html_content, re.DOTALL)
    
    if not match:
        print("  Error: Could not find originalData in JavaScript")
        return []
    
    # Extract the JavaScript array as a string
    js_array_str = match.group(1)
    
    # Clean up the string to make it valid JSON
    # Replace single quotes with double quotes
    js_array_str = js_array_str.replace("'", '"')
    
    # Replace [at] with @ in email addresses
    js_array_str = js_array_str.replace('[at]', '@')
    
    # Remove trailing commas (common in JS but invalid in JSON)
    js_array_str = re.sub(r',\s*}', '}', js_array_str)
    js_array_str = re.sub(r',\s*]', ']', js_array_str)
    
    try:
        # Parse as JSON
        faculty_data = json.loads(js_array_str)
        return faculty_data
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        # Try a more aggressive cleaning approach
        # Extract each entry manually with regex
        entries = re.findall(r"\{[^}]+\}", js_array_str)
        faculty_data = []
        for entry in entries:
            try:
                # Clean and parse each entry
                clean_entry = entry.replace("'", '"')
                clean_entry = re.sub(r',\s*}', '}', clean_entry)
                obj = json.loads(clean_entry)
                faculty_data.append(obj)
            except:
                pass
        return faculty_data


def scrape_ens_faculty(url: str, session: requests.Session = None) -> list:
    """Scrape ENS faculty directory page and extract data from JavaScript."""
    
    if session is None:
        session = requests.Session()
    
    print(f"  Scraping ENS faculty from: {url}")
    
    try:
        response = session.get(url, timeout=30, verify=False)
        response.raise_for_status()
        
        # Sauvegarder le HTML brut
        save_raw_html("ens_rabat", url, response.text, "faculty")
        
        faculty_list = extract_faculty_from_javascript(response.text)
        
        # Clean and format the data
        formatted_faculty = []
        for faculty in faculty_list:
            # Handle different key names in the data
            formatted = {
                "last_name": faculty.get("Nom ", "").strip(),
                "first_name": faculty.get("Prénom", "").strip(),
                "department": faculty.get("Département", "").strip(),
                "email": faculty.get("Email Institutionnel", "").strip(),
                "source_url": url,
                "institution": "Ecole Normale Superieure (ENS) Rabat"
            }
            # Only add if we have at least a name
            if formatted["last_name"] or formatted["first_name"]:
                formatted_faculty.append(formatted)
        
        print(f"      Found {len(formatted_faculty)} faculty members")
        return formatted_faculty
        
    except Exception as e:
        print(f"      Error: {e}")
        return []


# ==============================================================
# SCRAPER 4: FSJES AGDAL - FACULTY (VERSION CORRIGÉE)
# ==============================================================
def scrape_fsjes_faculty(url: str, session: requests.Session = None) -> list:
    """
    Extrait les professeurs de la FSJES Agdal avec emails.
    """
    if session is None:
        session = requests.Session()
    
    print(f"  Scraping FSJES Agdal faculty from: {url}")
    faculty_list = []
    
    try:
        # Ajouter des headers pour simuler un navigateur
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        }
        
        response = session.get(url, timeout=30, verify=False, headers=headers)
        response.raise_for_status()
        
        # Sauvegarder le HTML brut
        save_raw_html("fsjes_agdal", url, response.text, "faculty")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
        
        # === MÉTHODE 1: Recherche par classe gsc-team ===
        # Trouver tous les widgets team (chaque professeur)
        team_widgets = soup.find_all('div', class_='widget gsc-team team-vertical-small')
        
        current_department = "General"
        
        for team_widget in team_widgets:
            # Trouver le département parent (gsc-heading)
            # Remonter jusqu'au parent avec gsc-column, puis chercher le heading précédent
            parent_col = team_widget.find_parent('div', class_=re.compile(r'gsc-column'))
            
            # Chercher le département dans le heading
            if parent_col:
                # Chercher le heading dans la même colonne ou dans les colonnes précédentes
                heading = parent_col.find_previous('div', class_='gsc-heading')
                if heading:
                    title_span = heading.find('span', class_='title')
                    if title_span:
                        dept_text = title_span.get_text(strip=True)
                        if 'DEPARTEMENT' in dept_text.upper():
                            current_department = dept_text.replace('DEPARTEMENT DE', '').replace('DEPARTMENT OF', '').strip()
            
            # Extraire le nom
            team_name_elem = team_widget.find('h3', class_='team-name')
            if not team_name_elem:
                continue
            
            full_name = team_name_elem.get_text(strip=True)
            full_name = re.sub(r'^(Pr|Prof\.?)\s+', '', full_name, flags=re.IGNORECASE)
            full_name = full_name.strip()
            
            if not full_name:
                continue
            
            # === CAPTURE DE L'EMAIL ===
            email = ""
            
            # Chercher dans team-info (la méthode principale)
            team_info = team_widget.find('div', class_='team-info')
            if team_info:
                # Chercher dans tout le texte du team-info
                info_text = team_info.get_text()
                email_match = email_pattern.search(info_text)
                if email_match:
                    email = email_match.group()
            
            # Si pas trouvé, chercher dans les balises <a> et <span>
            if not email:
                # Chercher tous les liens dans team-info
                links = team_widget.find_all('a')
                for link in links:
                    link_text = link.get_text()
                    email_match = email_pattern.search(link_text)
                    if email_match:
                        email = email_match.group()
                        break
                
                # Chercher dans les spans
                if not email:
                    spans = team_widget.find_all('span')
                    for span in spans:
                        span_text = span.get_text()
                        email_match = email_pattern.search(span_text)
                        if email_match:
                            email = email_match.group()
                            break
            
            # Si toujours pas d'email, chercher dans tout le widget
            if not email:
                widget_text = team_widget.get_text()
                email_match = email_pattern.search(widget_text)
                if email_match:
                    email = email_match.group()
            
            # Extraire le département depuis le team-position
            position = team_widget.find('div', class_='team-position')
            if position:
                dept_from_position = position.get_text(strip=True)
                if dept_from_position and len(dept_from_position) > 2:
                    current_department = dept_from_position
            
            # Séparer le nom
            name_parts = full_name.split()
            if len(name_parts) >= 2:
                # Détecter le format "NOM Prénom" (nom en majuscules)
                if name_parts[0].isupper() and len(name_parts) > 1:
                    last_name = name_parts[0]
                    first_name = ' '.join(name_parts[1:])
                else:
                    first_name = name_parts[0]
                    last_name = ' '.join(name_parts[1:])
            elif len(name_parts) == 1:
                first_name = name_parts[0]
                last_name = ""
            else:
                first_name = full_name
                last_name = ""
            
            # Nettoyer
            first_name = ' '.join(first_name.split())
            last_name = ' '.join(last_name.split())
            
            faculty_list.append({
                "last_name": last_name,
                "first_name": first_name,
                "email": email,
                "department": current_department,
                "source_url": url,
                "institution": "Faculte des Sciences Juridiques, Economiques et Sociales (FSJES) Agdal"
            })
        
        # === MÉTHODE 2: Fallback si rien trouvé ===
        if len(faculty_list) == 0:
            print("      Méthode 1: 0 trouvé, tentative méthode 2 (fallback)...")
            
            page_text = soup.get_text()
            lines = page_text.split('\n')
            
            current_dept = "General"
            for line in lines:
                line = line.strip()
                
                if 'DEPARTEMENT' in line.upper():
                    dept_match = re.search(r'DEPARTEMENT\s+DE\s+(.+?)(?:\s*$)', line, re.IGNORECASE)
                    if dept_match:
                        current_dept = dept_match.group(1).strip()
                    continue
                
                prof_match = re.match(r'^(Pr|Prof\.?|PES|PH|PA)\s+(.+)$', line, re.IGNORECASE)
                if prof_match:
                    full_name = prof_match.group(2).strip()
                    
                    email = ""
                    for i in range(1, 6):
                        if i < len(lines):
                            email_match = email_pattern.search(lines[i])
                            if email_match:
                                email = email_match.group()
                                break
                    
                    name_parts = full_name.split()
                    if len(name_parts) >= 2:
                        if name_parts[0].isupper() and len(name_parts) > 1:
                            last_name = name_parts[0]
                            first_name = ' '.join(name_parts[1:])
                        else:
                            first_name = name_parts[0]
                            last_name = ' '.join(name_parts[1:])
                    else:
                        first_name = full_name
                        last_name = ""
                    
                    faculty_list.append({
                        "last_name": last_name,
                        "first_name": first_name,
                        "email": email,
                        "department": current_dept,
                        "source_url": url,
                        "institution": "Faculte des Sciences Juridiques, Economiques et Sociales (FSJES) Agdal"
                    })
        
        # Supprimer les doublons
        unique_faculty = []
        seen = set()
        for faculty in faculty_list:
            key = f"{faculty['first_name']}_{faculty['last_name']}"
            if key not in seen and (faculty['first_name'] or faculty['last_name']):
                seen.add(key)
                unique_faculty.append(faculty)
        
        emails_found = sum(1 for f in unique_faculty if f['email'])
        print(f"      Found {len(unique_faculty)} faculty members from FSJES Agdal ({emails_found} with email)")
        return unique_faculty
        
    except Exception as e:
        print(f"      Error scraping FSJES Agdal: {e}")
        return []


# ==============================================================
# MAIN FUNCTION - COMBINED SCRAPER (AVEC BUCKETS)
# ==============================================================
def run():
    """Main function to scrape all faculty data and upload to MinIO with buckets."""
    
    client = MinIOClient(endpoint="localhost:9000")
    partition = get_date_partition()
    timestamp = partition["timestamp"]
    
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Academic Scraper/1.0)"})
    
    print("="*60)
    print("ACADEMIC FACULTY SCRAPER (EST + EMI + ENS + FSJES)")
    print("="*60)
    print(f"Date: {partition['year']}-{partition['month']}-{partition['day']}")
    print("\n📦 Buckets utilisés:")
    print("  - raw-web-html: HTML brut des pages (page_type=faculty)")
    print("  - raw-json: Données structurées")
    print("="*60)
    
    all_faculty = []
    
    # -----------------------------------------------------------------
    # A. SCRAPE EST FACULTY
    # -----------------------------------------------------------------
    print("\n[1/4] Scraping Faculty Profiles from EST Sale...")
    est_url = "https://est.um5.ac.ma/corps-professoral/"
    est_faculty = scrape_est_faculty(est_url, session)
    all_faculty.extend(est_faculty)
    print(f"  Total EST faculty: {len(est_faculty)}")
    
    # -----------------------------------------------------------------
    # B. SCRAPE EMI FACULTY (AMÉLIORÉ)
    # -----------------------------------------------------------------
    print("\n[2/4] Scraping Faculty Profiles from EMI departments...")
    emi_departments = {
        "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-civil/": "Genie Civil",
        "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-electrique/": "Genie Electrique",
        "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-industriel/": "Genie Industriel",
        "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-informatique/": "Genie Informatique",
        "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-mecanique/": "Genie Mecanique",
        "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-mineral/": "Genie Mineral",
        "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-modelisation-et-informatique-scientifique/": "Modelisation et Informatique Scientifique",
        "https://www.emi.ac.ma/emi/liste-des-enseignants/enseignants-du-departement-genie-des-procedes/": "Genie des Procedes"
    }
    
    total_emi = 0
    for url, dept_name in emi_departments.items():
        emi_faculty = scrape_emi_faculty_improved(url, session, dept_name)
        all_faculty.extend(emi_faculty)
        total_emi += len(emi_faculty)
        print(f"    {dept_name}: {len(emi_faculty)} faculty members")
    
    print(f"  Total EMI faculty: {total_emi}")
    
    # -----------------------------------------------------------------
    # C. SCRAPE ENS FACULTY (JavaScript version)
    # -----------------------------------------------------------------
    print("\n[3/4] Scraping Faculty Profiles from ENS (JavaScript)...")
    ens_js_url = "https://ens.um5.ac.ma/annuaire-des-enseignants"
    ens_js_faculty = scrape_ens_faculty(ens_js_url, session)
    all_faculty.extend(ens_js_faculty)
    print(f"  Total ENS faculty: {len(ens_js_faculty)}")
    
    # -----------------------------------------------------------------
    # D. SCRAPE FSJES AGDAL FACULTY
    # -----------------------------------------------------------------
    print("\n[4/4] Scraping Faculty Profiles from FSJES Agdal...")
    fsjes_url = "https://fsjes-agdal.um5.ac.ma/fr/corps-professoral"
    fsjes_faculty = scrape_fsjes_faculty(fsjes_url, session)
    all_faculty.extend(fsjes_faculty)
    print(f"  Total FSJES faculty: {len(fsjes_faculty)}")
    
    # -----------------------------------------------------------------
    # SAVE EVERYTHING TO MINIO (raw-json bucket)
    # -----------------------------------------------------------------
    print("\n" + "="*60)
    print("Saving all data to MinIO...")
    print("="*60)
    
    # Remove duplicates from faculty list (based on name and email)
    unique_faculty = []
    seen = set()
    for faculty in all_faculty:
        key = f"{faculty['first_name']}_{faculty['last_name']}_{faculty['email']}"
        if key not in seen and (faculty['first_name'] or faculty['last_name']):
            seen.add(key)
            
            # Ajouter les champs communs
            faculty_with_metadata = create_common_fields(
                source_system="faculty_web_scraper",
                source_url=faculty.get("source_url", ""),
                data=faculty
            )
            unique_faculty.append(faculty_with_metadata)
    
    if unique_faculty:
        faculty_data = {
            "source": "est_emi_ens_fsjes_combined",
            "table_type": "faculty_profiles",
            "scrape_timestamp": partition["iso"],
            "scrape_date": f"{partition['year']}-{partition['month']}-{partition['day']}",
            "total_faculty": len(unique_faculty),
            "faculty_members": unique_faculty
        }
        
        # Chemin avec partitionnement dans bucket raw-json
        faculty_path = (
            f"faculty_profiles/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"faculty_profiles_{timestamp}.json"
        )
        
        client.upload_json(
            bucket_name="raw-json",
            object_name=faculty_path,
            data=faculty_data
        )
        
        print(f"\n   ✅ Faculty saved: {len(unique_faculty)} -> {faculty_path}")
        print(f"   📦 Bucket: raw-json")
        
        # Breakdown by institution
        print(f"\n   📊 Breakdown by institution:")
        for institution in sorted(set(f['institution'] for f in unique_faculty)):
            count = sum(1 for f in unique_faculty if f['institution'] == institution)
            email_count = sum(1 for f in unique_faculty if f['institution'] == institution and f['email'])
            print(f"    - {institution}: {count} (with email: {email_count})")
        
        # Print sample faculty members with metadata
        print(f"\n   📝 Sample faculty members:")
        for faculty in unique_faculty[:5]:
            dept = faculty['department'] if faculty['department'] else "No department"
            email = faculty['email'] if faculty['email'] else "No email"
            print(f"    - {faculty['first_name']} {faculty['last_name']} ({dept}) - {email}")
            print(f"      Record ID: {faculty.get('record_id', '')[:20]}...")
    else:
        print("\n   ❌ No faculty data extracted")
    
    # Final summary
    print("\n" + "="*60)
    print("SCRAPING COMPLETE")
    print("="*60)
    print(f"Total faculty profiles found (before dedup): {len(all_faculty)}")
    print(f"Total unique faculty profiles: {len(unique_faculty)}")
    print(f"\n📦 Résumé des buckets utilisés:")
    print("  - raw-web-html: HTML brut sauvegardé (page_type=faculty)")
    print("  - raw-json: Données structurées sauvegardées")
    print("="*60)


if __name__ == "__main__":
    run()
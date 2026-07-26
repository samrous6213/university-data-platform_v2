import hashlib
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.storage.minio.fahd_client import MinIOClient

CONNECTOR_VERSION = "2.1.0"
SOURCE_SYSTEM = "web_crawler"

RAW_HTML_BUCKET = "raw-web-html"
RAW_JSON_BUCKET = "raw-json"
RAW_DOCUMENTS_BUCKET = "raw-documents"
LOG_BUCKET = "raw-logs"

DOCUMENT_EXTENSIONS = (".pdf", ".docx", ".xlsx")

# Garde-fous de politesse / de portee, indispensables pour un crawl recursif (fix #7)
MAX_PAGES_PER_SCHOOL = 60
CRAWL_DELAY_SECONDS = 0.5

UNIVERSAL_KEYWORDS = {
    "course_catalog": ["formation", "licence", "master", "ingenieur", "filiere", "filières", "cursus", "etudes", "departement"],
    "faculty_profiles": ["professeur", "enseignant", "corps", "recherche", "laboratoire", "staff", "annuaire", "administration"],
}

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"ingestion_web_{datetime.now():%Y%m%d}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(SOURCE_SYSTEM)


def build_session(max_retries: int = 5) -> requests.Session:
    """Meme pattern retry/backoff que les connecteurs API et documents."""
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "UniversityDataPlatformBot/1.0 (+contact: data-team@example.ma)"})
    return session


class UniversalDataCrawler:

    def __init__(self, school_id: str, school_name: str, base_url: str,
                 minio_client: MinIOClient, session: requests.Session):
        self.school_id = school_id
        self.school_name = school_name
        self.base_url = base_url
        self.minio_client = minio_client
        self.session = session
        self.domain = urlparse(base_url).netloc
        self.visited_urls = set()

        self.buckets = [RAW_HTML_BUCKET, RAW_JSON_BUCKET, RAW_DOCUMENTS_BUCKET]
        self._ensure_buckets()
        self.robot_parser = self._load_robots_txt()

        self.pages_ingested = 0
        self.documents_ingested = 0
        self.errors = 0

    def _ensure_buckets(self) -> None:
        """FIX #1/#2 : verifie/cree CHAQUE bucket (le check etait hors boucle avant)."""
        logger.info("Verification des buckets : %s", self.buckets)
        for bucket in self.buckets:
            if not self.minio_client.client.bucket_exists(bucket):
                logger.info("Creation du bucket : %s", bucket)
                self.minio_client.client.make_bucket(bucket)
        logger.info("Buckets prets.")

    def _load_robots_txt(self) -> RobotFileParser:
        """Respecte robots.txt quand il existe. Si le fichier est illisible/absent,
        RobotFileParser.can_fetch() bascule par defaut sur un comportement RESTRICTIF
        (bloque tout) plutot que permissif -> on force explicitement l'autorisation
        via self.robots_loaded pour que le comportement corresponde au message loggue."""
        parser = RobotFileParser()
        self.robots_loaded = False
        robots_url = urljoin(self.base_url, "/robots.txt")
        try:
            parser.set_url(robots_url)
            parser.read()
            self.robots_loaded = True
        except Exception:
            logger.warning("robots.txt introuvable ou illisible pour %s, on continue sans restriction.", self.domain)
        return parser

    def _can_fetch(self, url: str) -> bool:
        if not self.robots_loaded:
            return True  # pas de robots.txt exploitable -> autorise par defaut
        try:
            return self.robot_parser.can_fetch("UniversityDataPlatformBot", url)
        except Exception:
            return True

    def _classify_url(self, url: str) -> str:
        url_lower = url.lower()
        if any(kw in url_lower for kw in UNIVERSAL_KEYWORDS["course_catalog"]):
            return "course_catalog"
        if any(kw in url_lower for kw in UNIVERSAL_KEYWORDS["faculty_profiles"]):
            return "faculty_profiles"
        return "general_pages"

    def _generate_object_key(self, entity: str, content_hash: str, extension: str) -> str:
        """FIX #6 : cle basee sur le hash de contenu -> idempotent (rerun == meme objet si inchange)."""
        now = datetime.now()
        return (
            f"source={self.school_id}/entity={entity}/"
            f"year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/"
            f"{content_hash[:12]}.{extension}"
        )

    def _upload_binary(self, bucket: str, path: str, data: bytes, content_type: str, metadata: dict) -> None:
        """FIX #5 : metadata de tracabilite attachee a CHAQUE objet, pas seulement au JSON."""
        self.minio_client.upload_binary(bucket, path, data, content_type, metadata=metadata)

    def crawl_and_ingest(self, current_url: str, max_depth: int = 2, current_depth: int = 0) -> None:
        if self.pages_ingested >= MAX_PAGES_PER_SCHOOL:
            return
        if current_depth > max_depth or current_url in self.visited_urls:
            return
        if urlparse(current_url).netloc != self.domain:
            return
        if not self._can_fetch(current_url):
            logger.info("robots.txt interdit : %s", current_url)
            return

        self.visited_urls.add(current_url)
        time.sleep(CRAWL_DELAY_SECONDS)  # politesse envers le serveur cible
        logger.info("[depth=%s] Ingestion : %s", current_depth, current_url)

        try:
            response = self.session.get(current_url, timeout=(5, 10))
            if response.status_code != 200:
                logger.warning("HTTP %s pour %s, page ignoree.", response.status_code, current_url)
                return

            entity_type = self._classify_url(current_url)
            content_bytes = response.content
            content_hash = hashlib.sha256(content_bytes).hexdigest()
            extraction_ts = datetime.now(timezone.utc).isoformat()

            # Cles calculees a l'avance pour pouvoir se referencer mutuellement
            # (fix traçabilite : avant, le lien HTML<->JSON n'etait qu'implicite
            # via le hash partage, rien ne le rendait explicite dans les metadata).
            html_path = self._generate_object_key(entity_type, content_hash, "html")
            json_path = self._generate_object_key(entity_type, content_hash, "json")
            html_object_uri = f"s3://{RAW_HTML_BUCKET}/{html_path}"
            json_object_uri = f"s3://{RAW_JSON_BUCKET}/{json_path}"

            base_metadata = {
                "source_system": SOURCE_SYSTEM,
                "source_url": current_url,
                "extraction_timestamp": extraction_ts,
                "http_status": str(response.status_code),
                "content_checksum": content_hash,
                "connector_version": CONNECTOR_VERSION,
                "school_id": self.school_id,
                "school_name": self.school_name,
                "entity_type": entity_type,
            }

            # 1. HTML brut (reference vers son jumeau JSON)
            html_metadata = {**base_metadata, "json_object_path": json_object_uri}
            self._upload_binary(RAW_HTML_BUCKET, html_path, content_bytes, "text/html", html_metadata)

            # 2. Metadonnees + texte extrait (couche Bronze), reference vers son jumeau HTML
            soup = BeautifulSoup(content_bytes, "html.parser")
            json_metadata = {**base_metadata, "html_object_path": html_object_uri}
            json_payload = {**json_metadata, "extracted_text": soup.get_text(separator=" ", strip=True)}
            self.minio_client.upload_json(RAW_JSON_BUCKET, json_path, json_payload, metadata=json_metadata)

            self.pages_ingested += 1

            # 3. Documents lies (PDF/DOCX/XLSX)
            for link in soup.find_all(["a", "link", "iframe"], href=True):
                file_url = urljoin(current_url, link["href"])
                if any(file_url.lower().endswith(ext) for ext in DOCUMENT_EXTENSIONS):
                    self._ingest_document(file_url, entity_type)

            # Traitement recursif des liens internes
            for link in soup.find_all("a", href=True):
                next_url = urljoin(current_url, link["href"]).split("#")[0]
                self.crawl_and_ingest(next_url, max_depth, current_depth + 1)

        except Exception as e:
            self.errors += 1
            logger.exception("Erreur sur %s : %s", current_url, e)

    def _ingest_document(self, file_url: str, entity_type: str) -> None:
        try:
            file_res = self.session.get(file_url, timeout=15)
            if file_res.status_code != 200:
                return
            doc_bytes = file_res.content
            doc_hash = hashlib.sha256(doc_bytes).hexdigest()
            ext = file_url.split(".")[-1].split("?")[0]
            doc_path = self._generate_object_key(entity_type, doc_hash, ext)

            metadata = {
                "source_system": SOURCE_SYSTEM,
                "source_url": file_url,
                "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
                "http_status": str(file_res.status_code),
                "content_checksum": doc_hash,
                "connector_version": CONNECTOR_VERSION,
                "school_id": self.school_id,
                "entity_type": entity_type,
            }
            self._upload_binary(RAW_DOCUMENTS_BUCKET, doc_path, doc_bytes, "application/octet-stream", metadata)
            self.documents_ingested += 1
            logger.info("Document stocke : s3://%s/%s", RAW_DOCUMENTS_BUCKET, doc_path)
        except Exception as e:
            self.errors += 1
            logger.warning("Echec telechargement document %s : %s", file_url, e)

    def write_run_log(self, ingestion_id: str) -> None:
        """FIX (nouveau) : log d'execution par ecole, meme pattern que les autres connecteurs."""
        log_payload = {
            "ingestion_id": ingestion_id,
            "source": SOURCE_SYSTEM,
            "school_id": self.school_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pages_ingested": self.pages_ingested,
            "documents_ingested": self.documents_ingested,
            "errors": self.errors,
            "connector_version": CONNECTOR_VERSION,
        }
        now = datetime.now()
        log_path = (
            f"source={self.school_id}/year={now.strftime('%Y')}/month={now.strftime('%m')}/"
            f"day={now.strftime('%d')}/run_{ingestion_id}.json"
        )
        self.minio_client.upload_json(LOG_BUCKET, log_path, log_payload)
        logger.info("Log d'execution stocke : s3://%s/%s", LOG_BUCKET, log_path)


def _find_project_root(marker: str = "configs", max_levels: int = 6) -> Path:
    """Remonte l'arborescence depuis ce fichier jusqu'a trouver le dossier 'configs/'.
    Robuste independamment de la profondeur reelle du fichier (src/ingestion/web/...),
    contrairement a un nombre fixe de .parent qui casse si le fichier est deplace."""
    current = Path(__file__).resolve().parent
    for _ in range(max_levels):
        if (current / marker).is_dir():
            return current
        current = current.parent
    raise FileNotFoundError(
        f"Impossible de localiser le dossier '{marker}/' en remontant depuis {Path(__file__).resolve()}"
    )


DEFAULT_CONFIG_PATH = _find_project_root() / "configs" / "schools_config.json"


def run(config_path: str | Path | None = None, max_depth: int = 2) -> dict:
    """Point d'entree appelable depuis Airflow (@task) ou en CLI.

    FIX chemin de config : un chemin relatif ("schools_config.json") casse des
    que le script tourne depuis un autre repertoire de travail (le cas normal
    avec Airflow, dont le worker n'a pas le repo comme cwd). Par defaut, on
    resout le chemin relativement a l'emplacement de CE fichier, independamment
    du repertoire depuis lequel Python a ete lance.
    """
    config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    session = build_session()
    minio_client = MinIOClient()
    summaries = []

    for school in config["schools"]:
        ingestion_id = str(uuid.uuid4())
        crawler = UniversalDataCrawler(
            school_id=school["id"],
            school_name=school["name"],
            base_url=school["base_url"],
            minio_client=minio_client,
            session=session,
        )
        try:
            crawler.crawl_and_ingest(school["base_url"], max_depth=max_depth)
        finally:
            crawler.write_run_log(ingestion_id)

        summaries.append({
            "school_id": school["id"],
            "pages_ingested": crawler.pages_ingested,
            "documents_ingested": crawler.documents_ingested,
            "errors": crawler.errors,
        })

    result = {"summaries": summaries, "schools_with_errors": sum(1 for s in summaries if s["errors"] > 0)}
    logger.info("Resume du run web : %s", result)

    # Echec critique : une ecole n'a RIEN recupere du tout (site injoignable,
    # MinIO down, robots.txt bloque tout...). C'est un vrai signal a remonter.
    critical_failures = [s["school_id"] for s in summaries if s["pages_ingested"] == 0]
    if critical_failures:
        raise RuntimeError(f"Ingestion web : echec total pour {critical_failures} -- {result}")

    # Erreurs mineures ponctuelles (PDF casse, page 404 isolee...) : tracees
    # dans les logs et dans raw-logs (write_run_log), mais ne bloquent pas
    # le pipeline puisque l'essentiel des donnees a ete recupere.
    if result["schools_with_errors"]:
        logger.warning("Crawl termine avec des erreurs mineures ponctuelles : %s", result)

    return result


if __name__ == "__main__":
    import sys
    try:
        run()
    except RuntimeError:
        sys.exit(1)
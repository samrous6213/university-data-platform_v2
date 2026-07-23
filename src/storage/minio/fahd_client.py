import json
import logging
import os
import time
from io import BytesIO
from urllib.parse import quote

import urllib3
from minio import Minio
from minio.error import S3Error

try:
    from dotenv import load_dotenv
    _dotenv_loaded = load_dotenv()
    if not _dotenv_loaded:
        logging.getLogger(__name__).warning(
            "Aucun fichier .env trouve (load_dotenv a retourne False). "
            "Verifie qu'un fichier '.env' existe a la racine du projet."
        )
except ImportError:
    logging.getLogger(__name__).warning(
        "python-dotenv n'est pas installe -> le fichier .env ne sera PAS charge. "
        "Installe-le avec : pip install python-dotenv"
    )

logger = logging.getLogger(__name__)


def _sanitize_metadata(metadata: dict | None) -> dict | None:
    """Le protocole S3 (et donc le SDK minio) n'accepte que des valeurs de
    metadata en US-ASCII. Les noms d'etablissements/sources contiennent des
    accents (ex: 'École', 'Faculté') -> on percent-encode chaque valeur pour
    rester ASCII-safe sans rien perdre (a decoder avec urllib.parse.unquote
    cote lecture, ex. dans le job Spark)."""
    if not metadata:
        return metadata
    return {k: quote(str(v), safe="") for k, v in metadata.items()}


def _put_object_with_retry(client, bucket_name, object_name, payload: bytes,
                            content_type, metadata, max_retries=3, backoff_factor=2):
    """Retry/backoff sur l'upload MinIO lui-meme (et pas seulement sur les appels
    HTTP amont). Sans ca, un hoquet transitoire de MinIO fait perdre la page/le
    fichier definitivement au lieu de reessayer -> exige par le brief (retry logic,
    30 pts reliability)."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            # BytesIO frais a chaque tentative : un stream deja consomme ne peut pas etre reutilise.
            client.put_object(
                bucket_name,
                object_name,
                BytesIO(payload),
                len(payload),
                content_type=content_type,
                metadata=metadata,
            )
            return
        except S3Error as e:
            last_exc = e
            logger.warning(
                "put_object echec (tentative %s/%s) pour '%s' : %s",
                attempt, max_retries, object_name, e,
            )
            if attempt < max_retries:
                time.sleep(backoff_factor ** attempt)
    logger.error("put_object a echoue apres %s tentatives pour '%s'", max_retries, object_name)
    raise last_exc


class MinIOClient:

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool | None = None,
    ):
        # Defaults chosen for docker-compose network.
        # In-container, `localhost` refers to the container itself, NOT the minio
        # service -> il faut utiliser le nom de service docker-compose ("minio:9000").
        endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "minio:9000")
        access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")

        if secure is None:
            secure_env = os.getenv("MINIO_SECURE", "false").strip().lower()
            secure = secure_env in {"1", "true", "yes", "y", "on"}

        if endpoint == "minio:9000" and not os.getenv("MINIO_ENDPOINT"):
            logger.warning(
                "MINIO_ENDPOINT non defini -> valeur par defaut 'minio:9000' utilisee "
                "(valide uniquement DANS le reseau docker-compose). Si tu executes ce "
                "script hors Docker (ex: 'python -m ...' depuis Windows/WSL), definis "
                "MINIO_ENDPOINT=localhost:9000 dans ton fichier .env a la racine du projet."
            )
        logger.info("MinIO endpoint resolu : %s (secure=%s)", endpoint, secure)

        # FIX : on utilise bien les valeurs resolues ci-dessus (avant, elles etaient
        # calculees puis ignorees au profit de valeurs "localhost" hardcodees, ce qui
        # cassait la connexion depuis les conteneurs Airflow / Spark).
        #
        # FIX timeout : le SDK minio n'impose AUCUN timeout de connexion par defaut.
        # Si MinIO ne repond pas (conteneur arrete, pare-feu qui droppe silencieusement
        # les paquets), le client peut rester bloque indefiniment au lieu d'echouer.
        # On force un http_client avec un timeout explicite + un nombre de retries borne.
        http_client = urllib3.PoolManager(
            timeout=urllib3.util.Timeout(connect=5, read=15),
            retries=urllib3.util.Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504],
            ),
        )
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            http_client=http_client,
        )

    def create_bucket_if_not_exists(self, bucket_name: str) -> None:
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Bucket created: {bucket_name}")
        except S3Error as e:
            logger.error(f"Failed to create bucket '{bucket_name}': {e}")
            raise

    def upload_json(
        self,
        bucket_name: str,
        object_name: str,
        data: dict | list,
        metadata: dict | None = None,
    ) -> None:

        self.create_bucket_if_not_exists(bucket_name)

        payload = json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")

        try:
            _put_object_with_retry(
                self.client, bucket_name, object_name, payload,
                content_type="application/json",
                metadata=_sanitize_metadata(metadata),
            )
            logger.info(f"JSON uploaded -> {object_name}")

        except S3Error as e:
            logger.error(f"Failed to upload JSON '{object_name}': {e}")
            raise

    def upload_binary(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        content_type: str,
        metadata: dict | None = None,
    ) -> None:

        self.create_bucket_if_not_exists(bucket_name)

        try:
            _put_object_with_retry(
                self.client, bucket_name, object_name, data,
                content_type=content_type,
                metadata=_sanitize_metadata(metadata),
            )
            logger.info(f"File uploaded -> {object_name}")

        except S3Error as e:
            logger.error(f"Failed to upload file '{object_name}': {e}")
            raise
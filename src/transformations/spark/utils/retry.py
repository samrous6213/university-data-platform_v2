"""
Pattern retry/backoff generique pour les etapes Spark sensibles aux erreurs
transitoires (ecriture Hudi, lecture S3A, sync Hive). Meme logique que
_put_object_with_retry dans fahd_client.py, mais reutilisable via decorateur.
"""

import functools
import logging
import time

logger = logging.getLogger(__name__)


def retry(max_attempts: int = 3, backoff_factor: float = 2, exceptions: tuple = (Exception,)):
    """
    Reessaie une fonction en cas d'exception transitoire, avec backoff exponentiel
    (2s, 4s, 8s, ...). Relance la derniere exception si toutes les tentatives echouent.

    Usage:
        @retry(max_attempts=3, backoff_factor=2, exceptions=(IOError,))
        def write_table(df, path):
            ...
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    logger.warning(
                        "Echec '%s' (tentative %s/%s) : %s",
                        func.__name__, attempt, max_attempts, e,
                    )
                    if attempt < max_attempts:
                        time.sleep(backoff_factor ** attempt)
            logger.error("'%s' a echoue apres %s tentatives", func.__name__, max_attempts)
            raise last_exc

        return wrapper

    return decorator
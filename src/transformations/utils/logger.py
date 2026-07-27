"""
logger.py
==========

Configuration centralisee du logging pour tous les modules de la couche
transformations du projet University Data Platform.

Ce module fournit une fonction utilitaire ``get_logger`` qui retourne un
logger Python preconfigure, reutilisable par chaque module du pipeline ETL.

Le format de log est identique a celui utilise dans les parsers existants
(hiba_json_parser, hiba_html_parser, etc.) pour garantir la cohesion
des journaux d'execution.

Utilisation :
    from src.transformations.utils.logger import get_logger
    logger = get_logger(__name__)
"""

from __future__ import annotations

import logging
import sys

_CONFIGURED: bool = False


def _configure_root() -> None:
    """
    Configure le root logger une seule fois.

    Le format de log est aligne sur celui des parsers HibA existants :
        %(asctime)s - %(levelname)s - %(name)s - %(message)s

    La configuration n'est appliquee qu'une seule fois, meme si
    ``get_logger`` est appele depuis plusieurs modules.
    """
    global _CONFIGURED

    if _CONFIGURED:
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        stream=sys.stdout,
    )

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """
    Retourne un logger Python configure pour le module appele.

    Configure automatiquement le root logger la premiere fois que cette
    fonction est invoquee, puis retourne un logger portant le nom
    specifie.

    Args:
        name: nom du logger (generalement ``__name__`` du module appelant).

    Returns:
        logging.Logger: instance de logger prete a l'emploi.
    """
    _configure_root()
    return logging.getLogger(name)

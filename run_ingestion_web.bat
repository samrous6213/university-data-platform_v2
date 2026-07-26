@echo off
REM ============================================================
REM Ingestion web statique - generic_crawler (BeautifulSoup)
REM Classification automatique course_catalog / faculty_profiles
REM selon schools_config.json (configs/schools_config.json a la
REM racine du projet, resolu automatiquement par _find_project_root()
REM dans generic_crawler.py - aucun argument requis ici).
REM ============================================================

chcp 65001 >nul
set PYTHONIOENCODING=utf-8

cd /d C:\Users\Fahds\university-data-platform_v2
if errorlevel 1 (
    echo [ERREUR] Impossible d'acceder a C:\Users\Fahds\university-data-platform_v2
    exit /b 1
)

C:\Users\Fahds\university-data-platform_v2\.venv\Scripts\python.exe -m src.ingestion.web.generic_crawler

exit /b %ERRORLEVEL%
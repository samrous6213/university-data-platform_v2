@echo off
REM ============================================================
REM Ingestion fichiers/documents - data.gov.ma (CKAN)
REM Meme pattern que les autres .bat d'ingestion/transformation.
REM ============================================================

chcp 65001 >nul
set PYTHONIOENCODING=utf-8

cd /d C:\Users\Fahds\university-data-platform_v2
if errorlevel 1 (
    echo [ERREUR] Impossible d'acceder a C:\Users\Fahds\university-data-platform_v2
    exit /b 1
)

C:\Users\Fahds\university-data-platform_v2\.venv\Scripts\python.exe -m src.ingestion.docs.fahd_datagov

exit /b %ERRORLEVEL%
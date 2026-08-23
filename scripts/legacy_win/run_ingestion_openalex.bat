@echo off
REM ============================================================
REM Ingestion API - OpenAlex (publications par institution)
REM Meme pattern que run_faculty_profiles.bat / run_course_catalog.bat :
REM   - chcp 65001 + PYTHONIOENCODING=utf-8 -> evite UnicodeEncodeError
REM     sur les caracteres arabes/accents en session SSH non-interactive
REM     (console Windows en cp1252 par defaut)
REM   - toute la logique est dans CE fichier -> aucun guillemet imbrique
REM     n'est transmis via SSHOperator/Win32-OpenSSH/cmd.exe
REM ============================================================

chcp 65001 >nul
set PYTHONIOENCODING=utf-8

cd /d C:\Users\Fahds\university-data-platform_v2
if errorlevel 1 (
    echo [ERREUR] Impossible d'acceder a C:\Users\Fahds\university-data-platform_v2
    exit /b 1
)

C:\Users\Fahds\university-data-platform_v2\.venv\Scripts\python.exe -m src.ingestion.api.Fahd_openalex

REM Propage le code de sortie reel du script Python (sys.exit(1) en cas
REM d'echec) vers SSHOperator, pour que retries/alerting Airflow fonctionnent.
exit /b %ERRORLEVEL%
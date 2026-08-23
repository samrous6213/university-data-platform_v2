@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
cd /d C:\Users\Fahds\university-data-platform_v2
call .venv\Scripts\activate.bat
python -m src.transformations.spark.jobs.faculty_profiles_job

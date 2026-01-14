@echo off
REM ----------------------------
REM Script pour lancer le scheduler Python dans le venv
REM ----------------------------

REM 1. Aller dans le dossier du projet
cd /d "C:\Users\hp\Desktop\procPipeline\PipelineProc"



REM 2. Lancer le script Python
python "C:\Users\hp\Desktop\procPipeline\PipelineProc\daily_scheduler.py"

REM 3. Optionnel : laisser la fenêtre ouverte après l'exécution
pause
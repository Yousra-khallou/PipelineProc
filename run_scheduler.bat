@echo off
REM ========================================
REM Script d'Exécution Quotidienne du Pipeline
REM À planifier avec Windows Task Scheduler pour 22:00
REM ========================================

REM Rediriger la sortie vers un fichier log
set LOGFILE=logs\scheduler_%date:~-4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%.log
echo ============================================ > %LOGFILE%
echo Pipeline de Procurement - Execution Quotidienne >> %LOGFILE%
echo Date: %date% %time% >> %LOGFILE%
echo ============================================ >> %LOGFILE%

REM Aller dans le bon répertoire
cd /d "C:\Users\hp\Desktop\procPipeline\PipelineProc"

REM Utiliser directement l'interpréteur Python du venv
set PYTHON=venv\Scripts\python.exe

REM Générer les données du jour
echo. >> %LOGFILE%
echo [1/4] Generation des commandes... >> %LOGFILE%
%PYTHON% generate_orders.py >> %LOGFILE% 2>&1

if %ERRORLEVEL% NEQ 0 (
    echo ERREUR: Generation des commandes echouee >> %LOGFILE%
    exit /b 1
)

echo. >> %LOGFILE%
echo [2/4] Generation des stocks... >> %LOGFILE%
%PYTHON% generate_stock.py >> %LOGFILE% 2>&1

if %ERRORLEVEL% NEQ 0 (
    echo ERREUR: Generation des stocks echouee >> %LOGFILE%
    exit /b 1
)

echo. >> %LOGFILE%
echo [3/4] Execution du pipeline... >> %LOGFILE%
%PYTHON% procurement_pipeline.py >> %LOGFILE% 2>&1

if %ERRORLEVEL% NEQ 0 (
    echo ERREUR: Pipeline echoue >> %LOGFILE%
    exit /b 1
)

echo. >> %LOGFILE%
echo [4/4] Pipeline termine avec succes! >> %LOGFILE%
echo ============================================ >> %LOGFILE%
echo Logs disponibles dans: %LOGFILE% >> %LOGFILE%

exit /b 0
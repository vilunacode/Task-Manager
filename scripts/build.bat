@echo off
powershell -ExecutionPolicy Bypass -File "%~dp0build.ps1"
if %ERRORLEVEL% neq 0 (
    echo.
    echo [FEHLER] Script mit Fehlercode %ERRORLEVEL% beendet.
    pause
)

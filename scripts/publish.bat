@echo off
powershell -ExecutionPolicy Bypass -File "%~dp0publish.ps1"
echo.
echo Druecke eine beliebige Taste zum Beenden...
pause >nul

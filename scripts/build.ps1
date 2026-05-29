# EXE-Build-Script fuer Ticket-System
# Ausfuehren mit: Doppelklick auf build.bat im Projektordner

$ProjectRoot = Split-Path $PSScriptRoot
Set-Location $ProjectRoot

$AppName   = "Ticket-System"
$IconFile  = "thp_large.ico"
$DistDir   = "dist"
$BuildDir  = "build"
$SpecFile  = "$AppName.spec"

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Ticket-System EXE-Builder" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# ── Python pruefen ────────────────────────────────────────
Write-Host ">> Python pruefen..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "   $pythonVersion gefunden." -ForegroundColor Green
} catch {
    Write-Host "   FEHLER: Python nicht gefunden. Bitte Python installieren." -ForegroundColor Red
    Read-Host "Druecke Enter zum Beenden"
    exit 1
}

# ── Abhaengigkeiten installieren ──────────────────────────
Write-Host ""
Write-Host ">> Abhaengigkeiten installieren..." -ForegroundColor Yellow
pip install -r requirements.txt --quiet
pip install pyinstaller tzdata pystray Pillow --quiet
Write-Host "   Fertig." -ForegroundColor Green

# ── Alte Build-Artefakte bereinigen ───────────────────────
Write-Host ""
Write-Host ">> Alte Build-Dateien bereinigen..." -ForegroundColor Yellow

if (Test-Path $DistDir)  { Remove-Item $DistDir  -Recurse -Force }
if (Test-Path $BuildDir) { Remove-Item $BuildDir -Recurse -Force }
if (Test-Path $SpecFile) { Remove-Item $SpecFile -Force }

Write-Host "   Fertig." -ForegroundColor Green

# ── EXE erstellen ─────────────────────────────────────────
Write-Host ""
Write-Host ">> EXE wird erstellt (kann einige Minuten dauern)..." -ForegroundColor Yellow
Write-Host ""

python -m PyInstaller app.py `
    --onefile `
    --name $AppName `
    --add-data "templates;templates" `
    --add-data "static;static" `
    --add-data "config.ini;." `
    --add-data "$IconFile;." `
    --hidden-import zoneinfo `
    --collect-data tzdata `
    --hidden-import flask `
    --hidden-import werkzeug `
    --hidden-import tkinter `
    --hidden-import pystray `
    --hidden-import PIL `
    --noconsole `
    --icon $IconFile `
    --version-file "version.txt"

# ── Ergebnis pruefen ──────────────────────────────────────
Write-Host ""
$ExePath = Join-Path $DistDir "$AppName.exe"

if (Test-Path $ExePath) {
    $SizeMB = [math]::Round((Get-Item $ExePath).Length / 1MB, 1)
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "  EXE erfolgreich erstellt!" -ForegroundColor Green
    Write-Host "  Pfad : $((Resolve-Path $ExePath).Path)" -ForegroundColor Green
    Write-Host "  Groesse: $SizeMB MB" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
} else {
    Write-Host "============================================" -ForegroundColor Red
    Write-Host "  FEHLER: EXE wurde nicht erstellt." -ForegroundColor Red
    Write-Host "  Bitte die Ausgabe oben auf Fehler pruefen." -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Red
}

Write-Host ""
Read-Host "Druecke Enter zum Beenden"

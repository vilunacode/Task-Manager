# EXE-Build-Script fuer Ticket-System
# Ausfuehren mit: Doppelklick auf build.bat im Projektordner

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path $PSScriptRoot
Set-Location $ProjectRoot

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Ticket-System EXE-Builder" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# ── Benutzereingaben ──────────────────────────────────────
$AppName = Read-Host ">> App-Name (Enter fuer 'Ticket-System')"
if ([string]::IsNullOrWhiteSpace($AppName)) { $AppName = "Ticket-System" }

Write-Host ""
$IcoFiles = Get-ChildItem -Path $ProjectRoot -Filter "*.ico" | Select-Object -ExpandProperty Name
if ($IcoFiles.Count -gt 0) {
    Write-Host "   Verfuegbare Icon-Dateien:" -ForegroundColor DarkGray
    $IcoFiles | ForEach-Object { Write-Host "   - $_" -ForegroundColor DarkGray }
    Write-Host ""
}
$IconFile = Read-Host ">> Icon-Datei (Enter fuer 'ts.ico')"
if ([string]::IsNullOrWhiteSpace($IconFile)) { $IconFile = "ts.ico" }

if (-not (Test-Path $IconFile)) {
    Write-Host ""
    Write-Host "   FEHLER: '$IconFile' nicht gefunden." -ForegroundColor Red
    Read-Host "Druecke Enter zum Beenden"
    exit 1
}

Write-Host ""
Write-Host "   App-Name : $AppName" -ForegroundColor Green
Write-Host "   Icon     : $IconFile" -ForegroundColor Green
Write-Host ""

$DistDir   = "dist"
$BuildDir  = "build"
$SpecFile  = "$AppName.spec"

# ── Python pruefen ────────────────────────────────────────
Write-Host ">> Python pruefen..." -ForegroundColor Yellow
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "   FEHLER: Python nicht gefunden. Bitte Python installieren und PATH pruefen." -ForegroundColor Red
    Read-Host "Druecke Enter zum Beenden"
    exit 1
}
$pythonVersion = python --version 2>&1
Write-Host "   $pythonVersion gefunden." -ForegroundColor Green

# ── Abhaengigkeiten installieren ──────────────────────────
Write-Host ""
Write-Host ">> Abhaengigkeiten installieren..." -ForegroundColor Yellow
$ErrorActionPreference = "Continue"
python -m pip install -r requirements.txt --quiet
python -m pip install pyinstaller tzdata pystray Pillow --quiet
$ErrorActionPreference = "Stop"
Write-Host "   Fertig." -ForegroundColor Green

# ── Alte Build-Artefakte bereinigen ───────────────────────
Write-Host ""
Write-Host ">> Alte Build-Dateien bereinigen..." -ForegroundColor Yellow

if (Test-Path $DistDir)  { Remove-Item $DistDir  -Recurse -Force }
if (Test-Path $BuildDir) { Remove-Item $BuildDir -Recurse -Force }
if (Test-Path $SpecFile) { Remove-Item $SpecFile -Force }

Write-Host "   Fertig." -ForegroundColor Green

# ── Icon mit festem Namen kopieren ───────────────────────
$TrayIcon = "_tray.ico"
Copy-Item $IconFile $TrayIcon

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
    --add-data "$TrayIcon;." `
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

# ── Temporaere Icon-Kopie entfernen ───────────────────────
Remove-Item $TrayIcon -ErrorAction SilentlyContinue

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

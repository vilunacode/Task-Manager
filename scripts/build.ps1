# EXE-Build-Script fuer Ticket-System
# Ausfuehren mit: Doppelklick auf build.bat im Projektordner

$ErrorActionPreference = "Stop"

try {

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
$IconFiles = Get-ChildItem -Path $ProjectRoot -File | Where-Object { $_.Extension -in ".ico",".png" } | Select-Object -ExpandProperty Name
if ($IconFiles.Count -gt 0) {
    Write-Host "   Verfuegbare Icon-Dateien (.ico / .png):" -ForegroundColor DarkGray
    $IconFiles | ForEach-Object { Write-Host "   - $_" -ForegroundColor DarkGray }
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

$IconExt = [System.IO.Path]::GetExtension($IconFile).ToLower()

Write-Host ""
Write-Host "   App-Name : $AppName" -ForegroundColor Green
Write-Host "   Icon     : $IconFile" -ForegroundColor Green
Write-Host ""

$DistDir   = "dist"
$BuildDir  = "build"
$SpecFile  = "$AppName.spec"

# ── Python pruefen ────────────────────────────────────────
Write-Host ">> Python pruefen..." -ForegroundColor Yellow
$PythonCmd = $null
foreach ($candidate in @("py", "python3", "python")) {
    $cmd = Get-Command $candidate -ErrorAction SilentlyContinue
    if (-not $cmd) { continue }
    try {
        $testOut = & $candidate --version 2>&1
        if ($testOut -match "Python \d+\.\d+") {
            $PythonCmd = $candidate
            break
        }
    } catch { }
}
if (-not $PythonCmd) {
    Write-Host ""
    Write-Host "   FEHLER: Kein Python gefunden." -ForegroundColor Red
    Write-Host "   Bitte Python von https://www.python.org/downloads/ installieren" -ForegroundColor Red
    Write-Host "   und sicherstellen, dass 'Add Python to PATH' beim Setup aktiviert war." -ForegroundColor Red
    Write-Host "   Tipp: Den Microsoft-Store-Alias unter Einstellungen > Apps >" -ForegroundColor DarkGray
    Write-Host "         Erweiterte App-Einstellungen > App-Ausfuehrungsaliase deaktivieren." -ForegroundColor DarkGray
    Read-Host "Druecke Enter zum Beenden"
    exit 1
}
$pythonVersion = & $PythonCmd --version 2>&1
Write-Host "   $pythonVersion gefunden (Befehl: $PythonCmd)." -ForegroundColor Green

# ── Abhaengigkeiten installieren ──────────────────────────
Write-Host ""
Write-Host ">> Abhaengigkeiten installieren..." -ForegroundColor Yellow
$ErrorActionPreference = "Continue"
& $PythonCmd -m pip install -r requirements.txt --quiet
& $PythonCmd -m pip install pyinstaller tzdata pystray Pillow --quiet
$ErrorActionPreference = "Stop"
Write-Host "   Fertig." -ForegroundColor Green

# ── Alte Build-Artefakte bereinigen ───────────────────────
Write-Host ""
Write-Host ">> Alte Build-Dateien bereinigen..." -ForegroundColor Yellow

if (Test-Path $DistDir)  { Remove-Item $DistDir  -Recurse -Force }
if (Test-Path $BuildDir) { Remove-Item $BuildDir -Recurse -Force }
if (Test-Path $SpecFile) { Remove-Item $SpecFile -Force }

Write-Host "   Fertig." -ForegroundColor Green

# ── Icon vorbereiten ─────────────────────────────────────
# Quellbild pruefen und Multi-Size-ICO erstellen
$IcoForBuild = "_build_icon_temp.ico"
Write-Host ">> Icon wird aufbereitet..." -ForegroundColor Yellow
& $PythonCmd -c "
from PIL import Image
import sys
img = Image.open('$IconFile').convert('RGBA')
w, h = img.size
if w < 256 or h < 256:
    print(f'   HINWEIS: Quellbild ist nur {w}x{h} px. Fuer scharfe Icons mind. 256x256 verwenden.')
sizes = [(256,256),(128,128),(64,64),(48,48),(32,32),(16,16)]
imgs = [img.resize(s, Image.LANCZOS) for s in sizes]
imgs[0].save('$IcoForBuild', format='ICO', sizes=[i.size for i in imgs], append_images=imgs[1:])
print('   ICO mit allen Groessen erstellt.')
"
if (-not (Test-Path $IcoForBuild)) {
    Write-Host "   FEHLER: ICO-Erstellung fehlgeschlagen." -ForegroundColor Red
    Read-Host "Druecke Enter zum Beenden"
    exit 1
}
$IconForPyInstaller = $IcoForBuild
Write-Host "   Fertig." -ForegroundColor Green

if ($IconExt -eq ".png") {
    $TrayIcon = "_tray.png"
    Copy-Item $IconFile $TrayIcon
} else {
    $TrayIcon = "_tray.ico"
    Copy-Item $IconFile $TrayIcon
}
$IcoForBuild = "_build_icon_temp.ico"

# ── version.txt mit App-Name generieren ──────────────────
$VersionFile = "version.txt"
$VersionContent = @"
VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=(1, 0, 0, 0),
    prodvers=(1, 0, 0, 0),
  ),
  kids=[
    StringFileInfo([
      StringTable(
        u'040904B0',
        [
          StringStruct(u'CompanyName', u'vilunacode'),
          StringStruct(u'FileDescription', u'$AppName'),
          StringStruct(u'FileVersion', u'1.0.0'),
          StringStruct(u'ProductName', u'$AppName'),
          StringStruct(u'ProductVersion', u'1.0.0'),
          StringStruct(u'LegalCopyright', u'Made by vilunacode'),
        ]
      )
    ]),
    VarFileInfo([VarStruct(u'Translation', [1033, 1200])])
  ]
)
"@
Set-Content -Path $VersionFile -Value $VersionContent -Encoding UTF8

# ── EXE erstellen ─────────────────────────────────────────
Write-Host ""
Write-Host ">> EXE wird erstellt (kann einige Minuten dauern)..." -ForegroundColor Yellow
Write-Host ""

& $PythonCmd -m PyInstaller app.py `
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
    --icon $IconForPyInstaller `
    --version-file "version.txt"

# ── Temporaere Dateien entfernen ─────────────────────────
Remove-Item $TrayIcon -ErrorAction SilentlyContinue
if ($IcoForBuild) { Remove-Item $IcoForBuild -ErrorAction SilentlyContinue }

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
Write-Host ">> Icon-Cache leeren..." -ForegroundColor Yellow
$ErrorActionPreference = "Continue"
ie4uinit.exe -show 2>$null
$ErrorActionPreference = "Stop"
Write-Host "   Fertig. Falls das Icon im Explorer noch falsch angezeigt wird, bitte den Explorer neu starten." -ForegroundColor DarkGray

} catch {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Red
    Write-Host "  UNERWARTETER FEHLER:" -ForegroundColor Red
    Write-Host "  $_" -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Red
    Write-Host ""
    Read-Host "Druecke Enter zum Beenden"
    exit 1
}

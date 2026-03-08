# Automatisches Git-Publish-Script
# Führe dieses Script aus, um alle Änderungen zu committen und zu pushen

param(
    [string]$CommitMessage = "Auto-commit: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
)

# Git zum PATH hinzufügen
$env:PATH += ";C:\Program Files\Git\cmd"

# Zum Projekt-Verzeichnis wechseln
Set-Location $PSScriptRoot

# Git-Status überprüfen
$status = git status --porcelain
if ($status) {
    Write-Host "Änderungen gefunden. Committe und pushe..." -ForegroundColor Green

    # Dateien hinzufügen
    git add .

    # Commit erstellen
    git commit -m $CommitMessage

    # Pushen
    git push origin main

    Write-Host "Erfolgreich veröffentlicht!" -ForegroundColor Green
} else {
    Write-Host "Keine Änderungen gefunden." -ForegroundColor Yellow
}
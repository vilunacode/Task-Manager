# Automatisches Git-Publish-Script
# Ausfuehren mit: Doppelklick auf publish.bat im Projektordner

param(
    [string]$CommitMessage = "Auto-commit: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
)

$env:PATH += ";C:\Program Files\Git\cmd"

Set-Location (Split-Path $PSScriptRoot)

$status = git status --porcelain
if ($status) {
    Write-Host "Aenderungen gefunden. Committe und pushe..." -ForegroundColor Green
    git add .
    git commit -m $CommitMessage
    git push origin main
    Write-Host "Erfolgreich veroeffentlicht!" -ForegroundColor Green
} else {
    Write-Host "Keine Aenderungen gefunden." -ForegroundColor Yellow
}

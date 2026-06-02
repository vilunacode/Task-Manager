# Task Manager

Browserbasierter Task-Manager für Teams. Läuft lokal als Python-App oder als eigenständige Windows-EXE — kein Server, keine Cloud, keine externe Abhängigkeit.

---

## Was dich erwartet

Nach dem ersten Start landest du auf einer Einrichtungsseite, auf der du den Admin-Account anlegst. Danach führt ein Onboarding durch die Grundkonfiguration: Rollen definieren, Team-Accounts anlegen, Kategorien erstellen. Anschließend ist die App sofort einsatzbereit.

---

## Funktionen

### Tasks & Dashboard
- Tasks mit Titel, Beschreibung, Priorität (1–5), Kategorie, Raum, Ansprechpartner und Fälligkeitsdatum
- Status-Board: **Offen · In Bearbeitung · Geschlossen**
- Drag & Drop für Statuswechsel und Zuweisung aus der Team-Sidebar
- Kommentare mit Nutzer-Erwähnungen (Pings) und Ping-Postfach

### Live-Übersicht
- Automatisch aktualisierendes Board aller Tasks nach Status
- Farblich markierte Spalten (gelb / blau / grün)
- Neue Tasks werden kurz hervorgehoben, optional mit Ton
- Uhr mit Wochentag im Kopfbereich

### Archiv & Protokoll
- Geschlossene Tasks archivieren, wiederherstellen oder endgültig löschen
- Aktivitätsprotokoll mit konfigurierbarer Löschfrist (Einheit frei wählbar: Minuten / Stunden / Tage)

### Benutzerverwaltung
- Rollen mit eigener Farbe und optionalen Adminrechten
- Benutzertypen: Mitarbeiter, Trainingsmitarbeiter, Inaktiv
- Nutzer können aus Dashboard-Ansichten ausgeblendet werden

### Kalender *(optional)*
- Persönliche Termine und Task-Fälligkeiten im Monatskalender
- Standardmäßig deaktiviert — in den Admin-Einstellungen aktivierbar

### Einstellungen
- Light / Dark Theme pro Nutzer
- Webseitenname, Favicon, Refresh-Intervall, Highlight-Dauer, Benachrichtigungston
- Löschfrist für Archiv & Protokoll mit wählbarer Einheit

---

## Starten

### Als Python-App

```bash
pip install -r requirements.txt
python app.py
```

Dann im Browser: `http://localhost:5000`

### Als Windows-EXE

```powershell
.\scripts\build.ps1
```

Das Skript fragt nach App-Name und Icon, installiert alle Abhängigkeiten automatisch und erzeugt eine eigenständige EXE unter `dist\<App-Name>.exe`. Die EXE legt beim ersten Start automatisch eine Datenbank im selben Ordner an.

---

## Konfiguration

Anpassungen vor dem Start in `config.ini`:

```ini
[server]
host = 0.0.0.0   # 127.0.0.1 = nur lokal, 0.0.0.0 = im Netzwerk erreichbar
port = 5000

[app]
secret_key = dein-sicherer-schluessel
timezone = Europe/Berlin

[database]
driver = sqlite
path = task_manager.db
```

---

## Technologie

Python · Flask · SQLite · Jinja2 · Vanilla JS/CSS — kein externes Frontend-Framework.

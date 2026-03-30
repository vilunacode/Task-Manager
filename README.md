# Task Manager

Browserbasierter Task-Manager auf Flask-Basis mit Rollen, Dashboard-Board, Ping-Benachrichtigungen, Kalender und Admin-Verwaltung.

## Überblick

Die Anwendung richtet sich an Teams, die Aufgaben in einem visuellen Board verwalten möchten. 
Neben klassischem Task-Tracking gibt es Nutzer- und Rollenverwaltung, Ping-Workflow über Erwähnungen, Kalenderfunktionen sowie umfangreiche Einstellungen für Admins.

## Funktionsumfang

### Auth & Grundfunktionen

- Login/Logout mit Session-Auth
- Passwort-Hashing mit `werkzeug.security`
- Initiales Setup über `/setup` für den ersten Admin

### Tasks & Dashboard

- Status-Board mit:
  - `Offen`
  - `In Bearbeitung`
  - `Geschlossen`
- Drag-and-Drop für Statuswechsel
- Bearbeiter per Drag-and-Drop aus der Team-Sidebar auf Tasks zuweisen
- Task-Erstellung mit:
  - Titel, Beschreibung
  - Kategorie
  - Priorität `1-5`
  - Raum
  - Ansprechpartner
  - Fälligkeitsdatum/-zeit
- Prioritäts-Badge auf Task-Karten (1 bis 5, farblich abgestuft)
- Bestehende Tasks können in der Detailansicht inkl. Priorität bearbeitet werden

### Pings & Kommentare

- Kommentare pro Task
- Nutzer-Markierungen (Mentions) in Kommentaren
- Ping-Filter im Dashboard mit Tabs:
  - `ungelesene Pings`
  - `gelesene Pings`
- Pro Task zwischen gelesen/ungelesen umschalten
- Ungelesene Pings als Counter am Ping-Filter

### Benutzer, Rollen & Teamdarstellung

- Rollenmodell:
  - Admin
  - Built-in Rollen
  - Benutzerdefinierte Rollen
- Benutzerverwaltung (anlegen, bearbeiten, löschen) mit Schutzlogik (z. B. letzter Admin)
- Zusätzliche Nutzerattribute:
  - Aktiv/Inaktiv
  - Mitarbeitertyp: `Mitarbeiter` oder `Trainingsmitarbeiter`
  - `Im Dashboard ausblenden` (Invisible)
- Teamliste im Dashboard gruppiert und visuell getrennt:
  - Trainingsmitarbeiter
  - Mitarbeiter
  - Inaktive
- Inaktive Nutzer werden visuell gedimmt dargestellt
- Dashboard-invisible Nutzer erscheinen nicht in:
  - Team-Sidebar
  - Bearbeiter-/Ansprechpartner-Auswahllisten im Dashboard
  - Zuweisungsworkflows

### Kalender & Einstellungen

- Monatskalender mit persönlichen Terminen und Task-Terminen
- Persönliche Ansichten und Team-Filter
- User-Einstellungen:
  - Light/Dark Theme
  - Kartenansicht (kompakt/erweitert)
- Admin-Einstellungen für UI/Tuning:
  - Farben (Rollen)
  - Größen/Layoutwerte
  - Refresh-Intervalle
  - Benachrichtigungston

## Technischer Stack

- Backend: Python 3.12, Flask
- Datenbank: SQLite (`task_manager.db`)
- Frontend: Jinja2, HTML, CSS, JavaScript

## Projektstruktur

```text
Task-Manager/
|- app.py
|- requirements.txt
|- publish.sh
|- publish.ps1
|- publish.bat
|- templates/
|- static/
`- README.md
```

## Schnellstart

1. Virtuelle Umgebung erstellen

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Abhängigkeiten installieren

```bash
pip install -r requirements.txt
```

3. Anwendung starten

```bash
python app.py
```

Optional: Vor dem Start kannst du die Datei `config.ini` anpassen (Host, Port, Debug, Zeitzone, DB-Pfad/DB-URL).

4. Browser öffnen

```text
http://localhost:5000
```

Beim ersten Start erfolgt eine Weiterleitung auf `/setup`.

## Konfiguration (einfach anpassbar)

Die Anwendung liest beim Start automatisch die Datei `config.ini` im Projektordner ein.

Beispielwerte:

```ini
[server]
host = 0.0.0.0
port = 5000
debug = true

[app]
secret_key = dev-secret-change-me
timezone = Europe/Berlin

[database]
driver = sqlite
host = 127.0.0.1
port = 5432
name = task_manager
username =
password =
url =
path = task_manager.db
```

Hinweise:

- `host`: `127.0.0.1` nur lokal, `0.0.0.0` im Netzwerk erreichbar
- `port`: z. B. `5000`, `8080`, `9000`
- Datenbankfelder: `database.host`, `database.port`, `database.name`, `database.username`, `database.password`
- `database.driver`: `sqlite`, `postgres`, `postgresql`, `mysql`, `mariadb`
- `database.path`: Pfad zur SQLite-Datei (relativ oder absolut, wenn `driver=sqlite`)
- `database.url`: direkte Verbindungs-URL (hat Prioritaet)
- Hinweis: Aktuell arbeitet die Anwendung intern mit SQLite. Externe Treiber sind vorbereitet, aber noch nicht aktiv nutzbar.

### Environment-Variablen (optional)

Folgende Variablen koennen die `config.ini` ueberschreiben:

- `TASK_MANAGER_CONFIG` (Pfad zu alternativer ini-Datei)
- `TASK_MANAGER_HOST`
- `TASK_MANAGER_PORT`
- `TASK_MANAGER_DEBUG`
- `SECRET_KEY`
- `APP_TIMEZONE`
- `DATABASE_PATH`
- `DATABASE_URL`
- `TASK_MANAGER_DB_DRIVER`
- `TASK_MANAGER_DB_HOST`
- `TASK_MANAGER_DB_PORT`
- `TASK_MANAGER_DB_NAME`
- `TASK_MANAGER_DB_USER`
- `TASK_MANAGER_DB_PASSWORD`

## Wichtige Hinweise

- Das DB-Schema wird beim Start automatisch migriert/ergänzt.
- Für den Produktionseinsatz sollten mindestens gesetzt sein:
  - `SECRET_KEY`
  - HTTPS / Reverse Proxy
  - produktionsfähiger WSGI-Server

## Lizenz

Interne Nutzung / projektabhängig. Bei Bedarf konkrete Lizenz ergänzen (z. B. MIT).

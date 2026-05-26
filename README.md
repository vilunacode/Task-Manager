# Task Manager

Browserbasierter Task-Manager auf Flask-Basis mit Rollen, Dashboard-Board, Live-Übersicht, Ping-Benachrichtigungen, Kalender und Admin-Verwaltung.

## Überblick

Die Anwendung richtet sich an Teams, die Aufgaben in einem visuellen Board verwalten möchten.
Neben klassischem Task-Tracking gibt es Nutzer- und Rollenverwaltung, Ping-Workflow über Erwähnungen, Kalenderfunktionen sowie umfangreiche Einstellungen für Admins.

## Funktionsumfang

### Ersteinrichtung & Onboarding

- Beim ersten Start Weiterleitung auf `/setup` zur Anlage des Admin-Accounts
- Geführtes Onboarding (`/onboarding`) nach dem Setup:
  - **Rollen**: Admin-Rolle (Name und Farbe konfigurierbar) sowie eigene Rollen erstellen, bearbeiten, löschen
  - **Benutzer**: Team-Accounts anlegen, bearbeiten, löschen
  - **Ticket-Kategorien**: Kategorien erstellen, bearbeiten, löschen
- Während des Onboardings zeigt die Navigationsleiste nur Dark/Light-Mode-Schalter und Logout — keine Ablenkung durch weitere Links
- Über „Überspringen" kann das Onboarding jederzeit zum Dashboard verlassen werden; der Button „Einrichtung abschließen" schließt es abschließend ab
- Lösch-Aktionen im Onboarding öffnen einen Bestätigungs-Dialog im einheitlichen Seiten-Design

### Auth & Grundfunktionen

- Login/Logout mit Session-Auth
- Passwort-Hashing mit `werkzeug.security`

### Tasks & Dashboard

- Status-Board mit drei Spalten:
  - `Offen`
  - `In Bearbeitung`
  - `Geschlossen`
- Drag-and-Drop für Statuswechsel
- Bearbeiter per Drag-and-Drop aus der Team-Sidebar auf Tasks zuweisen
- Task-Erstellung mit:
  - Titel, Beschreibung
  - Kategorie (frei definierbar)
  - Priorität `1–5` (farblich abgestufte Ecke auf der Karte)
  - Raum
  - Ansprechpartner (registrierter Nutzer oder freier Text über „Andere...")
  - Fälligkeitsdatum/-zeit (optional)
- Bestehende Tasks können in der Detailansicht inkl. aller Felder bearbeitet werden
- Beim Erstellen ohne angelegte Kategorie erscheint ein Hinweis-Popup

### Live-Übersicht (`/overview`)

- Kompaktes Übersichts-Board aller Tasks nach Status
- Automatische Aktualisierung in einstellbarem Intervall (Standard: 1 Sekunde)
- Neu eingegangene Tasks werden kurzzeitig farblich hervorgehoben (Dauer konfigurierbar)
- Tooltip mit Aufgaben-Details beim Hovern über eine Karte

### Pings & Kommentare

- Kommentare pro Task
- Nutzer-Markierungen (Mentions) in Kommentaren
- Ping-Filter im Dashboard mit Tabs:
  - `Ungelesene Pings`
  - `Gelesene Pings`
- Pro Task zwischen gelesen/ungelesen umschalten
- Ungelesene Pings als Counter am Ping-Filter

### Archiv & geschlossene Tickets

- Geschlossene Tasks über `/admin/closed` einsehbar (Admin)
- Archiv (`/archive`) für abgeschlossene und archivierte Aufgaben mit Filterung

### Benutzer, Rollen & Teamdarstellung

- Rollenmodell:
  - **Admin-Rolle**: immer vorhanden, Name und Farbe frei anpassbar (Standardfarbe: `#fc5f5f`)
  - **Benutzerdefinierte Rollen**: vom Admin erstellt, mit eigener Farbe
- Rollenverwaltung: erstellen, umbenennen, Farbe ändern, löschen
- Benutzerverwaltung (anlegen, bearbeiten, löschen) mit Schutzlogik (z. B. letzter Admin)
- Zusätzliche Nutzerattribute:
  - Aktiv / Inaktiv
  - Mitarbeitertyp: `Mitarbeiter` oder `Trainingsmitarbeiter`
  - `Im Dashboard ausblenden` (Invisible)
- Teamliste im Dashboard gruppiert und visuell getrennt:
  - Mitarbeiter
  - Trainingsmitarbeiter
  - Inaktive (gedimmt)
- Dashboard-invisible Nutzer erscheinen nicht in Team-Sidebar, Bearbeiter- und Ansprechpartner-Auswahlen

### Kalender

- Monatskalender mit persönlichen Terminen und Task-Fälligkeitsdaten
- Persönliche Ansicht und Team-Filter
- Navigation zwischen Monaten

### Einstellungen

- **Nutzer-Einstellungen**: Light/Dark Theme, Kartenansicht (kompakt/erweitert), Passwort ändern
- **Admin-Einstellungen**:
  - Highlight-Dauer für neue Tasks in der Live-Übersicht
  - Live-Refresh-Intervall
  - Benachrichtigungston bei neuer Task
  - Ticket-Kategorien verwalten

## Technischer Stack

- Backend: Python 3.12, Flask
- Datenbank: SQLite (`task_manager.db`)
- Frontend: Jinja2, HTML, CSS, JavaScript (kein externes Framework)

## Projektstruktur

```text
Task-Manager/
├── app.py
├── requirements.txt
├── config.ini
├── publish.sh / publish.ps1 / publish.bat
├── templates/
│   ├── base.html
│   ├── setup.html
│   ├── onboarding.html
│   ├── login.html
│   ├── dashboard.html
│   ├── overview.html
│   ├── calendar.html
│   ├── task_detail.html
│   ├── settings.html
│   ├── admin_users.html
│   ├── admin_closed.html
│   └── archive.html
├── static/
│   └── styles.css
└── README.md
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

Optional: Vor dem Start `config.ini` anpassen (Host, Port, Debug, Zeitzone, DB-Pfad).

4. Browser öffnen

```text
http://localhost:5000
```

Beim ersten Start erfolgt eine Weiterleitung auf `/setup` zur Anlage des Admin-Accounts, danach auf `/onboarding` zur Grundkonfiguration.

## Konfiguration

Die Anwendung liest beim Start automatisch `config.ini` im Projektordner ein.

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
path = task_manager.db
```

Hinweise:

- `host`: `127.0.0.1` nur lokal, `0.0.0.0` im Netzwerk erreichbar
- `database.driver`: `sqlite`, `postgres`, `postgresql`, `mysql`, `mariadb`
- `database.path`: Pfad zur SQLite-Datei (relativ oder absolut, wenn `driver=sqlite`)
- `database.url`: direkte Verbindungs-URL (hat Priorität über Einzelfelder)
- Hinweis: Aktuell arbeitet die Anwendung intern mit SQLite. Externe Treiber sind vorbereitet, aber noch nicht aktiv nutzbar.

### Environment-Variablen (optional)

Folgende Variablen können `config.ini` überschreiben:

- `TASK_MANAGER_CONFIG` — Pfad zu alternativer ini-Datei
- `TASK_MANAGER_HOST`, `TASK_MANAGER_PORT`, `TASK_MANAGER_DEBUG`
- `SECRET_KEY`, `APP_TIMEZONE`
- `DATABASE_PATH`, `DATABASE_URL`
- `TASK_MANAGER_DB_DRIVER`, `TASK_MANAGER_DB_HOST`, `TASK_MANAGER_DB_PORT`
- `TASK_MANAGER_DB_NAME`, `TASK_MANAGER_DB_USER`, `TASK_MANAGER_DB_PASSWORD`

## Wichtige Hinweise

- Das DB-Schema wird beim Start automatisch migriert/ergänzt — kein manuelles Setup nötig.
- Für den Produktionseinsatz sollten mindestens gesetzt sein:
  - `SECRET_KEY` (sicherer Zufallswert)
  - HTTPS / Reverse Proxy (z. B. nginx)
  - Produktionsfähiger WSGI-Server (z. B. gunicorn)

## Lizenz

Interne Nutzung / projektabhängig. Bei Bedarf konkrete Lizenz ergänzen (z. B. MIT).

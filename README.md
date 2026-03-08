# Task Manager

<p align="center">
  <strong>Browserbasierter Task-Manager mit Rollen, Kalender, Admin-Workflows und modernem Dashboard.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Version-1.0.0-blue" alt="Version 1.0.0">
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.12">
  <img src="https://img.shields.io/badge/Flask-3.x-000000?style=for-the-badge&logo=flask&logoColor=white" alt="Flask">
  <img src="https://img.shields.io/badge/SQLite-3-003B57?style=for-the-badge&logo=sqlite&logoColor=white" alt="SQLite">
  <img src="https://img.shields.io/badge/HTML5-CSS3-JS-F16529?style=for-the-badge&logo=html5&logoColor=white" alt="HTML CSS JS">
</p>

## Ueberblick

Task Manager ist eine Flask-Anwendung fuer Team-Taskverwaltung mit klaren Rollen, visuellem Workflow und Admin-Steuerung. 
Der Fokus liegt auf schneller Uebersicht, sauberen Berechtigungen und einem pragmatischen Alltagseinsatz.

## Features

- Login/Logout mit Session-Auth und Passwort-Hashing.
- Erstsetup via `/setup` fuer den initialen Admin.
- Rollenmodell mit Admin, Standardrollen und benutzerdefinierten Rollen.
- Benutzerverwaltung (anlegen, bearbeiten, loeschen) inkl. Schutzlogik.
- Aufgabenboard mit Status:
- `Offen`
- `In Bearbeitung`
- `Geschlossen`
- Drag-and-Drop fuer Statuswechsel im Dashboard.
- Bearbeiter-Management inkl. Drag-and-Drop aus `Das Team` auf Task-Karten.
- Tasks koennen ohne Bearbeiter gespeichert werden.
- Kommentare pro Task mit Bearbeiten/Loeschen-Regeln.
- Geschlossene Tasks mit Admin-Review (zuruecksenden/loeschen).
- Kalenderansicht (Monatsgrid) mit persoenlichen Terminen und Task-Terminen.
- Dark-/Standard-Modus pro Benutzer in den Einstellungen.
- Umfangreiche Admin-Designeinstellungen (Farben, Groessen, Refresh, Ton).

## Tech Stack

- Backend: `Python`, `Flask`
- Datenbank: `SQLite` (`task_manager.db`)
- Frontend: `Jinja2`, `HTML`, `CSS`, `JavaScript`
- Auth: `werkzeug.security` (Password Hashing)

## Projektstruktur

```text
Task-Manager/
|- app.py
|- requirements.txt
|- task_manager.db
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

2. Abhaengigkeiten installieren

```bash
pip install -r requirements.txt
```

3. Anwendung starten

```bash
python app.py
```

4. Im Browser oeffnen

```text
http://localhost:5000
```

Beim ersten Start wirst du automatisch auf `/setup` geleitet.

## Rollen und Berechtigungen

- `Admin`
- Vollzugriff auf Benutzerverwaltung, Einstellungen und geschlossene Tasks.
- Darf geschlossene Tasks zuruecksetzen und final loeschen.
- `Nicht-Admin`
- Je nach Task-Zuweisung/Erstellerstatus eingeschraenkte Bearbeitungsrechte.
- Kann nur erlaubte Aktionen in Task-Detail und Dashboard ausfuehren.

## Hinweise

- Die App nutzt SQLite lokal und fuehrt notwendige Schema-Erweiterungen beim Start aus.
- Fuer Produktion sollten `SECRET_KEY`, HTTPS und ein produktionsfaehiger WSGI-Server gesetzt werden.

## Lizenz

Interne Nutzung / projektabhaengig. Bei Bedarf hier eine konkrete Lizenz ergaenzen (z. B. MIT).

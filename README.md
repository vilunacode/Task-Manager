# Task-Manager

Browserbasierter Task-Manager mit Rollenmodell (Admin/Team), Benutzerverwaltung und Workflow fuer Aufgaben in drei Statusstufen.

## Funktionen

- Erstes Setup erstellt genau einen initialen Administrator.
- Login/Logout mit passwortbasiertem Account-System.
- Nur Administratoren duerfen Benutzer anlegen und entfernen.
- Jeder Benutzer hat ein Pflicht-Kuerzel mit genau 3 Zeichen (A-Z/0-9).
- Benutzerfarben nach Rolle:
	- Admin: Gelb
	- Systemintegrator: Rot
	- Anwendungsentwickler: Blau
	- Team: Gruen
- Kuerzel werden links als Teamliste angezeigt und direkt an Tasks visualisiert.
- Tasks mit folgenden Pflichtangaben:
	- Titel
	- Worum geht es (Beschreibung)
	- Wer soll es bearbeiten
	- Ansprechpartner
- Task-Status:
	- Offene Tasks
	- In Bearbeitung
	- Geschlossene Tasks
- Geschlossene Tasks koennen vom Admin geprueft werden:
	- Zuruecksenden ans Team (Status wieder `In Bearbeitung`)
	- Endgueltig loeschen

## Technik

- Python + Flask
- SQLite (lokale Datei `task_manager.db`)
- HTML/Jinja Templates + CSS

## Lokal starten

1. Virtuelle Umgebung anlegen und aktivieren:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Abhaengigkeiten installieren:

```bash
pip install -r requirements.txt
```

3. Anwendung starten:

```bash
python app.py
```

4. Browser oeffnen:

```text
http://localhost:5000
```

Beim allerersten Start wirst du auf `/setup` geleitet und legst den ersten Admin an.

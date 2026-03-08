import os
import calendar as pycalendar
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from zoneinfo import ZoneInfo

from flask import (
    Flask,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATABASE = os.path.join(BASE_DIR, "task_manager.db")

STATUS_OPEN = "open"
STATUS_IN_PROGRESS = "in_progress"
STATUS_CLOSED = "closed"
VALID_STATUSES = {STATUS_OPEN, STATUS_IN_PROGRESS, STATUS_CLOSED}
ROLE_SYSTEM_INTEGRATOR = "system_integrator"
ROLE_APPLICATION_DEVELOPER = "application_developer"
ROLE_TEAM = "team"
VALID_ROLES = {ROLE_SYSTEM_INTEGRATOR, ROLE_APPLICATION_DEVELOPER, ROLE_TEAM}

DEFAULT_APP_SETTINGS = {
    "new_task_highlight_seconds": "120",
    "overview_refresh_interval_seconds": "1",
    "general_container_width_px": "1760",
    "general_main_min_height_px": "0",
    "general_dashboard_shell_width_px": "1080",
    "dashboard_general_min_height_px": "0",
    "overview_general_width_px": "1720",
    "overview_general_min_height_px": "0",
    "dashboard_category_min_width_px": "320",
    "dashboard_category_min_height_px": "0",
    "overview_category_width_px": "0",
    "overview_category_min_height_px": "0",
    "dashboard_task_width_px": "360",
    "dashboard_task_min_height_px": "0",
    "overview_task_width_px": "220",
    "overview_task_min_height_px": "94",
    "role_color_admin": "#facc15",
    "role_color_system": "#dc2626",
    "role_color_dev": "#2563eb",
    "role_color_team": "#0f766e",
    "new_task_tone": "classic",
}

TONE_OPTIONS = {
    "classic": {"type": "sine", "frequency": 880},
    "soft": {"type": "triangle", "frequency": 660},
    "alert": {"type": "square", "frequency": 980},
    "none": {"type": "none", "frequency": 0},
}


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")
APP_TIMEZONE = ZoneInfo(os.environ.get("APP_TIMEZONE", "Europe/Berlin"))


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(exception):  # pylint: disable=unused-argument
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            initials TEXT,
            role TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            assignee_id INTEGER,
            due_date TEXT,
            contact_person TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (assignee_id) REFERENCES users(id) ON DELETE SET NULL,
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS task_assignees (
            task_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (task_id, user_id),
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS task_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS calendar_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            notes TEXT NOT NULL DEFAULT '',
            start_at TEXT NOT NULL,
            end_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """
    )

    # Ensure migrations for existing databases.
    columns = {row["name"] for row in db.execute("PRAGMA table_info(users)").fetchall()}
    if "initials" not in columns:
        db.execute("ALTER TABLE users ADD COLUMN initials TEXT")
    if "role" not in columns:
        db.execute("ALTER TABLE users ADD COLUMN role TEXT")

    task_columns = {row["name"] for row in db.execute("PRAGMA table_info(tasks)").fetchall()}
    if "due_date" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN due_date TEXT")
    if "close_reason" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN close_reason TEXT")
    if "closed_at" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN closed_at TEXT")
    if "closed_by" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN closed_by INTEGER")

    comment_columns = {row["name"] for row in db.execute("PRAGMA table_info(task_comments)").fetchall()}
    if "updated_at" not in comment_columns:
        db.execute("ALTER TABLE task_comments ADD COLUMN updated_at TEXT")

    db.execute(
        """
        UPDATE tasks
        SET due_date = substr(created_at, 1, 16)
        WHERE due_date IS NULL OR TRIM(due_date) = ''
        """
    )

    db.execute(
        """
        UPDATE tasks
        SET due_date = due_date || 'T00:00'
        WHERE due_date IS NOT NULL
          AND length(TRIM(due_date)) = 10
          AND instr(due_date, 'T') = 0
        """
    )

    legacy_users = db.execute(
        "SELECT id, username FROM users WHERE initials IS NULL OR TRIM(initials) = ''"
    ).fetchall()
    for user in legacy_users:
        fallback = make_initials_from_username(user["username"])
        db.execute("UPDATE users SET initials = ? WHERE id = ?", (fallback, user["id"]))

    db.execute(
        """
        UPDATE users
        SET role = ?
        WHERE (role IS NULL OR TRIM(role) = '') AND is_admin = 0
        """,
        (ROLE_APPLICATION_DEVELOPER,),
    )

    # Migrate legacy single assignee values into the many-to-many table.
    db.execute(
        """
        INSERT OR IGNORE INTO task_assignees (task_id, user_id)
        SELECT id, assignee_id
        FROM tasks
        WHERE assignee_id IS NOT NULL
        """
    )

    for key, value in DEFAULT_APP_SETTINGS.items():
        db.execute(
            "INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)",
            (key, value),
        )

    # Keep existing installs in sync with the narrower dashboard default.
    db.execute(
        """
        UPDATE app_settings
        SET value = '1080'
        WHERE key = 'general_dashboard_shell_width_px' AND value = '1200'
        """
    )

    # Old installs used 0 for category min width which had little visible effect.
    # Normalize that legacy value to a practical default width.
    db.execute(
        """
        UPDATE app_settings
        SET value = '320'
        WHERE key = 'dashboard_category_min_width_px' AND value = '0'
        """
    )
    db.commit()


@app.before_request
def ensure_db_initialized():
    init_db()


def query_one(query: str, params=()):
    db = get_db()
    return db.execute(query, params).fetchone()


def query_all(query: str, params=()):
    db = get_db()
    return db.execute(query, params).fetchall()


def execute(query: str, params=()):
    db = get_db()
    cur = db.execute(query, params)
    db.commit()
    return cur


def app_settings() -> dict[str, str]:
    stored = {
        row["key"]: row["value"]
        for row in query_all("SELECT key, value FROM app_settings")
    }
    merged = dict(DEFAULT_APP_SETTINGS)
    merged.update(stored)
    return merged


def set_app_setting(key: str, value: str) -> None:
    execute(
        """
        INSERT INTO app_settings (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def parse_int_setting(value: str, *, min_value: int, max_value: int) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < min_value or parsed > max_value:
        return None
    return parsed


def now_iso() -> str:
    # Persist timestamps with local timezone offset to avoid UTC display drift.
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def is_hex_color(value: str) -> bool:
    return bool(re.fullmatch(r"#[0-9a-fA-F]{6}", value.strip()))


def normalize_datetime_value(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""

    candidate = raw
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        try:
            dt = datetime.strptime(candidate, "%Y-%m-%d")
        except ValueError:
            return ""
    if dt.tzinfo is not None:
        dt = dt.astimezone(APP_TIMEZONE).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%dT%H:%M")


def format_datetime_for_display(value: str | None) -> str:
    if not value:
        return "-"
    normalized = normalize_datetime_value(value)
    if not normalized:
        return "-"
    dt = datetime.fromisoformat(normalized)
    return dt.strftime("%d.%m.%Y %H:%M Uhr")


def format_system_datetime_for_display(value: str | None) -> str:
    if not value:
        return "-"

    raw = value.strip()
    if not raw:
        return "-"

    candidate = raw
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        return "-"

    # Legacy values were stored without timezone and should be treated as UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    local_dt = dt.astimezone(APP_TIMEZONE)
    return local_dt.strftime("%d.%m.%Y %H:%M Uhr")


def format_datetime_for_input(value: str | None) -> str:
    if not value:
        return ""
    return normalize_datetime_value(value)


def is_due_today(value: str | None) -> bool:
    if not value:
        return False
    normalized = normalize_datetime_value(value)
    if not normalized:
        return False
    due_dt = datetime.fromisoformat(normalized)
    return due_dt.date() == datetime.now().date()


def make_initials_from_username(username: str) -> str:
    letters_only = re.sub(r"[^A-Za-z0-9]", "", username.upper())
    if not letters_only:
        letters_only = "USR"
    padded = (letters_only + "XXX")[:3]
    return padded


def normalize_initials(value: str) -> str:
    initials = value.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{3}", initials):
        return ""
    return initials


def normalize_role(value: str) -> str:
    if value in VALID_ROLES or value == "admin":
        return value
    return ""


def role_label(role: str, is_admin: int) -> str:
    if is_admin:
        return "Admin"
    if role == ROLE_SYSTEM_INTEGRATOR:
        return "Systemintegrator"
    if role == ROLE_TEAM:
        return "Team"
    return "Anwendungsentwickler"


def badge_color_class(role: str, is_admin: int) -> str:
    if is_admin:
        return "badge-admin"
    if role == ROLE_SYSTEM_INTEGRATOR:
        return "badge-system"
    if role == ROLE_TEAM:
        return "badge-team"
    return "badge-dev"


def task_assignees_map(task_ids: list[int]):
    if not task_ids:
        return {}

    placeholders = ",".join(["?"] * len(task_ids))
    rows = query_all(
        f"""
        SELECT
            ta.task_id,
            u.id,
            u.username,
            u.initials,
            u.role,
            u.is_admin
        FROM task_assignees ta
        JOIN users u ON u.id = ta.user_id
        WHERE ta.task_id IN ({placeholders})
        ORDER BY u.is_admin DESC, u.username ASC
        """,
        tuple(task_ids),
    )

    mapping = {task_id: [] for task_id in task_ids}
    for row in rows:
        mapping[row["task_id"]].append(
            {
                "id": row["id"],
                "username": row["username"],
                "initials": row["initials"] or make_initials_from_username(row["username"]),
                "role_label": role_label(row["role"], row["is_admin"]),
                "color_class": badge_color_class(row["role"], row["is_admin"]),
            }
        )
    return mapping


def enrich_tasks_with_assignees(tasks):
    task_ids = [int(task["id"]) for task in tasks]
    mapping = task_assignees_map(task_ids)
    enriched = []
    for task in tasks:
        task_dict = dict(task)
        task_dict["assignees"] = mapping.get(task["id"], [])
        enriched.append(task_dict)
    return enriched


def sidebar_users():
    rows = query_all(
        """
        SELECT id, username, initials, role, is_admin
        FROM users
        ORDER BY is_admin DESC,
          CASE role
            WHEN ? THEN 0
            WHEN ? THEN 1
                        WHEN ? THEN 2
                        ELSE 3
          END,
          username ASC
        """,
                (ROLE_TEAM, ROLE_SYSTEM_INTEGRATOR, ROLE_APPLICATION_DEVELOPER),
    )
    return [
        {
            "id": row["id"],
            "username": row["username"],
            "initials": row["initials"] or make_initials_from_username(row["username"]),
            "role_label": role_label(row["role"], row["is_admin"]),
            "color_class": badge_color_class(row["role"], row["is_admin"]),
        }
        for row in rows
    ]


def fetch_tasks(*, status: str | None = None, only_assigned_to: int | None = None):
    where_parts = []
    params = []

    if status is not None:
        where_parts.append("t.status = ?")
        params.append(status)

    if only_assigned_to is not None:
        where_parts.append(
            "EXISTS (SELECT 1 FROM task_assignees ta2 WHERE ta2.task_id = t.id AND ta2.user_id = ?)"
        )
        params.append(only_assigned_to)

    where_sql = ""
    if where_parts:
        where_sql = "WHERE " + " AND ".join(where_parts)

    return query_all(
        f"""
        SELECT
            t.*,
            c.username AS creator_name,
            COALESCE(GROUP_CONCAT(DISTINCT au.username), '') AS assignee_names
        FROM tasks t
        JOIN users c ON c.id = t.created_by
        LEFT JOIN task_assignees ta ON ta.task_id = t.id
        LEFT JOIN users au ON au.id = ta.user_id
        {where_sql}
        GROUP BY t.id
        ORDER BY t.updated_at DESC
        """,
        tuple(params),
    )


def parse_int_value(value: str | None) -> int | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_month_value(raw_value: str | None) -> date:
    if raw_value:
        try:
            parsed = datetime.strptime(raw_value.strip(), "%Y-%m").date()
            return parsed.replace(day=1)
        except ValueError:
            pass
    today = datetime.now(APP_TIMEZONE).date()
    return today.replace(day=1)


def shift_month(month_start: date, delta_months: int) -> date:
    base = month_start.year * 12 + (month_start.month - 1) + delta_months
    year = base // 12
    month = (base % 12) + 1
    return date(year, month, 1)


def build_month_cells(month_start: date, events: list[dict]):
    events_by_day = {}
    for event in events:
        normalized = normalize_datetime_value(event.get("start_at", ""))
        if not normalized:
            continue
        day_key = normalized[:10]
        events_by_day.setdefault(day_key, []).append(event)

    for day_events in events_by_day.values():
        day_events.sort(key=lambda item: item.get("start_at", ""))

    first_weekday, days_in_month = pycalendar.monthrange(month_start.year, month_start.month)
    first_cell = month_start - timedelta(days=first_weekday)

    cells = []
    for offset in range(42):
        day = first_cell + timedelta(days=offset)
        day_key = day.strftime("%Y-%m-%d")
        cells.append(
            {
                "date": day,
                "day_key": day_key,
                "in_month": day.month == month_start.month,
                "is_today": day == datetime.now(APP_TIMEZONE).date(),
                "events": events_by_day.get(day_key, []),
            }
        )

    return {
        "month_label": month_start.strftime("%B %Y"),
        "month_value": month_start.strftime("%Y-%m"),
        "prev_month": shift_month(month_start, -1).strftime("%Y-%m"),
        "next_month": shift_month(month_start, 1).strftime("%Y-%m"),
        "cells": cells,
        "days_in_month": days_in_month,
    }


def list_all_users_for_filters():
    return query_all(
        """
        SELECT id, username, initials, role, is_admin
        FROM users
        ORDER BY is_admin DESC, username ASC
        """
    )


def calendar_scope_user_ids(user, scope: str, filter_user_id: int | None):
    all_users = list_all_users_for_filters()
    all_user_ids = [int(row["id"]) for row in all_users]

    if not user["is_admin"]:
        return [int(user["id"])], "me", int(user["id"]), all_users

    resolved_scope = scope if scope in {"me", "team"} else "me"
    if resolved_scope == "me":
        return [int(user["id"])], resolved_scope, int(user["id"]), all_users

    if filter_user_id is not None and filter_user_id in all_user_ids:
        return [int(filter_user_id)], resolved_scope, int(filter_user_id), all_users

    return all_user_ids, resolved_scope, None, all_users


def calendar_personal_events(user_ids: list[int]):
    if not user_ids:
        return []

    placeholders = ",".join(["?"] * len(user_ids))
    rows = query_all(
        f"""
        SELECT
            ce.id,
            ce.user_id,
            ce.title,
            ce.notes,
            ce.start_at,
            ce.end_at,
            u.username,
            u.initials,
            u.role,
            u.is_admin
        FROM calendar_events ce
        JOIN users u ON u.id = ce.user_id
        WHERE ce.user_id IN ({placeholders})
        ORDER BY ce.start_at ASC
        """,
        tuple(user_ids),
    )

    events = []
    for row in rows:
        owner_initials = row["initials"] or make_initials_from_username(row["username"])
        events.append(
            {
                "kind": "personal",
                "id": int(row["id"]),
                "user_id": int(row["user_id"]),
                "title": row["title"],
                "notes": row["notes"] or "",
                "start_at": row["start_at"],
                "end_at": row["end_at"] or "",
                "start_display": format_datetime_for_display(row["start_at"]),
                "end_display": format_datetime_for_display(row["end_at"]) if row["end_at"] else "",
                "owner_name": row["username"],
                "owner_initials": owner_initials,
                "owner_short": owner_initials,
                "owner_hint": row["username"],
                "owner_color_class": badge_color_class(row["role"], row["is_admin"]),
            }
        )
    return events


def calendar_task_events(user_ids: list[int]):
    if not user_ids:
        return []

    tasks = enrich_tasks_with_assignees(fetch_tasks())
    user_id_set = set(user_ids)
    events = []
    for task in tasks:
        due_date = task.get("due_date")
        if not due_date:
            continue

        assignee_ids = {int(a["id"]) for a in task.get("assignees", [])}
        if not (assignee_ids & user_id_set):
            continue

        assignees = task.get("assignees", [])
        assignee_initials = [a["initials"] for a in assignees]
        owner_short = ", ".join(assignee_initials[:2])
        if len(assignee_initials) > 2:
            owner_short = f"{owner_short} +{len(assignee_initials) - 2}"
        owner_hint = ", ".join([a["username"] for a in assignees]) if assignees else "Nicht zugewiesen"

        events.append(
            {
                "kind": "task",
                "id": int(task["id"]),
                "title": task["title"],
                "notes": task.get("description", ""),
                "start_at": due_date,
                "end_at": "",
                "start_display": format_datetime_for_display(due_date),
                "end_display": "",
                "status": task["status"],
                "assignees": assignees,
                "owner_short": owner_short,
                "owner_hint": owner_hint,
            }
        )

    events.sort(key=lambda event: event["start_at"])
    return events


def calendar_combined_events(user_ids: list[int]):
    personal = calendar_personal_events(user_ids)
    task_events = calendar_task_events(user_ids)
    combined = personal + task_events
    combined.sort(key=lambda event: event["start_at"])
    return combined


def assigned_task_ids_for_user(user_id: int):
    rows = query_all("SELECT task_id FROM task_assignees WHERE user_id = ?", (user_id,))
    return {row["task_id"] for row in rows}


def sync_task_primary_assignee(task_id: int):
    row = query_one(
        "SELECT user_id FROM task_assignees WHERE task_id = ? ORDER BY user_id ASC LIMIT 1",
        (task_id,),
    )
    primary_assignee_id = row["user_id"] if row is not None else None
    execute("UPDATE tasks SET assignee_id = ? WHERE id = ?", (primary_assignee_id, task_id))


def user_count() -> int:
    row = query_one("SELECT COUNT(*) AS cnt FROM users")
    return int(row["cnt"])


def current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    return query_one("SELECT * FROM users WHERE id = ?", (user_id,))


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if current_user() is None:
            flash("Bitte zuerst anmelden.", "error")
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped_view


def admin_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        user = current_user()
        if user is None:
            flash("Bitte zuerst anmelden.", "error")
            return redirect(url_for("login"))
        if not user["is_admin"]:
            flash("Nur Administratoren haben Zugriff auf diese Seite.", "error")
            return redirect(url_for("dashboard"))
        return view(*args, **kwargs)

    return wrapped_view


def status_label(status: str) -> str:
    labels = {
        STATUS_OPEN: "Offene Tasks",
        STATUS_IN_PROGRESS: "In Bearbeitung",
        STATUS_CLOSED: "Geschlossene Tasks",
    }
    return labels.get(status, status)


def closed_task_count_for_admin(user) -> int:
    if user is None or not user["is_admin"]:
        return 0
    row = query_one("SELECT COUNT(*) AS cnt FROM tasks WHERE status = ?", (STATUS_CLOSED,))
    return int(row["cnt"]) if row is not None else 0


@app.context_processor
def inject_helpers():
    user = current_user()
    badges = []
    settings = app_settings()
    closed_task_count = 0
    if user is not None:
        badges = sidebar_users()
        closed_task_count = closed_task_count_for_admin(user)
    return {
        "status_label": status_label,
        "sidebar_users": badges,
        "format_datetime": format_datetime_for_display,
        "format_system_datetime": format_system_datetime_for_display,
        "datetime_input_value": format_datetime_for_input,
        "is_due_today": is_due_today,
        "app_settings": settings,
        "tone_options": sorted(TONE_OPTIONS.keys()),
        "closed_task_count": closed_task_count,
    }


@app.route("/")
def index():
    if user_count() == 0:
        return redirect(url_for("setup"))
    if current_user() is None:
        return redirect(url_for("login"))
    return redirect(url_for("dashboard"))


@app.route("/setup", methods=["GET", "POST"])
def setup():
    if user_count() > 0:
        return redirect(url_for("login"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if not username or not password:
            flash("Benutzername und Passwort sind erforderlich.", "error")
            return render_template("setup.html")

        initials = normalize_initials(request.form.get("initials", ""))
        if not initials:
            flash("Kürzel muss genau 3 Zeichen (A-Z/0-9) lang sein.", "error")
            return render_template("setup.html")

        exists_initials = query_one("SELECT id FROM users WHERE initials = ?", (initials,))
        if exists_initials is not None:
            flash("Dieses Kürzel ist bereits vergeben.", "error")
            return render_template("setup.html")

        execute(
            """
            INSERT INTO users (username, password_hash, is_admin, initials, role, created_at)
            VALUES (?, ?, 1, ?, 'admin', ?)
            """,
            (
                username,
                generate_password_hash(password),
                initials,
                now_iso(),
            ),
        )

        flash("Administrator wurde erstellt. Bitte anmelden.", "success")
        return redirect(url_for("login"))

    return render_template("setup.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if user_count() == 0:
        return redirect(url_for("setup"))

    if current_user() is not None:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        user = query_one("SELECT * FROM users WHERE username = ?", (username,))
        if user is None or not check_password_hash(user["password_hash"], password):
            flash("Ungültige Anmeldedaten.", "error")
            return render_template("login.html")

        session.clear()
        session["user_id"] = user["id"]
        flash("Erfolgreich angemeldet.", "success")
        return redirect(url_for("dashboard"))

    return render_template("login.html")


@app.route("/logout", methods=["POST"])
@login_required
def logout():
    session.clear()
    flash("Abgemeldet.", "success")
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required
def dashboard():
    user = current_user()
    filter_mode = request.args.get("filter", "all").strip().lower()
    if filter_mode not in {"all", "mine"}:
        filter_mode = "all"

    users = query_all(
        """
        SELECT id, username, initials, role, is_admin
        FROM users
        ORDER BY is_admin DESC, username ASC
        """
    )
    tasks = fetch_tasks(only_assigned_to=user["id"] if filter_mode == "mine" else None)
    tasks = enrich_tasks_with_assignees(tasks)
    editable_task_ids = set() if user["is_admin"] else assigned_task_ids_for_user(user["id"])

    grouped = {
        STATUS_OPEN: [],
        STATUS_IN_PROGRESS: [],
        STATUS_CLOSED: [],
    }

    for task in tasks:
        grouped[task["status"]].append(task)

    return render_template(
        "dashboard.html",
        user=user,
        users=users,
        grouped=grouped,
        filter_mode=filter_mode,
        editable_task_ids=editable_task_ids,
    )


@app.route("/overview")
@login_required
def overview():
    user = current_user()
    tasks = enrich_tasks_with_assignees(fetch_tasks())

    grouped = {
        STATUS_OPEN: [],
        STATUS_IN_PROGRESS: [],
        STATUS_CLOSED: [],
    }

    for task in tasks:
        grouped[task["status"]].append(task)

    return render_template(
        "overview.html",
        user=user,
        grouped=grouped,
        show_sidebar=False,
    )


@app.route("/api/overview/tasks")
@login_required
def overview_tasks_api():
    tasks = enrich_tasks_with_assignees(fetch_tasks())
    payload = []
    for task in tasks:
        payload.append(
            {
                "id": task["id"],
                "title": task["title"],
                "status": task["status"],
                "created_at": task["created_at"],
                "due_date_display": format_datetime_for_display(task["due_date"]),
                "assignees": task["assignees"],
            }
        )
    return jsonify({"tasks": payload})


@app.route("/calendar", methods=["GET", "POST"])
@login_required
def calendar_page():
    user = current_user()

    req_scope = request.args.get("scope", "me").strip().lower()
    req_filter_user = parse_int_value(request.args.get("user_id"))
    req_month = request.args.get("month", "").strip()
    month_start = parse_month_value(req_month)

    scope_user_ids, resolved_scope, resolved_filter_user_id, filter_users = calendar_scope_user_ids(
        user,
        req_scope,
        req_filter_user,
    )

    if request.method == "POST":
        action = request.form.get("action", "").strip()

        if action == "create":
            title = request.form.get("title", "").strip()
            notes = request.form.get("notes", "").strip()
            start_at = normalize_datetime_value(request.form.get("start_at", ""))
            end_at_raw = request.form.get("end_at", "").strip()
            end_at = normalize_datetime_value(end_at_raw) if end_at_raw else ""

            if not title or not start_at:
                flash("Titel und Startzeit sind erforderlich.", "error")
                return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

            if end_at and end_at < start_at:
                flash("Ende darf nicht vor dem Start liegen.", "error")
                return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

            now = now_iso()
            execute(
                """
                INSERT INTO calendar_events (user_id, title, notes, start_at, end_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (user["id"], title, notes, start_at, end_at or None, now, now),
            )
            flash("Termin wurde gespeichert.", "success")
            return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

        if action == "update":
            event_id = parse_int_value(request.form.get("event_id"))
            if event_id is None:
                flash("Ungültiger Termin.", "error")
                return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

            event = query_one(
                "SELECT id, user_id FROM calendar_events WHERE id = ?",
                (event_id,),
            )
            if event is None:
                flash("Termin nicht gefunden.", "error")
                return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

            if int(event["user_id"]) != int(user["id"]):
                flash("Du kannst nur eigene Termine bearbeiten.", "error")
                return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

            title = request.form.get("title", "").strip()
            notes = request.form.get("notes", "").strip()
            start_at = normalize_datetime_value(request.form.get("start_at", ""))
            end_at_raw = request.form.get("end_at", "").strip()
            end_at = normalize_datetime_value(end_at_raw) if end_at_raw else ""

            if not title or not start_at:
                flash("Titel und Startzeit sind erforderlich.", "error")
                return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

            if end_at and end_at < start_at:
                flash("Ende darf nicht vor dem Start liegen.", "error")
                return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

            execute(
                """
                UPDATE calendar_events
                SET title = ?, notes = ?, start_at = ?, end_at = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (title, notes, start_at, end_at or None, now_iso(), event_id, user["id"]),
            )
            flash("Termin wurde aktualisiert.", "success")
            return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

        if action == "delete":
            event_id = parse_int_value(request.form.get("event_id"))
            if event_id is None:
                flash("Ungültiger Termin.", "error")
                return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

            event = query_one("SELECT id, user_id FROM calendar_events WHERE id = ?", (event_id,))
            if event is None:
                flash("Termin nicht gefunden.", "error")
                return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

            if int(event["user_id"]) != int(user["id"]):
                flash("Du kannst nur eigene Termine löschen.", "error")
                return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

            execute("DELETE FROM calendar_events WHERE id = ? AND user_id = ?", (event_id, user["id"]))
            flash("Termin wurde gelöscht.", "success")
            return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

        flash("Unbekannte Aktion.", "error")
        return redirect(url_for("calendar_page", scope=resolved_scope, user_id=resolved_filter_user_id, month=month_start.strftime("%Y-%m")))

    personal_events = calendar_personal_events(scope_user_ids)
    events = calendar_combined_events(scope_user_ids)
    own_event_ids = {event["id"] for event in personal_events if int(event["user_id"]) == int(user["id"])}
    month_grid = build_month_cells(month_start, events)

    return render_template(
        "calendar.html",
        user=user,
        events=events,
        month_grid=month_grid,
        own_event_ids=own_event_ids,
        filter_users=filter_users,
        scope=resolved_scope,
        selected_user_id=resolved_filter_user_id,
        selected_month=month_start.strftime("%Y-%m"),
    )


@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings_page():
    user = current_user()

    if request.method == "POST":
        action = request.form.get("action", "").strip()

        if action == "password":
            current_password = request.form.get("current_password", "")
            new_password = request.form.get("new_password", "")
            confirm_password = request.form.get("confirm_password", "")

            if not current_password or not new_password or not confirm_password:
                flash("Bitte alle Passwortfelder ausfüllen.", "error")
                return redirect(url_for("settings_page"))

            if not check_password_hash(user["password_hash"], current_password):
                flash("Aktuelles Passwort ist falsch.", "error")
                return redirect(url_for("settings_page"))

            if new_password != confirm_password:
                flash("Neues Passwort und Bestätigung stimmen nicht überein.", "error")
                return redirect(url_for("settings_page"))

            execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                (generate_password_hash(new_password), user["id"]),
            )
            flash("Passwort wurde aktualisiert.", "success")
            return redirect(url_for("settings_page"))

        if action == "admin-settings":
            if not user["is_admin"]:
                flash("Nur Administratoren dürfen diese Einstellungen ändern.", "error")
                return redirect(url_for("settings_page"))

            current_settings = app_settings()

            highlight_seconds = parse_int_setting(
                request.form.get("new_task_highlight_seconds", ""),
                min_value=10,
                max_value=3600,
            )
            refresh_seconds = parse_int_setting(
                request.form.get("overview_refresh_interval_seconds", ""),
                min_value=1,
                max_value=60,
            )
            general_container_width = parse_int_setting(
                request.form.get(
                    "general_container_width_px",
                    current_settings["general_container_width_px"],
                ),
                min_value=900,
                max_value=3000,
            )
            general_main_min_height = parse_int_setting(
                request.form.get(
                    "general_main_min_height_px",
                    current_settings["general_main_min_height_px"],
                ),
                min_value=0,
                max_value=3000,
            )
            general_dashboard_shell_width = parse_int_setting(
                request.form.get("general_dashboard_shell_width_px", ""),
                min_value=700,
                max_value=2600,
            )
            dashboard_general_min_height = parse_int_setting(
                request.form.get("dashboard_general_min_height_px", ""),
                min_value=0,
                max_value=3000,
            )
            overview_general_width = parse_int_setting(
                request.form.get("overview_general_width_px", ""),
                min_value=700,
                max_value=3000,
            )
            overview_general_min_height = parse_int_setting(
                request.form.get("overview_general_min_height_px", ""),
                min_value=0,
                max_value=3000,
            )
            dashboard_category_min_width = parse_int_setting(
                request.form.get("dashboard_category_min_width_px", ""),
                min_value=220,
                max_value=700,
            )
            dashboard_category_min_height = parse_int_setting(
                request.form.get("dashboard_category_min_height_px", ""),
                min_value=0,
                max_value=3000,
            )
            overview_category_width = parse_int_setting(
                request.form.get("overview_category_width_px", ""),
                min_value=0,
                max_value=2200,
            )
            overview_category_min_height = parse_int_setting(
                request.form.get("overview_category_min_height_px", ""),
                min_value=0,
                max_value=3000,
            )
            dashboard_task_width = parse_int_setting(
                request.form.get("dashboard_task_width_px", ""),
                min_value=180,
                max_value=700,
            )
            dashboard_task_min_height = parse_int_setting(
                request.form.get("dashboard_task_min_height_px", ""),
                min_value=0,
                max_value=800,
            )
            overview_task_width = parse_int_setting(
                request.form.get("overview_task_width_px", ""),
                min_value=140,
                max_value=600,
            )
            overview_task_min_height = parse_int_setting(
                request.form.get("overview_task_min_height_px", ""),
                min_value=60,
                max_value=600,
            )

            role_color_admin = request.form.get("role_color_admin", "").strip()
            role_color_system = request.form.get("role_color_system", "").strip()
            role_color_dev = request.form.get("role_color_dev", "").strip()
            role_color_team = request.form.get("role_color_team", "").strip()
            new_task_tone = request.form.get("new_task_tone", "classic").strip()

            numeric_values = [
                highlight_seconds,
                refresh_seconds,
                general_container_width,
                general_main_min_height,
                general_dashboard_shell_width,
                dashboard_general_min_height,
                overview_general_width,
                overview_general_min_height,
                dashboard_category_min_width,
                dashboard_category_min_height,
                overview_category_width,
                overview_category_min_height,
                dashboard_task_width,
                dashboard_task_min_height,
                overview_task_width,
                overview_task_min_height,
            ]
            if any(value is None for value in numeric_values):
                flash("Mindestens ein Zahlenwert ist ungültig oder außerhalb des erlaubten Bereichs.", "error")
                return redirect(url_for("settings_page"))

            colors = [role_color_admin, role_color_system, role_color_dev, role_color_team]
            if not all(is_hex_color(color) for color in colors):
                flash("Farben müssen im Format #RRGGBB angegeben werden.", "error")
                return redirect(url_for("settings_page"))

            if new_task_tone not in TONE_OPTIONS:
                flash("Ungültige Ton-Auswahl.", "error")
                return redirect(url_for("settings_page"))

            set_app_setting("new_task_highlight_seconds", str(highlight_seconds))
            set_app_setting("overview_refresh_interval_seconds", str(refresh_seconds))
            set_app_setting("general_container_width_px", str(general_container_width))
            set_app_setting("general_main_min_height_px", str(general_main_min_height))
            set_app_setting("general_dashboard_shell_width_px", str(general_dashboard_shell_width))
            set_app_setting("dashboard_general_min_height_px", str(dashboard_general_min_height))
            set_app_setting("overview_general_width_px", str(overview_general_width))
            set_app_setting("overview_general_min_height_px", str(overview_general_min_height))
            set_app_setting("dashboard_category_min_width_px", str(dashboard_category_min_width))
            set_app_setting("dashboard_category_min_height_px", str(dashboard_category_min_height))
            set_app_setting("overview_category_width_px", str(overview_category_width))
            set_app_setting("overview_category_min_height_px", str(overview_category_min_height))
            set_app_setting("dashboard_task_width_px", str(dashboard_task_width))
            set_app_setting("dashboard_task_min_height_px", str(dashboard_task_min_height))
            set_app_setting("overview_task_width_px", str(overview_task_width))
            set_app_setting("overview_task_min_height_px", str(overview_task_min_height))
            set_app_setting("role_color_admin", role_color_admin)
            set_app_setting("role_color_system", role_color_system)
            set_app_setting("role_color_dev", role_color_dev)
            set_app_setting("role_color_team", role_color_team)
            set_app_setting("new_task_tone", new_task_tone)

            flash("Admin-Einstellungen wurden gespeichert.", "success")
            return redirect(url_for("settings_page"))

        flash("Unbekannte Aktion.", "error")
        return redirect(url_for("settings_page"))

    return render_template(
        "settings.html",
        user=user,
        settings=app_settings(),
        tone_options=sorted(TONE_OPTIONS.keys()),
    )


@app.route("/tasks/create", methods=["POST"])
@login_required
def create_task():
    user = current_user()

    title = request.form.get("title", "").strip()
    description = request.form.get("description", "").strip()
    contact_person = request.form.get("contact_person", "").strip()
    due_date = request.form.get("due_date", "").strip()
    assignee_ids_raw = request.form.getlist("assignee_ids")

    if not title or not description or not contact_person or not due_date:
        flash("Bitte alle Pflichtfelder ausfüllen.", "error")
        return redirect(url_for("dashboard"))

    normalized_due_date = normalize_datetime_value(due_date)
    if not normalized_due_date:
        flash("Ungültiges Fälligkeitsdatum.", "error")
        return redirect(url_for("dashboard"))

    assignee_ids = []
    for raw_id in assignee_ids_raw:
        cleaned = raw_id.strip()
        if cleaned:
            try:
                assignee_ids.append(int(cleaned))
            except ValueError:
                flash("Ungültiger Bearbeiter ausgewählt.", "error")
                return redirect(url_for("dashboard"))

    assignee_ids = sorted(set(assignee_ids))
    if not assignee_ids:
        flash("Bitte mindestens einen Bearbeiter auswählen.", "error")
        return redirect(url_for("dashboard"))

    placeholders = ",".join(["?"] * len(assignee_ids))
    found = query_all(f"SELECT id FROM users WHERE id IN ({placeholders})", tuple(assignee_ids))
    if len(found) != len(assignee_ids):
        flash("Mindestens ein ausgewählter Bearbeiter existiert nicht.", "error")
        return redirect(url_for("dashboard"))

    now = now_iso()
    cur = execute(
        """
        INSERT INTO tasks (
            title,
            description,
            assignee_id,
            due_date,
            contact_person,
            created_by,
            status,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            title,
            description,
            assignee_ids[0],
            normalized_due_date,
            contact_person,
            user["id"],
            STATUS_OPEN,
            now,
            now,
        ),
    )

    task_id = cur.lastrowid
    for assignee_id in assignee_ids:
        execute(
            "INSERT OR IGNORE INTO task_assignees (task_id, user_id) VALUES (?, ?)",
            (task_id, assignee_id),
        )

    flash("Task wurde erstellt.", "success")
    return redirect(url_for("dashboard"))


def can_manage_task(user, task_id: int) -> bool:
    if user["is_admin"]:
        return True
    row = query_one(
        "SELECT 1 FROM task_assignees WHERE task_id = ? AND user_id = ?",
        (task_id, user["id"]),
    )
    return row is not None


def is_task_creator(user, task_id: int) -> bool:
    row = query_one("SELECT created_by FROM tasks WHERE id = ?", (task_id,))
    return bool(row is not None and row["created_by"] == user["id"])


def can_access_task_detail(user, task_id: int) -> bool:
    _ = task_id
    return user is not None


def can_edit_task_content(user, task_id: int) -> bool:
    task = query_one("SELECT status FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        return False
    if task["status"] == STATUS_CLOSED:
        return False
    if user["is_admin"]:
        return True
    return is_task_creator(user, task_id)


def task_comments(task_id: int):
    return query_all(
        """
        SELECT
            tc.id,
            tc.user_id,
            tc.content,
            tc.created_at,
            tc.updated_at,
            u.username,
            u.initials,
            u.role,
            u.is_admin
        FROM task_comments tc
        JOIN users u ON u.id = tc.user_id
        WHERE tc.task_id = ?
        ORDER BY tc.created_at DESC
        """,
        (task_id,),
    )


def task_with_details(task_id: int):
    task = query_one(
        """
        SELECT
            t.*,
            c.username AS creator_name
        FROM tasks t
        JOIN users c ON c.id = t.created_by
        WHERE t.id = ?
        """,
        (task_id,),
    )
    if task is None:
        return None

    task_dict = dict(task)
    assignees = task_assignees_map([task_id]).get(task_id, [])
    comments = []
    for comment in task_comments(task_id):
        created_at = format_system_datetime_for_display(comment["created_at"])
        updated_raw = comment["updated_at"]
        updated_at = format_system_datetime_for_display(updated_raw) if updated_raw else ""
        comments.append(
            {
                "id": comment["id"],
                "user_id": comment["user_id"],
                "content": comment["content"],
                "created_at": created_at,
                "updated_at": updated_at,
                "is_edited": bool(updated_raw),
                "username": comment["username"],
                "initials": comment["initials"] or make_initials_from_username(comment["username"]),
                "role_label": role_label(comment["role"], comment["is_admin"]),
                "color_class": badge_color_class(comment["role"], comment["is_admin"]),
            }
        )

    task_dict["assignees"] = assignees
    task_dict["comments"] = comments
    return task_dict


@app.route("/tasks/<int:task_id>/status", methods=["POST"])
@login_required
def update_task_status(task_id: int):
    user = current_user()
    new_status = request.form.get("status", "").strip()
    return_filter = request.form.get("return_filter", "all").strip().lower()
    if return_filter not in {"all", "mine"}:
        return_filter = "all"

    if new_status not in VALID_STATUSES:
        flash("Ungültiger Status.", "error")
        return redirect(url_for("dashboard"))

    task = query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    if not can_manage_task(user, task_id):
        flash("Keine Berechtigung für diese Task.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    if task["status"] == STATUS_CLOSED and not user["is_admin"] and new_status != STATUS_CLOSED:
        flash("Nur Administratoren dürfen geschlossene Tasks zurücksetzen.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    if task["status"] == STATUS_CLOSED and new_status == STATUS_CLOSED:
        flash("Task ist bereits geschlossen.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    if new_status == STATUS_CLOSED:
        close_reason = request.form.get("close_reason", "").strip()
        if not close_reason:
            flash("Bitte eine Begründung angeben, warum die Task geschlossen wird.", "error")
            return redirect(url_for("dashboard", filter=return_filter))

        execute(
            """
            UPDATE tasks
            SET status = ?, updated_at = ?, close_reason = ?, closed_at = ?, closed_by = ?
            WHERE id = ?
            """,
            (new_status, now_iso(), close_reason, now_iso(), user["id"], task_id),
        )
        flash("Task wurde geschlossen.", "success")
        return redirect(url_for("dashboard", filter=return_filter))

    execute(
        "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
        (new_status, now_iso(), task_id),
    )
    flash("Task-Status aktualisiert.", "success")
    return redirect(url_for("dashboard", filter=return_filter))


@app.route("/tasks/<int:task_id>")
@login_required
def task_detail(task_id: int):
    user = current_user()
    task = task_with_details(task_id)
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard"))

    if not can_access_task_detail(user, task_id):
        flash("Du bist dieser Task nicht zugewiesen.", "error")
        return redirect(url_for("dashboard"))

    users = query_all(
        """
        SELECT id, username, initials, role, is_admin
        FROM users
        ORDER BY is_admin DESC, username ASC
        """
    )

    return render_template(
        "task_detail.html",
        user=user,
        task=task,
        users=users,
        can_edit_task_content=can_edit_task_content(user, task_id),
        can_comment=(task["status"] != STATUS_CLOSED) and (user["is_admin"] or can_manage_task(user, task_id)),
    )


@app.route("/tasks/<int:task_id>/comments", methods=["POST"])
@login_required
def add_task_comment(task_id: int):
    user = current_user()
    task = query_one("SELECT id, status FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard"))

    if task["status"] == STATUS_CLOSED:
        flash("Geschlossene Tasks sind schreibgeschützt.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    if not (user["is_admin"] or can_manage_task(user, task_id)):
        flash("Nur zugewiesene Benutzer oder Admins dürfen kommentieren.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    content = request.form.get("content", "").strip()
    if not content:
        flash("Kommentar darf nicht leer sein.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    execute(
        """
        INSERT INTO task_comments (task_id, user_id, content, created_at, updated_at)
        VALUES (?, ?, ?, ?, NULL)
        """,
        (task_id, user["id"], content, now_iso()),
    )
    flash("Kommentar wurde gespeichert.", "success")
    return redirect(url_for("task_detail", task_id=task_id))


@app.route("/tasks/<int:task_id>/comments/<int:comment_id>/edit", methods=["POST"])
@login_required
def edit_task_comment(task_id: int, comment_id: int):
    user = current_user()
    task = query_one("SELECT id, status FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard"))

    if task["status"] == STATUS_CLOSED:
        flash("Geschlossene Tasks sind schreibgeschützt.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    comment = query_one(
        "SELECT id, task_id, user_id FROM task_comments WHERE id = ? AND task_id = ?",
        (comment_id, task_id),
    )
    if comment is None:
        flash("Kommentar nicht gefunden.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    if not (user["is_admin"] or int(comment["user_id"]) == int(user["id"])):
        flash("Nur Verfasser oder Admin dürfen diesen Kommentar bearbeiten.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    content = request.form.get("content", "").strip()
    if not content:
        flash("Kommentar darf nicht leer sein.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    execute(
        """
        UPDATE task_comments
        SET content = ?, updated_at = ?
        WHERE id = ? AND task_id = ?
        """,
        (content, now_iso(), comment_id, task_id),
    )
    flash("Kommentar wurde aktualisiert.", "success")
    return redirect(url_for("task_detail", task_id=task_id))


@app.route("/tasks/<int:task_id>/comments/<int:comment_id>/delete", methods=["POST"])
@login_required
def delete_task_comment(task_id: int, comment_id: int):
    user = current_user()
    task = query_one("SELECT id, status FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard"))

    if task["status"] == STATUS_CLOSED:
        flash("Geschlossene Tasks sind schreibgeschützt.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    comment = query_one(
        "SELECT id, task_id, user_id FROM task_comments WHERE id = ? AND task_id = ?",
        (comment_id, task_id),
    )
    if comment is None:
        flash("Kommentar nicht gefunden.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    if not (user["is_admin"] or int(comment["user_id"]) == int(user["id"])):
        flash("Nur Verfasser oder Admin dürfen diesen Kommentar löschen.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    execute("DELETE FROM task_comments WHERE id = ? AND task_id = ?", (comment_id, task_id))
    flash("Kommentar wurde gelöscht.", "success")
    return redirect(url_for("task_detail", task_id=task_id))


@app.route("/tasks/<int:task_id>/edit", methods=["POST"])
@login_required
def edit_task(task_id: int):
    user = current_user()
    task = query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard"))

    if task["status"] == STATUS_CLOSED:
        flash("Geschlossene Tasks sind schreibgeschützt.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    if not can_edit_task_content(user, task_id):
        flash("Nur Ersteller oder Admin dürfen diese Task bearbeiten.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    title = request.form.get("title", "").strip()
    description = request.form.get("description", "").strip()
    due_date = request.form.get("due_date", "").strip()
    assignee_ids_raw = request.form.getlist("assignee_ids")

    if not title or not description or not due_date:
        flash("Titel, Beschreibung und Fälligkeitsdatum sind erforderlich.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    normalized_due_date = normalize_datetime_value(due_date)
    if not normalized_due_date:
        flash("Ungültiges Fälligkeitsdatum.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    assignee_ids = []
    for raw_id in assignee_ids_raw:
        cleaned = raw_id.strip()
        if cleaned:
            try:
                assignee_ids.append(int(cleaned))
            except ValueError:
                flash("Ungültiger Bearbeiter ausgewählt.", "error")
                return redirect(url_for("task_detail", task_id=task_id))

    assignee_ids = sorted(set(assignee_ids))
    if not assignee_ids:
        flash("Bitte mindestens einen Bearbeiter auswählen.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    placeholders = ",".join(["?"] * len(assignee_ids))
    found = query_all(f"SELECT id FROM users WHERE id IN ({placeholders})", tuple(assignee_ids))
    if len(found) != len(assignee_ids):
        flash("Mindestens ein ausgewählter Bearbeiter existiert nicht.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    execute(
        """
        UPDATE tasks
        SET title = ?, description = ?, due_date = ?, assignee_id = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            title,
            description,
            normalized_due_date,
            assignee_ids[0],
            now_iso(),
            task_id,
        ),
    )

    execute("DELETE FROM task_assignees WHERE task_id = ?", (task_id,))
    for assignee_id in assignee_ids:
        execute(
            "INSERT INTO task_assignees (task_id, user_id) VALUES (?, ?)",
            (task_id, assignee_id),
        )

    flash("Task wurde aktualisiert.", "success")
    return redirect(url_for("task_detail", task_id=task_id))


@app.route("/admin/users", methods=["GET", "POST"])
@admin_required
def manage_users():
    if request.method == "POST":
        action = request.form.get("action", "")
        current = current_user()

        if action == "create":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "").strip()
            password_confirm = request.form.get("password_confirm", "").strip()
            initials = normalize_initials(request.form.get("initials", ""))
            role = normalize_role(request.form.get("role", ""))
            is_admin = 1 if role == "admin" else 0

            if not username or not password:
                flash("Benutzername und Passwort sind erforderlich.", "error")
                return redirect(url_for("manage_users"))

            if password != password_confirm:
                flash("Passwort und Passwort-Bestätigung stimmen nicht überein.", "error")
                return redirect(url_for("manage_users"))

            if not initials:
                flash("Kürzel muss genau 3 Zeichen (A-Z/0-9) lang sein.", "error")
                return redirect(url_for("manage_users"))

            if not role:
                flash("Bitte eine gültige Rolle auswählen.", "error")
                return redirect(url_for("manage_users"))

            exists = query_one("SELECT id FROM users WHERE username = ?", (username,))
            if exists is not None:
                flash("Benutzername ist bereits vergeben.", "error")
                return redirect(url_for("manage_users"))

            exists_initials = query_one("SELECT id FROM users WHERE initials = ?", (initials,))
            if exists_initials is not None:
                flash("Dieses Kürzel ist bereits vergeben.", "error")
                return redirect(url_for("manage_users"))

            execute(
                """
                INSERT INTO users (username, password_hash, is_admin, initials, role, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    username,
                    generate_password_hash(password),
                    is_admin,
                    initials,
                    role,
                    now_iso(),
                ),
            )
            flash("Benutzer wurde angelegt.", "success")
            return redirect(url_for("manage_users"))

        if action == "update":
            target_id = request.form.get("user_id", "").strip()
            username = request.form.get("username", "").strip()
            initials = normalize_initials(request.form.get("initials", ""))
            role = normalize_role(request.form.get("role", ""))
            new_password = request.form.get("new_password", "")
            confirm_password = request.form.get("confirm_password", "")

            target = query_one("SELECT * FROM users WHERE id = ?", (target_id,))
            if target is None:
                flash("Benutzer nicht gefunden.", "error")
                return redirect(url_for("manage_users"))

            if not username:
                flash("Benutzername ist erforderlich.", "error")
                return redirect(url_for("manage_users"))

            if not initials:
                flash("Kürzel muss genau 3 Zeichen (A-Z/0-9) lang sein.", "error")
                return redirect(url_for("manage_users"))

            if not role:
                flash("Bitte eine gültige Rolle auswählen.", "error")
                return redirect(url_for("manage_users"))

            username_exists = query_one(
                "SELECT id FROM users WHERE username = ? AND id != ?",
                (username, target_id),
            )
            if username_exists is not None:
                flash("Benutzername ist bereits vergeben.", "error")
                return redirect(url_for("manage_users"))

            initials_exists = query_one(
                "SELECT id FROM users WHERE initials = ? AND id != ?",
                (initials, target_id),
            )
            if initials_exists is not None:
                flash("Dieses Kürzel ist bereits vergeben.", "error")
                return redirect(url_for("manage_users"))

            if (new_password and not confirm_password) or (confirm_password and not new_password):
                flash("Bitte neues Passwort und Bestätigung vollständig ausfüllen.", "error")
                return redirect(url_for("manage_users"))

            if new_password and new_password != confirm_password:
                flash("Neues Passwort und Bestätigung stimmen nicht überein.", "error")
                return redirect(url_for("manage_users"))

            is_admin = 1 if role == "admin" else 0
            if target["is_admin"] and not is_admin:
                row = query_one("SELECT COUNT(*) AS cnt FROM users WHERE is_admin = 1")
                if int(row["cnt"]) <= 1:
                    flash("Der letzte Admin kann nicht zur Nicht-Admin-Rolle geändert werden.", "error")
                    return redirect(url_for("manage_users"))

            if new_password:
                execute(
                    """
                    UPDATE users
                    SET username = ?, initials = ?, role = ?, is_admin = ?, password_hash = ?
                    WHERE id = ?
                    """,
                    (
                        username,
                        initials,
                        role,
                        is_admin,
                        generate_password_hash(new_password),
                        target_id,
                    ),
                )
                if target["id"] == current["id"]:
                    flash("Eigener Account wurde inkl. Passwort aktualisiert.", "success")
                else:
                    flash("Benutzerprofil wurde inkl. Passwort aktualisiert.", "success")
            else:
                execute(
                    """
                    UPDATE users
                    SET username = ?, initials = ?, role = ?, is_admin = ?
                    WHERE id = ?
                    """,
                    (username, initials, role, is_admin, target_id),
                )
                if target["id"] == current["id"]:
                    flash("Eigener Account wurde aktualisiert.", "success")
                else:
                    flash("Benutzerprofil wurde aktualisiert.", "success")

            return redirect(url_for("manage_users"))

        if action == "delete":
            target_id = request.form.get("user_id", "").strip()

            target = query_one("SELECT * FROM users WHERE id = ?", (target_id,))
            if target is None:
                flash("Benutzer nicht gefunden.", "error")
                return redirect(url_for("manage_users"))

            if target["id"] == current["id"]:
                flash("Eigener Account kann nicht gelöscht werden.", "error")
                return redirect(url_for("manage_users"))

            if target["is_admin"]:
                row = query_one("SELECT COUNT(*) AS cnt FROM users WHERE is_admin = 1")
                if int(row["cnt"]) <= 1:
                    flash("Der letzte Admin kann nicht gelöscht werden.", "error")
                    return redirect(url_for("manage_users"))

            assigned_rows = query_all(
                "SELECT DISTINCT task_id FROM task_assignees WHERE user_id = ?",
                (target_id,),
            )
            affected_task_ids = {row["task_id"] for row in assigned_rows}

            execute("DELETE FROM task_assignees WHERE user_id = ?", (target_id,))
            execute("UPDATE tasks SET assignee_id = NULL WHERE assignee_id = ?", (target_id,))
            execute("DELETE FROM users WHERE id = ?", (target_id,))

            for task_id in affected_task_ids:
                sync_task_primary_assignee(task_id)

            flash("Benutzer wurde entfernt und aus allen Tasks ausgetragen.", "success")
            return redirect(url_for("manage_users"))

        flash("Unbekannte Aktion.", "error")
        return redirect(url_for("manage_users"))

    users = query_all(
        """
        SELECT
            id,
            username,
            is_admin,
            initials,
            role,
            created_at
        FROM users
                ORDER BY is_admin DESC,
                    CASE role
                        WHEN ? THEN 0
                        WHEN ? THEN 1
                        WHEN ? THEN 2
                        ELSE 3
                    END,
                    username ASC
        """
                ,
                (ROLE_TEAM, ROLE_SYSTEM_INTEGRATOR, ROLE_APPLICATION_DEVELOPER),
    )
    return render_template("admin_users.html", users=users, user=current_user())


@app.route("/admin/closed", methods=["GET", "POST"])
@admin_required
def admin_closed_tasks():
    if request.method == "POST":
        action = request.form.get("action", "").strip()
        task_id = request.form.get("task_id", "").strip()

        task = query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
        if task is None:
            flash("Task nicht gefunden.", "error")
            return redirect(url_for("admin_closed_tasks"))

        if task["status"] != STATUS_CLOSED:
            flash("Diese Task ist nicht geschlossen.", "error")
            return redirect(url_for("admin_closed_tasks"))

        if action == "reopen":
            execute(
                """
                UPDATE tasks
                SET status = ?, updated_at = ?, close_reason = NULL, closed_at = NULL, closed_by = NULL
                WHERE id = ?
                """,
                (STATUS_IN_PROGRESS, now_iso(), task_id),
            )
            flash("Task wurde ans Team zurückgesendet.", "success")
            return redirect(url_for("admin_closed_tasks"))

        if action == "delete":
            execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            flash("Task wurde endgültig gelöscht.", "success")
            return redirect(url_for("admin_closed_tasks"))

        flash("Unbekannte Aktion.", "error")
        return redirect(url_for("admin_closed_tasks"))

    closed_tasks = enrich_tasks_with_assignees(fetch_tasks(status=STATUS_CLOSED))

    return render_template("admin_closed.html", tasks=closed_tasks, user=current_user())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

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
VALID_DASHBOARD_FILTERS = {"all", "mine", "pings"}
VALID_PING_TABS = {"unread", "read"}
MIN_TASK_PRIORITY = 1
MAX_TASK_PRIORITY = 5
DEFAULT_TASK_PRIORITY = 3
TICKET_CATEGORY_HARDWARE = "hardware"
TICKET_CATEGORY_SOFTWARE = "software"
TICKET_CATEGORY_NETWORK_INTERNET = "network_internet"
TICKET_CATEGORY_SECURITY = "security"
TICKET_CATEGORY_IT_SERVICE_ORDER = "it_service_order"
TICKET_CATEGORY_WORKSTATION_SETUP = "workstation_setup"
TICKET_CATEGORY_OTHER = "other"
VALID_TICKET_CATEGORIES = {
    TICKET_CATEGORY_HARDWARE,
    TICKET_CATEGORY_SOFTWARE,
    TICKET_CATEGORY_NETWORK_INTERNET,
    TICKET_CATEGORY_SECURITY,
    TICKET_CATEGORY_IT_SERVICE_ORDER,
    TICKET_CATEGORY_WORKSTATION_SETUP,
    TICKET_CATEGORY_OTHER,
}
TICKET_CATEGORY_LABELS = {
    TICKET_CATEGORY_HARDWARE: "Hardware",
    TICKET_CATEGORY_SOFTWARE: "Software",
    TICKET_CATEGORY_NETWORK_INTERNET: "Netzwerk / Internet",
    TICKET_CATEGORY_SECURITY: "Sicherheit",
    TICKET_CATEGORY_IT_SERVICE_ORDER: "IT-Service / Bestellung",
    TICKET_CATEGORY_WORKSTATION_SETUP: "Arbeitsplatz / Setup",
    TICKET_CATEGORY_OTHER: "Sonstiges",
}
ROLE_SYSTEM_INTEGRATOR = "system_integrator"
ROLE_APPLICATION_DEVELOPER = "application_developer"
ROLE_TEAM = "team"
VALID_ROLES = {ROLE_SYSTEM_INTEGRATOR, ROLE_APPLICATION_DEVELOPER, ROLE_TEAM}
BUILTIN_ROLE_CONFIG = {
    ROLE_TEAM: {"label": "Team", "setting_key": "role_color_team", "badge_class": "badge-team"},
    ROLE_SYSTEM_INTEGRATOR: {
        "label": "Systemintegrator",
        "setting_key": "role_color_system",
        "badge_class": "badge-system",
    },
    ROLE_APPLICATION_DEVELOPER: {
        "label": "Anwendungsentwickler",
        "setting_key": "role_color_dev",
        "badge_class": "badge-dev",
    },
}

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

THEME_LIGHT = "light"
THEME_DARK = "dark"
VALID_THEME_MODES = {THEME_LIGHT, THEME_DARK}
CARD_VIEW_COMPACT = "compact"
CARD_VIEW_EXTENDED = "extended"
VALID_CARD_VIEW_MODES = {CARD_VIEW_COMPACT, CARD_VIEW_EXTENDED}
MEMBER_TYPE_REGULAR = "regular"
MEMBER_TYPE_TRAINEE = "trainee"
VALID_MEMBER_TYPES = {MEMBER_TYPE_REGULAR, MEMBER_TYPE_TRAINEE}
MAX_USERNAME_LENGTH = 13


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
            priority INTEGER NOT NULL DEFAULT 3,
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

        CREATE TABLE IF NOT EXISTS task_comment_mentions (
            comment_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (comment_id, user_id),
            FOREIGN KEY (comment_id) REFERENCES task_comments(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS user_ping_reads (
            user_id INTEGER NOT NULL,
            comment_id INTEGER NOT NULL,
            read_at TEXT NOT NULL,
            PRIMARY KEY (user_id, comment_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (comment_id) REFERENCES task_comments(id) ON DELETE CASCADE
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

        CREATE TABLE IF NOT EXISTS custom_roles (
            role_key TEXT PRIMARY KEY,
            label TEXT NOT NULL UNIQUE,
            color TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS disabled_roles (
            role_key TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        );
        """
    )

    # Ensure migrations for existing databases.
    columns = {row["name"] for row in db.execute("PRAGMA table_info(users)").fetchall()}
    if "initials" not in columns:
        db.execute("ALTER TABLE users ADD COLUMN initials TEXT")
    if "role" not in columns:
        db.execute("ALTER TABLE users ADD COLUMN role TEXT")
    if "theme_mode" not in columns:
        db.execute(f"ALTER TABLE users ADD COLUMN theme_mode TEXT NOT NULL DEFAULT '{THEME_LIGHT}'")
    if "card_view_mode" not in columns:
        db.execute(
            f"ALTER TABLE users ADD COLUMN card_view_mode TEXT NOT NULL DEFAULT '{CARD_VIEW_COMPACT}'"
        )
    if "last_seen_ping_at" not in columns:
        db.execute("ALTER TABLE users ADD COLUMN last_seen_ping_at TEXT")
    if "is_inactive" not in columns:
        db.execute("ALTER TABLE users ADD COLUMN is_inactive INTEGER NOT NULL DEFAULT 0")
    if "member_type" not in columns:
        db.execute(f"ALTER TABLE users ADD COLUMN member_type TEXT NOT NULL DEFAULT '{MEMBER_TYPE_REGULAR}'")
    if "is_dashboard_invisible" not in columns:
        db.execute("ALTER TABLE users ADD COLUMN is_dashboard_invisible INTEGER NOT NULL DEFAULT 0")

    task_columns = {row["name"] for row in db.execute("PRAGMA table_info(tasks)").fetchall()}
    if "due_date" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN due_date TEXT")
    if "close_reason" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN close_reason TEXT")
    if "closed_at" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN closed_at TEXT")
    if "closed_by" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN closed_by INTEGER")
    if "contact_person_user_id" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN contact_person_user_id INTEGER")
    if "ticket_category" not in task_columns:
        db.execute(
            f"ALTER TABLE tasks ADD COLUMN ticket_category TEXT NOT NULL DEFAULT '{TICKET_CATEGORY_OTHER}'"
        )
    if "room" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN room TEXT")
    if "priority" not in task_columns:
        db.execute(f"ALTER TABLE tasks ADD COLUMN priority INTEGER NOT NULL DEFAULT {DEFAULT_TASK_PRIORITY}")

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
        SET ticket_category = ?
        WHERE ticket_category IS NULL OR TRIM(ticket_category) = ''
        """,
        (TICKET_CATEGORY_OTHER,),
    )

    valid_categories = tuple(sorted(VALID_TICKET_CATEGORIES))
    placeholders = ",".join(["?"] * len(valid_categories))
    db.execute(
        f"""
        UPDATE tasks
        SET ticket_category = ?
        WHERE lower(ticket_category) NOT IN ({placeholders})
        """,
        (TICKET_CATEGORY_OTHER, *valid_categories),
    )

    # Backfill contact_person_user_id for legacy rows where contact person was stored as plain text.
    db.execute(
        """
        UPDATE tasks
        SET contact_person_user_id = (
            SELECT u.id
            FROM users u
            WHERE lower(u.username) = lower(tasks.contact_person)
            LIMIT 1
        )
        WHERE (contact_person_user_id IS NULL OR contact_person_user_id = 0)
          AND contact_person IS NOT NULL
          AND TRIM(contact_person) != ''
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

    db.execute(
        """
        UPDATE users
        SET theme_mode = ?
        WHERE theme_mode IS NULL OR TRIM(theme_mode) = '' OR lower(theme_mode) NOT IN ('light', 'dark')
        """,
        (THEME_LIGHT,),
    )

    db.execute(
        """
        UPDATE users
        SET card_view_mode = ?
        WHERE card_view_mode IS NULL OR TRIM(card_view_mode) = '' OR lower(card_view_mode) NOT IN ('compact', 'extended')
        """,
        (CARD_VIEW_COMPACT,),
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


def normalize_due_date_value(date_value: str, time_value: str | None = None) -> str:
    date_raw = (date_value or "").strip()
    if not date_raw:
        return ""

    try:
        due_date = datetime.strptime(date_raw, "%Y-%m-%d").date()
    except ValueError:
        return ""

    time_raw = (time_value or "").strip()
    if not time_raw:
        return due_date.strftime("%Y-%m-%d")

    try:
        due_time = datetime.strptime(time_raw, "%H:%M").time()
    except ValueError:
        return ""

    return f"{due_date.strftime('%Y-%m-%d')}T{due_time.strftime('%H:%M')}"


def due_date_parts(value: str | None) -> tuple[str, str]:
    if not value:
        return "", ""

    raw = value.strip()
    if not raw:
        return "", ""

    has_time = bool(re.search(r"\d{2}:\d{2}", raw))
    normalized = normalize_datetime_value(raw)
    if not normalized:
        return "", ""

    dt = datetime.fromisoformat(normalized)
    date_part = dt.strftime("%Y-%m-%d")
    time_part = dt.strftime("%H:%M") if has_time else ""
    return date_part, time_part


def format_datetime_for_display(value: str | None) -> str:
    if not value:
        return "-"
    raw = value.strip()
    has_time = bool(re.search(r"\d{2}:\d{2}", raw))
    normalized = normalize_datetime_value(raw)
    if not normalized:
        return "-"
    dt = datetime.fromisoformat(normalized)
    if not has_time:
        return dt.strftime("%d.%m.%Y")
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


def format_due_date_for_input(value: str | None) -> str:
    date_part, _ = due_date_parts(value)
    return date_part


def format_due_time_for_input(value: str | None) -> str:
    _, time_part = due_date_parts(value)
    return time_part


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


def custom_roles():
    return query_all(
        """
        SELECT role_key, label, color, created_at
        FROM custom_roles
        ORDER BY label COLLATE NOCASE ASC
        """
    )


def disabled_roles() -> set[str]:
    if "disabled_roles_set" not in g:
        rows = query_all("SELECT role_key FROM disabled_roles")
        g.disabled_roles_set = {row["role_key"] for row in rows}
    return g.disabled_roles_set


def active_builtin_roles() -> list[str]:
    disabled = disabled_roles()
    return [role for role in BUILTIN_ROLE_CONFIG if role not in disabled]


def custom_roles_map() -> dict[str, dict]:
    if "custom_roles_map" not in g:
        g.custom_roles_map = {row["role_key"]: dict(row) for row in custom_roles()}
    return g.custom_roles_map


def active_custom_roles() -> list[sqlite3.Row]:
    disabled = disabled_roles()
    return [role for role in custom_roles() if role["role_key"] not in disabled]


def normalize_custom_role_key(label: str) -> str:
    lowered = label.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
    return normalized[:40]


def role_options() -> list[dict[str, str]]:
    options = [
        {"value": "admin", "label": "Admin"},
    ]
    for role_key in active_builtin_roles():
        options.append({"value": role_key, "label": BUILTIN_ROLE_CONFIG[role_key]["label"]})
    for role in active_custom_roles():
        options.append({"value": role["role_key"], "label": role["label"]})
    return options


def normalize_role(value: str) -> str:
    if value in VALID_ROLES or value == "admin":
        return value if value in active_builtin_roles() or value == "admin" else ""
    if value in custom_roles_map():
        if value in disabled_roles():
            return ""
        return value
    return ""


def normalize_ticket_category(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in VALID_TICKET_CATEGORIES:
        return normalized
    return ""


def ticket_category_label(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    return TICKET_CATEGORY_LABELS.get(normalized, TICKET_CATEGORY_LABELS[TICKET_CATEGORY_OTHER])


def ticket_category_options() -> list[dict[str, str]]:
    return [
        {"value": key, "label": label}
        for key, label in TICKET_CATEGORY_LABELS.items()
    ]


def role_label(role: str, is_admin: int) -> str:
    if is_admin:
        return "Admin"
    if role in BUILTIN_ROLE_CONFIG:
        return BUILTIN_ROLE_CONFIG[role]["label"]
    custom = custom_roles_map().get(role)
    if custom is not None:
        return custom["label"]
    return "Anwendungsentwickler"


def badge_color_class(role: str, is_admin: int) -> str:
    if is_admin:
        return "badge-admin"
    if role in BUILTIN_ROLE_CONFIG:
        return BUILTIN_ROLE_CONFIG[role]["badge_class"]
    if role in custom_roles_map():
        return f"badge-role-{role}"
    return "badge-dev"


def badge_color_value(role: str, is_admin: int) -> str:
    settings = app_settings()
    if is_admin:
        return settings.get("role_color_admin", "#facc15")
    if role in BUILTIN_ROLE_CONFIG:
        key = BUILTIN_ROLE_CONFIG[role]["setting_key"]
        return settings.get(key, "#64748b")
    custom = custom_roles_map().get(role)
    if custom is not None:
        return custom.get("color", "#64748b")
    return settings.get("role_color_dev", "#2563eb")


def custom_role_css_rules():
    rules = []
    for role in active_custom_roles():
        rules.append(
            {
                "class_name": f"badge-role-{role['role_key']}",
                "color": role["color"],
            }
        )
    return rules


def builtin_role_color_fields(settings: dict[str, str]) -> list[dict[str, str]]:
    fields = []
    for role_key in active_builtin_roles():
        cfg = BUILTIN_ROLE_CONFIG[role_key]
        fields.append(
            {
                "role_key": role_key,
                "label": cfg["label"],
                "setting_key": cfg["setting_key"],
                "value": settings.get(cfg["setting_key"], "#64748b"),
            }
        )
    return fields


def role_management_entries():
    entries = []
    for role_key in active_builtin_roles():
        entries.append(
            {
                "role_key": role_key,
                "label": BUILTIN_ROLE_CONFIG[role_key]["label"],
                "is_builtin": True,
            }
        )
    for role in active_custom_roles():
        entries.append(
            {
                "role_key": role["role_key"],
                "label": role["label"],
                "is_builtin": False,
            }
        )
    return entries


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
                    AND COALESCE(u.is_dashboard_invisible, 0) = 0
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
                "badge_color": badge_color_value(row["role"], row["is_admin"]),
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


def contact_person_badge(task_row) -> dict | None:
    cp_user_id = task_row.get("contact_person_user_id")
    cp_name = task_row.get("contact_person_name") or task_row.get("contact_person")
    if cp_user_id is None and not cp_name:
        return None

    role = task_row.get("contact_person_role")
    is_admin = int(task_row.get("contact_person_is_admin") or 0)
    initials_raw = task_row.get("contact_person_initials")

    return {
        "id": int(cp_user_id) if cp_user_id is not None else None,
        "username": cp_name or "-",
        "initials": initials_raw or make_initials_from_username(cp_name or "USR"),
        "role_label": role_label(role, is_admin),
        "color_class": badge_color_class(role, is_admin),
        "badge_color": badge_color_value(role, is_admin),
    }


def sidebar_users():
    rows = query_all(
        """
        SELECT
            u.id,
            u.username,
            u.initials,
            u.role,
            u.is_admin,
            u.is_inactive,
                        u.member_type,
            COUNT(ta.task_id) AS assigned_task_count
        FROM users u
        LEFT JOIN task_assignees ta ON ta.user_id = u.id
        WHERE COALESCE(u.is_dashboard_invisible, 0) = 0
                GROUP BY u.id, u.username, u.initials, u.role, u.is_admin, u.is_inactive, u.member_type
        ORDER BY
          u.is_inactive ASC,
                    CASE u.member_type WHEN 'trainee' THEN 0 ELSE 1 END ASC,
                    u.is_admin DESC,
          CASE u.role
            WHEN ? THEN 0
            WHEN ? THEN 1
            WHEN ? THEN 2
            ELSE 3
          END,
          u.username ASC
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
            "assigned_task_count": int(row["assigned_task_count"] or 0),
            "is_inactive": bool(row["is_inactive"]),
            "member_type": row["member_type"] or MEMBER_TYPE_REGULAR,
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
            cp.id AS contact_person_user_id,
            cp.username AS contact_person_name,
            cp.initials AS contact_person_initials,
            cp.role AS contact_person_role,
            cp.is_admin AS contact_person_is_admin,
            COALESCE(GROUP_CONCAT(DISTINCT au.username), '') AS assignee_names
        FROM tasks t
        JOIN users c ON c.id = t.created_by
        LEFT JOIN users cp ON cp.id = t.contact_person_user_id
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


def ping_comment_map_for_user(user) -> dict[int, int]:
    user_id = int(user["id"])
    username = (user["username"] or "").strip()
    comment_to_task: dict[int, int] = {}

    structured_rows = query_all(
        """
        SELECT tc.id AS comment_id, tc.task_id
        FROM task_comment_mentions tcm
        JOIN task_comments tc ON tc.id = tcm.comment_id
        WHERE tcm.user_id = ?
        """,
        (user_id,),
    )
    for row in structured_rows:
        comment_to_task[int(row["comment_id"])] = int(row["task_id"])

    if not username:
        return comment_to_task

    mention_pattern = re.compile(
        rf"(?<![A-Za-z0-9_])@{re.escape(username)}(?![A-Za-z0-9_])",
        re.IGNORECASE,
    )
    legacy_rows = query_all(
        """
        SELECT id AS comment_id, task_id, content
        FROM task_comments
        WHERE content LIKE '%@%'
        """
    )
    for row in legacy_rows:
        comment_id = int(row["comment_id"])
        if comment_id in comment_to_task:
            continue
        content = row["content"] or ""
        if mention_pattern.search(content):
            comment_to_task[comment_id] = int(row["task_id"])
    return comment_to_task


def read_ping_comment_ids_for_user(user_id: int) -> set[int]:
    rows = query_all(
        "SELECT comment_id FROM user_ping_reads WHERE user_id = ?",
        (user_id,),
    )
    return {int(row["comment_id"]) for row in rows}


def ping_task_sets_for_user(user) -> tuple[set[int], set[int]]:
    comment_map = ping_comment_map_for_user(user)
    if not comment_map:
        return set(), set()

    read_ids = read_ping_comment_ids_for_user(int(user["id"]))
    all_task_ids = {task_id for task_id in comment_map.values()}
    unread_task_ids = {
        task_id
        for comment_id, task_id in comment_map.items()
        if comment_id not in read_ids
    }
    read_task_ids = all_task_ids - unread_task_ids
    return unread_task_ids, read_task_ids


def unread_ping_count_for_user(user) -> int:
    unread_task_ids, _ = ping_task_sets_for_user(user)
    return len(unread_task_ids)


def mark_ping_task_as_read(user, task_id: int) -> None:
    comment_map = ping_comment_map_for_user(user)
    target_comment_ids = [
        comment_id
        for comment_id, mapped_task_id in comment_map.items()
        if mapped_task_id == int(task_id)
    ]
    if not target_comment_ids:
        return

    read_at = now_iso()
    for comment_id in target_comment_ids:
        execute(
            """
            INSERT INTO user_ping_reads (user_id, comment_id, read_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, comment_id) DO UPDATE SET read_at = excluded.read_at
            """,
            (int(user["id"]), int(comment_id), read_at),
        )


def mark_ping_task_as_unread(user, task_id: int) -> int:
    comment_map = ping_comment_map_for_user(user)
    target_comment_ids = [
        comment_id
        for comment_id, mapped_task_id in comment_map.items()
        if mapped_task_id == int(task_id)
    ]
    if not target_comment_ids:
        return 0

    removed = 0
    for comment_id in target_comment_ids:
        cur = execute(
            "DELETE FROM user_ping_reads WHERE user_id = ? AND comment_id = ?",
            (int(user["id"]), int(comment_id)),
        )
        removed += int(cur.rowcount or 0)
    return removed


def mark_all_pings_as_read_for_user(user) -> int:
    comment_map = ping_comment_map_for_user(user)
    if not comment_map:
        return 0

    read_ids = read_ping_comment_ids_for_user(int(user["id"]))
    unread_comment_ids = [comment_id for comment_id in comment_map if comment_id not in read_ids]
    if not unread_comment_ids:
        return 0

    read_at = now_iso()
    for comment_id in unread_comment_ids:
        execute(
            """
            INSERT INTO user_ping_reads (user_id, comment_id, read_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, comment_id) DO UPDATE SET read_at = excluded.read_at
            """,
            (int(user["id"]), int(comment_id), read_at),
        )

    return len(unread_comment_ids)


def dashboard_tasks_for_filter(user, filter_mode: str, ping_tab: str = "unread"):
    if filter_mode == "mine":
        return fetch_tasks(only_assigned_to=user["id"])

    tasks = fetch_tasks()
    if filter_mode == "pings":
        unread_task_ids, read_task_ids = ping_task_sets_for_user(user)
        pinged_ids = unread_task_ids if ping_tab == "unread" else read_task_ids
        if not pinged_ids:
            return []
        return [task for task in tasks if int(task["id"]) in pinged_ids]
    return tasks


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
        "ticket_category_label": ticket_category_label,
        "ticket_category_options": ticket_category_options(),
        "sidebar_users": badges,
        "format_datetime": format_datetime_for_display,
        "format_system_datetime": format_system_datetime_for_display,
        "datetime_input_value": format_datetime_for_input,
        "due_date_input_value": format_due_date_for_input,
        "due_time_input_value": format_due_time_for_input,
        "is_due_today": is_due_today,
        "app_settings": settings,
        "tone_options": sorted(TONE_OPTIONS.keys()),
        "closed_task_count": closed_task_count,
        "custom_role_css_rules": custom_role_css_rules(),
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

        if len(username) > MAX_USERNAME_LENGTH:
            flash(f"Benutzername darf maximal {MAX_USERNAME_LENGTH} Zeichen lang sein.", "error")
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
    if filter_mode not in VALID_DASHBOARD_FILTERS:
        filter_mode = "all"

    ping_tab = request.args.get("ping_tab", "unread").strip().lower()
    if ping_tab not in VALID_PING_TABS:
        ping_tab = "unread"

    ping_unread_count = unread_ping_count_for_user(user)

    users = query_all(
        """
        SELECT id, username, initials, role, is_admin
        FROM users
        WHERE COALESCE(is_dashboard_invisible, 0) = 0
        ORDER BY is_admin DESC, username ASC
        """
    )
    tasks = dashboard_tasks_for_filter(user, filter_mode, ping_tab)
    tasks = enrich_tasks_with_assignees(tasks)
    tasks = [
        {
            **task,
            "contact_person_badge": contact_person_badge(task),
        }
        for task in tasks
    ]
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
        ping_tab=ping_tab,
        editable_task_ids=editable_task_ids,
        ping_unread_count=ping_unread_count,
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
                "created_at_display": format_system_datetime_for_display(task["created_at"]),
                "due_date_display": format_datetime_for_display(task["due_date"]),
                "assignees": task["assignees"],
            }
        )
    return jsonify({"tasks": payload})


@app.route("/api/dashboard/tasks")
@login_required
def dashboard_tasks_api():
    user = current_user()
    filter_mode = request.args.get("filter", "all").strip().lower()
    if filter_mode not in VALID_DASHBOARD_FILTERS:
        filter_mode = "all"

    ping_tab = request.args.get("ping_tab", "unread").strip().lower()
    if ping_tab not in VALID_PING_TABS:
        ping_tab = "unread"

    tasks = dashboard_tasks_for_filter(user, filter_mode, ping_tab)
    tasks = enrich_tasks_with_assignees(tasks)
    editable_task_ids = set() if user["is_admin"] else assigned_task_ids_for_user(user["id"])

    payload = []
    for task in tasks:
        assigned_to_me = any(int(assignee["id"]) == int(user["id"]) for assignee in task["assignees"])
        can_write_task = bool(
            user["is_admin"] or int(task["created_by"]) == int(user["id"]) or int(task["id"]) in editable_task_ids
        )
        can_assign_members = bool(user["is_admin"] or int(task["created_by"]) == int(user["id"]))
        task_read_only = bool(task["status"] == STATUS_CLOSED or not can_write_task)
        can_drag = bool(user["is_admin"] or int(task["id"]) in editable_task_ids)

        payload.append(
            {
                "id": int(task["id"]),
                "title": task["title"],
                "status": task["status"],
                "created_at": task["created_at"],
                "due_date_display": format_datetime_for_display(task["due_date"]),
                "priority": int(task.get("priority") or DEFAULT_TASK_PRIORITY),
                "ticket_category": task.get("ticket_category", ""),
                "ticket_category_label": ticket_category_label(task.get("ticket_category", "")),
                "room": task.get("room", "") or "",
                "contact_person": task.get("contact_person", ""),
                "contact_person_badge": contact_person_badge(task),
                "creator_name": task.get("creator_name", ""),
                "assignees": task["assignees"],
                "assigned_to_me": assigned_to_me,
                "due_today": is_due_today(task.get("due_date")),
                "task_read_only": task_read_only,
                "can_drag": can_drag,
                "can_assign": bool(task["status"] != STATUS_CLOSED and can_assign_members),
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
        show_sidebar=False,
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

        if action == "theme":
            theme_mode = request.form.get("theme_mode", THEME_LIGHT).strip().lower()
            card_view_mode = request.form.get("card_view_mode", CARD_VIEW_COMPACT).strip().lower()
            if theme_mode not in VALID_THEME_MODES:
                flash("Ungültiger Modus ausgewählt.", "error")
                return redirect(url_for("settings_page"))
            if card_view_mode not in VALID_CARD_VIEW_MODES:
                flash("Ungültige Kartenansicht ausgewählt.", "error")
                return redirect(url_for("settings_page"))

            execute(
                "UPDATE users SET theme_mode = ?, card_view_mode = ? WHERE id = ?",
                (theme_mode, card_view_mode, user["id"]),
            )
            flash("Darstellung wurde gespeichert.", "success")
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
            new_task_tone = request.form.get("new_task_tone", "classic").strip()
            builtin_color_updates = []
            for role_key in active_builtin_roles():
                setting_key = BUILTIN_ROLE_CONFIG[role_key]["setting_key"]
                color_value = request.form.get(setting_key, current_settings.get(setting_key, "")).strip()
                builtin_color_updates.append((setting_key, color_value))
            custom_color_updates = []
            for role in active_custom_roles():
                field_name = f"custom_role_color_{role['role_key']}"
                color_value = request.form.get(field_name, role["color"]).strip()
                custom_color_updates.append((role["role_key"], color_value))

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

            colors = [role_color_admin] + [color for _, color in builtin_color_updates]
            if not all(is_hex_color(color) for color in colors):
                flash("Farben müssen im Format #RRGGBB angegeben werden.", "error")
                return redirect(url_for("settings_page"))

            if not all(is_hex_color(color) for _, color in custom_color_updates):
                flash("Mindestens eine benutzerdefinierte Rollenfarbe ist ungültig.", "error")
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
            for setting_key, color_value in builtin_color_updates:
                set_app_setting(setting_key, color_value)
            set_app_setting("new_task_tone", new_task_tone)

            for role_key, color_value in custom_color_updates:
                execute(
                    "UPDATE custom_roles SET color = ? WHERE role_key = ?",
                    (color_value, role_key),
                )

            flash("Admin-Einstellungen wurden gespeichert.", "success")
            return redirect(url_for("settings_page"))

        flash("Unbekannte Aktion.", "error")
        return redirect(url_for("settings_page"))

    return render_template(
        "settings.html",
        user=user,
        settings=app_settings(),
        custom_roles=active_custom_roles(),
        builtin_role_color_fields=builtin_role_color_fields(app_settings()),
        tone_options=sorted(TONE_OPTIONS.keys()),
        show_sidebar=False,
    )


@app.route("/tasks/create", methods=["POST"])
@login_required
def create_task():
    user = current_user()

    title = request.form.get("title", "").strip()
    description = request.form.get("description", "").strip()
    contact_person_user_id_raw = request.form.get("contact_person_user_id", "").strip()
    ticket_category = normalize_ticket_category(request.form.get("ticket_category", ""))
    room = request.form.get("room", "").strip()
    due_date = request.form.get("due_date", "").strip()
    due_time = request.form.get("due_time", "").strip()
    priority_raw = request.form.get("priority", str(DEFAULT_TASK_PRIORITY)).strip()
    assignee_ids_raw = request.form.getlist("assignee_ids")

    if not title or not description or not due_date:
        flash("Bitte alle Pflichtfelder ausfüllen.", "error")
        return redirect(url_for("dashboard"))

    if not ticket_category:
        flash("Bitte eine gültige Ticket-Kategorie auswählen.", "error")
        return redirect(url_for("dashboard"))

    try:
        priority = int(priority_raw)
    except ValueError:
        flash("Bitte eine gültige Priorität (1-5) auswählen.", "error")
        return redirect(url_for("dashboard"))

    if priority < MIN_TASK_PRIORITY or priority > MAX_TASK_PRIORITY:
        flash("Bitte eine gültige Priorität (1-5) auswählen.", "error")
        return redirect(url_for("dashboard"))

    try:
        contact_person_user_id = int(contact_person_user_id_raw)
    except ValueError:
        flash("Bitte einen gültigen Ansprechpartner auswählen.", "error")
        return redirect(url_for("dashboard"))

    contact_user = query_one(
        "SELECT id, username FROM users WHERE id = ? AND COALESCE(is_dashboard_invisible, 0) = 0",
        (contact_person_user_id,),
    )
    if contact_user is None:
        flash("Ansprechpartner nicht gefunden.", "error")
        return redirect(url_for("dashboard"))

    normalized_due_date = normalize_due_date_value(due_date, due_time)
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
    if assignee_ids:
        placeholders = ",".join(["?"] * len(assignee_ids))
        found = query_all(
            f"SELECT id FROM users WHERE id IN ({placeholders}) AND COALESCE(is_dashboard_invisible, 0) = 0",
            tuple(assignee_ids),
        )
        if len(found) != len(assignee_ids):
            flash("Mindestens ein ausgewählter Bearbeiter existiert nicht.", "error")
            return redirect(url_for("dashboard"))

    now = now_iso()
    cur = execute(
        """
        INSERT INTO tasks (
            title,
            description,
            priority,
            assignee_id,
            due_date,
            contact_person,
            contact_person_user_id,
            ticket_category,
            room,
            created_by,
            status,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            title,
            description,
            priority,
            assignee_ids[0] if assignee_ids else None,
            normalized_due_date,
            contact_user["username"],
            contact_person_user_id,
            ticket_category,
            room,
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


def comment_mentions_map(comment_ids: list[int]):
    if not comment_ids:
        return {}

    placeholders = ",".join(["?"] * len(comment_ids))
    rows = query_all(
        f"""
        SELECT
            tcm.comment_id,
            u.id,
            u.username,
            u.initials,
            u.role,
            u.is_admin
        FROM task_comment_mentions tcm
        JOIN users u ON u.id = tcm.user_id
        WHERE tcm.comment_id IN ({placeholders})
        ORDER BY u.is_admin DESC, u.username ASC
        """,
        tuple(comment_ids),
    )

    mapping = {comment_id: [] for comment_id in comment_ids}
    for row in rows:
        mapping[row["comment_id"]].append(
            {
                "id": int(row["id"]),
                "username": row["username"],
                "initials": row["initials"] or make_initials_from_username(row["username"]),
                "role_label": role_label(row["role"], row["is_admin"]),
                "color_class": badge_color_class(row["role"], row["is_admin"]),
            }
        )
    return mapping


def task_with_details(task_id: int):
    task = query_one(
        """
        SELECT
            t.*,
            c.username AS creator_name,
            cp.id AS contact_person_user_id,
            cp.username AS contact_person_name,
            cp.initials AS contact_person_initials,
            cp.role AS contact_person_role,
            cp.is_admin AS contact_person_is_admin
        FROM tasks t
        JOIN users c ON c.id = t.created_by
        LEFT JOIN users cp ON cp.id = t.contact_person_user_id
        WHERE t.id = ?
        """,
        (task_id,),
    )
    if task is None:
        return None

    task_dict = dict(task)
    contact_name = (task_dict.get("contact_person_name") or task_dict.get("contact_person") or "").strip()
    if contact_name:
        contact_initials = task_dict.get("contact_person_initials") or make_initials_from_username(contact_name)
        task_dict["contact_person_display"] = f"{contact_initials} - {contact_name}"
    else:
        task_dict["contact_person_display"] = "-"

    assignees = task_assignees_map([task_id]).get(task_id, [])
    raw_comments = task_comments(task_id)
    mentions_by_comment_id = comment_mentions_map([int(comment["id"]) for comment in raw_comments])
    comments = []
    for comment in raw_comments:
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
                "mentions": mentions_by_comment_id.get(int(comment["id"]), []),
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
    if return_filter not in VALID_DASHBOARD_FILTERS:
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


@app.route("/tasks/<int:task_id>/assignees/add", methods=["POST"])
@login_required
def add_task_assignee(task_id: int):
    user = current_user()
    return_filter = request.form.get("return_filter", "all").strip().lower()
    if return_filter not in VALID_DASHBOARD_FILTERS:
        return_filter = "all"

    task = query_one("SELECT id, status FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    if not can_edit_task_content(user, task_id):
        flash("Keine Berechtigung, Bearbeiter zu dieser Task hinzuzufügen.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    raw_user_id = request.form.get("user_id", "").strip()
    try:
        assignee_user_id = int(raw_user_id)
    except ValueError:
        flash("Ungültiger Benutzer.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    assignee = query_one(
        "SELECT id FROM users WHERE id = ? AND COALESCE(is_dashboard_invisible, 0) = 0",
        (assignee_user_id,),
    )
    if assignee is None:
        flash("Benutzer nicht gefunden.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    existing = query_one(
        "SELECT 1 FROM task_assignees WHERE task_id = ? AND user_id = ?",
        (task_id, assignee_user_id),
    )
    if existing is not None:
        flash("Benutzer ist bereits als Bearbeiter zugewiesen.", "success")
        return redirect(url_for("dashboard", filter=return_filter))

    execute(
        "INSERT INTO task_assignees (task_id, user_id) VALUES (?, ?)",
        (task_id, assignee_user_id),
    )
    sync_task_primary_assignee(task_id)

    flash("Bearbeiter wurde zur Task hinzugefügt.", "success")
    return redirect(url_for("dashboard", filter=return_filter))


@app.route("/pings/mark-all-read", methods=["POST"])
@login_required
def mark_all_pings_read():
    user = current_user()
    marked_count = mark_all_pings_as_read_for_user(user)
    if marked_count > 0:
        flash("Alle ungelesenen Pings wurden als gelesen markiert.", "success")
    else:
        flash("Es gibt keine ungelesenen Pings.", "success")
    return redirect(url_for("dashboard", filter="pings", ping_tab="unread"))


@app.route("/tasks/<int:task_id>/pings/mark-unread", methods=["POST"])
@login_required
def mark_task_ping_unread(task_id: int):
    user = current_user()
    ping_tab = request.form.get("ping_tab", "read").strip().lower()
    if ping_tab not in VALID_PING_TABS:
        ping_tab = "read"

    task = query_one("SELECT id FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard", filter="pings", ping_tab=ping_tab))

    removed_count = mark_ping_task_as_unread(user, task_id)
    if removed_count > 0:
        flash("Ping wurde auf ungelesen gesetzt.", "success")
    else:
        flash("Für diese Task gibt es keinen gelesenen Ping zum Zurücksetzen.", "error")
    return redirect(url_for("dashboard", filter="pings", ping_tab=ping_tab))


@app.route("/tasks/<int:task_id>/pings/mark-read", methods=["POST"])
@login_required
def mark_task_ping_read(task_id: int):
    user = current_user()
    ping_tab = request.form.get("ping_tab", "unread").strip().lower()
    if ping_tab not in VALID_PING_TABS:
        ping_tab = "unread"

    task = query_one("SELECT id FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard", filter="pings", ping_tab=ping_tab))

    unread_before, _ = ping_task_sets_for_user(user)
    if int(task_id) not in unread_before:
        flash("Für diese Task gibt es keinen ungelesenen Ping.", "error")
        return redirect(url_for("dashboard", filter="pings", ping_tab=ping_tab))

    mark_ping_task_as_read(user, task_id)
    flash("Ping wurde auf gelesen gesetzt.", "success")
    return redirect(url_for("dashboard", filter="pings", ping_tab=ping_tab))


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
        WHERE COALESCE(is_dashboard_invisible, 0) = 0
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
        show_sidebar=False,
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

    raw_mention_ids = request.form.get("mention_user_ids", "").strip()
    mention_user_ids = []
    if raw_mention_ids:
        seen_ids = set()
        for raw_id in raw_mention_ids.split(","):
            cleaned = raw_id.strip()
            if not cleaned:
                continue
            try:
                mention_id = int(cleaned)
            except ValueError:
                flash("Ungültige Markierungsauswahl.", "error")
                return redirect(url_for("task_detail", task_id=task_id))
            if mention_id not in seen_ids:
                seen_ids.add(mention_id)
                mention_user_ids.append(mention_id)

    if mention_user_ids:
        placeholders = ",".join(["?"] * len(mention_user_ids))
        found_rows = query_all(
            f"SELECT id FROM users WHERE id IN ({placeholders})",
            tuple(mention_user_ids),
        )
        if len(found_rows) != len(mention_user_ids):
            flash("Mindestens eine Markierung ist ungültig.", "error")
            return redirect(url_for("task_detail", task_id=task_id))

    created_at = now_iso()
    comment_insert = execute(
        """
        INSERT INTO task_comments (task_id, user_id, content, created_at, updated_at)
        VALUES (?, ?, ?, ?, NULL)
        """,
        (task_id, user["id"], content, created_at),
    )

    comment_id = int(comment_insert.lastrowid)
    for mention_user_id in mention_user_ids:
        execute(
            """
            INSERT OR IGNORE INTO task_comment_mentions (comment_id, user_id, created_at)
            VALUES (?, ?, ?)
            """,
            (comment_id, mention_user_id, created_at),
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
    due_time = request.form.get("due_time", "").strip()
    priority_raw = request.form.get("priority", str(DEFAULT_TASK_PRIORITY)).strip()
    ticket_category = normalize_ticket_category(request.form.get("ticket_category", ""))
    room = request.form.get("room", "").strip()
    contact_person_user_id_raw = request.form.get("contact_person_user_id", "").strip()
    assignee_ids_raw = request.form.getlist("assignee_ids")

    if not title or not description or not due_date:
        flash("Titel, Beschreibung und Fälligkeitsdatum sind erforderlich.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    if not ticket_category:
        flash("Bitte eine gültige Ticket-Kategorie auswählen.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    try:
        priority = int(priority_raw)
    except ValueError:
        flash("Bitte eine gültige Priorität (1-5) auswählen.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    if priority < MIN_TASK_PRIORITY or priority > MAX_TASK_PRIORITY:
        flash("Bitte eine gültige Priorität (1-5) auswählen.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    try:
        contact_person_user_id = int(contact_person_user_id_raw)
    except ValueError:
        flash("Bitte einen gültigen Ansprechpartner auswählen.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    contact_user = query_one(
        "SELECT id, username FROM users WHERE id = ? AND COALESCE(is_dashboard_invisible, 0) = 0",
        (contact_person_user_id,),
    )
    if contact_user is None:
        flash("Ansprechpartner nicht gefunden.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    normalized_due_date = normalize_due_date_value(due_date, due_time)
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
    if assignee_ids:
        placeholders = ",".join(["?"] * len(assignee_ids))
        found = query_all(
            f"SELECT id FROM users WHERE id IN ({placeholders}) AND COALESCE(is_dashboard_invisible, 0) = 0",
            tuple(assignee_ids),
        )
        if len(found) != len(assignee_ids):
            flash("Mindestens ein ausgewählter Bearbeiter existiert nicht.", "error")
            return redirect(url_for("task_detail", task_id=task_id))

    execute(
        """
        UPDATE tasks
        SET title = ?, description = ?, due_date = ?, priority = ?, assignee_id = ?, contact_person = ?, contact_person_user_id = ?, ticket_category = ?, room = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            title,
            description,
            normalized_due_date,
            priority,
            assignee_ids[0] if assignee_ids else None,
            contact_user["username"],
            contact_person_user_id,
            ticket_category,
            room,
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
            member_type = request.form.get("member_type", MEMBER_TYPE_REGULAR)
            if member_type not in VALID_MEMBER_TYPES:
                member_type = MEMBER_TYPE_REGULAR
            is_dashboard_invisible = 1 if request.form.get("is_dashboard_invisible") == "1" else 0

            if not username or not password:
                flash("Benutzername und Passwort sind erforderlich.", "error")
                return redirect(url_for("manage_users"))

            if len(username) > MAX_USERNAME_LENGTH:
                flash(f"Benutzername darf maximal {MAX_USERNAME_LENGTH} Zeichen lang sein.", "error")
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
                INSERT INTO users (username, password_hash, is_admin, initials, role, member_type, is_dashboard_invisible, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    username,
                    generate_password_hash(password),
                    is_admin,
                    initials,
                    role,
                    member_type,
                    is_dashboard_invisible,
                    now_iso(),
                ),
            )
            flash("Benutzer wurde angelegt.", "success")
            return redirect(url_for("manage_users"))

        if action == "create-role":
            role_label_input = request.form.get("role_label", "").strip()
            if not role_label_input:
                flash("Bitte einen Rollennamen angeben.", "error")
                return redirect(url_for("manage_users"))

            role_key = normalize_custom_role_key(role_label_input)
            if not role_key:
                flash("Rollenname ist ungültig.", "error")
                return redirect(url_for("manage_users"))

            if role_key in {"admin", ROLE_TEAM, ROLE_SYSTEM_INTEGRATOR, ROLE_APPLICATION_DEVELOPER}:
                flash("Diese Rolle ist bereits reserviert.", "error")
                return redirect(url_for("manage_users"))

            existing_key = query_one("SELECT role_key FROM custom_roles WHERE role_key = ?", (role_key,))
            existing_label = query_one(
                "SELECT role_key FROM custom_roles WHERE lower(label) = lower(?)",
                (role_label_input,),
            )
            if existing_key is not None or existing_label is not None:
                flash("Diese Rolle existiert bereits.", "error")
                return redirect(url_for("manage_users"))

            execute(
                """
                INSERT INTO custom_roles (role_key, label, color, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (role_key, role_label_input, "#64748b", now_iso()),
            )
            flash("Rolle wurde erstellt. Farbe kann in Einstellungen angepasst werden.", "success")
            return redirect(url_for("manage_users"))

        if action == "delete-role":
            role_key = request.form.get("role_key", "").strip().lower()
            if not role_key:
                flash("Ungültige Rolle.", "error")
                return redirect(url_for("manage_users"))

            is_builtin = role_key in BUILTIN_ROLE_CONFIG
            if not is_builtin:
                existing = query_one("SELECT role_key, label FROM custom_roles WHERE role_key = ?", (role_key,))
                if existing is None:
                    flash("Rolle nicht gefunden.", "error")
                    return redirect(url_for("manage_users"))

            usage = query_one("SELECT COUNT(*) AS cnt FROM users WHERE role = ?", (role_key,))
            if usage is not None and int(usage["cnt"]) > 0:
                flash("Rolle kann nicht gelöscht werden, solange Benutzer dieser Rolle zugewiesen sind.", "error")
                return redirect(url_for("manage_users"))

            if is_builtin:
                execute(
                    "INSERT OR IGNORE INTO disabled_roles (role_key, created_at) VALUES (?, ?)",
                    (role_key, now_iso()),
                )
            else:
                execute("DELETE FROM custom_roles WHERE role_key = ?", (role_key,))
            flash("Rolle wurde entfernt.", "success")
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

            if len(username) > MAX_USERNAME_LENGTH:
                flash(f"Benutzername darf maximal {MAX_USERNAME_LENGTH} Zeichen lang sein.", "error")
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
            is_inactive = 1 if request.form.get("is_inactive") == "1" else 0
            member_type = request.form.get("member_type", MEMBER_TYPE_REGULAR)
            if member_type not in VALID_MEMBER_TYPES:
                member_type = MEMBER_TYPE_REGULAR
            is_dashboard_invisible = 1 if request.form.get("is_dashboard_invisible") == "1" else 0
            if target["is_admin"] and not is_admin:
                row = query_one("SELECT COUNT(*) AS cnt FROM users WHERE is_admin = 1")
                if int(row["cnt"]) <= 1:
                    flash("Der letzte Admin kann nicht zur Nicht-Admin-Rolle geändert werden.", "error")
                    return redirect(url_for("manage_users"))

            if new_password:
                execute(
                    """
                    UPDATE users
                    SET username = ?, initials = ?, role = ?, is_admin = ?, is_inactive = ?, member_type = ?, is_dashboard_invisible = ?, password_hash = ?
                    WHERE id = ?
                    """,
                    (
                        username,
                        initials,
                        role,
                        is_admin,
                        is_inactive,
                        member_type,
                        is_dashboard_invisible,
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
                    SET username = ?, initials = ?, role = ?, is_admin = ?, is_inactive = ?, member_type = ?, is_dashboard_invisible = ?
                    WHERE id = ?
                    """,
                    (username, initials, role, is_admin, is_inactive, member_type, is_dashboard_invisible, target_id),
                )
                if target["id"] == current["id"]:
                    flash("Eigener Account wurde aktualisiert.", "success")
                else:
                    flash("Benutzerprofil wurde aktualisiert.", "success")

            return redirect(url_for("manage_users"))

        if action == "toggle-inactive":
            target_id = request.form.get("user_id", "").strip()
            target = query_one("SELECT id, is_inactive FROM users WHERE id = ?", (target_id,))
            if target is None:
                flash("Benutzer nicht gefunden.", "error")
                return redirect(url_for("manage_users"))
            new_val = 0 if target["is_inactive"] else 1
            execute("UPDATE users SET is_inactive = ? WHERE id = ?", (new_val, target_id))
            if new_val:
                flash("Benutzer wurde als inaktiv markiert.", "success")
            else:
                flash("Benutzer wurde wieder als aktiv markiert.", "success")
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

            # tasks.created_by uses ON DELETE RESTRICT, so hand over ownership first.
            execute(
                "UPDATE tasks SET created_by = ? WHERE created_by = ?",
                (current["id"], target_id),
            )

            execute("DELETE FROM task_assignees WHERE user_id = ?", (target_id,))
            execute("UPDATE tasks SET assignee_id = NULL WHERE assignee_id = ?", (target_id,))
            execute("UPDATE tasks SET closed_by = NULL WHERE closed_by = ?", (target_id,))
            execute("DELETE FROM task_comments WHERE user_id = ?", (target_id,))
            execute("DELETE FROM calendar_events WHERE user_id = ?", (target_id,))
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
            is_inactive,
            is_dashboard_invisible,
            member_type,
            created_at
        FROM users
        ORDER BY
            is_inactive ASC,
            CASE member_type WHEN 'trainee' THEN 0 ELSE 1 END ASC,
            is_admin DESC,
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

    enriched_users = []
    for row in users:
        item = dict(row)
        item["role_label"] = role_label(row["role"], row["is_admin"])
        item["color_class"] = badge_color_class(row["role"], row["is_admin"])
        item["member_type"] = row["member_type"] or MEMBER_TYPE_REGULAR
        item["is_dashboard_invisible"] = bool(row["is_dashboard_invisible"])
        enriched_users.append(item)

    return render_template(
        "admin_users.html",
        users=enriched_users,
        user=current_user(),
        role_options=role_options(),
        custom_roles=active_custom_roles(),
        role_management_entries=role_management_entries(),
        show_sidebar=False,
    )


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

    return render_template(
        "admin_closed.html",
        tasks=closed_tasks,
        user=current_user(),
        show_sidebar=False,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

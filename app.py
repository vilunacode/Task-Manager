import os
import sys
import calendar as pycalendar
import configparser
import re
import sqlite3
from urllib.parse import quote_plus
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

if getattr(sys, "frozen", False):
    BASE_DIR = os.path.abspath(os.path.dirname(sys.executable))
    _BUNDLE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    _BUNDLE_DIR = BASE_DIR
DEFAULT_CONFIG_FILENAME = "config.ini"
DEFAULT_DATABASE_FILENAME = "task_manager.db"
SUPPORTED_DATABASE_DRIVERS = {"sqlite", "postgres", "postgresql", "mysql", "mariadb"}


def parse_bool(value: str, fallback: bool) -> bool:
    normalized = (value or "").strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return fallback


def parse_int(value: str, fallback: int, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        parsed = int((value or "").strip())
    except (TypeError, ValueError):
        return fallback

    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def resolve_sqlite_path(base_dir: str, database_url: str, database_path: str, database_name: str) -> str:
    if database_url:
        if not database_url.startswith("sqlite:///"):
            raise ValueError("Only sqlite:/// URLs are supported with sqlite driver.")
        sqlite_path = database_url[len("sqlite:///") :].strip()
        if not sqlite_path:
            raise ValueError("Database URL must include a path after sqlite:///.")
        return sqlite_path if os.path.isabs(sqlite_path) else os.path.join(base_dir, sqlite_path)

    cleaned_name = (database_name or "").strip()
    fallback_path = f"{cleaned_name}.db" if cleaned_name else DEFAULT_DATABASE_FILENAME
    cleaned_path = (database_path or "").strip() or fallback_path
    return cleaned_path if os.path.isabs(cleaned_path) else os.path.join(base_dir, cleaned_path)


def build_external_database_url(driver: str, host: str, port: int, name: str, username: str, password: str) -> str:
    scheme = "postgresql" if driver in {"postgres", "postgresql"} else "mysql"
    host_part = (host or "127.0.0.1").strip()
    name_part = (name or "task_manager").strip()
    user_part = quote_plus((username or "").strip())
    pass_part = quote_plus((password or "").strip())

    auth = ""
    if user_part and pass_part:
        auth = f"{user_part}:{pass_part}@"
    elif user_part:
        auth = f"{user_part}@"

    return f"{scheme}://{auth}{host_part}:{port}/{name_part}"


def load_database_config(parser: configparser.ConfigParser, base_dir: str) -> dict:
    driver = (
        os.environ.get("TASK_MANAGER_DB_DRIVER", "").strip().lower()
        or parser.get("database", "driver", fallback="sqlite").strip().lower()
    )
    if driver not in SUPPORTED_DATABASE_DRIVERS:
        raise ValueError(f"Unsupported database driver '{driver}'.")

    host = os.environ.get("TASK_MANAGER_DB_HOST", "").strip() or parser.get(
        "database", "host", fallback="127.0.0.1"
    )
    default_port = 5432 if driver in {"postgres", "postgresql"} else 3306
    port = parse_int(
        os.environ.get("TASK_MANAGER_DB_PORT", "").strip()
        or parser.get("database", "port", fallback=str(default_port)),
        fallback=default_port,
        minimum=1,
        maximum=65535,
    )
    name = os.environ.get("TASK_MANAGER_DB_NAME", "").strip() or parser.get(
        "database", "name", fallback="task_manager"
    )
    username = os.environ.get("TASK_MANAGER_DB_USER", "").strip() or parser.get(
        "database", "username", fallback=""
    )
    password = os.environ.get("TASK_MANAGER_DB_PASSWORD", "").strip() or parser.get(
        "database", "password", fallback=""
    )

    database_url = os.environ.get("DATABASE_URL", "").strip() or parser.get("database", "url", fallback="")
    database_path = os.environ.get("DATABASE_PATH", "").strip() or parser.get(
        "database", "path", fallback=DEFAULT_DATABASE_FILENAME
    )

    if driver == "sqlite":
        resolved_database_path = resolve_sqlite_path(base_dir, database_url, database_path, name)
        return {
            "backend": "sqlite",
            "path": resolved_database_path,
            "url": f"sqlite:///{resolved_database_path}",
            "driver": driver,
            "host": host,
            "port": port,
            "name": name,
            "username": username,
            "password": password,
        }

    resolved_url = database_url or build_external_database_url(driver, host, port, name, username, password)
    return {
        "backend": "external",
        "path": "",
        "url": resolved_url,
        "driver": driver,
        "host": host,
        "port": port,
        "name": name,
        "username": username,
        "password": password,
    }


_DEFAULT_CONFIG_CONTENT = """\
; Task Manager configuration
; Values from environment variables can still override these settings.

[server]
; 0.0.0.0 = reachable from network, 127.0.0.1 = local only.
host = 0.0.0.0

; Port of the web app.
port = 5000

; true/false - set false in production.
debug = false

[app]
; Change this to a long random string in production.
secret_key = dev-secret-change-me

; Time zone used in date/time display.
timezone = Europe/Berlin

[database]
; Driver: sqlite, postgres, postgresql, mysql, mariadb
driver = sqlite

; Optional sqlite file path (only relevant when driver=sqlite)
path = task_manager.db
"""


def _ensure_default_config(config_file_path: str) -> None:
    if not os.path.exists(config_file_path):
        try:
            with open(config_file_path, "w", encoding="utf-8") as f:
                f.write(_DEFAULT_CONFIG_CONTENT)
        except OSError:
            pass


def load_runtime_config(base_dir: str) -> dict:
    config_file_path = os.environ.get("TASK_MANAGER_CONFIG", "").strip() or os.path.join(
        base_dir, DEFAULT_CONFIG_FILENAME
    )

    _ensure_default_config(config_file_path)

    parser = configparser.ConfigParser()
    if os.path.exists(config_file_path):
        parser.read(config_file_path, encoding="utf-8")

    host = os.environ.get("TASK_MANAGER_HOST", "").strip() or parser.get(
        "server", "host", fallback="0.0.0.0"
    )
    port = parse_int(
        os.environ.get("TASK_MANAGER_PORT", "").strip()
        or parser.get("server", "port", fallback="5000"),
        fallback=5000,
        minimum=1,
        maximum=65535,
    )
    debug = parse_bool(
        os.environ.get("TASK_MANAGER_DEBUG", "").strip()
        or parser.get("server", "debug", fallback="false"),
        fallback=False,
    )

    secret_key = os.environ.get("SECRET_KEY", "").strip() or parser.get(
        "app", "secret_key", fallback="dev-secret-change-me"
    )
    timezone_name = os.environ.get("APP_TIMEZONE", "").strip() or parser.get(
        "app", "timezone", fallback="Europe/Berlin"
    )

    database_config = load_database_config(parser, base_dir)

    return {
        "host": host,
        "port": port,
        "debug": debug,
        "secret_key": secret_key,
        "timezone_name": timezone_name,
        "database_backend": database_config["backend"],
        "database_driver": database_config["driver"],
        "database_path": database_config["path"],
        "database_url": database_config["url"],
        "config_file_path": config_file_path,
    }


RUNTIME_CONFIG = load_runtime_config(BASE_DIR)
DATABASE = RUNTIME_CONFIG["database_path"]

STATUS_OPEN = "open"
STATUS_IN_PROGRESS = "in_progress"
STATUS_CLOSED = "closed"
VALID_STATUSES = {STATUS_OPEN, STATUS_IN_PROGRESS, STATUS_CLOSED}
VALID_DASHBOARD_FILTERS = {"all", "mine", "pings"}
VALID_PING_TABS = {"unread", "read"}
MIN_TASK_PRIORITY = 1
MAX_TASK_PRIORITY = 5
DEFAULT_TASK_PRIORITY = 3

DEFAULT_APP_SETTINGS = {
    "new_task_highlight_seconds": "120",
    "overview_refresh_interval_seconds": "1",
    "role_color_admin": "#fc5f5f",
    "role_label_admin": "Admin",
    "role_color_user": "#64748b",
    "role_label_user": "Benutzer",
    "new_task_tone": "classic",
    "calendar_disabled": "0",
    "favicon_filename": "",
    "site_name": "Task Manager",
}

FAVICON_ALLOWED_EXTENSIONS = {"ico", "png", "jpg", "jpeg", "svg"}
FAVICON_MAX_BYTES = 2 * 1024 * 1024  # 2 MB

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
MEMBER_TYPE_REGULAR = "regular"
MEMBER_TYPE_TRAINEE = "trainee"
VALID_MEMBER_TYPES = {MEMBER_TYPE_REGULAR, MEMBER_TYPE_TRAINEE}
MAX_USERNAME_LENGTH = 25
MAX_TASK_TITLE_LENGTH = 200
MAX_TASK_DESCRIPTION_LENGTH = 5000
MAX_TASK_ROOM_LENGTH = 100


app = Flask(
    __name__,
    template_folder=os.path.join(_BUNDLE_DIR, "templates"),
    static_folder=os.path.join(_BUNDLE_DIR, "static"),
)
app.config["SECRET_KEY"] = RUNTIME_CONFIG["secret_key"]
try:
    APP_TIMEZONE = ZoneInfo(RUNTIME_CONFIG["timezone_name"])
except Exception:  # pylint: disable=broad-except
    APP_TIMEZONE = ZoneInfo("Europe/Berlin")


def get_db() -> sqlite3.Connection:
    if RUNTIME_CONFIG["database_backend"] != "sqlite":
        raise RuntimeError(
            "Externe Datenbanken sind in dieser Version noch nicht direkt unterstützt. "
            "Bitte 'database.driver = sqlite' nutzen."
        )
    if "db" not in g:
        db_dir = os.path.dirname(os.path.abspath(DATABASE))
        os.makedirs(db_dir, exist_ok=True)
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


@app.before_request
def auto_reinit_db_if_missing():
    db = get_db()
    if db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
    ).fetchone() is None:
        init_db()


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
            is_admin INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ticket_categories (
            category_key TEXT PRIMARY KEY,
            label TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS task_contact_persons (
            task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            PRIMARY KEY (task_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS room_floors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS room_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            floor_id INTEGER NOT NULL REFERENCES room_floors(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(floor_id, name)
        );

        CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_id INTEGER,
            actor_name TEXT NOT NULL,
            event_type TEXT NOT NULL,
            description TEXT NOT NULL,
            task_id INTEGER,
            task_title TEXT,
            details TEXT,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_task_assignees_user ON task_assignees(user_id);
        CREATE INDEX IF NOT EXISTS idx_task_assignees_task ON task_assignees(task_id);
        CREATE INDEX IF NOT EXISTS idx_task_comments_task ON task_comments(task_id);
        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_updated_at ON tasks(updated_at);
        CREATE INDEX IF NOT EXISTS idx_user_ping_reads_user ON user_ping_reads(user_id);
        CREATE INDEX IF NOT EXISTS idx_calendar_events_user ON calendar_events(user_id);
        CREATE INDEX IF NOT EXISTS idx_activity_log_created ON activity_log(created_at DESC);
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
        db.execute("ALTER TABLE tasks ADD COLUMN ticket_category TEXT NOT NULL DEFAULT 'other'")
    if "room" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN room TEXT")
    if "priority" not in task_columns:
        db.execute(f"ALTER TABLE tasks ADD COLUMN priority INTEGER NOT NULL DEFAULT {DEFAULT_TASK_PRIORITY}")
    if "is_archived" not in task_columns:
        db.execute("ALTER TABLE tasks ADD COLUMN is_archived INTEGER NOT NULL DEFAULT 0")

    custom_role_columns = {row["name"] for row in db.execute("PRAGMA table_info(custom_roles)").fetchall()}
    if "is_admin" not in custom_role_columns:
        db.execute("ALTER TABLE custom_roles ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")

    comment_columns = {row["name"] for row in db.execute("PRAGMA table_info(task_comments)").fetchall()}
    if "updated_at" not in comment_columns:
        db.execute("ALTER TABLE task_comments ADD COLUMN updated_at TEXT")

    db.execute(
        """
        UPDATE tasks
        SET ticket_category = 'other'
        WHERE ticket_category IS NULL OR TRIM(ticket_category) = ''
        """
    )

    # Migrate old hardcoded ticket categories to the ticket_categories table.
    legacy_category_seeds = [
        ("hardware", "Hardware"),
        ("software", "Software"),
        ("network_internet", "Netzwerk / Internet"),
        ("security", "Sicherheit"),
        ("it_service_order", "IT-Service / Bestellung"),
        ("workstation_setup", "Arbeitsplatz / Setup"),
        ("other", "Sonstiges"),
    ]
    _migration_ts = now_iso()
    for _cat_key, _cat_label in legacy_category_seeds:
        used = db.execute(
            "SELECT 1 FROM tasks WHERE ticket_category = ? LIMIT 1", (_cat_key,)
        ).fetchone()
        if used is not None:
            db.execute(
                "INSERT OR IGNORE INTO ticket_categories (category_key, label, created_at) VALUES (?, ?, ?)",
                (_cat_key, _cat_label, _migration_ts),
            )

    # Migrate old builtin roles to custom_roles so existing user data is preserved.
    legacy_role_seeds = [
        ("team", "Team", "#0f766e"),
        ("system_integrator", "Systemintegrator", "#dc2626"),
        ("application_developer", "Anwendungsentwickler", "#2563eb"),
    ]
    for _role_key, _role_label, _role_color in legacy_role_seeds:
        used = db.execute(
            "SELECT 1 FROM users WHERE role = ? LIMIT 1", (_role_key,)
        ).fetchone()
        if used is not None:
            db.execute(
                "INSERT OR IGNORE INTO custom_roles (role_key, label, color, created_at) VALUES (?, ?, ?, ?)",
                (_role_key, _role_label, _role_color, _migration_ts),
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
        SET role = 'application_developer'
        WHERE (role IS NULL OR TRIM(role) = '') AND is_admin = 0
        """
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

    db.execute(
        """
        INSERT OR IGNORE INTO task_contact_persons (task_id, user_id)
        SELECT id, contact_person_user_id FROM tasks
        WHERE contact_person_user_id IS NOT NULL
        """
    )

    _room_seeds = [
        ("BE", ["BEG41", "BEG45", "BEG47", "BE01", "BE02", "BE03", "BE04", "BE05", "BE06"]),
        ("BO", [
            "BO02", "BO03", "BO07", "BO10", "BO11", "BO12", "BO13", "BO14", "BO15", "BO16",
            "BO17", "BO18", "BO19", "BO22", "BO23", "BO24", "BO25", "BO28", "BO30", "BO31",
            "BO32", "BO33", "BO35", "BO36", "BO37", "BO39", "BO40", "BO41", "BO42", "BO43",
            "BO45", "BO46", "BO47", "BO48", "BO50", "BO51", "BO52", "BO53", "BO54", "BO55",
            "BO56", "BO58", "BO60", "BO61", "BO62", "BO63", "BO64", "BO65", "BO66",
        ]),
        ("EE", ["EE03", "EE04", "EE07", "EE08", "EE09", "EE10", "EE11", "EE13", "EE15"]),
        ("EO", ["EO01", "EO02", "EO05", "Kantine"]),
        ("EU", [
            "EU01", "EU03", "EU04", "EU05", "EU06", "EU07", "EU08", "EU09", "EU10",
            "EU11", "EU13", "EU14", "EU18", "EU19", "EU20", "EU22",
        ]),
    ]
    _seed_ts = now_iso()
    for _floor_name, _rooms in _room_seeds:
        db.execute(
            "INSERT OR IGNORE INTO room_floors (name, created_at) VALUES (?, ?)",
            (_floor_name, _seed_ts),
        )
        _floor_row = db.execute(
            "SELECT id FROM room_floors WHERE name = ?", (_floor_name,)
        ).fetchone()
        for _room_name in _rooms:
            db.execute(
                "INSERT OR IGNORE INTO room_entries (floor_id, name, created_at) VALUES (?, ?, ?)",
                (_floor_row["id"], _room_name, _seed_ts),
            )

    db.commit()


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


def execute_many(query: str, params_list) -> None:
    db = get_db()
    db.executemany(query, params_list)
    db.commit()


def log_event(
    actor,
    event_type: str,
    description: str,
    task_id: int | None = None,
    task_title: str | None = None,
    details: str | None = None,
) -> None:
    actor_name = actor["username"] if actor else "System"
    actor_id = int(actor["id"]) if actor else None
    execute(
        """
        INSERT INTO activity_log (actor_id, actor_name, event_type, description, task_id, task_title, details, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (actor_id, actor_name, event_type, description, task_id, task_title, details, now_iso()),
    )


LOG_RETENTION_DAYS = 7


def cleanup_old_log_entries() -> None:
    cutoff = (datetime.now().astimezone().replace(microsecond=0) - timedelta(days=LOG_RETENTION_DAYS)).isoformat()
    execute("DELETE FROM activity_log WHERE created_at < ?", (cutoff,))


def app_settings() -> dict[str, str]:
    if "app_settings" not in g:
        stored = {
            row["key"]: row["value"]
            for row in query_all("SELECT key, value FROM app_settings")
        }
        merged = dict(DEFAULT_APP_SETTINGS)
        merged.update(stored)
        g.app_settings = merged
    return g.app_settings


def set_app_setting(key: str, value: str) -> None:
    execute(
        """
        INSERT INTO app_settings (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )
    g.pop("app_settings", None)


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
        SELECT role_key, label, color, is_admin, created_at
        FROM custom_roles
        ORDER BY label COLLATE NOCASE ASC
        """
    )


def custom_roles_map() -> dict[str, dict]:
    if "custom_roles_map" not in g:
        g.custom_roles_map = {row["role_key"]: dict(row) for row in custom_roles()}
    return g.custom_roles_map


def active_custom_roles() -> list[sqlite3.Row]:
    return list(custom_roles())


def normalize_custom_role_key(label: str) -> str:
    lowered = label.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
    return normalized[:40]


def normalize_category_key(label: str) -> str:
    lowered = label.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
    return normalized[:40]


def get_ticket_categories():
    if "ticket_categories" not in g:
        g.ticket_categories = query_all(
            "SELECT category_key, label, created_at FROM ticket_categories ORDER BY label COLLATE NOCASE ASC"
        )
    return g.ticket_categories


def role_options() -> list[dict]:
    settings = app_settings()
    admin_label = settings.get("role_label_admin", "Admin")
    user_label = settings.get("role_label_user", "Benutzer")
    options = [
        {"value": "admin", "label": admin_label, "is_admin": True},
        {"value": "user", "label": user_label, "is_admin": False},
    ]
    for role in active_custom_roles():
        options.append({
            "value": role["role_key"],
            "label": role["label"],
            "is_admin": bool(role["is_admin"]),
        })
    return options


def resolve_role_assignment(role_value: str) -> tuple[int, str]:
    """Returns (is_admin, role_key) for a given role dropdown value."""
    if role_value == "admin":
        return 1, ""
    if role_value == "user":
        return 0, "user"
    custom = custom_roles_map().get(role_value)
    if custom:
        return (1 if custom.get("is_admin") else 0), role_value
    return 0, ""


def get_all_rooms_by_floor() -> list[dict]:
    if "rooms_by_floor" not in g:
        floors = query_all("SELECT id, name FROM room_floors ORDER BY name ASC")
        result = []
        for floor in floors:
            rooms = query_all(
                "SELECT id, name FROM room_entries WHERE floor_id = ? ORDER BY name ASC",
                (floor["id"],),
            )
            result.append({
                "id": floor["id"],
                "name": floor["name"],
                "rooms": [{"id": r["id"], "name": r["name"]} for r in rooms],
            })
        g.rooms_by_floor = result
    return g.rooms_by_floor


def normalize_ticket_category(value: str) -> str:
    normalized = value.strip().lower()
    for cat in get_ticket_categories():
        if cat["category_key"] == normalized:
            return normalized
    return ""


def ticket_category_label(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    for cat in get_ticket_categories():
        if cat["category_key"] == normalized:
            return cat["label"]
    return normalized or "-"


def ticket_category_options() -> list[dict[str, str]]:
    return [{"value": cat["category_key"], "label": cat["label"]} for cat in get_ticket_categories()]


def role_label(role: str, is_admin: int) -> str:
    if role == "user":
        return app_settings().get("role_label_user", "Benutzer")
    custom = custom_roles_map().get(role)
    if custom is not None:
        return custom["label"]
    if is_admin:
        return app_settings().get("role_label_admin", "Admin")
    return role or "-"


def badge_color_class(role: str, is_admin: int) -> str:
    if role == "user":
        return "badge-user"
    if role in custom_roles_map():
        return f"badge-role-{role}"
    if is_admin:
        return "badge-admin"
    return "badge-role-unknown"


def badge_color_value(role: str, is_admin: int) -> str:
    if role == "user":
        return app_settings().get("role_color_user", "#64748b")
    custom = custom_roles_map().get(role)
    if custom is not None:
        return custom.get("color", "#64748b")
    if is_admin:
        return app_settings().get("role_color_admin", "#facc15")
    return "#64748b"


def custom_role_css_rules():
    return [
        {"class_name": f"badge-role-{role['role_key']}", "color": role["color"]}
        for role in active_custom_roles()
    ]


def role_management_entries():
    return [
        {
            "role_key": role["role_key"],
            "label": role["label"],
            "color": role["color"],
            "is_admin": bool(role["is_admin"]),
        }
        for role in active_custom_roles()
    ]


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


def contact_persons_map(task_ids: list[int]) -> dict[int, list[dict]]:
    if not task_ids:
        return {}
    placeholders = ",".join(["?"] * len(task_ids))
    rows = query_all(
        f"""
        SELECT tcp.task_id, u.id, u.username, u.initials, u.role, u.is_admin
        FROM task_contact_persons tcp
        JOIN users u ON u.id = tcp.user_id
        WHERE tcp.task_id IN ({placeholders})
        ORDER BY u.username ASC
        """,
        tuple(task_ids),
    )
    mapping: dict[int, list[dict]] = {tid: [] for tid in task_ids}
    for row in rows:
        mapping[int(row["task_id"])].append({
            "id": int(row["id"]),
            "username": row["username"],
            "initials": row["initials"] or make_initials_from_username(row["username"]),
            "role_label": role_label(row["role"], row["is_admin"]),
            "color_class": badge_color_class(row["role"], row["is_admin"]),
            "badge_color": badge_color_value(row["role"], row["is_admin"]),
        })
    return mapping


def enrich_tasks_with_contact_persons(tasks: list) -> list:
    if not tasks:
        return tasks
    task_ids = [int(t["id"]) for t in tasks]
    mapping = contact_persons_map(task_ids)
    result = []
    for task in tasks:
        d = dict(task)
        cps = mapping.get(int(task["id"]), [])
        d["contact_persons"] = cps
        parts = [cp["username"] for cp in cps]
        custom = (task.get("contact_person") or "").strip()
        if custom:
            parts.append(custom)
        d["contact_person_display"] = ", ".join(parts) if parts else "-"
        result.append(d)
    return result


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
            COUNT(t.id) AS assigned_task_count
        FROM users u
        LEFT JOIN task_assignees ta ON ta.user_id = u.id
        LEFT JOIN tasks t ON t.id = ta.task_id
            AND COALESCE(t.is_archived, 0) = 0
            AND t.status IN ('open', 'in_progress')
        WHERE COALESCE(u.is_dashboard_invisible, 0) = 0
        GROUP BY u.id, u.username, u.initials, u.role, u.is_admin, u.is_inactive, u.member_type
        ORDER BY
            u.is_inactive ASC,
            CASE u.member_type WHEN 'trainee' THEN 0 ELSE 1 END ASC,
            u.is_admin DESC,
            u.username ASC
        """
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


def fetch_tasks(*, status: str | None = None, only_assigned_to: int | None = None, archived_only: bool = False):
    where_parts = []
    params = []

    if archived_only:
        where_parts.append("COALESCE(t.is_archived, 0) = 1")
    else:
        where_parts.append("COALESCE(t.is_archived, 0) = 0")

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
        "month_label": (
            ["Januar","Februar","März","April","Mai","Juni",
             "Juli","August","September","Oktober","November","Dezember"]
            [month_start.month - 1]
        ) + f" {month_start.year}",
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

    placeholders = ",".join(["?"] * len(user_ids))
    raw_tasks = query_all(
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
        WHERE t.due_date IS NOT NULL AND TRIM(t.due_date) != ''
          AND COALESCE(t.is_archived, 0) = 0
          AND EXISTS (
            SELECT 1 FROM task_assignees ta2
            WHERE ta2.task_id = t.id AND ta2.user_id IN ({placeholders})
          )
        GROUP BY t.id
        ORDER BY t.due_date ASC
        """,
        tuple(user_ids),
    )
    tasks = enrich_tasks_with_assignees(raw_tasks)

    events = []
    for task in tasks:
        due_date = task.get("due_date")
        if not due_date:
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
        JOIN tasks t ON t.id = tc.task_id
        WHERE tcm.user_id = ? AND COALESCE(t.is_archived, 0) = 0
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
        SELECT tc.id AS comment_id, tc.task_id, tc.content
        FROM task_comments tc
        JOIN tasks t ON t.id = tc.task_id
        WHERE tc.content LIKE '%@%' AND COALESCE(t.is_archived, 0) = 0
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
    execute_many(
        """
        INSERT INTO user_ping_reads (user_id, comment_id, read_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, comment_id) DO UPDATE SET read_at = excluded.read_at
        """,
        [(int(user["id"]), int(comment_id), read_at) for comment_id in target_comment_ids],
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
    execute_many(
        """
        INSERT INTO user_ping_reads (user_id, comment_id, read_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, comment_id) DO UPDATE SET read_at = excluded.read_at
        """,
        [(int(user["id"]), int(comment_id), read_at) for comment_id in unread_comment_ids],
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
    row = query_one(
        "SELECT COUNT(*) AS cnt FROM tasks WHERE status = ? AND COALESCE(is_archived, 0) = 0",
        (STATUS_CLOSED,),
    )
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
        "room_floors_data": get_all_rooms_by_floor(),
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

        new_user = query_one("SELECT id FROM users WHERE username = ?", (username,))
        if new_user:
            session.clear()
            session["user_id"] = new_user["id"]

        return redirect(url_for("onboarding"))

    return render_template("setup.html")


@app.route("/onboarding", methods=["GET", "POST"])
def onboarding():
    user = current_user()
    if user is None:
        return redirect(url_for("login"))
    if not user["is_admin"]:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        action = request.form.get("action", "")
        active_tab = request.form.get("active_tab", "rollen")

        if action == "create-role":
            role_label_input = request.form.get("role_label", "").strip()
            role_color = request.form.get("role_color", "#64748b").strip()
            role_is_admin = 1 if request.form.get("role_is_admin") == "1" else 0

            if not role_label_input:
                flash("Bitte einen Rollennamen angeben.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            if not is_hex_color(role_color):
                flash("Farbe muss im Format #RRGGBB angegeben werden.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            role_key = normalize_custom_role_key(role_label_input)
            if not role_key or role_key in ("admin", "user"):
                flash("Rollenname ist ungültig.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            existing = query_one(
                "SELECT role_key FROM custom_roles WHERE role_key = ? OR lower(label) = lower(?)",
                (role_key, role_label_input),
            )
            if existing is not None:
                flash("Diese Rolle existiert bereits.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            execute(
                "INSERT INTO custom_roles (role_key, label, color, is_admin, created_at) VALUES (?, ?, ?, ?, ?)",
                (role_key, role_label_input, role_color, role_is_admin, now_iso()),
            )
            flash(f"Rolle \"{role_label_input}\" wurde erstellt.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        if action == "create-user":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "").strip()
            initials = normalize_initials(request.form.get("initials", ""))
            role_value = request.form.get("role", "").strip()
            is_admin, role = resolve_role_assignment(role_value)
            member_type = request.form.get("member_type", MEMBER_TYPE_REGULAR)
            if member_type not in VALID_MEMBER_TYPES:
                member_type = MEMBER_TYPE_REGULAR

            if not username or not password:
                flash("Benutzername und Passwort sind erforderlich.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            if len(username) > MAX_USERNAME_LENGTH:
                flash(f"Benutzername darf maximal {MAX_USERNAME_LENGTH} Zeichen lang sein.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            if not initials:
                flash("Kürzel muss genau 3 Zeichen (A-Z/0-9) lang sein.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            if not role_value or (not is_admin and not role):
                flash("Bitte eine gültige Rolle auswählen.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            if query_one("SELECT id FROM users WHERE username = ?", (username,)):
                flash("Benutzername ist bereits vergeben.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            if query_one("SELECT id FROM users WHERE initials = ?", (initials,)):
                flash("Dieses Kürzel ist bereits vergeben.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            execute(
                """
                INSERT INTO users (username, password_hash, is_admin, initials, role, member_type, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (username, generate_password_hash(password), is_admin, initials, role, member_type, now_iso()),
            )
            flash(f"Benutzer \"{username}\" wurde angelegt.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        if action == "create-category":
            category_label_input = request.form.get("category_label", "").strip()

            if not category_label_input:
                flash("Bitte einen Kategorienamen angeben.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            category_key = normalize_category_key(category_label_input)
            if not category_key:
                flash("Kategoriename ist ungültig.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            existing = query_one(
                "SELECT category_key FROM ticket_categories WHERE category_key = ? OR lower(label) = lower(?)",
                (category_key, category_label_input),
            )
            if existing is not None:
                flash("Diese Kategorie existiert bereits.", "error")
                return redirect(url_for("onboarding", tab=active_tab))

            execute(
                "INSERT INTO ticket_categories (category_key, label, created_at) VALUES (?, ?, ?)",
                (category_key, category_label_input, now_iso()),
            )
            g.pop("ticket_categories", None)
            flash(f"Kategorie \"{category_label_input}\" wurde erstellt.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        if action == "delete-role":
            role_key = request.form.get("role_key", "").strip().lower()
            if not role_key:
                flash("Ungültige Rolle.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            existing = query_one("SELECT role_key, label FROM custom_roles WHERE role_key = ?", (role_key,))
            if existing is None:
                flash("Rolle nicht gefunden.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            usage = query_one("SELECT COUNT(*) AS cnt FROM users WHERE role = ?", (role_key,))
            if usage and int(usage["cnt"]) > 0:
                flash("Rolle kann nicht gelöscht werden, solange Benutzer dieser Rolle zugewiesen sind.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            execute("DELETE FROM custom_roles WHERE role_key = ?", (role_key,))
            g.pop("custom_roles_map", None)
            flash(f"Rolle \"{existing['label']}\" wurde entfernt.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        if action == "edit-admin-color":
            role_color = request.form.get("role_color", "").strip()
            role_label = request.form.get("role_label", "").strip()
            if not is_hex_color(role_color):
                flash("Farbe muss im Format #RRGGBB angegeben werden.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if not role_label:
                flash("Bitte einen Namen für die Admin-Rolle angeben.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            set_app_setting("role_color_admin", role_color)
            set_app_setting("role_label_admin", role_label)
            flash("Admin-Rolle wurde aktualisiert.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        if action == "edit-user-role":
            role_color = request.form.get("role_color", "").strip()
            role_label_val = request.form.get("role_label", "").strip()
            if not is_hex_color(role_color):
                flash("Farbe muss im Format #RRGGBB angegeben werden.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if not role_label_val:
                flash("Bitte einen Namen für die Benutzer-Rolle angeben.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            set_app_setting("role_color_user", role_color)
            set_app_setting("role_label_user", role_label_val)
            flash("Benutzer-Rolle wurde aktualisiert.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        if action == "edit-role":
            role_key = request.form.get("role_key", "").strip().lower()
            role_label = request.form.get("role_label", "").strip()
            role_color = request.form.get("role_color", "").strip()
            role_is_admin = 1 if request.form.get("role_is_admin") == "1" else 0
            if not role_key or not role_label:
                flash("Bitte einen Rollennamen angeben.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if not is_hex_color(role_color):
                flash("Farbe muss im Format #RRGGBB angegeben werden.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            existing = query_one("SELECT role_key FROM custom_roles WHERE role_key = ?", (role_key,))
            if existing is None:
                flash("Rolle nicht gefunden.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            conflict = query_one(
                "SELECT role_key FROM custom_roles WHERE lower(label) = lower(?) AND role_key != ?",
                (role_label, role_key),
            )
            if conflict:
                flash("Eine Rolle mit diesem Namen existiert bereits.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            execute(
                "UPDATE custom_roles SET label = ?, color = ?, is_admin = ? WHERE role_key = ?",
                (role_label, role_color, role_is_admin, role_key),
            )
            execute("UPDATE users SET is_admin = ? WHERE role = ?", (role_is_admin, role_key))
            g.pop("custom_roles_map", None)
            flash("Rolle wurde aktualisiert.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        if action == "delete-user":
            target_id = request.form.get("user_id", "").strip()
            if not target_id:
                flash("Ungültiger Benutzer.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            target = query_one("SELECT id, username FROM users WHERE id = ?", (target_id,))
            if target is None:
                flash("Benutzer nicht gefunden.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if str(target["id"]) == str(user["id"]):
                flash("Du kannst deinen eigenen Account nicht löschen.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            execute("DELETE FROM users WHERE id = ?", (target_id,))
            flash(f"Benutzer \"{target['username']}\" wurde entfernt.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        if action == "edit-user":
            target_id = request.form.get("user_id", "").strip()
            new_username = request.form.get("username", "").strip()
            new_initials = normalize_initials(request.form.get("initials", ""))
            role_value = request.form.get("role", "").strip()
            new_is_admin, new_role = resolve_role_assignment(role_value)
            new_member_type = request.form.get("member_type", MEMBER_TYPE_REGULAR)
            if new_member_type not in VALID_MEMBER_TYPES:
                new_member_type = MEMBER_TYPE_REGULAR

            target = query_one("SELECT * FROM users WHERE id = ?", (target_id,))
            if target is None:
                flash("Benutzer nicht gefunden.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if not new_username:
                flash("Benutzername ist erforderlich.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if len(new_username) > MAX_USERNAME_LENGTH:
                flash(f"Benutzername darf maximal {MAX_USERNAME_LENGTH} Zeichen lang sein.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if not new_initials:
                flash("Kürzel muss genau 3 Zeichen (A-Z/0-9) lang sein.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if not role_value or (not new_is_admin and not new_role):
                flash("Bitte eine gültige Rolle auswählen.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if str(target_id) == str(user["id"]) and not new_is_admin:
                flash("Du kannst deinen eigenen Admin-Status nicht entfernen.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if query_one("SELECT id FROM users WHERE username = ? AND id != ?", (new_username, target_id)):
                flash("Benutzername ist bereits vergeben.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            if query_one("SELECT id FROM users WHERE initials = ? AND id != ?", (new_initials, target_id)):
                flash("Dieses Kürzel ist bereits vergeben.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            execute(
                "UPDATE users SET username = ?, initials = ?, is_admin = ?, role = ?, member_type = ? WHERE id = ?",
                (new_username, new_initials, new_is_admin, new_role, new_member_type, target_id),
            )
            flash("Benutzer wurde aktualisiert.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        if action == "delete-category":
            category_key = request.form.get("category_key", "").strip().lower()
            if not category_key:
                flash("Ungültige Kategorie.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            existing = query_one("SELECT category_key, label FROM ticket_categories WHERE category_key = ?", (category_key,))
            if existing is None:
                flash("Kategorie nicht gefunden.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            usage = query_one("SELECT COUNT(*) AS cnt FROM tasks WHERE ticket_category = ?", (category_key,))
            if usage and int(usage["cnt"]) > 0:
                flash("Kategorie kann nicht gelöscht werden, solange Tasks dieser Kategorie zugewiesen sind.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            execute("DELETE FROM ticket_categories WHERE category_key = ?", (category_key,))
            g.pop("ticket_categories", None)
            flash(f"Kategorie \"{existing['label']}\" wurde entfernt.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        if action == "edit-category":
            category_key = request.form.get("category_key", "").strip().lower()
            new_label = request.form.get("category_label", "").strip()
            if not category_key or not new_label:
                flash("Bitte einen Kategorienamen angeben.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            existing = query_one("SELECT category_key FROM ticket_categories WHERE category_key = ?", (category_key,))
            if existing is None:
                flash("Kategorie nicht gefunden.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            conflict = query_one(
                "SELECT category_key FROM ticket_categories WHERE lower(label) = lower(?) AND category_key != ?",
                (new_label, category_key),
            )
            if conflict:
                flash("Eine Kategorie mit diesem Namen existiert bereits.", "error")
                return redirect(url_for("onboarding", tab=active_tab))
            execute("UPDATE ticket_categories SET label = ? WHERE category_key = ?", (new_label, category_key))
            g.pop("ticket_categories", None)
            flash("Kategorie wurde aktualisiert.", "success")
            return redirect(url_for("onboarding", tab=active_tab))

        return redirect(url_for("onboarding"))

    active_tab = request.args.get("tab", "rollen")
    roles = role_management_entries()
    users = query_all(
        "SELECT id, username, initials, role, is_admin, member_type FROM users ORDER BY is_admin DESC, username ASC"
    )
    categories = get_ticket_categories()
    role_opts = role_options()
    settings = app_settings()
    admin_color = settings.get("role_color_admin", "#facc15")
    admin_label = settings.get("role_label_admin", "Admin")
    user_color = settings.get("role_color_user", "#64748b")
    user_label = settings.get("role_label_user", "Benutzer")

    return render_template(
        "onboarding.html",
        active_tab=active_tab,
        roles=roles,
        users=users,
        categories=categories,
        role_opts=role_opts,
        user=user,
        admin_color=admin_color,
        admin_label=admin_label,
        user_color=user_color,
        user_label=user_label,
    )


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


@app.route("/toggle-theme", methods=["POST"])
@login_required
def toggle_theme():
    user = current_user()
    new_theme = THEME_DARK if user["theme_mode"] == THEME_LIGHT else THEME_LIGHT
    execute("UPDATE users SET theme_mode = ? WHERE id = ?", (new_theme, user["id"]))
    return redirect(request.referrer or url_for("dashboard"))


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
    tasks = enrich_tasks_with_contact_persons(tasks)
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
    tasks = enrich_tasks_with_contact_persons(enrich_tasks_with_assignees(fetch_tasks()))

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
    tasks = enrich_tasks_with_contact_persons(enrich_tasks_with_assignees(fetch_tasks()))
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
                "priority": int(task.get("priority") or DEFAULT_TASK_PRIORITY),
                "assignees": task["assignees"],
                "description": task.get("description", "") or "",
                "room": task.get("room", "") or "",
                "ticket_category_label": ticket_category_label(task.get("ticket_category", "")),
                "creator_name": task.get("creator_name", ""),
                "contact_person_display": task.get("contact_person_display", "-"),
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
    tasks = enrich_tasks_with_contact_persons(enrich_tasks_with_assignees(tasks))
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
                "description": task.get("description", "") or "",
                "room": task.get("room", "") or "",
                "contact_persons": task.get("contact_persons", []),
                "contact_person_display": task.get("contact_person_display", "-"),
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
    if app_settings().get("calendar_disabled") == "1":
        flash("Der Kalender ist derzeit deaktiviert.", "error")
        return redirect(url_for("dashboard"))

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
            if theme_mode not in VALID_THEME_MODES:
                flash("Ungültiger Modus ausgewählt.", "error")
                return redirect(url_for("settings_page"))

            execute(
                "UPDATE users SET theme_mode = ? WHERE id = ?",
                (theme_mode, user["id"]),
            )
            flash("Darstellung wurde gespeichert.", "success")
            return redirect(url_for("settings_page"))

        if action == "admin-settings":
            if not user["is_admin"]:
                flash("Nur Administratoren dürfen diese Einstellungen ändern.", "error")
                return redirect(url_for("settings_page"))

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
            if highlight_seconds is None or refresh_seconds is None:
                flash("Mindestens ein Zahlenwert ist ungültig oder außerhalb des erlaubten Bereichs.", "error")
                return redirect(url_for("settings_page"))

            new_task_tone = request.form.get("new_task_tone", "classic").strip()

            if new_task_tone not in TONE_OPTIONS:
                flash("Ungültige Ton-Auswahl.", "error")
                return redirect(url_for("settings_page"))

            calendar_disabled = "1" if request.form.get("calendar_disabled") == "1" else "0"

            site_name = request.form.get("site_name", "").strip()
            if not site_name:
                site_name = DEFAULT_APP_SETTINGS["site_name"]

            set_app_setting("new_task_highlight_seconds", str(highlight_seconds))
            set_app_setting("overview_refresh_interval_seconds", str(refresh_seconds))
            set_app_setting("new_task_tone", new_task_tone)
            set_app_setting("calendar_disabled", calendar_disabled)
            set_app_setting("site_name", site_name)

            flash("Einstellungen wurden gespeichert.", "success")
            return redirect(url_for("settings_page"))

        if action == "create-category":
            if not user["is_admin"]:
                flash("Nur Administratoren dürfen Kategorien erstellen.", "error")
                return redirect(url_for("settings_page"))

            category_label_input = request.form.get("category_label", "").strip()
            if not category_label_input:
                flash("Bitte einen Kategorienamen angeben.", "error")
                return redirect(url_for("settings_page"))

            category_key = normalize_category_key(category_label_input)
            if not category_key:
                flash("Kategoriename ist ungültig.", "error")
                return redirect(url_for("settings_page"))

            existing_key = query_one(
                "SELECT category_key FROM ticket_categories WHERE category_key = ?", (category_key,)
            )
            existing_label = query_one(
                "SELECT category_key FROM ticket_categories WHERE lower(label) = lower(?)",
                (category_label_input,),
            )
            if existing_key is not None or existing_label is not None:
                flash("Diese Kategorie existiert bereits.", "error")
                return redirect(url_for("settings_page"))

            execute(
                "INSERT INTO ticket_categories (category_key, label, created_at) VALUES (?, ?, ?)",
                (category_key, category_label_input, now_iso()),
            )
            g.pop("ticket_categories", None)
            flash("Kategorie wurde erstellt.", "success")
            return redirect(url_for("settings_page"))

        if action == "delete-category":
            if not user["is_admin"]:
                flash("Nur Administratoren dürfen Kategorien löschen.", "error")
                return redirect(url_for("settings_page"))

            category_key = request.form.get("category_key", "").strip().lower()
            if not category_key:
                flash("Ungültige Kategorie.", "error")
                return redirect(url_for("settings_page"))

            existing = query_one(
                "SELECT category_key FROM ticket_categories WHERE category_key = ?", (category_key,)
            )
            if existing is None:
                flash("Kategorie nicht gefunden.", "error")
                return redirect(url_for("settings_page"))

            usage = query_one(
                "SELECT COUNT(*) AS cnt FROM tasks WHERE ticket_category = ?", (category_key,)
            )
            if usage is not None and int(usage["cnt"]) > 0:
                flash(
                    "Kategorie kann nicht gelöscht werden, solange Tasks dieser Kategorie zugewiesen sind.",
                    "error",
                )
                return redirect(url_for("settings_page"))

            execute("DELETE FROM ticket_categories WHERE category_key = ?", (category_key,))
            g.pop("ticket_categories", None)
            flash("Kategorie wurde entfernt.", "success")
            return redirect(url_for("settings_page"))

        if action == "upload-favicon":
            if not user["is_admin"]:
                flash("Nur Administratoren dürfen das Favicon ändern.", "error")
                return redirect(url_for("settings_page"))

            file = request.files.get("favicon_file")
            if not file or file.filename == "":
                flash("Bitte eine Datei auswählen.", "error")
                return redirect(url_for("settings_page"))

            ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
            if ext not in FAVICON_ALLOWED_EXTENSIONS:
                flash("Erlaubte Formate: ICO, PNG, JPG, SVG.", "error")
                return redirect(url_for("settings_page"))

            file.seek(0, 2)
            size = file.tell()
            file.seek(0)
            if size > FAVICON_MAX_BYTES:
                flash("Datei zu groß (max. 2 MB).", "error")
                return redirect(url_for("settings_page"))

            old_filename = app_settings().get("favicon_filename", "")
            if old_filename:
                old_path = os.path.join(app.static_folder, old_filename)
                if os.path.exists(old_path):
                    os.remove(old_path)

            new_filename = f"favicon.{ext}"
            file.save(os.path.join(app.static_folder, new_filename))
            set_app_setting("favicon_filename", new_filename)
            flash("Favicon wurde gespeichert.", "success")
            return redirect(url_for("settings_page"))

        if action == "delete-favicon":
            if not user["is_admin"]:
                flash("Nur Administratoren dürfen das Favicon entfernen.", "error")
                return redirect(url_for("settings_page"))

            filename = app_settings().get("favicon_filename", "")
            if filename:
                path = os.path.join(app.static_folder, filename)
                if os.path.exists(path):
                    os.remove(path)
                set_app_setting("favicon_filename", "")
                flash("Favicon wurde entfernt.", "success")
            return redirect(url_for("settings_page"))

        if action == "create-floor":
            if not user["is_admin"]:
                flash("Nur Administratoren dürfen Ebenen erstellen.", "error")
                return redirect(url_for("settings_page"))
            floor_name = request.form.get("floor_name", "").strip()
            if not floor_name:
                flash("Bitte einen Ebenennamen angeben.", "error")
                return redirect(url_for("settings_page"))
            if len(floor_name) > 40:
                flash("Ebenenname darf maximal 40 Zeichen lang sein.", "error")
                return redirect(url_for("settings_page"))
            existing = query_one("SELECT id FROM room_floors WHERE lower(name) = lower(?)", (floor_name,))
            if existing:
                flash("Eine Ebene mit diesem Namen existiert bereits.", "error")
                return redirect(url_for("settings_page"))
            execute("INSERT INTO room_floors (name, created_at) VALUES (?, ?)", (floor_name, now_iso()))
            g.pop("rooms_by_floor", None)
            flash(f'Ebene "{floor_name}" wurde erstellt.', "success")
            return redirect(url_for("settings_page"))

        if action == "delete-floor":
            if not user["is_admin"]:
                flash("Nur Administratoren dürfen Ebenen löschen.", "error")
                return redirect(url_for("settings_page"))
            floor_id = parse_int_value(request.form.get("floor_id"))
            if floor_id is None:
                flash("Ungültige Ebene.", "error")
                return redirect(url_for("settings_page"))
            floor = query_one("SELECT id, name FROM room_floors WHERE id = ?", (floor_id,))
            if floor is None:
                flash("Ebene nicht gefunden.", "error")
                return redirect(url_for("settings_page"))
            execute("DELETE FROM room_floors WHERE id = ?", (floor_id,))
            g.pop("rooms_by_floor", None)
            flash(f'Ebene "{floor["name"]}" und alle zugehörigen Räume wurden entfernt.', "success")
            return redirect(url_for("settings_page"))

        if action == "create-room":
            if not user["is_admin"]:
                flash("Nur Administratoren dürfen Räume erstellen.", "error")
                return redirect(url_for("settings_page"))
            floor_id = parse_int_value(request.form.get("floor_id"))
            room_name = request.form.get("room_name", "").strip()
            if floor_id is None or not room_name:
                flash("Bitte Ebene und Raumname angeben.", "error")
                return redirect(url_for("settings_page"))
            if len(room_name) > 60:
                flash("Raumname darf maximal 60 Zeichen lang sein.", "error")
                return redirect(url_for("settings_page"))
            floor = query_one("SELECT id FROM room_floors WHERE id = ?", (floor_id,))
            if floor is None:
                flash("Ebene nicht gefunden.", "error")
                return redirect(url_for("settings_page"))
            existing = query_one(
                "SELECT id FROM room_entries WHERE floor_id = ? AND lower(name) = lower(?)",
                (floor_id, room_name),
            )
            if existing:
                flash("Dieser Raum existiert auf der Ebene bereits.", "error")
                return redirect(url_for("settings_page"))
            execute(
                "INSERT INTO room_entries (floor_id, name, created_at) VALUES (?, ?, ?)",
                (floor_id, room_name, now_iso()),
            )
            g.pop("rooms_by_floor", None)
            flash(f'Raum "{room_name}" wurde hinzugefügt.', "success")
            return redirect(url_for("settings_page"))

        if action == "delete-room":
            if not user["is_admin"]:
                flash("Nur Administratoren dürfen Räume löschen.", "error")
                return redirect(url_for("settings_page"))
            room_id = parse_int_value(request.form.get("room_id"))
            if room_id is None:
                flash("Ungültiger Raum.", "error")
                return redirect(url_for("settings_page"))
            room = query_one("SELECT id, name FROM room_entries WHERE id = ?", (room_id,))
            if room is None:
                flash("Raum nicht gefunden.", "error")
                return redirect(url_for("settings_page"))
            execute("DELETE FROM room_entries WHERE id = ?", (room_id,))
            g.pop("rooms_by_floor", None)
            flash(f'Raum "{room["name"]}" wurde entfernt.', "success")
            return redirect(url_for("settings_page"))

        flash("Unbekannte Aktion.", "error")
        return redirect(url_for("settings_page"))

    return render_template(
        "settings.html",
        user=user,
        settings=app_settings(),
        custom_roles=active_custom_roles(),
        ticket_categories=get_ticket_categories(),
        tone_options=sorted(TONE_OPTIONS.keys()),
        show_sidebar=False,
    )


def parse_task_form(form) -> tuple[dict | None, str | None]:
    title = form.get("title", "").strip()
    description = form.get("description", "").strip()
    due_date = form.get("due_date", "").strip()
    due_time = form.get("due_time", "").strip()
    priority_raw = form.get("priority", str(DEFAULT_TASK_PRIORITY)).strip()
    ticket_category = normalize_ticket_category(form.get("ticket_category", ""))
    room = form.get("room", "").strip()
    contact_person_ids_raw = form.getlist("contact_person_ids")
    contact_person_custom = form.get("contact_person_custom", "").strip()
    assignee_ids_raw = form.getlist("assignee_ids")

    if not title or not description:
        return None, "Bitte alle Pflichtfelder ausfüllen."

    if len(title) > MAX_TASK_TITLE_LENGTH:
        return None, f"Titel darf maximal {MAX_TASK_TITLE_LENGTH} Zeichen lang sein."

    if len(description) > MAX_TASK_DESCRIPTION_LENGTH:
        return None, f"Beschreibung darf maximal {MAX_TASK_DESCRIPTION_LENGTH} Zeichen lang sein."

    if room and len(room) > MAX_TASK_ROOM_LENGTH:
        return None, f"Raum darf maximal {MAX_TASK_ROOM_LENGTH} Zeichen lang sein."

    if not ticket_category:
        return None, "Bitte eine gültige Task-Kategorie auswählen."

    try:
        priority = int(priority_raw)
    except ValueError:
        return None, "Bitte eine gültige Priorität (1-5) auswählen."

    if priority < MIN_TASK_PRIORITY or priority > MAX_TASK_PRIORITY:
        return None, "Bitte eine gültige Priorität (1-5) auswählen."

    contact_person_ids = []
    for raw_id in contact_person_ids_raw:
        cleaned = raw_id.strip()
        if cleaned:
            try:
                contact_person_ids.append(int(cleaned))
            except ValueError:
                return None, "Ungültiger Ansprechpartner ausgewählt."

    contact_person_ids = sorted(set(contact_person_ids))
    contact_users = []
    if contact_person_ids:
        placeholders = ",".join(["?"] * len(contact_person_ids))
        contact_users = query_all(
            f"SELECT id, username FROM users WHERE id IN ({placeholders}) AND COALESCE(is_dashboard_invisible, 0) = 0",
            tuple(contact_person_ids),
        )
        if len(contact_users) != len(contact_person_ids):
            return None, "Mindestens ein Ansprechpartner wurde nicht gefunden."

    if due_date:
        normalized_due_date = normalize_due_date_value(due_date, due_time)
        if not normalized_due_date:
            return None, "Ungültiges Fälligkeitsdatum."
    else:
        normalized_due_date = None

    assignee_ids = []
    for raw_id in assignee_ids_raw:
        cleaned = raw_id.strip()
        if cleaned:
            try:
                assignee_ids.append(int(cleaned))
            except ValueError:
                return None, "Ungültiger Bearbeiter ausgewählt."

    assignee_ids = sorted(set(assignee_ids))
    if assignee_ids:
        placeholders = ",".join(["?"] * len(assignee_ids))
        found = query_all(
            f"SELECT id FROM users WHERE id IN ({placeholders}) AND COALESCE(is_dashboard_invisible, 0) = 0",
            tuple(assignee_ids),
        )
        if len(found) != len(assignee_ids):
            return None, "Mindestens ein ausgewählter Bearbeiter existiert nicht."

    if contact_person_custom and len(contact_person_custom) > 200:
        return None, "Externe Ansprechpartner dürfen maximal 200 Zeichen lang sein."

    contact_person_name = ", ".join(u["username"] for u in contact_users)

    return {
        "title": title,
        "description": description,
        "due_date": normalized_due_date,
        "priority": priority,
        "ticket_category": ticket_category,
        "room": room,
        "contact_person_ids": contact_person_ids,
        "contact_person_name": contact_person_name,
        "contact_person_custom": contact_person_custom,
        "assignee_ids": assignee_ids,
    }, None


@app.route("/tasks/create", methods=["POST"])
@login_required
def create_task():
    user = current_user()

    parsed, error = parse_task_form(request.form)
    if error:
        flash(error, "error")
        return redirect(url_for("dashboard"))

    title = parsed["title"]
    description = parsed["description"]
    priority = parsed["priority"]
    ticket_category = parsed["ticket_category"]
    room = parsed["room"]
    contact_person_ids = parsed["contact_person_ids"]
    contact_person_name = parsed["contact_person_name"]
    contact_person_custom = parsed["contact_person_custom"]
    normalized_due_date = parsed["due_date"]
    assignee_ids = parsed["assignee_ids"]

    now = now_iso()
    cur = execute(
        """
        INSERT INTO tasks (
            title, description, priority, assignee_id, due_date,
            contact_person, contact_person_user_id,
            ticket_category, room, created_by, status, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            title, description, priority,
            assignee_ids[0] if assignee_ids else None,
            normalized_due_date,
            contact_person_custom,
            contact_person_ids[0] if contact_person_ids else None,
            ticket_category, room, user["id"], STATUS_OPEN, now, now,
        ),
    )

    task_id = cur.lastrowid
    for assignee_id in assignee_ids:
        execute(
            "INSERT OR IGNORE INTO task_assignees (task_id, user_id) VALUES (?, ?)",
            (task_id, assignee_id),
        )
    for cp_id in contact_person_ids:
        execute(
            "INSERT OR IGNORE INTO task_contact_persons (task_id, user_id) VALUES (?, ?)",
            (task_id, cp_id),
        )

    create_details_parts = [
        f"Priorität: {priority}",
        f"Kategorie: {ticket_category_label(ticket_category)}",
    ]
    if normalized_due_date:
        create_details_parts.append(f"Fällig: {format_datetime_for_display(normalized_due_date)}")
    if assignee_ids:
        name_rows = query_all(
            f"SELECT username FROM users WHERE id IN ({','.join(['?'] * len(assignee_ids))})",
            tuple(assignee_ids),
        )
        create_details_parts.append(f"Bearbeiter: {', '.join(r['username'] for r in name_rows)}")

    log_event(
        user, "task_created", "Task erstellt",
        task_id=task_id, task_title=title,
        details=", ".join(create_details_parts),
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
        SELECT t.*, c.username AS creator_name
        FROM tasks t
        JOIN users c ON c.id = t.created_by
        WHERE t.id = ?
        """,
        (task_id,),
    )
    if task is None:
        return None

    task_dict = dict(task)
    contact_persons = contact_persons_map([task_id]).get(task_id, [])
    task_dict["contact_persons"] = contact_persons
    parts = [cp["username"] for cp in contact_persons]
    custom = (task_dict.get("contact_person") or "").strip()
    if custom:
        parts.append(custom)
    task_dict["contact_person_display"] = ", ".join(parts) if parts else "-"
    task_dict["contact_person_custom"] = custom

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
        log_event(
            user, "task_closed", "Task geschlossen",
            task_id=task_id, task_title=task["title"],
            details=f"Grund: {close_reason}",
        )
        flash("Task wurde geschlossen.", "success")
        return redirect(url_for("dashboard", filter=return_filter))

    _status_labels = {STATUS_OPEN: "Offen", STATUS_IN_PROGRESS: "In Bearbeitung"}
    execute(
        "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
        (new_status, now_iso(), task_id),
    )
    log_event(
        user, "status_changed", f"Status geändert zu '{_status_labels.get(new_status, new_status)}'",
        task_id=task_id, task_title=task["title"],
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


@app.route("/tasks/<int:task_id>/assignees/self-assign", methods=["POST"])
@login_required
def self_assign_task(task_id: int):
    user = current_user()
    return_filter = request.form.get("return_filter", "all").strip().lower()
    if return_filter not in VALID_DASHBOARD_FILTERS:
        return_filter = "all"

    task = query_one("SELECT id, status FROM tasks WHERE id = ? AND COALESCE(is_archived, 0) = 0", (task_id,))
    if task is None or task["status"] == STATUS_CLOSED:
        flash("Diese Task kann nicht bearbeitet werden.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    existing = query_one(
        "SELECT 1 FROM task_assignees WHERE task_id = ? AND user_id = ?",
        (task_id, int(user["id"])),
    )
    if existing is None:
        execute(
            "INSERT INTO task_assignees (task_id, user_id) VALUES (?, ?)",
            (task_id, int(user["id"])),
        )
        sync_task_primary_assignee(task_id)

    return redirect(url_for("dashboard", filter=return_filter))


@app.route("/tasks/<int:task_id>/assignees/remove", methods=["POST"])
@login_required
def remove_task_assignee(task_id: int):
    user = current_user()
    return_filter = request.form.get("return_filter", "all").strip().lower()
    if return_filter not in VALID_DASHBOARD_FILTERS:
        return_filter = "all"

    task = query_one("SELECT id, status FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    if not can_edit_task_content(user, task_id):
        flash("Keine Berechtigung, Bearbeiter von dieser Task zu entfernen.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    raw_user_id = request.form.get("user_id", "").strip()
    try:
        assignee_user_id = int(raw_user_id)
    except ValueError:
        flash("Ungültiger Benutzer.", "error")
        return redirect(url_for("dashboard", filter=return_filter))

    execute(
        "DELETE FROM task_assignees WHERE task_id = ? AND user_id = ?",
        (task_id, assignee_user_id),
    )
    sync_task_primary_assignee(task_id)

    flash("Bearbeiter wurde aus der Task entfernt.", "success")
    return redirect(url_for("dashboard", filter=return_filter))


@app.route("/tasks/<int:task_id>/assignees/self-remove", methods=["POST"])
@login_required
def self_remove_task(task_id: int):
    user = current_user()
    return_filter = request.form.get("return_filter", "all").strip().lower()
    if return_filter not in VALID_DASHBOARD_FILTERS:
        return_filter = "all"

    execute(
        "DELETE FROM task_assignees WHERE task_id = ? AND user_id = ?",
        (task_id, int(user["id"])),
    )
    sync_task_primary_assignee(task_id)

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
    task = query_one("SELECT id, title, status FROM tasks WHERE id = ?", (task_id,))
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

    preview = content[:80] + ("…" if len(content) > 80 else "")
    log_event(
        user, "comment_added", "Kommentar hinzugefügt",
        task_id=task_id, task_title=task["title"],
        details=preview,
    )
    flash("Kommentar wurde gespeichert.", "success")
    return redirect(url_for("task_detail", task_id=task_id))


@app.route("/tasks/<int:task_id>/comments/<int:comment_id>/edit", methods=["POST"])
@login_required
def edit_task_comment(task_id: int, comment_id: int):
    user = current_user()
    task = query_one("SELECT id, title, status FROM tasks WHERE id = ?", (task_id,))
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
    log_event(
        user, "comment_edited", "Kommentar bearbeitet",
        task_id=task_id, task_title=task["title"],
    )
    flash("Kommentar wurde aktualisiert.", "success")
    return redirect(url_for("task_detail", task_id=task_id))


@app.route("/tasks/<int:task_id>/comments/<int:comment_id>/delete", methods=["POST"])
@login_required
def delete_task_comment(task_id: int, comment_id: int):
    user = current_user()
    task = query_one("SELECT id, title, status FROM tasks WHERE id = ?", (task_id,))
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
    log_event(
        user, "comment_deleted", "Kommentar gelöscht",
        task_id=task_id, task_title=task["title"],
    )
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

    parsed, error = parse_task_form(request.form)
    if error:
        flash(error, "error")
        return redirect(url_for("task_detail", task_id=task_id))

    title = parsed["title"]
    description = parsed["description"]
    priority = parsed["priority"]
    ticket_category = parsed["ticket_category"]
    room = parsed["room"]
    contact_person_ids = parsed["contact_person_ids"]
    contact_person_name = parsed["contact_person_name"]
    contact_person_custom = parsed["contact_person_custom"]
    normalized_due_date = parsed["due_date"]
    assignee_ids = parsed["assignee_ids"]

    old_assignee_rows = query_all(
        "SELECT user_id FROM task_assignees WHERE task_id = ?", (task_id,)
    )
    old_assignee_ids = sorted(row["user_id"] for row in old_assignee_rows)
    old_cp_rows = query_all(
        "SELECT user_id FROM task_contact_persons WHERE task_id = ?", (task_id,)
    )
    old_cp_ids = sorted(row["user_id"] for row in old_cp_rows)

    changes = []
    if (task["title"] or "") != title:
        changes.append("Titel geändert")
    if (task["description"] or "") != description:
        changes.append("Beschreibung geändert")
    old_priority = int(task["priority"] or DEFAULT_TASK_PRIORITY)
    if old_priority != priority:
        changes.append(f"Priorität: {old_priority} → {priority}")
    old_cat = (task["ticket_category"] or "").strip()
    if old_cat != ticket_category:
        changes.append(
            f"Kategorie: {ticket_category_label(old_cat)} → {ticket_category_label(ticket_category)}"
        )
    old_room = (task["room"] or "").strip()
    new_room = room.strip()
    if old_room != new_room:
        if old_room and new_room:
            changes.append(f"Raum: {old_room} → {new_room}")
        elif new_room:
            changes.append(f"Raum gesetzt: {new_room}")
        else:
            changes.append("Raum entfernt")
    old_due = (task["due_date"] or "").strip()
    new_due = (normalized_due_date or "").strip()
    if old_due != new_due:
        if old_due and new_due:
            changes.append(
                f"Fälligkeit: {format_datetime_for_display(old_due)} → {format_datetime_for_display(new_due)}"
            )
        elif new_due:
            changes.append(f"Fälligkeit gesetzt: {format_datetime_for_display(new_due)}")
        else:
            changes.append("Fälligkeit entfernt")
    if sorted(contact_person_ids) != old_cp_ids:
        changes.append("Ansprechpartner geändert")
    if sorted(assignee_ids) != old_assignee_ids:
        changes.append("Bearbeiter geändert")

    edit_details = ", ".join(changes) if changes else None

    execute(
        """
        UPDATE tasks
        SET title = ?, description = ?, due_date = ?, priority = ?, assignee_id = ?,
            contact_person = ?, contact_person_user_id = ?, ticket_category = ?, room = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            title, description, normalized_due_date, priority,
            assignee_ids[0] if assignee_ids else None,
            contact_person_custom,
            contact_person_ids[0] if contact_person_ids else None,
            ticket_category, room, now_iso(), task_id,
        ),
    )

    execute("DELETE FROM task_assignees WHERE task_id = ?", (task_id,))
    for assignee_id in assignee_ids:
        execute(
            "INSERT INTO task_assignees (task_id, user_id) VALUES (?, ?)",
            (task_id, assignee_id),
        )
    execute("DELETE FROM task_contact_persons WHERE task_id = ?", (task_id,))
    for cp_id in contact_person_ids:
        execute(
            "INSERT OR IGNORE INTO task_contact_persons (task_id, user_id) VALUES (?, ?)",
            (task_id, cp_id),
        )

    log_event(user, "task_edited", "Task bearbeitet", task_id=task_id, task_title=title, details=edit_details)
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
            role_value_create = request.form.get("role", "").strip()
            is_admin, role = resolve_role_assignment(role_value_create)
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

            if not role_value_create or (not is_admin and not role):
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
            role_color = request.form.get("role_color", "#64748b").strip()
            role_is_admin = 1 if request.form.get("role_is_admin") == "1" else 0

            if not role_label_input:
                flash("Bitte einen Rollennamen angeben.", "error")
                return redirect(url_for("manage_users"))

            if not is_hex_color(role_color):
                flash("Farbe muss im Format #RRGGBB angegeben werden.", "error")
                return redirect(url_for("manage_users"))

            role_key = normalize_custom_role_key(role_label_input)
            if not role_key:
                flash("Rollenname ist ungültig.", "error")
                return redirect(url_for("manage_users"))

            if role_key in ("admin", "user"):
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
                "INSERT INTO custom_roles (role_key, label, color, is_admin, created_at) VALUES (?, ?, ?, ?, ?)",
                (role_key, role_label_input, role_color, role_is_admin, now_iso()),
            )
            flash("Rolle wurde erstellt.", "success")
            return redirect(url_for("manage_users"))

        if action == "update-role":
            role_key = request.form.get("role_key", "").strip().lower()
            role_label_upd = request.form.get("role_label", "").strip()
            role_color = request.form.get("role_color", "").strip()
            role_is_admin = 1 if request.form.get("role_is_admin") == "1" else 0

            if not role_key:
                flash("Ungültige Rolle.", "error")
                return redirect(url_for("manage_users"))

            if not is_hex_color(role_color):
                flash("Farbe muss im Format #RRGGBB angegeben werden.", "error")
                return redirect(url_for("manage_users"))

            if role_key == "admin":
                set_app_setting("role_color_admin", role_color)
                if role_label_upd:
                    set_app_setting("role_label_admin", role_label_upd)
                flash("Admin-Rolle wurde aktualisiert.", "success")
                return redirect(url_for("manage_users"))

            if role_key == "user":
                set_app_setting("role_color_user", role_color)
                if role_label_upd:
                    set_app_setting("role_label_user", role_label_upd)
                flash("Benutzer-Rolle wurde aktualisiert.", "success")
                return redirect(url_for("manage_users"))

            if not role_label_upd:
                flash("Bitte einen Rollennamen angeben.", "error")
                return redirect(url_for("manage_users"))

            existing = query_one("SELECT role_key, is_admin FROM custom_roles WHERE role_key = ?", (role_key,))
            if existing is None:
                flash("Rolle nicht gefunden.", "error")
                return redirect(url_for("manage_users"))

            conflict = query_one(
                "SELECT role_key FROM custom_roles WHERE lower(label) = lower(?) AND role_key != ?",
                (role_label_upd, role_key),
            )
            if conflict:
                flash("Eine Rolle mit diesem Namen existiert bereits.", "error")
                return redirect(url_for("manage_users"))

            if int(existing["is_admin"]) == 1 and role_is_admin == 0:
                affected = query_one("SELECT COUNT(*) AS cnt FROM users WHERE role = ? AND is_admin = 1", (role_key,))
                if affected and int(affected["cnt"]) > 0:
                    remaining = query_one(
                        "SELECT COUNT(*) AS cnt FROM users WHERE is_admin = 1 AND (role != ? OR role IS NULL)",
                        (role_key,),
                    )
                    if not remaining or int(remaining["cnt"]) < 1:
                        flash("Adminrechte können nicht entfernt werden: es würde kein Admin mehr verbleiben.", "error")
                        return redirect(url_for("manage_users"))

            execute(
                "UPDATE custom_roles SET label = ?, color = ?, is_admin = ? WHERE role_key = ?",
                (role_label_upd, role_color, role_is_admin, role_key),
            )
            execute("UPDATE users SET is_admin = ? WHERE role = ?", (role_is_admin, role_key))
            g.pop("custom_roles_map", None)
            flash("Rolle wurde aktualisiert.", "success")
            return redirect(url_for("manage_users"))

        if action == "delete-role":
            role_key = request.form.get("role_key", "").strip().lower()
            if not role_key:
                flash("Ungültige Rolle.", "error")
                return redirect(url_for("manage_users"))

            existing = query_one("SELECT role_key, label FROM custom_roles WHERE role_key = ?", (role_key,))
            if existing is None:
                flash("Rolle nicht gefunden.", "error")
                return redirect(url_for("manage_users"))

            usage = query_one("SELECT COUNT(*) AS cnt FROM users WHERE role = ?", (role_key,))
            if usage is not None and int(usage["cnt"]) > 0:
                flash("Rolle kann nicht gelöscht werden, solange Benutzer dieser Rolle zugewiesen sind.", "error")
                return redirect(url_for("manage_users"))

            execute("DELETE FROM custom_roles WHERE role_key = ?", (role_key,))
            flash("Rolle wurde entfernt.", "success")
            return redirect(url_for("manage_users"))

        if action == "update":
            target_id = request.form.get("user_id", "").strip()
            username = request.form.get("username", "").strip()
            initials = normalize_initials(request.form.get("initials", ""))
            role_value_upd = request.form.get("role", "").strip()
            is_admin, role = resolve_role_assignment(role_value_upd)
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

            if not role_value_upd or (not is_admin and not role):
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
            execute("DELETE FROM task_contact_persons WHERE user_id = ?", (target_id,))
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
            username ASC
        """
    )

    enriched_users = []
    for row in users:
        item = dict(row)
        item["role_label"] = role_label(row["role"], row["is_admin"])
        item["color_class"] = badge_color_class(row["role"], row["is_admin"])
        item["member_type"] = row["member_type"] or MEMBER_TYPE_REGULAR
        item["is_dashboard_invisible"] = bool(row["is_dashboard_invisible"])
        enriched_users.append(item)

    settings = app_settings()
    return render_template(
        "admin_users.html",
        users=enriched_users,
        user=current_user(),
        role_options=role_options(),
        role_management_entries=role_management_entries(),
        admin_color=settings.get("role_color_admin", "#facc15"),
        admin_label=settings.get("role_label_admin", "Admin"),
        user_color=settings.get("role_color_user", "#64748b"),
        user_label=settings.get("role_label_user", "Benutzer"),
        show_sidebar=False,
    )


@app.route("/admin/closed")
@admin_required
def admin_closed_tasks():
    return redirect(url_for("archive", tab="closed"))


@app.route("/archive", methods=["GET", "POST"])
@admin_required
def archive():
    user = current_user()
    tab = request.args.get("tab", "closed").strip().lower()
    if tab not in {"closed", "archived", "log"}:
        tab = "closed"

    if request.method == "POST":
        action = request.form.get("action", "").strip()
        task_id_raw = request.form.get("task_id", "").strip()
        return_tab = request.form.get("return_tab", "closed").strip().lower()
        if return_tab not in {"closed", "archived"}:
            return_tab = "closed"

        task = query_one("SELECT * FROM tasks WHERE id = ?", (task_id_raw,))
        if task is None:
            flash("Task nicht gefunden.", "error")
            return redirect(url_for("archive", tab=return_tab))

        if action == "reopen":
            if task["status"] != STATUS_CLOSED:
                flash("Diese Task ist nicht geschlossen.", "error")
                return redirect(url_for("archive", tab="closed"))
            execute(
                """
                UPDATE tasks
                SET status = ?, updated_at = ?, close_reason = NULL, closed_at = NULL, closed_by = NULL,
                    is_archived = 0
                WHERE id = ?
                """,
                (STATUS_IN_PROGRESS, now_iso(), task_id_raw),
            )
            log_event(user, "task_reopened", "Task zurückgesendet", task_id=int(task_id_raw), task_title=task["title"])
            flash("Task wurde ans Team zurückgesendet.", "success")
            return redirect(url_for("archive", tab="closed"))

        if action == "archive_task":
            if task["status"] != STATUS_CLOSED:
                flash("Nur geschlossene Tasks können archiviert werden.", "error")
                return redirect(url_for("archive", tab="closed"))
            execute(
                "UPDATE tasks SET is_archived = 1, contact_person = '', contact_person_user_id = NULL, updated_at = ? WHERE id = ?",
                (now_iso(), task_id_raw),
            )
            log_event(user, "task_archived", "Task archiviert", task_id=int(task_id_raw), task_title=task["title"])
            flash("Task wurde archiviert.", "success")
            return redirect(url_for("archive", tab="closed"))

        if action == "unarchive_task":
            if not task["is_archived"]:
                flash("Dieser Task ist nicht archiviert.", "error")
                return redirect(url_for("archive", tab="archived"))
            execute(
                "UPDATE tasks SET is_archived = 0, updated_at = ? WHERE id = ?",
                (now_iso(), task_id_raw),
            )
            log_event(user, "task_unarchived", "Task aus Archiv zurückgesetzt", task_id=int(task_id_raw), task_title=task["title"])
            flash("Task wurde aus dem Archiv zurückgesetzt.", "success")
            return redirect(url_for("archive", tab="archived"))

        if action == "delete":
            log_event(user, "task_deleted", "Task gelöscht", task_id=int(task_id_raw), task_title=task["title"])
            execute("DELETE FROM tasks WHERE id = ?", (task_id_raw,))
            flash("Task wurde endgültig gelöscht.", "success")
            return redirect(url_for("archive", tab=return_tab))

        flash("Unbekannte Aktion.", "error")
        return redirect(url_for("archive", tab=return_tab))

    cleanup_old_log_entries()

    closed_tasks = enrich_tasks_with_contact_persons(enrich_tasks_with_assignees(fetch_tasks(status=STATUS_CLOSED)))
    archived_tasks = enrich_tasks_with_contact_persons(enrich_tasks_with_assignees(fetch_tasks(archived_only=True)))
    log_entries = query_all(
        """
        SELECT id, actor_name, event_type, description, task_id, task_title, details, created_at
        FROM activity_log
        ORDER BY created_at DESC
        LIMIT 500
        """
    )

    return render_template(
        "archive.html",
        tasks=closed_tasks,
        archived_tasks=archived_tasks,
        log_entries=log_entries,
        log_retention_days=LOG_RETENTION_DAYS,
        tab=tab,
        user=user,
        show_sidebar=False,
    )


with app.app_context():
    init_db()


if __name__ == "__main__":
    _host = RUNTIME_CONFIG["host"]
    _port = RUNTIME_CONFIG["port"]
    _debug = RUNTIME_CONFIG["debug"]

    if getattr(sys, "frozen", False):
        import socket
        import threading
        import time
        import tkinter
        import webbrowser
        from tkinter import messagebox

        import pystray
        from PIL import Image

        _display_host = "127.0.0.1" if _host == "0.0.0.0" else _host
        _url = f"http://{_display_host}:{_port}"

        # Tray-Icon laden – nimmt automatisch die erste .ico-Datei im Bundle
        import glob
        _ico_candidates = glob.glob(os.path.join(_BUNDLE_DIR, "*.ico"))
        try:
            _tray_image = Image.open(_ico_candidates[0])
        except Exception:
            _tray_image = Image.new("RGBA", (64, 64), (59, 130, 246, 255))

        def _on_open(icon, item):
            webbrowser.open(_url)

        def _on_stop(icon, item):
            icon.stop()
            os._exit(0)

        _tray_icon = pystray.Icon(
            "Ticket-System",
            _tray_image,
            "Ticket-System läuft",
            menu=pystray.Menu(
                pystray.MenuItem("Im Browser öffnen", _on_open),
                pystray.Menu.SEPARATOR,
                pystray.MenuItem("Beenden", _on_stop),
            ),
        )

        def _show_startup_popup():
            deadline = time.time() + 15
            ready = False
            while time.time() < deadline:
                try:
                    with socket.create_connection((_display_host, _port), timeout=0.5):
                        ready = True
                        break
                except OSError:
                    time.sleep(0.2)

            root = tkinter.Tk()
            root.withdraw()
            if ready:
                messagebox.showinfo(
                    "Ticket-System gestartet",
                    f"Der Dienst wurde erfolgreich gestartet.\n\nWebseite erreichbar unter:\n{_url}",
                )
            else:
                messagebox.showerror(
                    "Fehler beim Starten",
                    f"Der Dienst konnte nicht gestartet werden.\n\n"
                    f"Mögliche Ursache: Port {_port} ist bereits belegt.",
                )
            root.destroy()

        threading.Thread(target=_show_startup_popup, daemon=True).start()
        threading.Thread(
            target=lambda: app.run(host=_host, port=_port, debug=False, use_reloader=False),
            daemon=True,
        ).start()
        _tray_icon.run()
    else:
        app.run(host=_host, port=_port, debug=_debug)

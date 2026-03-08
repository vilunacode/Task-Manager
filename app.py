import os
import re
import sqlite3
from datetime import datetime
from functools import wraps

from flask import (
    Flask,
    flash,
    g,
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


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")


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
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
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


def normalize_datetime_value(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""

    candidate = raw.replace("Z", "")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        try:
            dt = datetime.strptime(candidate, "%Y-%m-%d")
        except ValueError:
            return ""
    return dt.strftime("%Y-%m-%dT%H:%M")


def format_datetime_for_display(value: str | None) -> str:
    if not value:
        return "-"
    normalized = normalize_datetime_value(value)
    if not normalized:
        return "-"
    dt = datetime.fromisoformat(normalized)
    return dt.strftime("%d.%m.%Y %H:%M Uhr")


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


@app.context_processor
def inject_helpers():
    user = current_user()
    badges = []
    if user is not None:
        badges = sidebar_users()
    return {
        "status_label": status_label,
        "sidebar_users": badges,
        "format_datetime": format_datetime_for_display,
        "datetime_input_value": format_datetime_for_input,
        "is_due_today": is_due_today,
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
                datetime.utcnow().isoformat(),
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

    now = datetime.utcnow().isoformat()
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
    if user["is_admin"]:
        return True
    return is_task_creator(user, task_id)


def task_comments(task_id: int):
    return query_all(
        """
        SELECT
            tc.id,
            tc.content,
            tc.created_at,
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
        comments.append(
            {
                "id": comment["id"],
                "content": comment["content"],
                "created_at": comment["created_at"],
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

    execute(
        "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
        (new_status, datetime.utcnow().isoformat(), task_id),
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
        can_comment=user["is_admin"] or can_manage_task(user, task_id),
    )


@app.route("/tasks/<int:task_id>/comments", methods=["POST"])
@login_required
def add_task_comment(task_id: int):
    user = current_user()
    task = query_one("SELECT id FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard"))

    if not (user["is_admin"] or can_manage_task(user, task_id)):
        flash("Nur zugewiesene Benutzer oder Admins dürfen kommentieren.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    content = request.form.get("content", "").strip()
    if not content:
        flash("Kommentar darf nicht leer sein.", "error")
        return redirect(url_for("task_detail", task_id=task_id))

    execute(
        """
        INSERT INTO task_comments (task_id, user_id, content, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (task_id, user["id"], content, datetime.utcnow().isoformat()),
    )
    flash("Kommentar wurde gespeichert.", "success")
    return redirect(url_for("task_detail", task_id=task_id))


@app.route("/tasks/<int:task_id>/edit", methods=["POST"])
@login_required
def edit_task(task_id: int):
    user = current_user()
    task = query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        flash("Task nicht gefunden.", "error")
        return redirect(url_for("dashboard"))

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
            datetime.utcnow().isoformat(),
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

        if action == "create":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "").strip()
            initials = normalize_initials(request.form.get("initials", ""))
            role = normalize_role(request.form.get("role", ""))
            is_admin = 1 if role == "admin" else 0

            if not username or not password:
                flash("Benutzername und Passwort sind erforderlich.", "error")
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
                    datetime.utcnow().isoformat(),
                ),
            )
            flash("Benutzer wurde angelegt.", "success")
            return redirect(url_for("manage_users"))

        if action == "delete":
            target_id = request.form.get("user_id", "").strip()
            current = current_user()

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
                "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
                (STATUS_IN_PROGRESS, datetime.utcnow().isoformat(), task_id),
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

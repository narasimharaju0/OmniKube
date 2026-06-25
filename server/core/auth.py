import functools
import hashlib
import logging
import os
import secrets
import sqlite3
import threading
import time
from typing import Any, Callable, TypeVar

from werkzeug.security import check_password_hash, generate_password_hash

from core.database import User, get_db, init_orm_tables
from core.oauth2 import organization_id_from_email

logger = logging.getLogger(__name__)

ROLE_ADMIN = "Admin"
ROLE_EDITOR = "Editor"
ROLE_VIEWER = "Viewer"

ALL_ROLES = {ROLE_ADMIN, ROLE_EDITOR, ROLE_VIEWER}
EDITOR_ROLES = {ROLE_ADMIN, ROLE_EDITOR}

SESSION_COOKIE_NAME = "omnikube_session"
SESSION_TTL_SEC = 24 * 60 * 60
DEFAULT_TENANT_ID = "default"
DEFAULT_MOCK_PASSWORD = "changeme"
DEFAULT_REGISTERED_ROLE = ROLE_VIEWER

DEFAULT_USERS: tuple[tuple[str, str, str], ...] = (
    ("admin_user", ROLE_ADMIN, DEFAULT_TENANT_ID),
    ("editor_user", ROLE_EDITOR, DEFAULT_TENANT_ID),
    ("viewer_user", ROLE_VIEWER, DEFAULT_TENANT_ID),
)

_sessions: dict[str, dict[str, Any]] = {}
_session_lock = threading.Lock()

F = TypeVar("F", bound=Callable[..., Any])


def hash_password(password: str) -> str:
    """Return a secure password hash suitable for ORM and legacy user storage."""
    return generate_password_hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plaintext password against werkzeug or legacy sha256 hashes."""
    if hashed_password.startswith("sha256:"):
        legacy_digest = hashlib.sha256(plain_password.encode("utf-8")).hexdigest()
        return hashed_password == f"sha256:{legacy_digest}"
    return check_password_hash(hashed_password, plain_password)


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _orm_user_to_session(user: User, *, role: str = DEFAULT_REGISTERED_ROLE) -> dict[str, Any]:
    organization_id = organization_id_from_email(user.email) or DEFAULT_ORGANIZATION_ID
    display_name = (user.company_name or user.email.split("@", 1)[0]).strip()
    return {
        "id": int(user.id),
        "username": user.email,
        "role": role,
        "tenant_id": organization_id,
        "organization_id": organization_id,
        "email": user.email,
        "display_name": display_name,
        "company_name": user.company_name,
        "auth_provider": "local",
    }


def register_user(email: str, password: str, company_name: str) -> User:
    """
    Register a platform user in the ORM users table.

    Raises ValueError when the email is already registered or inputs are invalid.
    """
    normalized_email = _normalize_email(email)
    if not normalized_email or "@" not in normalized_email:
        raise ValueError("A valid email address is required.")
    if not password:
        raise ValueError("Password is required.")
    if not company_name.strip():
        raise ValueError("Company name is required.")

    init_orm_tables()

    with get_db() as db:
        existing = db.query(User).filter(User.email == normalized_email).one_or_none()
        if existing is not None:
            raise ValueError("An account with this email already exists.")

        user = User(
            email=normalized_email,
            password_hash=hash_password(password),
            company_name=company_name.strip(),
        )
        db.add(user)
        db.flush()
        db.refresh(user)
        logger.info("Registered ORM user %s for company %s", normalized_email, company_name.strip())
        return user


def _authenticate_orm_user(identifier: str, password: str) -> dict[str, Any] | None:
    normalized = _normalize_email(identifier) if "@" in identifier else identifier.strip().lower()
    if "@" not in normalized:
        return None

    try:
        with get_db() as db:
            user = db.query(User).filter(User.email == normalized).one_or_none()
            if user is None:
                return None
            if not verify_password(password, user.password_hash):
                return None
            return _orm_user_to_session(user)
    except Exception as exc:
        logger.error("ORM authentication failed for %s: %s", normalized, exc)
        return None


def _authenticate_legacy_user(db_path: str, username: str, password: str) -> dict[str, Any] | None:
    """Authenticate against legacy RBAC users table (username-based mock accounts)."""
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT id, username, password_hash, role, tenant_id, email,
                       organization_id, display_name
                FROM users
                WHERE username = ?
                """,
                (username.strip(),),
            ).fetchone()

        if row is None:
            return None
        if not verify_password(password, str(row["password_hash"])):
            return None

        organization_id = str(row["organization_id"] or row["tenant_id"] or DEFAULT_ORGANIZATION_ID)
        return {
            "id": int(row["id"]),
            "username": str(row["username"]),
            "role": str(row["role"]),
            "tenant_id": str(row["tenant_id"]),
            "organization_id": organization_id,
            "email": str(row["email"] or ""),
            "display_name": str(row["display_name"] or row["username"]),
            "auth_provider": "local",
        }
    except sqlite3.Error as exc:
        logger.error("Legacy authentication query failed: %s", exc)
        return None


def authenticate_user(db_path: str, identifier: str, password: str) -> dict[str, Any] | None:
    """
    Verify credentials and establish a session.

    Accepts either an ORM email address or a legacy RBAC username.
    Returns a payload containing authenticated user metadata and session token.
    """
    identifier = str(identifier or "").strip()
    if not identifier or not password:
        return None

    user = _authenticate_orm_user(identifier, password)
    if user is None:
        user = _authenticate_legacy_user(db_path, identifier, password)
    if user is None:
        return None

    token = create_session(db_path, user)
    return {
        "user": user,
        "token": token,
    }


def login_required(*, roles: set[str] | None = None) -> Callable[[F], F]:
    """
    Decorator for ManagementHandler methods that require an authenticated session.

    Usage:
        @login_required(roles=EDITOR_ROLES)
        def _handle_example(self) -> None:
            ...
    """
    allowed_roles = roles or ALL_ROLES

    def decorator(method: F) -> F:
        @functools.wraps(method)
        def wrapper(self, *args: Any, **kwargs: Any) -> Any:
            user = self._require_user(allowed_roles)
            if user is None:
                return None
            return method(self, *args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def _add_column_if_missing(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    if column in _table_columns(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    logger.info("Migrated %s: added %s column", table, column)


def init_auth_sessions(db_path: str) -> None:
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_sessions (
                    token TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    role TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    organization_id TEXT NOT NULL DEFAULT 'default',
                    email TEXT NOT NULL DEFAULT '',
                    display_name TEXT NOT NULL DEFAULT '',
                    auth_provider TEXT NOT NULL DEFAULT 'local',
                    expires_at REAL NOT NULL
                )
                """
            )
            _add_column_if_missing(
                conn,
                "auth_sessions",
                "organization_id",
                "TEXT NOT NULL DEFAULT 'default'",
            )
            _add_column_if_missing(conn, "auth_sessions", "email", "TEXT NOT NULL DEFAULT ''")
            _add_column_if_missing(
                conn,
                "auth_sessions",
                "display_name",
                "TEXT NOT NULL DEFAULT ''",
            )
            _add_column_if_missing(
                conn,
                "auth_sessions",
                "auth_provider",
                "TEXT NOT NULL DEFAULT 'local'",
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires "
                "ON auth_sessions(expires_at)"
            )
            conn.commit()
        logger.info("Auth sessions table initialized")
    except sqlite3.Error as exc:
        logger.error("Auth session initialization failed: %s", exc)


def init_users(db_path: str) -> None:
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    email TEXT NOT NULL DEFAULT '',
                    organization_id TEXT NOT NULL DEFAULT 'default',
                    display_name TEXT NOT NULL DEFAULT ''
                )
                """
            )
            _add_column_if_missing(conn, "users", "email", "TEXT NOT NULL DEFAULT ''")
            _add_column_if_missing(
                conn,
                "users",
                "organization_id",
                "TEXT NOT NULL DEFAULT 'default'",
            )
            _add_column_if_missing(
                conn,
                "users",
                "display_name",
                "TEXT NOT NULL DEFAULT ''",
            )
            conn.executemany(
                """
                INSERT OR IGNORE INTO users (username, password_hash, role, tenant_id)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (username, hash_password(DEFAULT_MOCK_PASSWORD), role, tenant_id)
                    for username, role, tenant_id in DEFAULT_USERS
                ],
            )
            conn.commit()
        init_auth_sessions(db_path)
        logger.info("Users table initialized with default mock accounts")
    except sqlite3.Error as exc:
        logger.error("User initialization failed: %s", exc)


def _session_payload(user: dict[str, Any]) -> dict[str, Any]:
    organization_id = str(
        user.get("organization_id")
        or user.get("tenant_id")
        or DEFAULT_ORGANIZATION_ID
    )
    return {
        "id": int(user["id"]),
        "username": str(user["username"]),
        "role": str(user["role"]),
        "tenant_id": str(user.get("tenant_id", DEFAULT_TENANT_ID)),
        "organization_id": organization_id,
        "email": str(user.get("email") or ""),
        "display_name": str(user.get("display_name") or user.get("username") or ""),
        "auth_provider": str(user.get("auth_provider") or "local"),
        "company_name": str(user.get("company_name") or ""),
    }


def provision_sso_user(
    db_path: str,
    *,
    email: str,
    name: str,
    organization_id: str,
    role: str = ROLE_VIEWER,
) -> dict[str, Any]:
    """Create or update a user provisioned from an external identity provider."""
    normalized_email = email.strip().lower()
    org_id = str(organization_id or organization_id_from_email(normalized_email)).strip()
    display_name = name.strip() or normalized_email.split("@", 1)[0]
    username = normalized_email

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        existing = conn.execute(
            """
            SELECT id, username, role, tenant_id, email, organization_id, display_name
            FROM users
            WHERE username = ? OR email = ?
            """,
            (username, normalized_email),
        ).fetchone()

        if existing is None:
            cursor = conn.execute(
                """
                INSERT INTO users (
                    username, password_hash, role, tenant_id, email, organization_id, display_name
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    username,
                    hash_password(secrets.token_urlsafe(32)),
                    role,
                    org_id,
                    normalized_email,
                    org_id,
                    display_name,
                ),
            )
            user_id = int(cursor.lastrowid)
            assigned_role = role
            tenant_id = org_id
        else:
            user_id = int(existing["id"])
            assigned_role = str(existing["role"])
            tenant_id = str(existing["tenant_id"] or org_id)
            conn.execute(
                """
                UPDATE users
                SET email = ?, organization_id = ?, display_name = ?, tenant_id = ?
                WHERE id = ?
                """,
                (normalized_email, org_id, display_name, org_id, user_id),
            )
        conn.commit()

    return {
        "id": user_id,
        "username": username,
        "role": assigned_role,
        "tenant_id": tenant_id,
        "organization_id": org_id,
        "email": normalized_email,
        "display_name": display_name,
        "auth_provider": "sso",
    }


def create_session(db_path: str, user: dict[str, Any]) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = time.time() + SESSION_TTL_SEC
    session_user = _session_payload(user)
    session_user["expires_at"] = expires_at
    auth_provider = str(user.get("auth_provider") or "local")

    with _session_lock:
        _sessions[token] = dict(session_user)

    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO auth_sessions (
                    token, user_id, username, role, tenant_id, organization_id,
                    email, display_name, auth_provider, expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    token,
                    session_user["id"],
                    session_user["username"],
                    session_user["role"],
                    session_user["tenant_id"],
                    session_user["organization_id"],
                    session_user["email"],
                    session_user["display_name"],
                    auth_provider,
                    expires_at,
                ),
            )
            conn.commit()
    except sqlite3.Error as exc:
        logger.error("Failed to persist auth session: %s", exc)
    return token


def get_session_user(db_path: str, token: str | None) -> dict[str, Any] | None:
    if not token:
        return None

    with _session_lock:
        session = _sessions.get(token)
        if session and time.time() <= float(session["expires_at"]):
            return _session_payload(session)

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT user_id, username, role, tenant_id, organization_id, email,
                       display_name, auth_provider, expires_at
                FROM auth_sessions
                WHERE token = ?
                """,
                (token,),
            ).fetchone()
        if row is None:
            return None
        if time.time() > float(row["expires_at"]):
            destroy_session(db_path, token)
            return None

        session = {
            "id": int(row["user_id"]),
            "username": str(row["username"]),
            "role": str(row["role"]),
            "tenant_id": str(row["tenant_id"]),
            "organization_id": str(row["organization_id"] or row["tenant_id"] or DEFAULT_ORGANIZATION_ID),
            "email": str(row["email"] or ""),
            "display_name": str(row["display_name"] or row["username"]),
            "auth_provider": str(row["auth_provider"] or "local"),
            "expires_at": float(row["expires_at"]),
        }
        with _session_lock:
            _sessions[token] = session
        return _session_payload(session)
    except sqlite3.Error as exc:
        logger.error("Auth session lookup failed: %s", exc)
        return None


def destroy_session(db_path: str, token: str | None) -> None:
    if not token:
        return
    with _session_lock:
        _sessions.pop(token, None)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM auth_sessions WHERE token = ?", (token,))
            conn.commit()
    except sqlite3.Error as exc:
        logger.error("Failed to destroy auth session: %s", exc)


def role_is_allowed(role: str, allowed_roles: set[str]) -> bool:
    return role in allowed_roles


def is_global_admin(user: dict[str, Any] | None) -> bool:
    return bool(user and user.get("role") == ROLE_ADMIN)


def resolve_query_tenant_id(user: dict[str, Any] | None) -> str | None:
    """Return a tenant filter for reads, or None when the caller has global visibility."""
    if user is None or is_global_admin(user):
        return None
    return str(user.get("tenant_id", DEFAULT_TENANT_ID))


def resolve_session_organization_id(user: dict[str, Any] | None) -> str:
    """Return the active organization scope stored in the authenticated session."""
    if user is None:
        return DEFAULT_ORGANIZATION_ID
    return str(user.get("organization_id") or user.get("tenant_id") or DEFAULT_ORGANIZATION_ID)

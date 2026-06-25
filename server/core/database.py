"""Database path configuration, schema initialization, and tenant-scoped data access."""

import os
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker

# Absolute container path for Kubernetes PVC mount at /data (legacy sqlite telemetry + auth)
DATA_DIR = os.environ.get("OMNIKUBE_DATA_DIR", "/data")
DB_FILENAME = "omnikube.db"
CONFIG_FILENAME = "omnikube-config.json"

DB_PATH = os.path.join(DATA_DIR, DB_FILENAME)
CONFIG_PATH = os.path.join(DATA_DIR, CONFIG_FILENAME)

DEFAULT_ORGANIZATION_ID = "default"

# SQLAlchemy-compatible URI for legacy sqlite helpers (production /data volume)
SQLALCHEMY_DATABASE_URI = f"sqlite:///{DB_PATH.replace(os.sep, '/')}"

# ORM sqlite database — local development / test store under server/.test_data/
_SERVER_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORM_DATA_DIR = os.path.join(_SERVER_ROOT, ".test_data")
ORM_DB_PATH = os.path.join(ORM_DATA_DIR, DB_FILENAME)
ORM_DATABASE_URI = f"sqlite:///{ORM_DB_PATH.replace(os.sep, '/')}"


class Base(DeclarativeBase):
    """Declarative base for OmniKube ORM models."""


class User(Base):
    """Platform user account."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    subscription_tier: Mapped[str] = mapped_column(String(64), nullable=False, default="developer")
    subscription_status: Mapped[str] = mapped_column(String(64), nullable=False, default="inactive")
    stripe_customer_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    stripe_subscription_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    clusters: Mapped[list["Cluster"]] = relationship(
        "Cluster",
        back_populates="user",
        cascade="all, delete-orphan",
    )


class Cluster(Base):
    """Registered Kubernetes cluster linked to a platform user."""

    __tablename__ = "clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    cluster_name: Mapped[str] = mapped_column(String(255), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False, default="Kind")
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="pending")
    connected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped["User"] = relationship("User", back_populates="clusters")
    metrics: Mapped[list["ClusterMetrics"]] = relationship(
        "ClusterMetrics",
        back_populates="cluster",
        cascade="all, delete-orphan",
    )
    cost_recommendations: Mapped[list["CostRecommendations"]] = relationship(
        "CostRecommendations",
        back_populates="cluster",
        cascade="all, delete-orphan",
    )


class ClusterMetrics(Base):
    """Point-in-time utilization sample for a registered cluster."""

    __tablename__ = "cluster_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cluster_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("clusters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )
    cpu_utilization: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    memory_utilization: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    node_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    active_pods: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    cluster: Mapped["Cluster"] = relationship("Cluster", back_populates="metrics")


class CostRecommendations(Base):
    """FinOps optimization recommendation tied to a cluster."""

    __tablename__ = "cost_recommendations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cluster_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("clusters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    type: Mapped[str] = mapped_column(String(128), nullable=False)
    current_monthly_cost: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    projected_monthly_cost: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    potential_savings: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="Active")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    cluster: Mapped["Cluster"] = relationship("Cluster", back_populates="cost_recommendations")


_engine: Engine | None = None
SessionLocal: sessionmaker[Session] | None = None


def ensure_orm_data_directory() -> None:
    """Create the ORM sqlite directory (server/.test_data)."""
    os.makedirs(ORM_DATA_DIR, exist_ok=True)


def get_engine() -> Engine:
    """Return the shared SQLAlchemy engine bound to server/.test_data/omnikube.db."""
    global _engine
    if _engine is None:
        ensure_orm_data_directory()
        _engine = create_engine(
            ORM_DATABASE_URI,
            connect_args={"check_same_thread": False},
            future=True,
        )
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    """Return the configured SessionLocal sessionmaker."""
    global SessionLocal
    if SessionLocal is None:
        SessionLocal = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return SessionLocal


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """Context manager that yields a SQLAlchemy session with commit/rollback handling."""
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def migrate_user_billing_columns(db_path: str | None = None) -> None:
    """Add billing columns to ORM users table when upgrading existing databases."""
    path = db_path or ORM_DB_PATH
    if not os.path.isfile(path):
        return

    columns = {
        "subscription_tier": "TEXT NOT NULL DEFAULT 'developer'",
        "subscription_status": "TEXT NOT NULL DEFAULT 'inactive'",
        "stripe_customer_id": "TEXT",
        "stripe_subscription_id": "TEXT",
    }
    try:
        with sqlite3.connect(path) as conn:
            existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(users)").fetchall()}
            for column, definition in columns.items():
                if column in existing:
                    continue
                conn.execute(f"ALTER TABLE users ADD COLUMN {column} {definition}")
            conn.commit()
    except sqlite3.Error as exc:
        print(f"[OmniKube Database] User billing migration failed: {exc}")


def init_orm_tables(db_path: str | None = None) -> None:
    """Create ORM tables for User, Cluster, ClusterMetrics, and CostRecommendations."""
    if db_path is not None:
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        engine = create_engine(
            f"sqlite:///{db_path.replace(os.sep, '/')}",
            connect_args={"check_same_thread": False},
            future=True,
        )
        Base.metadata.create_all(bind=engine)
        migrate_user_billing_columns(db_path)
        return

    ensure_orm_data_directory()
    Base.metadata.create_all(bind=get_engine())
    migrate_user_billing_columns()
    print(
        f"[OmniKube Database] ORM tables ready at {ORM_DB_PATH} "
        "(users, clusters, cluster_metrics, cost_recommendations)"
    )


def ensure_data_directory() -> None:
    """Create the persistent data directory before any database bind or table creation."""
    os.makedirs(DATA_DIR, exist_ok=True)


def connect_db() -> sqlite3.Connection:
    """Open a SQLite connection to the persistent OmniKube database."""
    ensure_data_directory()
    return sqlite3.connect(DB_PATH)


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


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
    print(f"[OmniKube Database] Migrated {table}: added {column} column")


def init_cluster_metrics_schema(db_path: str | None = None) -> None:
    """Ensure cluster_metrics exists with organization-scoped isolation."""
    path = db_path or DB_PATH
    ensure_data_directory()
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cluster_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                cpu REAL NOT NULL,
                memory REAL NOT NULL,
                labels TEXT NOT NULL DEFAULT '{}',
                granularity TEXT NOT NULL DEFAULT 'raw',
                tenant_id TEXT NOT NULL DEFAULT 'default',
                organization_id TEXT NOT NULL DEFAULT 'default'
            )
            """
        )
        _add_column_if_missing(
            conn,
            "cluster_metrics",
            "labels",
            "TEXT NOT NULL DEFAULT '{}'",
        )
        _add_column_if_missing(
            conn,
            "cluster_metrics",
            "granularity",
            "TEXT NOT NULL DEFAULT 'raw'",
        )
        _add_column_if_missing(
            conn,
            "cluster_metrics",
            "tenant_id",
            "TEXT NOT NULL DEFAULT 'default'",
        )
        _add_column_if_missing(
            conn,
            "cluster_metrics",
            "organization_id",
            "TEXT NOT NULL DEFAULT 'default'",
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cluster_metrics_organization_id
            ON cluster_metrics(organization_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cluster_metrics_org_timestamp
            ON cluster_metrics(organization_id, id DESC)
            """
        )
        conn.commit()


def init_cluster_snapshots_schema(db_path: str | None = None) -> None:
    """Ensure the cluster_snapshots table exists for historical utilization tracking."""
    path = db_path or DB_PATH
    ensure_data_directory()
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cluster_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                organization_id TEXT NOT NULL DEFAULT 'default',
                cluster_id TEXT NOT NULL DEFAULT 'omnikube-cluster',
                node_count INTEGER NOT NULL DEFAULT 0,
                pod_count INTEGER NOT NULL DEFAULT 0,
                cpu_utilization REAL NOT NULL DEFAULT 0,
                memory_utilization REAL NOT NULL DEFAULT 0
            )
            """
        )
        _add_column_if_missing(
            conn,
            "cluster_snapshots",
            "organization_id",
            "TEXT NOT NULL DEFAULT 'default'",
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cluster_snapshots_org_timestamp
            ON cluster_snapshots(organization_id, id DESC)
            """
        )
        conn.commit()


def init_system_configs_schema(db_path: str | None = None) -> None:
    """Ensure system_configs is organization-scoped."""
    path = db_path or DB_PATH
    ensure_data_directory()
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS system_configs (
                organization_id TEXT NOT NULL DEFAULT 'default',
                config_key TEXT NOT NULL,
                config_value TEXT NOT NULL,
                PRIMARY KEY (organization_id, config_key)
            )
            """
        )
        columns = _table_columns(conn, "system_configs")
        if columns and "organization_id" not in columns and "config_key" in columns:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS system_configs__org_migration (
                    organization_id TEXT NOT NULL DEFAULT 'default',
                    config_key TEXT NOT NULL,
                    config_value TEXT NOT NULL,
                    PRIMARY KEY (organization_id, config_key)
                )
                """
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO system_configs__org_migration
                    (organization_id, config_key, config_value)
                SELECT 'default', config_key, config_value
                FROM system_configs
                """
            )
            conn.execute("DROP TABLE system_configs")
            conn.execute(
                "ALTER TABLE system_configs__org_migration RENAME TO system_configs"
            )
            print("[OmniKube Database] Migrated system_configs: added organization_id scope")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_system_configs_organization_id
            ON system_configs(organization_id)
            """
        )
        conn.commit()


def init_core_data_schemas(db_path: str | None = None) -> None:
    """Initialize all organization-scoped core telemetry and configuration tables."""
    init_cluster_metrics_schema(db_path)
    init_cluster_snapshots_schema(db_path)
    init_system_configs_schema(db_path)
    init_orm_tables()


def insert_cluster_snapshot(
    db_path: str,
    *,
    node_count: int,
    pod_count: int,
    cpu_utilization: float,
    memory_utilization: float,
    organization_id: str,
    tenant_id: str | None = None,
    cluster_id: str = "omnikube-cluster",
    timestamp: str | None = None,
) -> None:
    """Persist a cluster resource utilization snapshot for one organization."""
    init_cluster_snapshots_schema(db_path)
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    tenant = str(tenant_id or org_id).strip() or org_id
    if timestamp is None:
        timestamp = utc_timestamp()

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO cluster_snapshots (
                timestamp, tenant_id, organization_id, cluster_id,
                node_count, pod_count, cpu_utilization, memory_utilization
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                tenant,
                org_id,
                cluster_id,
                int(node_count),
                int(pod_count),
                round(float(cpu_utilization), 2),
                round(float(memory_utilization), 2),
            ),
        )
        conn.commit()


def fetch_cluster_snapshots(
    db_path: str,
    organization_id: str,
    *,
    hours: int = 24,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    """Return cluster snapshots for an organization within the last N hours."""
    init_cluster_snapshots_schema(db_path)
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    id, timestamp, tenant_id, organization_id, cluster_id,
                    node_count, pod_count, cpu_utilization, memory_utilization
                FROM cluster_snapshots
                WHERE organization_id = ?
                  AND datetime(substr(timestamp, 1, 19)) >= datetime('now', ?)
                ORDER BY id DESC
                LIMIT ?
                """,
                (org_id, f"-{int(hours)} hours", limit),
            ).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.Error:
        return []


def fetch_cluster_metrics_history(
    db_path: str,
    organization_id: str,
    *,
    limit: int = 500,
    timeframe_sql_offset: str | None = None,
    granularity: str | None = None,
) -> list[dict[str, Any]]:
    """Return cluster metric rows scoped to a single organization."""
    init_cluster_metrics_schema(db_path)
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    clauses = ["organization_id = ?"]
    params: list[Any] = [org_id]

    if timeframe_sql_offset:
        clauses.append("datetime(substr(timestamp, 1, 19)) >= datetime('now', ?)")
        params.append(timeframe_sql_offset)
    if granularity:
        clauses.append("granularity = ?")
        params.append(granularity)

    where_sql = " AND ".join(clauses)
    params.append(int(limit))

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT
                    id, timestamp, cpu, memory, labels, granularity,
                    tenant_id, organization_id
                FROM cluster_metrics
                WHERE {where_sql}
                ORDER BY id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.Error:
        return []


def fetch_cluster_metrics_analytics(
    db_path: str,
    organization_id: str,
) -> dict[str, Any]:
    """Aggregate cluster metric statistics for one organization."""
    init_cluster_metrics_schema(db_path)
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT
                    MAX(cpu),
                    AVG(cpu),
                    MAX(memory),
                    AVG(memory),
                    COUNT(*)
                FROM cluster_metrics
                WHERE organization_id = ?
                """,
                (org_id,),
            ).fetchone()
        if not row or int(row[4] or 0) == 0:
            return {
                "max_cpu": 0.0,
                "avg_cpu": 0.0,
                "max_memory": 0.0,
                "avg_memory": 0.0,
                "sample_count": 0,
            }
        return {
            "max_cpu": round(float(row[0]), 1),
            "avg_cpu": round(float(row[1]), 1),
            "max_memory": round(float(row[2]), 1),
            "avg_memory": round(float(row[3]), 1),
            "sample_count": int(row[4]),
        }
    except sqlite3.Error:
        return {
            "max_cpu": 0.0,
            "avg_cpu": 0.0,
            "max_memory": 0.0,
            "avg_memory": 0.0,
            "sample_count": 0,
        }


def fetch_latest_cluster_metric(
    db_path: str,
    organization_id: str,
) -> dict[str, Any] | None:
    """Return the newest cluster metric row for an organization."""
    rows = fetch_cluster_metrics_history(db_path, organization_id, limit=1)
    return rows[0] if rows else None

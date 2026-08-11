"""
Tests for src/database/postgres.py.

Split deliberately into two groups:

- Unit tests: no real database needed, run in every environment (CI, local,
  no PostgreSQL installed). These check the SQL files and pure logic.

- Integration tests: require a real, reachable PostgreSQL instance with the
  schema from sql/create_tables.sql already applied. They are skipped
  automatically if the DB isn't reachable, rather than failing the whole
  suite -- see docs/testing.md for how to run them manually.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.database.postgres import get_connection

REPO_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_create_tables_sql_defines_events_table():
    sql = (REPO_ROOT / "sql" / "create_tables.sql").read_text()
    assert "CREATE TABLE" in sql
    assert "events" in sql
    assert "PRIMARY KEY" in sql  # event_id uniqueness / duplicate protection


def test_create_tables_sql_has_expected_columns():
    sql = (REPO_ROOT / "sql" / "create_tables.sql").read_text()
    expected_columns = [
        "event_id", "user_id", "product_id", "event_type",
        "event_timestamp", "quantity", "price", "total_amount", "ingested_at",
    ]
    for column in expected_columns:
        assert column in sql, f"Missing expected column: {column}"


def test_validation_queries_file_is_nonempty_and_has_select_statements():
    sql = (REPO_ROOT / "sql" / "validation_queries.sql").read_text()
    assert sql.strip() != ""
    assert sql.upper().count("SELECT") >= 5


# ---------------------------------------------------------------------------
# Integration tests (require a live PostgreSQL instance)
# ---------------------------------------------------------------------------

def _db_available() -> bool:
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


requires_db = pytest.mark.skipif(
    not _db_available(),
    reason="No reachable PostgreSQL instance configured via .env -- see docs/testing.md",
)


@requires_db
def test_events_table_exists():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'events')"
            )
            (exists,) = cur.fetchone()
            assert exists
    finally:
        conn.close()


@requires_db
def test_duplicate_event_id_is_rejected():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events (event_id, user_id, product_id, event_type,
                                     event_timestamp, product_name, quantity, price, total_amount)
                VALUES ('test-dup-id', 1, 1, 'view', NOW(), 'Test Product', 0, 1.00, 0.00)
                ON CONFLICT (event_id) DO NOTHING
                """
            )
            conn.commit()

            with pytest.raises(Exception):
                cur.execute(
                    """
                    INSERT INTO events (event_id, user_id, product_id, event_type,
                                         event_timestamp, product_name, quantity, price, total_amount)
                    VALUES ('test-dup-id', 1, 1, 'view', NOW(), 'Test Product', 0, 1.00, 0.00)
                    """
                )
            conn.rollback()
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM events WHERE event_id = 'test-dup-id'")
            conn.commit()
        conn.close()

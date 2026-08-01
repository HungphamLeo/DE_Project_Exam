"""
platforms/storage/postgre/postgres_client.py
─────────────────────────────────────────────
psycopg2 wrapper cho assessment database.

Cung cấp:
  - execute()          : chạy DML / DDL với auto-commit hoặc trong transaction
  - execute_many()     : executemany cho batch insert
  - execute_sql_file() : đọc và chạy file .sql (schema.sql, load.sql)
  - insert()           : batch insert đơn giản (dùng cho SCD2 đã pre-filter)
  - upsert_lookup()    : INSERT ... ON CONFLICT DO UPDATE cho SCD1 dims
  - insert_ignore()    : INSERT ... ON CONFLICT DO NOTHING cho fact tables

Logger: logger.storage_log.postgre (đã cấu hình trong logger_config.yaml)
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from shared.utils.de_assessment_utils import PostgresConfig
from shared.log.logger import LoggerManager


# ── Module-level logger (singleton, cấu hình từ logger_config.yaml) ────────
_logger_manager = LoggerManager()
_LOG = _logger_manager.get_logger("logger.storage_log.postgre")


class PostgresClient:
    """
    Thread-safe psycopg2 client với connection pooling.

    Cách dùng:
        client = PostgresClient.from_config(cfg.postgres)
        with client.connection() as conn:
            client.execute(conn, "SELECT 1")

    Hoặc convenience method:
        client.run("INSERT INTO ...", params)
    """

    def __init__(self, config: PostgresConfig) -> None:
        self._config = config
        self._pool: Optional[ThreadedConnectionPool] = None
        self._connect()

    # ── Connection pool ────────────────────────────────────────────────────

    def _connect(self) -> None:
        for attempt in range(1, self._config.retry_attempts + 1):
            try:
                self._pool = ThreadedConnectionPool(
                    minconn=self._config.min_conn,
                    maxconn=self._config.max_conn,
                    dsn=self._config.dsn,
                )
                _LOG.info(
                    f"[PostgresClient] Connected pool → "
                    f"{self._config.host}:{self._config.port}/{self._config.db}"
                )
                return
            except psycopg2.OperationalError as exc:
                _LOG.warning(
                    f"[PostgresClient] Connection attempt {attempt}/{self._config.retry_attempts} failed: {exc}"
                )
                if attempt < self._config.retry_attempts:
                    time.sleep(self._config.retry_delay_s)
        raise ConnectionError(
            f"[PostgresClient] Không thể kết nối PostgreSQL sau {self._config.retry_attempts} lần thử."
        )

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()
            _LOG.info("[PostgresClient] Connection pool closed.")

    # ── Context manager cho connection ────────────────────────────────────

    @contextmanager
    def connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Lấy connection từ pool; tự trả về pool sau khi dùng xong."""
        assert self._pool is not None, "Pool chưa được khởi tạo"
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    # ── Core execute methods ───────────────────────────────────────────────

    def execute(
        self,
        conn: psycopg2.extensions.connection,
        sql: str,
        params: Optional[Tuple | Dict] = None,
    ) -> Optional[List[Tuple]]:
        """
        Chạy một câu SQL đơn.
        Trả về rows nếu là SELECT, None nếu là DML/DDL.
        """
        _LOG.debug(f"[execute] {sql[:120].strip()}")
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                return cur.fetchall()
            return None

    def execute_many(
        self,
        conn: psycopg2.extensions.connection,
        sql: str,
        params_seq: Sequence[Tuple | Dict],
    ) -> int:
        """
        Batch insert/update bằng executemany.
        Trả về số rows affected.
        """
        if not params_seq:
            return 0
        _LOG.debug(f"[execute_many] {sql[:80].strip()} — {len(params_seq)} rows")
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, params_seq, page_size=500)
            return cur.rowcount

    def execute_sql_file(self, sql_path: str | Path) -> None:
        """
        Đọc và chạy toàn bộ file .sql (schema.sql, seed data...).
        Dùng autocommit để DDL không cần explicit COMMIT.
        """
        sql_path = Path(sql_path)
        _LOG.info(f"[execute_sql_file] Chạy file: {sql_path}")
        sql_text = sql_path.read_text(encoding="utf-8")

        with self.connection() as conn:
            old_autocommit = conn.autocommit
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute(sql_text)
                _LOG.info(f"[execute_sql_file] Hoàn thành: {sql_path.name}")
            finally:
                conn.autocommit = old_autocommit

    # ── Convenience methods ────────────────────────────────────────────────

    def run(
        self,
        sql: str,
        params: Optional[Tuple | Dict] = None,
    ) -> Optional[List[Tuple]]:
        """Shorthand: lấy connection, chạy sql, trả về kết quả."""
        with self.connection() as conn:
            return self.execute(conn, sql, params)

    def insert(
        self,
        conn: psycopg2.extensions.connection,
        table: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        """
        Thực hiện batch insert đơn giản.
        Dùng cho SCD2 dims khi chỉ insert các bản ghi mới đã được lọc ở Python.

        Returns:
            Số rows đã xử lý.
        """
        if not rows:
            return 0

        cols = list(rows[0].keys())
        col_list = ", ".join(cols)
        val_list = ", ".join(f"%({c})s" for c in cols)

        sql = f"INSERT INTO {table} ({col_list}) VALUES %s"
        _LOG.debug(f"[insert] {table} — {len(rows)} rows")

        with conn.cursor() as cur:
            # Chuyển sang execute_values để có rowcount chính xác
            values = [tuple(r[c] for c in cols) for r in rows]
            psycopg2.extras.execute_values(cur, sql, values, page_size=500)
            return cur.rowcount

    def upsert_lookup(
        self,
        conn: psycopg2.extensions.connection,
        table: str,
        rows: List[Dict[str, Any]],
        conflict_cols: List[str],
        update_cols: List[str],
    ) -> int:
        """
        INSERT ... ON CONFLICT (...) DO UPDATE SET ... cho lookup dims (SCD1).

        Args:
            table:         tên bảng đầy đủ, vd "staging.dim_vendor"
            rows:          list of dicts, key = tên cột
            conflict_cols: list cột conflict target, vd ["vendor_id"]
            update_cols:   list cột cần update khi conflict, vd ["vendor_name", "updated_at"]

        Returns:
            Số rows xử lý.
        """
        if not rows:
            return 0

        cols = list(rows[0].keys())
        col_list   = ", ".join(cols)
        val_list   = ", ".join(f"%({c})s" for c in cols)
        conflict   = ", ".join(conflict_cols)
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        sql = f"INSERT INTO {table} ({col_list}) VALUES %s ON CONFLICT ({conflict}) DO UPDATE SET {update_set}"
        _LOG.debug(f"[upsert_lookup] {table} — {len(rows)} rows")

        with conn.cursor() as cur:
            values = [tuple(r[c] for c in cols) for r in rows]
            psycopg2.extras.execute_values(cur, sql, values, page_size=500)
            return cur.rowcount

    def insert_ignore(
        self,
        conn: psycopg2.extensions.connection,
        table: str,
        rows: List[Dict[str, Any]],
        conflict_col: str,
    ) -> int:
        """
        INSERT ... ON CONFLICT (conflict_col) DO NOTHING — idempotent insert.
        Dùng cho fact_trips và populate dimension tables.

        Returns:
            Số rows thực sự đã được insert.
        """
        if not rows:
            return 0

        cols     = list(rows[0].keys())
        col_list = ", ".join(cols)

        sql = f"INSERT INTO {table} ({col_list}) VALUES %s ON CONFLICT ({conflict_col}) DO NOTHING"
        _LOG.debug(f"[insert_ignore] {table} — {len(rows)} rows")

        with conn.cursor() as cur:
            values = [tuple(r[c] for c in cols) for r in rows]
            psycopg2.extras.execute_values(cur, sql, values, page_size=500)
            return cur.rowcount

    def update_batch(
        self,
        conn: psycopg2.extensions.connection,
        table: str,
        updates: Dict[str, Any],
        where_col: str,
        where_values: List[Any],
    ) -> int:
        """
        Thực hiện batch update hiệu quả.
        VD: UPDATE table SET col1=%s, col2=%s WHERE id = ANY(%s)
        """
        if not where_values or not updates:
            return 0

        set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_col} = ANY(%s)"

        # Sắp xếp các giá trị cho SET clause, giá trị cho ANY là cuối cùng
        params = list(updates.values()) + [where_values]

        _LOG.debug(f"[update_batch] {table} — {len(where_values)} keys")
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    # ── Factory ────────────────────────────────────────────────────────────

    @classmethod
    def from_config(cls, config: PostgresConfig) -> "PostgresClient":
        return cls(config)

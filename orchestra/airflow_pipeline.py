"""
orchestra/airflow_pipeline.py
──────────────────────────────
ETL Pipeline DAG — DE Assessment

Tasks:
  task_ingest_bronze : CSV → MinIO s3://bronze/raw/events/  (landing zone)
  task_load_staging  : MinIO bronze → PostgreSQL staging schema
                       (dims + fact_trips)

Idempotency:
  - batch_id = {{ ds }} (Airflow execution date)
  - Bronze ghi partition ingest_date=<ds>, re-run ghi đè partition đó
  - Dims SCD1 : ON CONFLICT DO UPDATE
  - Dims SCD2 : WHERE NOT EXISTS (is_current=TRUE) → chỉ insert NK mới
  - fact_trips: ON CONFLICT (event_id) DO NOTHING

Cấu trúc nội bộ file:
  [1] Imports & constants
  [2] StagingProcessor  — class xử lý toàn bộ staging logic
  [3] build_staging_processor() — factory khởi tạo processor từ EnvConfig
  [4] Task callables    — ingest_bronze(), load_staging()
  [5] DAG definition
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import polars as pl
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
import time

from airflow import DAG
from airflow.operators.python import PythonOperator

from platforms.processing.polars.base_processing import BasepolarssProcessor
from platforms.processing.polars.polars_engine import PolarsEngine, PolarsConfig
from platforms.storage.postgre.postgres_client import PostgresClient
from platforms.ingestion.raw_ingestor import build_bronze_ingestor
from shared.utils.de_assessment_utils import EnvConfig
from shared.log.logger import LoggerManager


# ── Schema SQL — nằm trong platforms/schema_manage/ (SRP) ─────────────────
_SCHEMA_SQL = (
    Path(__file__).resolve().parents[1]  # project root
    / "platforms"
    / "schema_manage"
    / "de_assessment_schema.sql"
)

# ── Bronze Parquet schema (audit cols từ BronzeIngestor) ───────────────────
_BRONZE_SCHEMA = {
    "event_id":         pl.Utf8,
    "event_timestamp":  pl.Utf8,
    "entity_id":        pl.Int32,
    "zone_id":          pl.Int32,
    "destination_id":   pl.Int32,
    "vendor_id":        pl.Int16,
    "event_type":       pl.Utf8,
    "rate_type":        pl.Int16,
    "duration":         pl.Int32,
    "passenger_count":  pl.Float32,
    "value":            pl.Float64,
    "sub_value":        pl.Float64,
    "total_value":      pl.Float64,
    "payment_method":   pl.Utf8,
    "ingest_timestamp": pl.Utf8,
    "ingest_date":      pl.Utf8,
    "batch_id":         pl.Utf8,
    "source_system":    pl.Utf8,
    "_source_file":     pl.Utf8,
}


# ===========================================================================
# [2] StagingProcessor
# ===========================================================================

class StagingProcessor(BasepolarssProcessor):
    """
    Đọc Parquet từ MinIO bronze → transform → load vào PostgreSQL staging.

    Thứ tự load:
      SCD1 dims → dim_vendor, dim_rate, dim_event_type, dim_payment_method
      SCD2 dims → dim_zone, dim_destination, dim_entity
      Fact      → fact_trips  (append-only)
    """

    def __init__(self, engine: PolarsEngine, pg_client: PostgresClient) -> None:
        super().__init__(engine)
        self._pg = pg_client

    # ── Entry point ────────────────────────────────────────────────────────

    def process(
        self,
        bronze_path: str,
        batch_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Chạy toàn bộ staging pipeline.

        Args:
            bronze_path: S3 path tới partition bronze,
                         vd "s3://bronze/raw/events/ingest_date=2024-10-01/"
            batch_id:    Airflow ds ({{ ds }}) hoặc tự sinh nếu None.

        Returns:
            Dict thống kê số rows đã xử lý mỗi bảng.
        """
        batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self.logger.info(
            f"[StagingProcessor] START batch_id={batch_id} path={bronze_path}"
        )

        # 0. Tạo schema nếu chưa có (idempotent)
        self._ensure_schema()

        # 1. Đọc Parquet từ MinIO
        df = self.engine.read_parquet(bronze_path).collect()
        self.logger.info(f"[StagingProcessor] {len(df):,} rows from bronze.")

        stats: Dict[str, int] = {}
        with self._pg.connection() as conn:
            # 2. SCD1 lookup dims
            stats["dim_vendor"]         = self._load_dim_vendor(conn, df)
            stats["dim_rate"]           = self._load_dim_rate(conn, df)
            stats["dim_event_type"]     = self._load_dim_event_type(conn, df)
            stats["dim_payment_method"] = self._load_dim_payment_method(conn, df)
            # 3. SCD2 dims
            stats["dim_zone"]        = self._load_dim_zone(conn, df)
            stats["dim_destination"] = self._load_dim_destination(conn, df)
            stats["dim_entity"]      = self._load_dim_entity(conn, df)
            # 4. Fact (append-only)
            stats["fact_trips"] = self._load_fact_trips(conn, df, batch_id)

        self.logger.info(f"[StagingProcessor] DONE stats={stats}")
        return stats

    # ── Schema bootstrap ───────────────────────────────────────────────────

    def _ensure_schema(self) -> None:
        """Chạy de_assessment_schema.sql — CREATE TABLE IF NOT EXISTS (idempotent)."""
        if _SCHEMA_SQL.exists():
            self.logger.info(f"[StagingProcessor] Applying schema: {_SCHEMA_SQL}")
            self._pg.execute_sql_file(_SCHEMA_SQL)
        else:
            self.logger.warning(
                f"[StagingProcessor] Schema file not found: {_SCHEMA_SQL}. "
                "Bỏ qua — giả sử bảng đã tồn tại."
            )

    # ── SCD1 dim loaders ───────────────────────────────────────────────────

    def _load_dim_vendor(self, conn, df: pl.DataFrame) -> int:
        rows = [
            {"vendor_id": int(v), "vendor_name": f"Vendor {v}",
             "updated_at": datetime.now(timezone.utc)}
            for v in df["vendor_id"].drop_nulls().unique().to_list()
        ]
        return self._pg.upsert_lookup(conn, "staging.dim_vendor", rows,
                                       ["vendor_id"], ["vendor_name", "updated_at"])

    def _load_dim_rate(self, conn, df: pl.DataFrame) -> int:
        _names = {1: "Standard Rate", 2: "JFK", 3: "Newark",
                  4: "Nassau or Westchester", 5: "Negotiated Fare"}
        rows = [
            {"rate_type": int(r), "rate_name": _names.get(int(r), f"Rate {r}"),
             "updated_at": datetime.now(timezone.utc)}
            for r in df["rate_type"].drop_nulls().unique().to_list()
        ]
        return self._pg.upsert_lookup(conn, "staging.dim_rate", rows,
                                       ["rate_type"], ["rate_name", "updated_at"])

    def _load_dim_event_type(self, conn, df: pl.DataFrame) -> int:
        rows = [
            {"event_type_name": e, "updated_at": datetime.now(timezone.utc)}
            for e in df["event_type"].drop_nulls().unique().to_list()
        ]
        return self._pg.upsert_lookup(conn, "staging.dim_event_type", rows,
                                       ["event_type_name"], ["updated_at"])

    def _load_dim_payment_method(self, conn, df: pl.DataFrame) -> int:
        rows = [
            {"payment_method_name": p, "updated_at": datetime.now(timezone.utc)}
            for p in df["payment_method"].drop_nulls().unique().to_list()
        ]
        return self._pg.upsert_lookup(conn, "staging.dim_payment_method", rows,
                                       ["payment_method_name"], ["updated_at"])

    # ── SCD2 dim loaders ───────────────────────────────────────────────────

    def _load_dim_zone(self, conn, df: pl.DataFrame) -> int:
        now = datetime.now(timezone.utc)
        existing = {r[0] for r in (
            self._pg.run("SELECT zone_id FROM staging.dim_zone WHERE is_current = TRUE") or []
        )}
        rows = [
            {"zone_id": int(z), "zone_name": None, "borough": None,
             "valid_from": now, "valid_to": None, "is_current": True, "created_at": now}
            for z in df["zone_id"].drop_nulls().unique().to_list()
            if int(z) not in existing
        ]
        return self._pg.insert_ignore(conn, "staging.dim_zone", rows, "zone_id")

    def _load_dim_destination(self, conn, df: pl.DataFrame) -> int:
        now = datetime.now(timezone.utc)
        existing = {r[0] for r in (
            self._pg.run("SELECT destination_id FROM staging.dim_destination WHERE is_current = TRUE") or []
        )}
        zone_map: Dict[int, int] = {
            r[0]: r[1] for r in (
                self._pg.run("SELECT zone_id, zone_key FROM staging.dim_zone WHERE is_current = TRUE") or []
            )
        }
        rows = [
            {"destination_id": int(d), "zone_key": zone_map.get(int(d)),
             "destination_name": None, "valid_from": now, "valid_to": None,
             "is_current": True, "created_at": now}
            for d in df["destination_id"].drop_nulls().unique().to_list()
            if int(d) not in existing
        ]
        return self._pg.insert_ignore(conn, "staging.dim_destination", rows, "destination_id")

    def _load_dim_entity(self, conn, df: pl.DataFrame) -> int:
        now = datetime.now(timezone.utc)
        existing = {r[0] for r in (
            self._pg.run("SELECT entity_id FROM staging.dim_entity WHERE is_current = TRUE") or []
        )}
        home_zone: Dict[int, int] = {
            row[0]: row[1] for row in (
                df.group_by(["entity_id", "zone_id"])
                .agg(pl.len().alias("cnt"))
                .sort("cnt", descending=True)
                .group_by("entity_id")
                .agg(pl.first("zone_id").alias("hz"))
                .rows()
            )
        }
        rows = [
            {"entity_id": eid, "zone_id": home_zone.get(eid),
             "valid_from": now, "valid_to": None, "is_current": True, "created_at": now}
            for eid in home_zone
            if eid not in existing
        ]
        return self._pg.insert_ignore(conn, "staging.dim_entity", rows, "entity_id")

    # ── Fact loader ────────────────────────────────────────────────────────

    def _load_fact_trips(self, conn, df: pl.DataFrame, batch_id: str) -> int:
        now = datetime.now(timezone.utc)

        def _map(sql: str) -> Dict:
            return {r[0]: r[1] for r in (self._pg.run(sql) or [])}

        entity_map  = _map("SELECT entity_id, entity_key FROM staging.dim_entity WHERE is_current=TRUE")
        zone_map    = _map("SELECT zone_id, zone_key FROM staging.dim_zone WHERE is_current=TRUE")
        dest_map    = _map("SELECT destination_id, destination_key FROM staging.dim_destination WHERE is_current=TRUE")
        vendor_map  = _map("SELECT vendor_id, vendor_key FROM staging.dim_vendor")
        rate_map    = _map("SELECT rate_type, rate_key FROM staging.dim_rate")
        etype_map   = _map("SELECT event_type_name, event_type_key FROM staging.dim_event_type")
        payment_map = _map("SELECT payment_method_name, payment_method_key FROM staging.dim_payment_method")

        existing_ids = {r[0] for r in (
            self._pg.run("SELECT event_id FROM staging.fact_trips WHERE _batch_id = %s",
                         (batch_id,)) or []
        )}

        rows: List[Dict[str, Any]] = []
        skipped = 0
        for row in df.iter_rows(named=True):
            eid = row["event_id"]
            if eid in existing_ids:
                skipped += 1
                continue
            try:
                ts = datetime.fromisoformat(row["event_timestamp"])
            except (ValueError, TypeError):
                self.logger.warning(f"[fact_trips] bad timestamp event_id={eid}, skip.")
                skipped += 1
                continue
            rows.append({
                "event_id":           eid,
                "event_timestamp":    ts,
                "entity_key":         entity_map.get(row["entity_id"]),
                "destination_key":    dest_map.get(row["destination_id"]),
                "vendor_key":         vendor_map.get(row["vendor_id"]),
                "rate_key":           rate_map.get(row["rate_type"]),
                "event_type_key":     etype_map.get(row["event_type"]),
                "payment_method_key": payment_map.get(row["payment_method"]),
                "zone_key":           zone_map.get(row["zone_id"]),
                "duration":           row["duration"],
                "passenger_count":    int(row["passenger_count"]) if row["passenger_count"] is not None else None,
                "value":              row["value"],
                "sub_value":          row["sub_value"],
                "total_value":        row["total_value"],
                "_inserted_at":       now,
                "_batch_id":          batch_id,
            })

        n = self._pg.insert_ignore(conn, "staging.fact_trips", rows, "event_id")
        self.logger.info(f"[fact_trips] inserted={n} skipped={skipped}")
        return n


# ===========================================================================
# [3] Factory
# ===========================================================================

def build_staging_processor(env: Optional[EnvConfig] = None) -> StagingProcessor:
    """
    Khởi tạo StagingProcessor từ EnvConfig (shared.utils.de_assessment_utils).
    Đây là điểm duy nhất toàn pipeline gọi EnvConfig để lấy MinIO + Postgres config.
    """
    cfg   = env or EnvConfig()
    minio = cfg.minio

    logger = LoggerManager().get_logger("logger.storage_log.postgre")
    logger.info(
        f"[build_staging_processor] minio={minio.endpoint_url} "
        f"bucket={minio.bucket_bronze} pg={cfg.postgres.host}/{cfg.postgres.db}"
    )

    engine = PolarsEngine(
        config=PolarsConfig(
            enable_streaming=True,
            storage_options={
                **minio.storage_options,
                "_bucket_bronze": minio.bucket_bronze,
                "_bucket_silver": minio.bucket_silver,
                "_bucket_gold":   minio.bucket_gold,
            },
        ),
        logger=logger,
    )
    return StagingProcessor(engine=engine, pg_client=PostgresClient.from_config(cfg.postgres))


# ===========================================================================
# [4] DAG default args
# ===========================================================================

_DEFAULT_ARGS = {
    "owner":            "de-assessment",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}


# ===========================================================================
# [5] Task callables
# ===========================================================================

def ingest_bronze(**context) -> str:
    """
    Phase 1: Đọc CSV nguồn → ghi Parquet lên MinIO bronze bucket.
    batch_id = ds → idempotent: re-run cùng ngày ghi đè partition cũ.
    """
    ds: str = context["ds"]
    cfg      = EnvConfig()
    ingestor = build_bronze_ingestor(env=cfg)

    written_path = ingestor.process(batch_id=ds, target_prefix="raw/events")
    context["ti"].xcom_push(key="bronze_path", value=written_path)
    return written_path


def load_staging(**context) -> dict:
    """
    Phase 2: Đọc Parquet từ MinIO bronze → load vào PostgreSQL staging.
    Nhận bronze_path từ XCom của task ingest_bronze.
    """
    ds: str      = context["ds"]
    bronze_path: str = context["ti"].xcom_pull(task_ids="ingest_bronze", key="bronze_path")

    if not bronze_path:
        cfg         = EnvConfig()
        bronze_path = f"s3://{cfg.minio.bucket_bronze}/raw/events/ingest_date={ds}/"

    cfg       = EnvConfig()
    processor = build_staging_processor(env=cfg)
    stats     = processor.process(bronze_path=bronze_path, batch_id=ds)

    context["ti"].xcom_push(key="staging_stats", value=stats)
    return stats


# ===========================================================================
# [6] DAG definition
# ===========================================================================

with DAG(
    dag_id="de_assessment_etl",
    description="ETL pipeline: CSV → Bronze (MinIO) → Staging (Postgres)",
    schedule="@daily",
    start_date=datetime(2024, 10, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["de-assessment", "bronze", "staging", "etl"],
) as dag:

    task_ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=ingest_bronze,
        doc_md="""
        **Bronze Layer — Landing Zone**
        - Source : `platforms/ingestion/source_data/de_assessment_data.csv`
        - Sink   : `s3://bronze/raw/events/ingest_date=<ds>/`
        - Format : Parquet, partition by `ingest_date`
        - Audit  : `ingest_timestamp`, `ingest_date`, `batch_id`, `source_system`, `_source_file`
        - Idempotent: re-run cùng ngày ghi đè partition cũ
        """,
    )

    task_load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=load_staging,
        doc_md="""
        **Staging Layer — Star Schema**
        Đọc Parquet từ MinIO bronze → PostgreSQL schema `staging`:

        | Bảng | SCD | Strategy |
        |------|-----|----------|
        | `dim_vendor` | Type 1 | UPSERT |
        | `dim_rate` | Type 1 | UPSERT |
        | `dim_event_type` | Lookup | UPSERT |
        | `dim_payment_method` | Lookup | UPSERT |
        | `dim_zone` | Type 2 | Insert new NKs only |
        | `dim_destination` | Type 2 | Insert new NKs only |
        | `dim_entity` | Type 2 | Insert new NKs only |
        | `fact_trips` | Append-only | ON CONFLICT DO NOTHING |
        """,
    )

    # task_ingest_bronze >> task_load_staging

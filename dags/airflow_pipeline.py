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
from platforms.ingestion.raw_ingestor import build_bronze_ingestor, BronzeIngestor
from shared.utils.de_assessment_utils import EnvConfig
from shared.log.logger import LoggerManager


# dags/airflow_pipeline.py:48-53 — thay parents[1] bằng Path của project mount
_SCHEMA_SQL = (
    Path("/opt/airflow/project")   # trỏ thẳng tới mount point
    / "platforms"
    / "schema_manage"
    / "de_assessment_schema.sql"
)

# ── Schema CSV gốc (raw, giữ nguyên kiểu TEXT cho event_timestamp) ─────────
_CSV_SCHEMA = {
    "event_id":         pl.Utf8,
    "event_timestamp":  pl.Utf8,       # giữ TEXT tại raw layer
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
        self.logger.info("[dim_zone] Inserting new zones if not exist...")
        rows = [
            {"zone_id": int(z)}
            for z in df["zone_id"].drop_nulls().unique().to_list()
        ]
        # Dùng INSERT ... ON CONFLICT DO NOTHING, dựa vào unique index trên (zone_id) WHERE is_current = TRUE
        # Hiệu quả hơn nhiều so với việc select-then-insert trong Python.
        n = self._pg.insert_ignore(conn, "staging.dim_zone", rows, "zone_id")
        self.logger.info(f"[dim_zone] Found {len(rows)} unique zones in batch. Inserted {n} new zones.")
        return n

    def _load_dim_destination(self, conn, df: pl.DataFrame) -> int:
        now = datetime.now(timezone.utc)
        self.logger.info("[dim_destination] Inserting new destinations if not exist...")
        # FIX: Logic cũ bị sai khi map destination_id với zone_key.
        # Cách làm đúng là chỉ insert các destination_id mới. zone_key sẽ là NULL
        # cho đến khi có dữ liệu bổ sung.
        rows = [
            {"destination_id": int(d)}
            for d in df["destination_id"].drop_nulls().unique().to_list()
        ]
        n = self._pg.insert_ignore(conn, "staging.dim_destination", rows, "destination_id")
        self.logger.info(f"[dim_destination] Found {len(rows)} unique destinations in batch. Inserted {n} new destinations.")
        return n

    def _load_dim_entity(self, conn, df: pl.DataFrame) -> int:
        now = datetime.now(timezone.utc)
        self.logger.info("[dim_entity] Processing SCD2 logic...")

        # 1. Tính home_zone mới từ batch dữ liệu hiện tại
        df_new_home_zones = (
            df.lazy()
            .group_by(["entity_id", "zone_id"])
            .agg(pl.len().alias("cnt"))
            .sort("cnt", descending=True)
            .group_by("entity_id")
            .agg(pl.first("zone_id").alias("new_zone_id"))
            .collect()
        )

        # 2. Lấy các bản ghi entity hiện tại từ DB
        current_entities_data = self._pg.run("SELECT entity_key, entity_id, zone_id FROM staging.dim_entity WHERE is_current = TRUE")
        if not current_entities_data:
            df_current_entities = pl.DataFrame(schema={"entity_key": pl.Int64, "entity_id": pl.Int32, "zone_id": pl.Int32})
        else:
            df_current_entities = pl.DataFrame(current_entities_data, schema=["entity_key", "entity_id", "zone_id"])

        # 3. Join để tìm ra các thay đổi và các bản ghi mới
        df_merged = df_new_home_zones.join(df_current_entities, on="entity_id", how="left")

        # 4. Xác định các bản ghi cần vô hiệu hóa (expire)
        df_to_expire = df_merged.filter(
            (pl.col("new_zone_id").is_not_null()) &
            (pl.col("zone_id").is_not_null()) &
            (pl.col("new_zone_id") != pl.col("zone_id"))
        )
        keys_to_expire = df_to_expire["entity_key"].drop_nulls().to_list()

        expired_count = 0
        if keys_to_expire:
            self.logger.info(f"[dim_entity] Expiring {len(keys_to_expire)} old records due to changed home_zone.")
            # FIX: Không gọi psycopg2 trực tiếp. Sử dụng phương thức đã đóng gói trong PostgresClient.
            expired_count = self._pg.update_batch(
                conn, "staging.dim_entity",
                updates={"is_current": False, "valid_to": now},
                where_col="entity_key", where_values=keys_to_expire)

        # 5. Xác định các bản ghi cần chèn mới (entity mới + phiên bản mới của entity đã thay đổi)
        df_to_insert = df_merged.filter(
            (pl.col("zone_id").is_null()) | # Entity mới
            (pl.col("entity_key").is_in(keys_to_expire)) # Entity có thay đổi
        ).select(
            pl.col("entity_id"),
            pl.col("new_zone_id").alias("zone_id")
        )

        rows = [
            {
                "entity_id": r["entity_id"],
                "zone_id": r["zone_id"],
                "valid_from": now,
                "valid_to": None,
                "is_current": True,
                "created_at": now
            }
            for r in df_to_insert.to_dicts() if r["entity_id"] is not None
        ]

        # 6. Chèn các bản ghi mới
        inserted_count = self._pg.insert(conn, "staging.dim_entity", rows)
        self.logger.info(f"[dim_entity] Inserted={inserted_count}, Expired={expired_count}")
        return inserted_count

    # ── Fact loader ────────────────────────────────────────────────────────

    def _load_fact_trips(self, conn, df: pl.DataFrame, batch_id: str) -> int:
        now = datetime.now(timezone.utc)
        self.logger.info("[fact_trips] Starting vectorized fact table load.")

        # 1. Load các bảng dimension dưới dạng LazyFrames để join hiệu quả
        def _load_dim_as_lf(query: str) -> pl.LazyFrame:
            # Polars đọc hiệu quả nhất qua connection string
            return pl.read_database(query=query, connection=self._pg._config.conn_string).lazy()

        lf_entity = _load_dim_as_lf("SELECT entity_key, entity_id FROM staging.dim_entity WHERE is_current=TRUE")
        lf_zone = _load_dim_as_lf("SELECT zone_key, zone_id FROM staging.dim_zone WHERE is_current=TRUE")
        lf_dest = _load_dim_as_lf("SELECT destination_key, destination_id FROM staging.dim_destination WHERE is_current=TRUE")
        lf_vendor = _load_dim_as_lf("SELECT vendor_key, vendor_id FROM staging.dim_vendor")
        lf_rate = _load_dim_as_lf("SELECT rate_key, rate_type FROM staging.dim_rate")
        lf_etype = _load_dim_as_lf("SELECT event_type_key, event_type_name FROM staging.dim_event_type")
        lf_payment = _load_dim_as_lf("SELECT payment_method_key, payment_method_name FROM staging.dim_payment_method")

        # 2. Chuẩn bị và join dữ liệu fact bằng Polars
        lf_facts = (
            df.lazy()
            .with_columns(
                pl.col("event_timestamp").str.to_datetime(strict=False),
                pl.col("passenger_count").cast(pl.Int32, strict=False)
            )
            .join(lf_entity, on="entity_id", how="left")
            .join(lf_zone, on="zone_id", how="left")
            .join(lf_dest, on="destination_id", how="left")
            .join(lf_vendor, on="vendor_id", how="left")
            .join(lf_rate, on="rate_type", how="left")
            .join(lf_etype, left_on="event_type", right_on="event_type_name", how="left")
            .join(lf_payment, left_on="payment_method", right_on="payment_method_name", how="left")
            .with_columns(
                pl.lit(now).alias("_inserted_at"),
                pl.lit(batch_id).alias("_batch_id")
            )
            # FIX: Lỗi copy-paste nghiêm trọng. Phải select các cột đã được join vào.
            .select(
                "event_id",
                "event_timestamp",
                "entity_key",
                "destination_key",
                "vendor_key",
                "rate_key",
                "event_type_key",
                "payment_method_key",
                "zone_key",
                "destination_id", # Degenerate dimension
                "duration",
                "passenger_count",
                "value", "sub_value", "total_value",
                "_inserted_at", "_batch_id",
            )
        )

        # 3. Collect kết quả và chuyển thành list of dicts để insert
        df_final_facts = lf_facts.collect()
        rows = df_final_facts.to_dicts()

        # 4. Insert vào database
        if not rows:
            self.logger.info("[fact_trips] Không có dòng mới để insert.")
            return 0

        n = self._pg.insert_ignore(conn, "staging.fact_trips", rows, "event_id")
        self.logger.info(f"[fact_trips] Thử insert {len(rows)} dòng. Đã insert {n} dòng mới (idempotent).")
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

    # Đường dẫn file nguồn nên được quản lý bởi Airflow (Variables, Connections)
    # thay vì hardcode. Ở đây, ta giả định nó được mount vào một đường dẫn cố định.
    source_file_path = "/opt/airflow/data/de_assessment_data.csv"

    ingestor = build_bronze_ingestor(
        env=cfg,
        source_path=source_file_path,
        schema=_CSV_SCHEMA  # Truyền schema đã định nghĩa ở pipeline
    )
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

    task_ingest_bronze >> task_load_staging

"""
platforms/ingestion/raw_ingestor.py
────────────────────────────────────
Bronze Layer Ingestor — Phase 1 của ETL pipeline.

Mục đích:
  - Đọc CSV thô từ source (local hoặc Airflow-mounted path)
  - Thêm audit columns: _loaded_at, _source_file
  - Ghi nguyên vẹn lên MinIO S3 dạng Parquet (landing zone)
  - Idempotent: cùng batch_id chạy lại sẽ ghi đè đúng partition

Không transform dữ liệu — Bronze = ghi trung thực từ CSV.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Union, Optional

import polars as pl

from platforms.processing.polars.base_processing import BasepolarssProcessor
from platforms.processing.polars.polars_engine import PolarsEngine, PolarsConfig
from shared.utils.de_assessment_utils import EnvConfig
from shared.log.logger import LoggerManager


# ── Schema CSV gốc (raw, giữ nguyên kiểu TEXT cho event_timestamp) ─────────
_CSV_SCHEMA = {
    "event_id":        pl.Utf8,
    "event_timestamp": pl.Utf8,       # giữ TEXT tại raw layer
    "entity_id":       pl.Int32,
    "zone_id":         pl.Int32,
    "destination_id":  pl.Int32,
    "vendor_id":       pl.Int16,
    "event_type":      pl.Utf8,
    "rate_type":       pl.Int16,
    "duration":        pl.Int32,
    "passenger_count": pl.Float32,
    "value":           pl.Float64,
    "sub_value":       pl.Float64,
    "total_value":     pl.Float64,
    "payment_method":  pl.Utf8,
}


class BronzeIngestor(BasepolarssProcessor):
    """
    Đọc CSV nguồn → thêm audit cols → ghi Parquet lên MinIO bronze bucket.

    Kế thừa BasepolarssProcessor để dùng:
      - self.engine  (PolarsEngine — write_parquet với S3 support)
      - self.logger  (đã bind vào handler logger.ingestion_log.bronze)
      - add_audit_metadata()
    """

    SOURCE_FILE_NAME = "de_assessment_data.csv"

    def __init__(self, engine: PolarsEngine, source_path: Optional[str] = None) -> None:
        super().__init__(engine)
        # Đường dẫn CSV: ưu tiên tham số → Airflow-mounted path → local project path
        if source_path:
            self._source_path = Path(source_path)
        else:
            # Airflow mount: /opt/airflow/data/de_assessment_data.csv
            airflow_path = Path("/opt/airflow/data") / self.SOURCE_FILE_NAME
            local_path = Path(__file__).resolve().parent / "source_data" / self.SOURCE_FILE_NAME
            self._source_path = airflow_path if airflow_path.exists() else local_path

    # ── public entry point ─────────────────────────────────────────────────

    def process(
        self,
        batch_id: Optional[str] = None,
        target_prefix: str = "raw/events",
    ) -> str:
        """
        Chạy toàn bộ bronze ingestion pipeline.

        Args:
            batch_id:      Mã định danh batch (mặc định: UUID ngắn). Airflow truyền {{ ds }}.
            target_prefix: Prefix trong bucket bronze. Kết quả: s3://bronze/raw/events/...

        Returns:
            Đường dẫn S3 đã ghi vào.
        """
        batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self.logger.info(
            f"[BronzeIngestor] START batch_id={batch_id} source={self._source_path}"
        )

        # 1. Đọc CSV
        df = self._read_source_csv()

        # 2. Thêm audit metadata (ingest_timestamp, ingest_date, batch_id, source_system)
        df = self.add_audit_metadata(
            df=df,
            batch_id=batch_id,
            source_system=str(self._source_path.name),
        )

        # 3. Thêm _source_file (tên file gốc) — yêu cầu từ roadmap
        df = df.with_columns(
            pl.lit(str(self._source_path.name)).alias("_source_file")
        )

        # 4. Collect từ LazyFrame → DataFrame để ghi
        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        row_count = len(df)
        self.logger.info(
            f"[BronzeIngestor] Loaded {row_count:,} rows from CSV. Writing to bronze..."
        )

        # 5. Ghi lên MinIO — partition by ingest_date để idempotent re-run
        target_path = self.engine.config.storage_options.get("_bronze_prefix", "") or \
                      self._build_s3_path(target_prefix, batch_id)

        written_path = self.engine.write_parquet(
            df=df,
            target_path=target_path,
            partition_by=["ingest_date"],
        )

        self.logger.info(
            f"[BronzeIngestor] DONE batch_id={batch_id} rows={row_count:,} path={written_path}"
        )
        return written_path

    # ── private helpers ────────────────────────────────────────────────────

    def _read_source_csv(self) -> pl.LazyFrame:
        """Đọc CSV với schema cố định — lazy để tiết kiệm bộ nhớ."""
        self.logger.debug(f"[BronzeIngestor] scan_csv: {self._source_path}")
        if not self._source_path.exists():
            raise FileNotFoundError(
                f"[BronzeIngestor] Không tìm thấy file CSV: {self._source_path}\n"
                "Kiểm tra lại đường dẫn hoặc mount volume trong Docker/Airflow."
            )
        return pl.scan_csv(
            source=str(self._source_path),
            schema=_CSV_SCHEMA,
            null_values=["", "NULL", "null", "NA"],
            try_parse_dates=False,   # giữ nguyên TEXT tại bronze
            infer_schema=False,
        )

    def _build_s3_path(self, prefix: str, batch_id: str) -> str:
        """
        Xây dựng đường dẫn S3 từ storage_options của engine.
        Engine đã được khởi tạo với storage_options chứa bucket bronze.
        """
        # Lấy bucket từ storage_options nếu có key "_bucket_bronze"
        # Nếu không có, dùng mặc định "bronze"
        bucket = self.engine.config.storage_options.get("_bucket_bronze", "bronze")
        # Kết quả: s3://bronze/raw/events/
        return f"s3://{bucket}/{prefix.strip('/')}/"


# ── Factory function — khởi tạo đầy đủ từ EnvConfig ──────────────────────


def build_bronze_ingestor(
    env: Optional[EnvConfig] = None,
    source_path: Optional[str] = None,
) -> BronzeIngestor:
    """
    Khởi tạo BronzeIngestor với PolarsEngine + Logger từ .env.

    Chỉ gọi hàm này khi pipeline thực sự cần chạy (lazy init pattern).

    Args:
        env:         EnvConfig instance. Nếu None thì tự tạo mới.
        source_path: Override đường dẫn CSV nếu cần.

    Returns:
        BronzeIngestor đã sẵn sàng để gọi .process()
    """
    # 1. Load config từ .env
    cfg = env or EnvConfig()
    minio = cfg.minio

    # 2. Khởi tạo Logger
    logger_manager = LoggerManager()
    logger = logger_manager.get_logger("logger.ingestion_log.bronze")
    logger.info(f"[build_bronze_ingestor] MinIO endpoint={minio.endpoint_url} bucket={minio.bucket_bronze}")

    # 3. Khởi tạo PolarsEngine với S3 storage_options từ MinIO config
    polars_cfg = PolarsConfig(
        enable_streaming=True,
        storage_options={
            **minio.storage_options,           # endpoint_url, access_key, secret_key
            "_bucket_bronze": minio.bucket_bronze,
            "_bucket_silver": minio.bucket_silver,
            "_bucket_gold":   minio.bucket_gold,
        },
    )
    engine = PolarsEngine(config=polars_cfg, logger=logger)

    # 4. Tạo và trả về ingestor
    return BronzeIngestor(engine=engine, source_path=source_path)

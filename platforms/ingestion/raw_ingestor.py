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
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Union, Optional, Dict

import polars as pl

from platforms.processing.polars.base_processing import BasepolarssProcessor
from platforms.processing.polars.polars_engine import PolarsEngine, PolarsConfig
from shared.utils.de_assessment_utils import EnvConfig
from shared.log.logger import LoggerManager
class BronzeIngestor(BasepolarssProcessor):
    """
    Đọc CSV nguồn → thêm audit cols → ghi Parquet lên MinIO bronze bucket.
    """
    def __init__(self, engine: PolarsEngine, bronze_bucket: str, source_path: str, schema: Dict[str, pl.DataType]) -> None:
        super().__init__(engine)
        self._bronze_bucket = bronze_bucket
        self._source_path = Path(source_path)
        self._schema = schema

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
        """Đọc CSV với schema được cung cấp — lazy để tiết kiệm bộ nhớ."""
        self.logger.debug(f"[BronzeIngestor] scan_csv: {self._source_path}")
        if not self._source_path.exists():
            raise FileNotFoundError(
                f"[BronzeIngestor] Không tìm thấy file CSV: {self._source_path}\n"
                "Kiểm tra lại đường dẫn hoặc mount volume trong Docker/Airflow."
            )
        return pl.scan_csv(
            source=str(self._source_path),
            schema=self._schema,
            null_values=["", "NULL", "null", "NA"],
            try_parse_dates=False,   # giữ nguyên TEXT tại bronze
            infer_schema_length=0, # Không infer schema, dùng schema cung cấp
        )

    def _build_s3_path(self, prefix: str, batch_id: str) -> str:
        """
        Xây dựng đường dẫn S3 từ storage_options của engine.
        Engine đã được khởi tạo với storage_options chứa bucket bronze.
        """
        # Kết quả: s3://bronze/raw/events/
        return f"s3://{self._bronze_bucket}/{prefix.strip('/')}/"


# ── Factory function — khởi tạo đầy đủ từ EnvConfig ──────────────────────


def build_bronze_ingestor(
    env: Optional[EnvConfig] = None,
    source_path: Optional[str] = None,
    schema: Optional[Dict[str, pl.DataType]] = None,
) -> BronzeIngestor:
    """
    Khởi tạo BronzeIngestor với PolarsEngine + Logger từ .env.

    Chỉ gọi hàm này khi pipeline thực sự cần chạy (lazy init pattern).

    Args:
        env:         EnvConfig instance. Nếu None thì tự tạo mới.
        source_path: Override đường dẫn CSV nếu cần.
        schema:      Schema của file CSV.

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

    if not source_path:
        raise ValueError("[build_bronze_ingestor] `source_path` là tham số bắt buộc.")
    if not schema:
        raise ValueError("[build_bronze_ingestor] `schema` là tham số bắt buộc.")

    # 3. Khởi tạo PolarsEngine với S3 storage_options từ MinIO config
    polars_cfg = PolarsConfig(
        enable_streaming=True,
        storage_options=minio.storage_options,
    )
    engine = PolarsEngine(config=polars_cfg, logger=logger)

    # 4. Tạo và trả về ingestor
    return BronzeIngestor(
        engine=engine,
        bronze_bucket=minio.bucket_bronze,
        source_path=source_path,
        schema=schema,
    )

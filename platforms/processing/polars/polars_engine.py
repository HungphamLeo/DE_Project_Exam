import os
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Union
import polars as pl
import pyarrow.parquet as pq

@dataclass
class PolarsConfig:
    """Dataclass chứa cấu hình cho Polars Engine"""
    thread_pool_size: Optional[int] = None
    enable_streaming: bool = True
    storage_options: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PolarsConfig":
        return cls(
            thread_pool_size=data.get("thread_pool_size"),
            enable_streaming=data.get("enable_streaming", True),
            storage_options=data.get("storage_options", {})
        )

class PolarsEngine:
    """
    Lớp quản lý engine xử lý dữ liệu bằng Polars (Tương tự SparkSession).
    Cung cấp các hàm I/O chuẩn với cấu hình tập trung.
    """
    def __init__(self, config: PolarsConfig, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._setup_environment()

    def _setup_environment(self):
        """Cấu hình môi trường (VD: giới hạn số lượng thread)"""
        if self.config.thread_pool_size:
            os.environ["POLARS_MAX_THREADS"] = str(self.config.thread_pool_size)
            self.logger.info(f"Set POLARS_MAX_THREADS = {self.config.thread_pool_size}")

    def read_parquet(self, source_path: str) -> pl.LazyFrame:
        """Đọc dữ liệu Parquet dưới dạng LazyFrame (tối ưu hóa bộ nhớ)"""
        self.logger.info(f"Đọc dữ liệu từ: {source_path}")
        return pl.scan_parquet(
            source_path, 
            storage_options=self.config.storage_options
        )

    def write_parquet(
        self,
        df: Union[pl.DataFrame, pl.LazyFrame],
        target_path: str,
        partition_by: Optional[List[str]] = None
    ) -> str:
        """Ghi dữ liệu Data/Lazy Frame ra S3/MinIO dạng Parquet (hỗ trợ phân vùng).

        Polars 1.x write_parquet() không nhận storage_options — cần dùng pyarrow
        kết hợp s3fs/PyArrowFileSystem khi ghi lên S3/MinIO.
        """
        self.logger.info(f" Ghi dữ liệu tới: {target_path} (Partition: {partition_by})")

        # Nếu là LazyFrame, thực thi (collect) theo streaming mode
        if isinstance(df, pl.LazyFrame):
            df = df.collect(streaming=self.config.enable_streaming)

        storage_options = self.config.storage_options

        if storage_options and target_path.startswith(("s3://", "s3a://")):
            self._write_parquet_s3(df, target_path, partition_by, storage_options)
        else:
            # Local filesystem — write_parquet đủ dùng
            if partition_by:
                df.write_parquet(target_path, use_pyarrow=True, partition_by=partition_by)
            else:
                df.write_parquet(target_path, use_pyarrow=True)

        return target_path

    def _write_parquet_s3(
        self,
        df: pl.DataFrame,
        target_path: str,
        partition_by: Optional[List[str]],
        storage_options: Dict[str, Any],
    ) -> None:
        """Ghi Parquet lên S3/MinIO bằng PyArrow FileSystem (bypass Polars storage_options limit).

        MinIO yêu cầu:
          - client_kwargs[endpoint_url]  : endpoint đầy đủ (http://host:port)
          - client_kwargs[region_name]   : bất kỳ string nào, thường 'us-east-1'
          - config_kwargs[signature_version] = 's3v4'
        Nếu thiếu một trong những giá trị này, boto3 / s3fs sẽ tính sai signature → 403.
        """
        import s3fs

        # Chuẩn hoá s3a:// → s3:// (s3fs chỉ hiểu s3://)
        s3_path = target_path.replace("s3a://", "s3://")

        endpoint = storage_options.get("endpoint_url", "")
        key      = storage_options.get("aws_access_key_id", "")
        secret   = storage_options.get("aws_secret_access_key", "")
        region_name = storage_options.get("region_name", "us-east-1")

        fs = s3fs.S3FileSystem(
            key=key,
            secret=secret,
            # endpoint_url ở top-level bị ignore ở 1 số version s3fs,
            # dùng client_kwargs để chắc chắn boto3 nhận đúng
            client_kwargs={
                "endpoint_url": endpoint,
                "region_name": region_name,
            },
            config_kwargs={
                "signature_version": "s3v4",
            },
        )

        arrow_table = df.to_arrow()

        if partition_by:
            pq.write_to_dataset(
                arrow_table,
                root_path=s3_path,
                partition_cols=partition_by,
                filesystem=fs,

            )
        else:
            # Ensure the path is a file, not a bare directory prefix.
            # s3_path may end with "/" (e.g. "s3://lakehouse/silver/dim_company/")
            # → write to "s3://lakehouse/silver/dim_company/data.parquet"
            file_path = s3_path.rstrip("/") + "/data.parquet"
            with fs.open(file_path, "wb") as f:
                pq.write_table(arrow_table, f)

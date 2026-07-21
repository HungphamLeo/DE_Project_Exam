from abc import ABC, abstractmethod
from typing import Union, Dict, Any
from platforms.processing.polars.polars_engine import polarssEngine
import polars as pl
from datetime import datetime, timezone, date

# Giả định polarssEngine được import từ module ở trên
# from platforms.processing.polarss.polarss_engine import polarssEngine

class BasepolarssProcessor(ABC):
    """
    Lớp cơ sở cho các Job xử lý dữ liệu bằng polarss.
    Bất cứ tác vụ Ingest / Transform nào cũng nên kế thừa class này.
    """
    def __init__(self, engine: 'polarssEngine'):
        self.engine = engine
        self.logger = engine.logger

    def add_audit_metadata(
        self, 
        df: Union[pl.DataFrame, pl.LazyFrame], 
        batch_id: str, 
        source_system: str
    ) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Thêm siêu dữ liệu (metadata/audit) chuẩn vào mọi bản ghi để phục vụ Lakehouse"""
        audit_exprs = [
            pl.lit(datetime.now(timezone.utc)).alias("ingest_timestamp"),
            pl.lit(date.today()).alias("ingest_date"),
            pl.lit(batch_id).alias("batch_id"),
            pl.lit(source_system).alias("source_system")
        ]
        return df.with_columns(audit_exprs)

    @abstractmethod
    def process(self, *args, **kwargs) -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        Nơi triển khai logic cụ thể (Fetch API, Cleaning, Transform...).
        Bắt buộc ghi đè ở class con.
        """
        pass

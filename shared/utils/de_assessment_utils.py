"""
shared/utils/de_assessment_utils.py
─────────────────────────────────────
Config trung tâm cho toàn bộ DE Assessment pipeline.

Tất cả config objects (PostgresConfig, MinioConfig, KafkaConfig...)
được định nghĩa tại đây và lazy-load từ .env thông qua EnvConfig.

Cách dùng ở bất kỳ module nào trong project:
    from shared.utils.de_assessment_utils import EnvConfig

    cfg = EnvConfig()
    pg    = cfg.postgres   # PostgresConfig  → psycopg2 DSN / SQLAlchemy conn_string
    minio = cfg.minio      # MinioConfig     → S3 storage_options cho PolarsEngine / s3fs
    kfk   = cfg.kafka      # KafkaConfig     → bootstrap_servers
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv


# ===========================================================================
# Config dataclasses — mỗi service là 1 frozen dataclass riêng (SRP)
# ===========================================================================

@dataclass(frozen=True)
class PostgresConfig:
    """Kết nối tới assessment database (de / de)."""
    host:     str
    port:     int
    db:       str
    user:     str
    password: str

    @property
    def conn_string(self) -> str:
        """SQLAlchemy / psycopg2 connection string."""
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )

    @property
    def dsn(self) -> str:
        """psycopg2-style DSN — dùng trực tiếp với psycopg2.connect() / ThreadedConnectionPool."""
        return (
            f"host={self.host} port={self.port} "
            f"dbname={self.db} user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class MinioConfig:
    """Kết nối tới MinIO (S3-compatible object storage)."""
    endpoint_url:  str
    access_key:    str
    secret_key:    str
    bucket_bronze: str
    bucket_silver: str
    bucket_gold:   str

    @property
    def storage_options(self) -> Dict[str, Any]:
        """Dict truyền thẳng vào s3fs.S3FileSystem / PolarsEngine storage_options."""
        return {
            "endpoint_url":          self.endpoint_url,
            "aws_access_key_id":     self.access_key,
            "aws_secret_access_key": self.secret_key,
        }


@dataclass(frozen=True)
class AirflowConfig:
    """Thông tin Airflow webserver + metadata DB."""
    www_user:        str
    www_password:    str
    port:            int
    sql_alchemy_conn: str
    executor:        str
    fernet_key:      str


@dataclass(frozen=True)
class KafkaConfig:
    """Kết nối tới Kafka broker."""
    bootstrap_servers:  str
    auto_create_topics: bool

    @property
    def broker_list(self) -> list[str]:
        """Trả về list server, tiện dùng cho KafkaProducer/Consumer."""
        return [s.strip() for s in self.bootstrap_servers.split(",")]


@dataclass(frozen=True)
class ZookeeperConfig:
    client_port: int
    tick_time:   int


# ===========================================================================
# EnvConfig — class chính, lazy-load từ .env
# ===========================================================================

class EnvConfig:
    """
    Load tất cả biến môi trường từ file .env một lần duy nhất.
    Chỉ khởi tạo object khi pipeline thực sự cần chạy (lazy init pattern).

    Cách dùng:
        cfg = EnvConfig()               # tự tìm .env từ PROJECT_ROOT
        cfg = EnvConfig("/path/.env")   # chỉ định rõ đường dẫn

        pg    = cfg.postgres   # PostgresConfig
        minio = cfg.minio      # MinioConfig
        kfk   = cfg.kafka      # KafkaConfig
        af    = cfg.airflow    # AirflowConfig
    """

    def __init__(self, env_path: Optional[str] = None) -> None:
        # Tìm file .env: ưu tiên tham số → PROJECT_ROOT env var → dò ngược từ file này
        if env_path:
            _path = Path(env_path)
        elif os.getenv("PROJECT_ROOT"):
            _path = Path(os.environ["PROJECT_ROOT"]) / ".env"
        else:
            # shared/utils/ → shared/ → project root (2 cấp)
            _path = Path(__file__).resolve().parents[2] / ".env"

        load_dotenv(dotenv_path=_path, override=False)

        self._env_path  = _path
        self._postgres:  Optional[PostgresConfig]  = None
        self._minio:     Optional[MinioConfig]      = None
        self._airflow:   Optional[AirflowConfig]    = None
        self._kafka:     Optional[KafkaConfig]      = None
        self._zookeeper: Optional[ZookeeperConfig]  = None

    # ── helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _require(key: str) -> str:
        """Lấy biến env bắt buộc, raise rõ ràng nếu thiếu."""
        value = os.getenv(key)
        if not value:
            raise EnvironmentError(
                f"[EnvConfig] Biến môi trường '{key}' chưa được khai báo trong .env"
            )
        return value

    # ── lazy properties ────────────────────────────────────────────────────

    @property
    def postgres(self) -> PostgresConfig:
        if self._postgres is None:
            self._postgres = PostgresConfig(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                db=self._require("POSTGRES_DB"),
                user=self._require("POSTGRES_USER"),
                password=self._require("POSTGRES_PASSWORD"),
            )
        return self._postgres

    @property
    def minio(self) -> MinioConfig:
        if self._minio is None:
            self._minio = MinioConfig(
                endpoint_url=os.getenv("MINIO_ENDPOINT_URL",  "http://localhost:9000"),
                access_key=os.getenv("MINIO_ACCESS_KEY",      "minio@admin"),
                secret_key=os.getenv("MINIO_SECRET_KEY",      "de_assessment@123#"),
                bucket_bronze=os.getenv("MINIO_BUCKET_BRONZE", "bronze"),
                bucket_silver=os.getenv("MINIO_BUCKET_SILVER", "silver"),
                bucket_gold=os.getenv("MINIO_BUCKET_GOLD",     "gold"),
            )
        return self._minio

    @property
    def airflow(self) -> AirflowConfig:
        if self._airflow is None:
            self._airflow = AirflowConfig(
                www_user=os.getenv("AIRFLOW_WWW_USER_USERNAME", "airflow"),
                www_password=os.getenv("AIRFLOW_WWW_USER_PASSWORD", "airflow"),
                port=int(os.getenv("AIRFLOW_PORT", "8080")),
                sql_alchemy_conn=os.getenv(
                    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
                    "postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow",
                ),
                executor=os.getenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor"),
                fernet_key=os.getenv("AIRFLOW__CORE__FERNET_KEY", ""),
            )
        return self._airflow

    @property
    def kafka(self) -> KafkaConfig:
        if self._kafka is None:
            self._kafka = KafkaConfig(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                auto_create_topics=(
                    os.getenv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true").lower() == "true"
                ),
            )
        return self._kafka

    @property
    def zookeeper(self) -> ZookeeperConfig:
        if self._zookeeper is None:
            self._zookeeper = ZookeeperConfig(
                client_port=int(os.getenv("ZOOKEEPER_CLIENT_PORT", "2181")),
                tick_time=int(os.getenv("ZOOKEEPER_TICK_TIME", "2000")),
            )
        return self._zookeeper

    @property
    def source_path(self):
        """Đường dẫn tới file CSV nguồn (source data)."""
        return os.getenv("SOURCE_PATH", "platforms/ingestion/source_data/de_assessment_data.csv")
    def __repr__(self) -> str:
        return f"EnvConfig(env_path='{self._env_path}')"

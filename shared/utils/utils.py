from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Sub-configs — mỗi nhóm service là 1 dataclass riêng
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PostgresConfig:
    """Kết nối tới assessment database (de / de)."""
    host: str
    port: int
    db: str
    user: str
    password: str

    @property
    def conn_string(self) -> str:
        """SQLAlchemy / psycopg2 connection string."""
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


@dataclass(frozen=True)
class AirflowConfig:
    """Thông tin Airflow webserver + metadata DB."""
    www_user: str
    www_password: str
    port: int
    sql_alchemy_conn: str
    executor: str
    fernet_key: str


@dataclass(frozen=True)
class KafkaConfig:
    """Kết nối tới Kafka broker."""
    bootstrap_servers: str
    auto_create_topics: bool

    @property
    def broker_list(self) -> list[str]:
        """Trả về list server, tiện dùng cho KafkaProducer/Consumer."""
        return [s.strip() for s in self.bootstrap_servers.split(",")]


@dataclass(frozen=True)
class ZookeeperConfig:
    client_port: int
    tick_time: int


# ---------------------------------------------------------------------------
# EnvConfig — class chính, lazy-load từ .env
# ---------------------------------------------------------------------------

class EnvConfig:
    """
    Load tất cả biến môi trường từ file .env một lần duy nhất.
    Chỉ khởi tạo object khi pipeline thực sự cần chạy.

    Cách dùng:
        cfg = EnvConfig()               # tự tìm .env từ PROJECT_ROOT
        cfg = EnvConfig("/path/.env")   # chỉ định rõ đường dẫn

        # Lấy config từng service
        pg  = cfg.postgres              # PostgresConfig
        kfk = cfg.kafka                 # KafkaConfig
        af  = cfg.airflow               # AirflowConfig
    """

    def __init__(self, env_path: Optional[str] = None) -> None:
        # Tìm file .env: ưu tiên tham số truyền vào → PROJECT_ROOT env var → tự dò lên từ file này
        if env_path:
            _path = Path(env_path)
        elif os.getenv("PROJECT_ROOT"):
            _path = Path(os.environ["PROJECT_ROOT"]) / ".env"
        else:
            # Dò ngược từ shared/config/ lên project root (2 cấp)
            _path = Path(__file__).resolve().parents[2] / ".env"

        load_dotenv(dotenv_path=_path, override=False)

        self._env_path = _path
        self._postgres: Optional[PostgresConfig] = None
        self._airflow: Optional[AirflowConfig] = None
        self._kafka: Optional[KafkaConfig] = None
        self._zookeeper: Optional[ZookeeperConfig] = None

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
                auto_create_topics=os.getenv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true").lower() == "true",
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

    def __repr__(self) -> str:
        return f"EnvConfig(env_path='{self._env_path}')"

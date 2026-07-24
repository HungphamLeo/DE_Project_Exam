"""
cli/main.py
───────────
Entrypoint dòng lệnh (CLI) để chạy các ứng dụng streaming.

Cách dùng:
  python -m cli.main produce <path/to/data.csv> [--delay 100]
  python -m cli.main consume
"""
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
import time
import csv
from shared.log.logger import LoggerManager
from shared.utils.de_assessment_utils import EnvConfig, KafkaConfig, PostgresConfig
from platforms.streaming.kafka.base import GenericProducer, GenericConsumer
from platforms.streaming.kafka.schema_registry import serialize, deserialize
from platforms.storage.postgre.postgres_client import PostgresClient

class EventProducer:
    """Đọc dữ liệu từ file CSV và gửi tới Kafka topic 'events'."""
    TOPIC = "events"

    def __init__(self, kafka_config: KafkaConfig, logger: logging.Logger, source_file_path: str, delay_ms: int = 100):
        self.producer = GenericProducer(kafka_config, logger)
        self.logger = logger
        self.source_file = Path(source_file_path)
        self.delay_s = delay_ms / 1000.0

    def _clean_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Chuyển đổi kiểu dữ liệu và làm sạch giá trị null."""
        cleaned = row.copy()
        for key, value in cleaned.items():
            if value in ('', 'NA', 'null', 'None'):
                cleaned[key] = None
                continue
            try:
                if key in ('entity_id', 'zone_id', 'duration', 'vendor_id', 'rate_type'):
                    cleaned[key] = int(float(value))
                elif key in ('value', 'sub_value', 'total_value', 'passenger_count'):
                    cleaned[key] = float(value)
            except (ValueError, TypeError):
                self.logger.warning(f"Không thể chuyển đổi giá trị '{value}' cho cột '{key}'. Giữ nguyên giá trị.")
        return cleaned

    def run(self):
        """Đọc file CSV và gửi từng dòng tới Kafka."""
        self.logger.info(f"Bắt đầu producer cho topic '{self.TOPIC}' từ file '{self.source_file}'")
        if not self.source_file.exists():
            raise FileNotFoundError(f"Không tìm thấy file dữ liệu nguồn tại: {self.source_file}")

        with open(self.source_file, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            count = 0
            for row in reader:
                try:
                    cleaned_row = self._clean_row(row)
                    event_id = cleaned_row.get("event_id", f"unknown-{count}")
                    message_bytes = serialize(cleaned_row)
                    self.producer.produce(self.TOPIC, key=str(event_id), value=message_bytes)
                    count += 1
                    if count % 1000 == 0:
                        self.logger.info(f"Đã gửi {count} tin nhắn...")
                    time.sleep(self.delay_s)
                except Exception as e:
                    self.logger.error(f"Lỗi khi xử lý dòng {count+1}: {row}. Lỗi: {e}", exc_info=True)
        self.logger.info(f"Hoàn tất. Tổng số tin nhắn đã gửi: {count}")
        self.producer.flush()

class EventConsumer(GenericConsumer):
    """Nhận sự kiện từ Kafka, xử lý và ghi vào PostgreSQL."""
    TOPIC = "events"
    GROUP_ID = "de-assessment-consumer-group"

    def __init__(self, kafka_config: KafkaConfig, pg_config: PostgresConfig, logger: logging.Logger):
        super().__init__(kafka_config, logger, group_id=self.GROUP_ID)
        self.pg_client = PostgresClient.from_config(pg_config)
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Chạy script SQL để tạo bảng nếu chưa tồn tại."""
        try:
            # Giả định project root là thư mục cha của 'streaming'
            schema_file = Path(__file__).resolve().parents[1] / "sql" / "streaming_schema.sql"
            if schema_file.exists():
                self.logger.info(f"Áp dụng schema từ {schema_file}...")
                self.pg_client.execute_sql_file(schema_file)
            else:
                self.logger.warning(f"Không tìm thấy file schema tại {schema_file}. Giả sử bảng 'staging.kafka_events' đã tồn tại.")
        except Exception as e:
            self.logger.error(f"Lỗi khi áp dụng schema cho streaming: {e}", exc_info=True)
            raise

    def process_message(self, msg_key: str, msg_value: bytes):
        """Xử lý một tin nhắn từ Kafka."""
        try:
            data = deserialize(msg_value)
            if not data:
                self.logger.warning(f"Nhận được tin nhắn rỗng với key {msg_key}.")
                return

            record = self._transform(data)

            with self.pg_client.connection() as conn:
                self.pg_client.insert_ignore(conn=conn, table="staging.kafka_events", rows=[record], conflict_col="event_id")
            self.logger.debug(f"Đã xử lý và ghi event_id: {msg_key}")
        except Exception as e:
            self.logger.error(f"Lỗi khi xử lý tin nhắn với key {msg_key}. Lỗi: {e}", exc_info=True)

    def _transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Chuyển đổi dictionary đã deserialize thành định dạng cho DB."""
        ts_str = data.get("event_timestamp")
        event_timestamp = datetime.fromisoformat(ts_str.replace('Z', '+00:00')) if ts_str else None

        return {
            "event_id": data.get("event_id"),
            "event_timestamp": event_timestamp,
            "entity_id": data.get("entity_id"),
            "zone_id": data.get("zone_id"),
            "event_type": data.get("event_type"),
            "duration": data.get("duration"),
            "value": data.get("value"),
            "sub_value": data.get("sub_value"),
            "total_value": data.get("total_value"),
            "payment_method": data.get("payment_method"),
            "passenger_count": int(data["passenger_count"]) if data.get("passenger_count") is not None else None,
            "vendor_id": data.get("vendor_id"),
        }

    def run(self):
        """Bắt đầu vòng lặp lắng nghe của consumer."""
        self.subscribe(topics=[self.TOPIC])


def run_producer(args):
    """Khởi tạo và chạy Kafka producer."""
    logger = LoggerManager().get_logger("logger.streaming_log.producer")
    logger.info("--- Bắt đầu Kafka Event Producer ---")
    try:
        cfg = EnvConfig()
        print(cfg)
        producer = EventProducer(
            kafka_config=cfg.kafka,
            logger=logger,
            source_file_path=cfg.source_path,
            delay_ms=args.delay
        )
        producer.run()
    except Exception as e:
        logger.critical(f"Producer gặp lỗi nghiêm trọng: {e}", exc_info=True)
    logger.info("--- Kafka Event Producer đã kết thúc ---")


def run_consumer(args):
    """Khởi tạo và chạy Kafka consumer."""
    logger = LoggerManager().get_logger("logger.streaming_log.consumer")
    logger.info("--- Bắt đầu Kafka Event Consumer ---")
    try:
        cfg = EnvConfig()
        consumer = EventConsumer(kafka_config=cfg.kafka, pg_config=cfg.postgres, logger=logger)
        consumer.run()
    except Exception as e:
        logger.critical(f"Consumer gặp lỗi nghiêm trọng: {e}", exc_info=True)
    logger.info("--- Kafka Event Consumer đã dừng ---")


def main():
    """Entrypoint chính cho CLI."""
    parser = argparse.ArgumentParser(description="DE Assessment Streaming CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prod_parser = subparsers.add_parser("produce", help="Chạy Kafka event producer.")
    prod_parser.add_argument("--delay", type=int, default=50, help="Độ trễ (ms) giữa các tin nhắn.")
    prod_parser.set_defaults(func=run_producer)

    cons_parser = subparsers.add_parser("consume", help="Chạy Kafka event consumer.")
    cons_parser.set_defaults(func=run_consumer)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
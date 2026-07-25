"""
platforms/kafka/schema_registry.py
──────────────────────────────────
Quản lý schema cho tin nhắn Kafka.

Trong một hệ thống thực tế, nên sử dụng một Schema Registry chuyên dụng
(VD: Confluent Schema Registry) và định dạng Avro/Protobuf để quản lý
schema một cách chặt chẽ và hiệu quả hơn.
"""
import json
from typing import Dict, Any


def serialize(data: Dict[str, Any]) -> bytes:
    """
    Tuần tự hóa (serialize) một dictionary thành chuỗi byte JSON (UTF-8).
    """
    # Có thể thêm bước xác thực dữ liệu với schema ở đây nếu cần.
    return json.dumps(data, default=str).encode('utf-8')


def deserialize(message_bytes: bytes) -> Dict[str, Any]:
    """
    Giải tuần tự hóa (deserialize) một chuỗi byte JSON (UTF-8) thành dictionary.
    """
    if not message_bytes:
        return {}
    return json.loads(message_bytes.decode('utf-8'))
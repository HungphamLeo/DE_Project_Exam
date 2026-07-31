"""platforms/kafka/base_client.py"""
import logging
from abc import ABC
from shared.utils.de_assessment_utils import KafkaConfig
from abc import abstractmethod
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, Message

class BaseKafkaClient(ABC):
    """Lớp cơ sở cho các client Kafka với cấu hình dùng chung."""

    def __init__(self, kafka_config: KafkaConfig, logger: logging.Logger):
        self.config = kafka_config
        self.logger = logger
        self._client = None

class GenericConsumer(BaseKafkaClient):
    """Một Kafka consumer chung sử dụng thư viện confluent-kafka."""

    def __init__(self, kafka_config, logger, group_id: str):
        super().__init__(kafka_config, logger)
        consumer_conf = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Tắt auto commit để kiểm soát thủ công
        }
        self._client = Consumer(consumer_conf)
        self.logger.info(f"Kafka Consumer đã khởi tạo cho servers: {self.config.bootstrap_servers} với group ID: {group_id}")

    @abstractmethod
    def process_batch(self, messages: list[Message]):
        """Phương thức trừu tượng, cần được implement ở lớp con để xử lý một lô tin nhắn."""
        pass

    def subscribe(self, topics: list[str]):
        """Đăng ký vào một danh sách các topic và bắt đầu vòng lặp tiêu thụ."""
        if self._client is None:
            raise ConnectionError("Consumer chưa được khởi tạo.")

        self._client.subscribe(topics)
        self.logger.info(f"Đã đăng ký vào các topic: {topics}")

        try:
            while True:
                # Tiêu thụ một lô tin nhắn, tối đa 500 tin hoặc đợi 2 giây
                messages = self._client.consume(num_messages=500, timeout=2.0)
                if not messages:
                    continue

                self.logger.info(f"Nhận được {len(messages)} tin nhắn từ Kafka.")
                valid_messages = []
                for msg in messages:
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            self.logger.info(f"Đã tới cuối partition: {msg.topic()} [{msg.partition()}]")
                        else:
                            self.logger.error(f"Lỗi Kafka: {msg.error()}")
                    else:
                        valid_messages.append(msg)

                if valid_messages:
                    self.process_batch(valid_messages)
                    self._client.commit(asynchronous=False) # Commit offset sau khi xử lý batch thành công

        except KeyboardInterrupt:
            self.logger.info("Consumer bị ngắt bởi người dùng.")
        finally:
            if self._client:
                self._client.close()
                self.logger.info("Kafka Consumer đã đóng.")
class GenericProducer(BaseKafkaClient):
    """Một Kafka producer chung sử dụng thư viện confluent-kafka."""

    def __init__(self, kafka_config, logger):
        super().__init__(kafka_config, logger)
        producer_conf = {
            'bootstrap.servers': self.config.bootstrap_servers,
        }
        self._client = Producer(producer_conf)
        self.logger.info(f"Kafka Producer đã khởi tạo cho servers: {self.config.bootstrap_servers}")

    def _delivery_report(self, err, msg):
        """Callback cho báo cáo gửi tin nhắn."""
        if err is not None:
            self.logger.error(f"Gửi tin nhắn thất bại: {err}")
        else:
            self.logger.debug(f"Tin nhắn đã được gửi tới {msg.topic()} [{msg.partition()}]")

    def produce(self, topic: str, key: str, value: bytes):
        """Gửi một tin nhắn tới một topic Kafka."""
        if self._client is None:
            raise ConnectionError("Producer chưa được khởi tạo.")
        self._client.produce(topic, key=key.encode('utf-8'), value=value, callback=self._delivery_report)
        self._client.poll(0)

    def flush(self):
        """Đợi tất cả tin nhắn trong hàng đợi được gửi đi."""
        if self._client:
            self.logger.info("Flushing producer...")
            self._client.flush()
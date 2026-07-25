# DE Assessment Submission

## **Tổng quan**

Repository này chứa giải pháp cho bài đánh giá Data Engineer, bao gồm:
- **Part A**: Mô hình hóa dữ liệu và truy vấn SQL.
- **Part B**: Pipeline ETL tự động với Airflow.
- **Part C**: Pipeline streaming với Kafka.
- **Part D**: Câu hỏi thiết kế hệ thống.

## **Thiết lập môi trường**

1.  **Yêu cầu**:
    - Docker và Docker Compose
    - Python 3.9+
    - Đặt file dữ liệu `de_assessment_data.csv` vào thư mục gốc của project này.

2.  **Cài đặt thư viện Python**:
    ```bash
    
    pip install -r requirements.txt
    ```

3.  **Khởi động các services (Postgres, Airflow, Kafka)**:
    ```bash
    cd infra
    docker compose up -d
    ```
    - Airflow UI: `http://localhost:8080` (user: `airflow`, pass: `airflow`)
    - Postgres: `localhost:5432` (db: `assessment`, user: `de`, pass: `de`)
    - Kafka: `localhost:9092`

## **Chạy các phần của bài làm**

### **Part C — Streaming Ingestion**

Để chạy mô phỏng streaming, bạn cần mở 2 cửa sổ terminal riêng biệt.

1.  **Terminal 1: Chạy Consumer**
    Consumer sẽ kết nối tới Kafka, lắng nghe topic `events` và ghi dữ liệu vào PostgreSQL.
    ```bash
    python -m cli/main consume
    ```

2.  **Terminal 2: Chạy Producer**
    Producer sẽ đọc file `de_assessment_data.csv` và gửi từng dòng vào Kafka.
    ```bash

    python -m cli/main produce de_assessment_data.csv --delay 50
    ```
    - Bạn có thể thay đổi giá trị `--delay` (miligiây) để điều chỉnh tốc độ gửi tin.
    - Bạn sẽ thấy log xử lý tin nhắn ở cửa sổ của consumer. Dữ liệu sẽ được ghi vào bảng `staging.kafka_events`.

Để dừng consumer, nhấn `Ctrl+C` ở cửa sổ terminal tương ứng.
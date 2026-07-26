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


- **Part D**: Câu hỏi thiết kế hệ thống.
| PART D — Design Question |
| :---- |

Answer the following question in writing (max 1 page):

| The data volume grows 100x and events must be available for querying within 30 seconds of occurring. What would you change in your current architecture? What would you keep? What are the main trade-offs in your proposed approach? |
| :---- |

Quan điểm:
Trong trường hợp nếu dữ liệu là realtime và có khả năng tăng đột ngột vào các thời điểm không xác định thì chắc chắn không nên dùng airflow hay prefect và các orchestra tech stack định kỳ, mà thay đó hãy dùng kafka đóng vai trò như là giải pháp ghi dữ liệu với độ trễ thấp.
- Thứ nhất là kafka theo cơ chế ghi tuần tự ,append only log với tốc độ cực nhanh, lưu trữ như 1 buffer cache  => đáp ứng về tốc độ
- Thứ 2 là về việc kafka có các đơn vị là message, chia thành nhiều topic, mỗi topic thì lại có nhiều partition, mỗi parttion thì nằm trên các máy tính là broker hoặc 1 cụm máy (cluster). Mô hình trong kiến trúc dữ liệu phân tán phổ biến là leader - followers, leader sẽ đọc/ghi dữ liệu và follower sẽ write dữ liệu từ leader, khi mà phải ghi dữ liệu ở tốc độ cao thì không thể tránh khỏi các vẫn đề lỗi thì việc 1 máy leader bị lỗi sẽ có thể nhanh chóng có các parrtions đã backup dữ liệu và bầu ra 1 leader mới để quá trình dữ liệu vẫn diễn ra bình thường.
- Thứ 3 là cơ chế mở rộng các parrtion, broker/cluster khi dữ liệu đột ngột tăng bất ngờ, giải quyết vẫn để về scale nguồn lực.
- Thứ 4 la việc lưu trữ dữ liệu thì kafka lưu trữ dữ liệu trên ổ đĩa thì nếu việc xử lý dữ liệu bị lỗi thì sau đó để cho tech stack như spark đọc lại dữ liệu có offset gần nhất là có thể tiếp tục xử lý dữ liệu như bình thường.

Còn tech stack xử lý dữ liệu thì dùng  spark.
- Đầu tiền spark sẽ ưu điểm là sẽ nhận dữ liêu từ kafka rồi lưu và xử lý ngay trên RAM chứ ko cần phải xuống ổ đĩa cứng lấy dữ liệu nên sẽ rất nhanh.
- Thứ 2 là spark có driver làm master điều hành quá trình xử lý dữ liệu, nếu nó nhận thấy kafka phải gia tăng đột ngột số lượng partition thì nó cũng sẽ huy động và cấp phát bộ nhớ, gia tăng số lượng executor tương ứng với từng partition để xử lý dữ liệu.
- Thứ 3 là spark sẽ xử lý dữ liệu theo từng microbatch trong mỗi khoảng thời gian ngắn, ví dụ 5s, 10s thì nó gom 1 lần rùi xử lý 1 lần.
- Thứ 4 là nếu đột nhiên 1 executor bị lỗi thì driver sẽ cử ra 1 executor mới và đọc bản offet gần nhất trên kafka rùi làm tiếp.

Lưu dữ liệu thì sẽ có iceberg
- lí do chọn sẽ xét vào 2 yếu tố đó là lưu trữ dữ liệu lớn 1 cách liên tục và khả năng query đòng thời.
Không thể chọn datalake trong trường hợp này vì tuy rằng có thể lưu trữ dữ liệu lớn nhưng ko tối ưu cho query
Không chọn datawarehouse vì lưu trữ dữ liệu phải đi qua bước transform để đảm bảo tính ACID thì sẽ tốn rất nhiều thời gian.
=> Chọn lakehouse lai giữa 2 yếu tố trên.
+ iceberg sẽ tối ưu truy vấn ở chổ là sẽ có cơ chế metadata, thay vì phải quét hết các bảng thì dựa vào metatdata sẽ trỏ và truy xuất dữ liệu ở những cột có liên quan đến câu lệnh truy vấn.
+ iceberg có cơ chế upsert, ví dụ cần phải insert or update 1 dòng dữ liệu mới, thay vì phải quét hết toàn bộ dữ liệu cũ để xem có tồn tại hay không thì đơn giản là ghi ra 2 file , 1 file thì là delete file, 1 file là data file mới, dòng này cần sửa thì để xóa dòng đó đi thì ghi vào delete file rồi ghi dòng dữ liệu muốn update vào data file mới
+ cơ chế snapshot isolate sẽ giúp việc query và việc upert dữ liệu diễn ra đồng thời, vì nếu query thì sẽ query ở bản snapshot dữ liệu hoàn thành gần nhất, trong khi ghi dữ liệu sẽ tiến hành ở 1 snapshot trong tương lai. Và nhờ có cơ chế này thì bản thân các snapshot sẽ có thêm là 1 chuỗi các bản ghi theo dòng thời gian, nếu muốn query dữ liệu lịch sử hay muốn back lại dữ liệu cũ củng rất dễ dàng.




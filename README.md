# Big Data điểm thi THPT TP.HCM 2022–2024

Đồ án xây dựng pipeline Big Data để phân tích phổ điểm thi tốt nghiệp THPT tại
TP.HCM giai đoạn 2022–2024. Hệ thống kết hợp HDFS, Spark, Kafka, Spark
Structured Streaming, Airflow, MLlib K-Means và Streamlit trong Docker Compose.

> Không chạy `docker compose down -v` nếu muốn giữ dữ liệu HDFS, Kafka và
> PostgreSQL của Airflow.

## Kiến trúc

```text
CSV 2022–2024
      |
      v
HDFS Bronze (dữ liệu nguồn, không sửa)
      |
      v
Spark Bronze -> Silver (chuẩn hóa, data quality, lọc TP.HCM)
      |
      v
Spark SQL Silver -> Gold (phổ điểm, môn thi, tổ hợp)
      |
      +----> MLlib K-Means A00 -> features/model/thống kê cluster

Kafka producer -> Kafka KRaft -> Spark Structured Streaming
                                      |
                                      v
                         HDFS streaming/bronze + quarantine

Airflow DAG điều phối pipeline batch
Streamlit đọc Gold/Delta và hiển thị dashboard
```

Các thành phần chính:

- Hadoop 3.3.6: một NameNode, hai DataNode, replication factor bằng 2.
- Spark 3.5.9 Standalone: một master, hai worker và một client riêng.
- Kafka 3.9.1 chạy KRaft, topic `diemthi_realtime`.
- Spark Structured Streaming đọc Kafka, validate JSON và ghi HDFS.
- Airflow 2.10.5 dùng LocalExecutor và PostgreSQL.
- Streamlit cung cấp dashboard tương tác.

## Cấu trúc repository

```text
.
├── airflow/
│   ├── dags/diemthi_pipeline.py
│   └── Dockerfile
├── dashboard/
├── docs/
├── hadoop/conf/
├── spark/
├── spark-client/
├── src/
│   ├── delta/
│   ├── ml/
│   ├── orchestration/
│   ├── streaming/
│   └── transform/
├── docker-compose.yml
├── .env.example
└── README.md
```

Dữ liệu CSV, named volume, log, checkpoint và `.env` không được commit.

## Luồng dữ liệu HDFS

```text
/lakehouse/diemthi_hcm/
├── bronze/year=2022|2023|2024
├── silver
├── gold
├── features
├── models
├── streaming
├── checkpoints
└── _quarantine
```

- Bronze giữ nguyên CSV nguồn.
- Silver chuẩn hóa BOM/tên cột/kiểu dữ liệu, kiểm tra chất lượng, tính A00,
  A01, B00, C00, D01 và lọc mã hội đồng TP.HCM đã được xác minh từ dữ liệu.
- Gold chứa bảng tổng hợp phục vụ phân tích và dashboard.
- `_quarantine` lưu record vi phạm hard validation; không biến NULL thành 0.

## K-Means

`src/ml/train_kmeans_a00.py` dùng ba đặc trưng `Toan`, `VatLy`, `HoaHoc`,
chuẩn hóa bằng `StandardScaler`, thử K từ 2 đến 6 và chọn mô hình có silhouette
cao nhất. Model, dự đoán và thống kê được ghi lần lượt vào HDFS `models`,
`features` và `gold`. Script kiểm tra nằm tại `src/ml/verify_kmeans_a00.py`.

## Nguồn dữ liệu

Ba CSV dùng trong đồ án lấy từ repository công khai:
[sdgedfegw/du-lieu-diem-thi](https://github.com/sdgedfegw/du-lieu-diem-thi).
Repository đồ án này không phân phối lại dataset.

Tải và đặt ba file vào `data/raw/` với tên:

```text
diemthi_2022.csv
diemthi_2023.csv
diemthi_2024.csv
```

Nếu file vẫn mang tên gốc `du_lieu_diem_thi_YYYY.csv`, đổi tên khi copy vào
thư mục project. Ví dụ từ ổ D của Windows trong WSL:

```bash
mkdir -p /home/nguyen/BigData_DiemThi/data/raw
cp /mnt/d/bigdata/Doan/du-lieu-diem-thi/du_lieu_diem_thi_2022.csv \
  /home/nguyen/BigData_DiemThi/data/raw/diemthi_2022.csv
cp /mnt/d/bigdata/Doan/du-lieu-diem-thi/du_lieu_diem_thi_2023.csv \
  /home/nguyen/BigData_DiemThi/data/raw/diemthi_2023.csv
cp /mnt/d/bigdata/Doan/du-lieu-diem-thi/du_lieu_diem_thi_2024.csv \
  /home/nguyen/BigData_DiemThi/data/raw/diemthi_2024.csv
```

Upload vào Bronze (chỉ thực hiện khi HDFS chưa có dữ liệu):

```bash
docker cp data/raw/diemthi_2022.csv namenode:/tmp/diemthi_2022.csv
docker cp data/raw/diemthi_2023.csv namenode:/tmp/diemthi_2023.csv
docker cp data/raw/diemthi_2024.csv namenode:/tmp/diemthi_2024.csv

docker exec namenode hdfs dfs -put -f /tmp/diemthi_2022.csv \
  /lakehouse/diemthi_hcm/bronze/year=2022/
docker exec namenode hdfs dfs -put -f /tmp/diemthi_2023.csv \
  /lakehouse/diemthi_hcm/bronze/year=2023/
docker exec namenode hdfs dfs -put -f /tmp/diemthi_2024.csv \
  /lakehouse/diemthi_hcm/bronze/year=2024/
```

## Cài đặt và khởi động

Yêu cầu: Windows + WSL2 Ubuntu 22.04, Docker Desktop và WSL integration đã
được bật cho Ubuntu.

```bash
git clone https://github.com/phucnguyen772000-max/bigdata.git
cd bigdata
cp .env.example .env
```

Điền các phiên bản và credential cục bộ trong `.env`. Không commit file này.
Các biến bắt buộc được liệt kê trong `.env.example`.

```bash
docker compose config
docker compose build spark-master spark-client
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d
docker compose ps
```

## URL dịch vụ

| Dịch vụ | URL/địa chỉ |
|---|---|
| HDFS NameNode UI | http://localhost:9870 |
| HDFS RPC | `hdfs://namenode:9000` |
| Spark Master UI | http://localhost:8080 |
| Spark Master | `spark://spark-master:7077` |
| Airflow | http://localhost:8088 |
| Streamlit | http://localhost:8501 |
| Kafka nội bộ | `kafka:9092` |

Tài khoản Airflow được đọc từ `AIRFLOW_ADMIN_USERNAME` và
`AIRFLOW_ADMIN_PASSWORD` trong `.env`; repository không chứa mật khẩu.

## Chạy pipeline batch

Kiểm tra Spark và HDFS:

```bash
docker compose exec spark-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /app/src/test_hdfs_read.py
```

Pipeline thủ công:

```bash
docker compose exec spark-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /app/src/transform/bronze_to_silver.py

docker compose exec spark-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /app/src/transform/silver_to_gold.py

docker compose exec spark-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /app/src/ml/train_kmeans_a00.py
```

## Airflow DAG

DAG `diemthi_bigdata_pipeline` chạy chuỗi:

```text
check_hdfs -> check_bronze -> bronze_to_silver -> verify_silver
-> silver_to_gold -> verify_gold -> train_kmeans -> verify_model
```

DAG chạy thủ công (`schedule=None`) và các task gọi `spark-submit` thật:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags trigger diemthi_bigdata_pipeline
docker compose exec -T airflow-scheduler \
  airflow dags list-runs -d diemthi_bigdata_pipeline
```

## Kafka và Spark Structured Streaming

Topic `diemthi_realtime` được `kafka-init` tạo tự động. Chạy demo bằng ba
terminal:

```bash
# Terminal 1: producer
docker compose exec spark-client \
  python3 /app/src/streaming/kafka_producer_diemthi.py

# Terminal 2: consumer quan sát offset
docker compose exec spark-client \
  python3 /app/src/streaming/kafka_consumer_diemthi.py

# Terminal 3: Structured Streaming -> HDFS
docker compose exec spark-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /app/src/streaming/kafka_to_hdfs.py
```

Record hợp lệ được ghi vào `/lakehouse/diemthi_hcm/streaming/bronze`, record
không hợp lệ vào `/lakehouse/diemthi_hcm/streaming/quarantine`. Checkpoint chỉ
nằm trên HDFS/runtime và không được commit.

## Kiểm tra hệ thống

```bash
docker compose ps
docker exec namenode hdfs dfsadmin -report
docker exec namenode hdfs fsck /lakehouse/diemthi_hcm -files -blocks -locations
docker exec namenode hdfs dfs -ls -h -R /lakehouse/diemthi_hcm
```

Các log kiểm thử và bằng chứng thực nghiệm không chứa dataset được lưu trong
`docs/`.

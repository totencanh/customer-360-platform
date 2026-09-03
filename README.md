# Customer 360 Platform

Pipeline ETL xây dựng hồ sơ khách hàng 360° cho nền tảng truyền hình/viễn thông, sử dụng **Apache Spark (PySpark)** để xử lý dữ liệu tương tác nội dung và hành vi tìm kiếm của khách hàng, sau đó nạp kết quả vào **PostgreSQL** phục vụ báo cáo/BI.

## Mục lục

- [Kiến trúc tổng quan](#kiến-trúc-tổng-quan)
- [Cấu trúc thư mục](#cấu-trúc-thư-mục)
- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Cài đặt](#cài-đặt)
- [Cách sử dụng](#cách-sử-dụng)
- [Chi tiết pipeline](#chi-tiết-pipeline)
- [Schema dữ liệu đầu ra](#schema-dữ-liệu-đầu-ra)
- [Chạy bằng Docker](#chạy-bằng-docker)
- [Hạn chế hiện tại & Hướng phát triển](#hạn-chế-hiện-tại--hướng-phát-triển)

## Kiến trúc tổng quan

```
                ┌───────────────────────┐
   log_content/ │   ETL Interaction     │
   (*.json)  ──▶│  (etl_interaction.py) │──┐
                └───────────────────────┘  │
                                            ▼
                                  customer_content_stats
                                     (PostgreSQL)

                ┌───────────────────────┐
   log_search/  │    ETL Behavior       │
   (*.parquet)─▶│   (etl_behavior.py)   │──┐
                └───────────────────────┘  │
                                            ▼
                                  customer_behavior_stats
                                     (PostgreSQL)
```

Dự án gồm **2 pipeline độc lập**, cùng chạy trên Spark và cùng ghi kết quả vào một database `customer360`:

1. **ETL Interaction** — phân tích hành vi xem nội dung (log JSON) → phân loại nội dung, tính mức độ hoạt động, phân khúc sở thích khách hàng.
2. **ETL Behavior** — phân tích hành vi tìm kiếm (log Parquet) → xác định từ khóa tìm kiếm nhiều nhất theo từng khách hàng.

## Cấu trúc thư mục

```
customer-360-platform/
├── ETL/
│   └── data/log_content/          # Log tương tác dạng JSON, đặt tên theo yyyyMMdd.json
├── ETL_behavior/
│   └── data/log_search/           # Log tìm kiếm dạng Parquet, đặt tên theo yyyyMMdd
├── ETL_interaction/
│   └── etl_interaction.py         # Module transform + load cho pipeline nội dung
├── etl_script.py                  # Entry point pipeline nội dung (Extract → Transform → Load)
├── etl_behavior.py                # Entry point pipeline hành vi tìm kiếm
├── Dockerfile
├── requirement.txt
└── README.md
```

> **Lưu ý:** cấu trúc trên được suy ra từ câu lệnh import trong `etl_script.py`
> (`from ETL_interaction.etl_interaction import ...`). Hãy đảm bảo `etl_interaction.py`
> nằm trong thư mục `ETL_interaction/` cùng cấp với `etl_script.py` khi chạy.

## Yêu cầu hệ thống

- Python 3.8+ (Docker image dùng Python 3.11-slim)
- Java Runtime Environment (JRE) — bắt buộc để chạy PySpark
- Apache Spark (được cài qua `pyspark`)
- PostgreSQL đang chạy sẵn, có database `customer360`
- JDBC driver: `org.postgresql:postgresql:42.7.3` (tự tải qua `spark.jars.packages`)

## Cài đặt

```bash
git clone https://github.com/totencanh/customer-360-platform.git
cd customer-360-platform

# Cài Java (bắt buộc cho PySpark)
# Ubuntu/Debian:
sudo apt-get install default-jre

# Cài dependency Python
pip install -r requirement.txt
```

Tạo database và cấu hình PostgreSQL trước khi chạy:

```sql
CREATE DATABASE customer360;
CREATE USER totencanh WITH PASSWORD 'totencanh';
GRANT ALL PRIVILEGES ON DATABASE customer360 TO totencanh;
```

## Cách sử dụng

### 1. Chạy pipeline phân tích nội dung (Interaction)

```bash
python etl_script.py
```

Script sẽ hỏi lần lượt:
- `Nhap ngay:` — ngày bắt đầu, định dạng `YYYYMMDD` (ví dụ `20250601`)
- `Nhap ngay:` — ngày kết thúc, định dạng `YYYYMMDD`

Pipeline đọc toàn bộ file `log_content/<ngày>.json` trong khoảng ngày đã chọn, gộp lại, transform, ghi CSV kết quả và nạp vào bảng `customer_content_stats`.

### 2. Chạy pipeline phân tích hành vi tìm kiếm (Behavior)

```bash
python etl_behavior.py
```

Tương tự, nhập ngày bắt đầu/kết thúc để đọc các file `log_search/<ngày>` (Parquet), tính từ khóa tìm kiếm nhiều nhất mỗi khách hàng và nạp vào bảng `customer_behavior_stats`.

> Đường dẫn input/output hiện đang **hard-code** trong code (ví dụ `D:/ChuyenNganh/Project/customer-360-platform/...`). Cần sửa lại theo môi trường máy bạn trước khi chạy — xem phần [Hạn chế hiện tại](#hạn-chế-hiện-tại--hướng-phát-triển).

## Chi tiết pipeline

### ETL Interaction (`etl_interaction.py`)

| Bước | Hàm | Mô tả |
|---|---|---|
| Phân loại nội dung | `transform_category()` | Map `AppName` (CHANNEL, KPLUS, VOD, SPORT...) sang nhóm nội dung: Truyền Hình, Phim Truyện, Giải Trí, Thiếu Nhi, Thể Thao |
| Tổng hợp thời lượng | `statistic_total()` → `pivot_data()` | Gộp `TotalDuration` theo `Contract`, `Type`, `date`, sau đó pivot thành các cột theo loại nội dung |
| Đếm thiết bị | `cal_device()` | Đếm số thiết bị (`Mac`) duy nhất trên mỗi hợp đồng |
| Mức độ hoạt động | `sum_activeness()` | Tính số ngày hoạt động và gán nhãn: very low (1–7), low (8–14), moderate (15–21), high (22–28), very high (29–31) |
| Nội dung xem nhiều nhất | `most_watch()` | Xác định loại nội dung có thời lượng lớn nhất trên mỗi hợp đồng |
| Hồ sơ sở thích | `customer_taste()` | Ghép các loại nội dung có thời lượng > 0 thành chuỗi (vd: `Phim Truyện,Thể Thao`) |
| Ghi kết quả | `save_file()`, `import_to_postgres()` | Xuất CSV (1 partition) và nạp vào bảng `customer_content_stats` |

### ETL Behavior (`etl_behavior.py`)

| Bước | Hàm | Mô tả |
|---|---|---|
| Đếm tần suất từ khóa | `process_log_search()` | Group theo `user_id`, `keyword`, đếm số lần xuất hiện (`TotalSearch`) |
| Xếp hạng | `Window.partitionBy('user_id').orderBy(TotalSearch desc)` + `row_number()` | Xếp hạng từ khóa theo từng người dùng |
| Lọc top 1 | `filter(Rank == 1)` | Giữ lại từ khóa được tìm nhiều nhất mỗi người dùng |
| Ghi kết quả | `import_to_postgres()` | Nạp vào bảng `customer_behavior_stats` |

## Schema dữ liệu đầu ra

**`customer_content_stats`**

| Cột | Kiểu | Mô tả |
|---|---|---|
| Contract | string | Mã hợp đồng khách hàng |
| Truyền Hình, Phim Truyện, Giải Trí, Thiếu Nhi, Thể Thao | double | Tổng thời lượng xem theo từng loại nội dung |
| activate_level | string | Mức độ hoạt động (very low → very high) |
| Total_device | long | Số thiết bị duy nhất đã dùng |
| most_watch | string | Loại nội dung được xem nhiều nhất |
| customer_taste | string | Danh sách loại nội dung khách từng xem, phân tách bởi dấu phẩy |

**`customer_behavior_stats`**

| Cột | Kiểu | Mô tả |
|---|---|---|
| user_id | string | Mã người dùng |
| Most_Search | string | Từ khóa được tìm kiếm nhiều nhất trong khoảng thời gian đã chọn |

## Chạy bằng Docker

```bash
docker build -t customer360-etl .
docker run --network host customer360-etl
```

Image build từ `python:3.11-slim`, tự cài Java (`default-jre`) để chạy PySpark, cài dependency từ `requirement.txt`.

> **Cần cập nhật Dockerfile:** `COPY` và `ENTRYPOINT` hiện đang trỏ tới `etl_method_1.py` / `etl_file.py`, không khớp tên file thực tế trong repo (`etl_script.py`, `etl_interaction.py`, `etl_behavior.py`). Cần sửa lại cho đúng trước khi build, ví dụ:
> ```dockerfile
> COPY etl_script.py etl_behavior.py .
> COPY ETL_interaction/ ./ETL_interaction/
> ENTRYPOINT ["python", "etl_script.py"]
> ```
> Vì kết nối tới PostgreSQL dùng `localhost`, cần chạy container với `--network host` (Linux) hoặc trỏ `url` sang địa chỉ container/host phù hợp.

## Hạn chế hiện tại & Hướng phát triển

- [ ] **Đường dẫn hard-code**: input/output path đang hard-code theo máy Windows cá nhân. `etl_script.py` đã có sẵn `argparse` (`--input_path`, `--output_path`, `--current_day`, `--to_day`) nhưng đang bị comment — nên bật lại để chạy linh hoạt qua CLI.
- [ ] **Thông tin kết nối DB hard-code**: user/password/host của PostgreSQL đang gán cứng trong code — nên chuyển sang biến môi trường (`.env`) hoặc file config.
- [ ] **Hàm `save_path()` trong `etl_behavior.py`** hiện chưa được gọi trong luồng chính và tham chiếu nhầm biến `save_path` (trùng tên hàm) thay vì tham số `df`/`output_path` — cần rà soát lại trước khi dùng để xuất CSV.
- [ ] **Chưa có test tự động**: nên bổ sung unit test cho các hàm transform bằng `pytest` + `pyspark.testing` hoặc dữ liệu mẫu nhỏ.
- [ ] **Điều phối pipeline**: có thể tích hợp Airflow/dbt để lập lịch và quản lý dependency giữa 2 pipeline thay vì chạy thủ công qua `input()`.
- [ ] **Kết nối 2 pipeline**: hiện `customer_content_stats` và `customer_behavior_stats` là 2 bảng tách biệt — có thể join theo khóa khách hàng chung để tạo view "Customer 360" hợp nhất thực sự.

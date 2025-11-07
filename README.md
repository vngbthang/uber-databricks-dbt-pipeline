# 🚕 Uber ELT Pipeline on Databricks

## 📋 Mục tiêu (Objective)

Project này xây dựng một **Data Lakehouse hoàn chỉnh** theo kiến trúc **Medallion (Bronze-Silver-Gold)** cho dữ liệu Uber, tạo ra các bảng phân tích (Fact/Dimension tables) sẵn sàng cho business intelligence và data analytics.

**Vấn đề giải quyết:**
- Ingestion dữ liệu từ nhiều nguồn CSV vào Data Lake
- Làm sạch, deduplication và upsert dữ liệu theo CDC (Change Data Capture)
- Tạo dữ liệu lịch sử với SCD Type 2 (Slowly Changing Dimensions)
- Cung cấp bảng fact incremental cho phân tích trips

---

## 🏗️ Kiến trúc (Architecture)

```
┌─────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   Source    │      │    Bronze    │      │    Silver    │      │     Gold     │
│  (Volumes)  │─────▶│ (Delta Lake) │─────▶│ (Delta Lake) │─────▶│ (Delta Lake) │
│             │      │              │      │              │      │              │
│ CSV Files:  │      │ Raw ingested │      │ Cleaned +    │      │ Fact Tables  │
│ - customers │      │ data with    │      │ Deduplicated │      │ - FactTrips  │
│ - trips     │      │ full history │      │ + Upserted   │      │              │
│ - locations │      │              │      │              │      │ Dim Tables   │
│ - payments  │      │              │      │              │      │ (SCD Type 2) │
│ - vehicles  │      │              │      │              │      │ - DimCustomers│
│ - drivers   │      │              │      │              │      │ - DimDrivers  │
└─────────────┘      └──────────────┘      └──────────────┘      │ - DimVehicles │
                              │                      │            │ - DimLocations│
                              │                      │            │ - DimPayments │
                              ▼                      ▼            └──────────────┘
                        PySpark Streaming      PySpark + MERGE           │
                        (trigger once)         (Upsert logic)            ▼
                                                                    dbt snapshots
                                                                  + incremental models
```

### Luồng dữ liệu chi tiết:
1. **Bronze Layer**: PySpark Streaming đọc CSV từ Volumes → Append vào Delta Tables
2. **Silver Layer**: PySpark transformations (dedup + upsert) → MERGE vào Delta Tables
3. **Gold Layer**: dbt snapshots (SCD Type 2) + incremental models → Tables cho Analytics

---

## 🛠️ Công nghệ sử dụng (Tech Stack)

| Component | Technology |
|-----------|-----------|
| **Nền tảng** | Databricks (Community Edition / Trial) |
| **Data Lake** | Delta Lake |
| **Storage** | Databricks Volumes (Unity Catalog) |
| **Ingestion (Bronze)** | PySpark Streaming với `trigger(once=True)` |
| **Transformation (Silver)** | PySpark + `DeltaTable.merge()` (Upsert logic) |
| **Transformation (Gold)** | dbt-databricks |
| **Mô hình hóa** | dbt Snapshots (SCD Type 2) + Incremental Models |
| **Orchestration** | Databricks Notebooks |
| **Deduplication** | Window functions với `row_number()` |

---

## 📊 Cấu trúc Dữ liệu (Schema)

### Catalog Structure:
```
pysparkdbt (catalog)
├── source (schema)
│   └── source_data (volume)
│       ├── customers/
│       ├── trips/
│       ├── locations/
│       ├── payments/
│       ├── vehicles/
│       └── drivers/
│
├── bronze (schema)
│   ├── customers (delta table)
│   ├── trips (delta table)
│   ├── locations (delta table)
│   ├── payments (delta table)
│   ├── vehicles (delta table)
│   ├── drivers (delta table)
│   └── checkpoints/ (streaming checkpoints)
│
├── silver (schema)
│   ├── customers (delta table - deduplicated + upserted)
│   ├── trips (delta table)
│   ├── locations (delta table)
│   ├── payments (delta table)
│   ├── vehicles (delta table)
│   └── drivers (delta table)
│
└── gold (schema)
    ├── FactTrips (incremental model)
    ├── DimCustomers (snapshot - SCD Type 2)
    ├── DimDrivers (snapshot - SCD Type 2)
    ├── DimVehicles (snapshot - SCD Type 2)
    ├── DimLocations (snapshot - SCD Type 2)
    └── DimPayments (snapshot - SCD Type 2)
```

### Key Entities:
- **customers**: Customer information
- **trips**: Trip details (trip_id, customer_id, driver_id, timestamps, fare, distance)
- **locations**: Pickup/dropoff locations
- **payments**: Payment transactions
- **vehicles**: Vehicle information
- **drivers**: Driver information

---

## 🚀 Cách thiết lập và chạy (Setup & Run)

### Prerequisites:
- Databricks workspace (Community Edition hoặc Trial)
- Python 3.8+
- dbt-databricks package

### Bước 1: Thiết lập Databricks Cluster
```bash
# Khởi tạo cluster với:
- Runtime: DBR 13.3 LTS hoặc mới hơn
- Node type: Standard (phù hợp với community edition)
- Enable Unity Catalog
```

### Bước 2: Upload dữ liệu lên Volumes
```sql
-- Tạo catalog và schema
CREATE CATALOG IF NOT EXISTS pysparkdbt;
CREATE SCHEMA IF NOT EXISTS pysparkdbt.source;
CREATE SCHEMA IF NOT EXISTS pysparkdbt.bronze;
CREATE SCHEMA IF NOT EXISTS pysparkdbt.silver;
CREATE SCHEMA IF NOT EXISTS pysparkdbt.gold;

-- Tạo Volume
CREATE VOLUME IF NOT EXISTS pysparkdbt.source.source_data;
```

Upload các file CSV vào `/Volumes/pysparkdbt/source/source_data/{entity}/`

### Bước 3: Chạy Bronze Ingestion
```bash
# Mở notebook: databricks/notebooks/bronze_ingestion.ipynb
# Chạy tất cả cells để ingest dữ liệu từ CSV vào Bronze Delta tables
```

**Công việc notebook này:**
- Đọc CSV từ Volumes với Spark Streaming
- Infer schema từ batch read
- Write stream với `trigger(once=True)` vào Bronze tables
- Lưu checkpoints để tracking progress

### Bước 4: Chạy Silver Transformation
```bash
# Mở notebook: databricks/notebooks/silver_transformation.ipynb
# Chạy tất cả cells để transform và upsert vào Silver tables
```

**Công việc notebook này:**
- **Deduplication**: Sử dụng `row_number()` window function để loại bỏ duplicates
- **CDC Processing**: So sánh timestamp để giữ bản ghi mới nhất
- **Upsert logic**: Dùng `DeltaTable.merge()` để update hoặc insert
- **Process timestamp**: Thêm metadata timestamp cho audit

### Bước 5: Cài đặt dbt
```bash
# Cài đặt dbt-databricks
pip install dbt-databricks

# Navigate to dbt project
cd dbt_project

# Cấu hình profiles.yml
# Tạo file ~/.dbt/profiles.yml với thông tin kết nối Databricks
```

**Cấu hình profiles.yml mẫu:**
```yaml
default:
  outputs:
    dev:
      type: databricks
      catalog: pysparkdbt
      schema: gold
      host: <your-databricks-workspace-url>
      http_path: <your-cluster-http-path>
      token: <your-access-token>
      threads: 4
  target: dev
```

### Bước 6: Chạy dbt models
```bash
# Test kết nối
dbt debug

# Chạy incremental models (FactTrips)
dbt run --select silver.trips

# Chạy snapshots để tạo Dimension tables với SCD Type 2
dbt snapshot
```

**Kết quả dbt snapshot:**
- Tạo các bảng Dimension với columns: `dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`
- Track lịch sử thay đổi của từng record
- `dbt_valid_to = '9999-12-31'` cho current records

### Bước 7: Kiểm tra dữ liệu
```sql
-- Kiểm tra FactTrips
SELECT * FROM pysparkdbt.gold.trips LIMIT 10;

-- Kiểm tra DimCustomers (SCD Type 2)
SELECT * FROM pysparkdbt.gold.dimcustomers 
WHERE customer_id = 'C001'
ORDER BY dbt_valid_from;

-- Kiểm tra số lượng records
SELECT 
  'Bronze' as layer, COUNT(*) as row_count FROM pysparkdbt.bronze.trips
UNION ALL
SELECT 
  'Silver' as layer, COUNT(*) as row_count FROM pysparkdbt.silver.trips
UNION ALL
SELECT 
  'Gold' as layer, COUNT(*) as row_count FROM pysparkdbt.gold.trips;
```

---

## 📈 Kết quả (Final Output)

### Gold Layer Tables:

**1. FactTrips (Incremental Model)**
- Chứa toàn bộ thông tin trips với incremental load
- Chỉ load records mới dựa trên `last_updated_timestamp`

**2. Dimension Tables (SCD Type 2)**
- **DimCustomers**: Lịch sử thay đổi thông tin customers
- **DimDrivers**: Lịch sử thay đổi thông tin drivers
- **DimVehicles**: Lịch sử thay đổi thông tin vehicles
- **DimLocations**: Lịch sử thay đổi thông tin locations
- **DimPayments**: Lịch sử thay đổi thông tin payments

### Sample Query Results:
```sql
-- Phân tích trips theo driver với historical data
SELECT 
  d.driver_id,
  d.driver_name,
  d.dbt_valid_from,
  d.dbt_valid_to,
  COUNT(t.trip_id) as total_trips,
  SUM(t.fare_amount) as total_revenue
FROM pysparkdbt.gold.trips t
JOIN pysparkdbt.gold.dimdriver d 
  ON t.driver_id = d.driver_id
  AND t.trip_start_time BETWEEN d.dbt_valid_from AND d.dbt_valid_to
GROUP BY 1, 2, 3, 4
ORDER BY total_revenue DESC;
```

---

## 📁 Cấu trúc Project

```
uber-databricks-dbt-pipeline/
├── databricks/
│   └── notebooks/
│       ├── bronze_ingestion.ipynb      # PySpark Streaming ingestion
│       └── silver_transformation.ipynb # Dedup + Upsert logic
├── dbt_project/
│   ├── dbt_project.yml                 # dbt configuration
│   ├── models/
│   │   ├── silver/
│   │   │   └── trips.sql               # Incremental model
│   │   └── sources/
│   │       └── sources.yaml            # Source definitions
│   ├── snapshots/
│   │   ├── SCDs.yaml                   # Dimension snapshots (SCD Type 2)
│   │   └── snap_fact.yaml
│   ├── macros/
│   │   └── generate_schema_name.sql    # Custom schema macro
│   ├── tests/                          # Custom data tests
│   └── analyses/
│       └── scratch.sql                 # Ad-hoc queries
├── requirements.txt                    # Python dependencies
└── README.md                           # This file
```

---

## 🎯 Key Features

✅ **Medallion Architecture**: Bronze → Silver → Gold layers với Delta Lake  
✅ **Incremental Processing**: Chỉ xử lý dữ liệu mới, tiết kiệm compute  
✅ **Data Quality**: Deduplication và upsert logic với CDC  
✅ **Historical Tracking**: SCD Type 2 cho Dimension tables  
✅ **Scalability**: Spark Streaming và Delta Lake optimization  
✅ **Idempotency**: Checkpoints và merge operations đảm bảo re-run safety  


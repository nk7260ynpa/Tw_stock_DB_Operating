# Tw_stock_DB_Operating

台股資料庫操作模組，提供資料上傳與 MySQL 連線管理功能。

## 功能說明

- **DB 存取層**：透過 SQLAlchemy 建立 MySQL 連線（`clients.py`、`routers.py`）
- **DB 上傳層**：爬蟲資料預處理、schema 驗證與批次上傳（`data_upload/`）
- **批次上傳**：支援日期範圍批次上傳（`upload.py`）
- **每日排程**：每日自動排程上傳最近 7 天資料（`DailyUpload.py`）

## 支援的資料來源

| 模組 | 說明 |
|------|------|
| TWSE | 台灣證券交易所每日行情 |
| TPEX | 證券櫃檯買賣中心每日行情 |
| TAIFEX | 台灣期貨交易所每日行情 |
| FAOI | 三大法人買賣超 |
| MGTS | 融資融券 |

## 專案結構

```
Tw_stock_DB_Operating/
├── clients.py                # MySQL 連線函式
├── routers.py                # MySQLRouter 路由類別
├── upload.py                 # 批次上傳入口程式
├── DailyUpload.py            # 每日排程上傳
├── requirements.txt          # Python 套件依賴
├── run.sh                    # 啟動主程式腳本
├── data_upload/              # 資料上傳模組
│   ├── __init__.py
│   ├── base.py               # DataUploadBase 抽象基類
│   ├── twse.py
│   ├── tpex.py
│   ├── taifex.py
│   ├── faoi.py
│   └── mgts.py
├── docker/                   # Docker 設定
│   ├── build.sh              # 建立 Docker image 腳本
│   ├── Dockerfile
│   └── docker-compose.yaml
├── test/                     # 單元測試
│   └── test_routers.py
└── logs/                     # 日誌資料夾
```

## 使用方式

### 1. 建立 Docker image

```bash
bash docker/build.sh
```

### 2. 批次上傳（指定日期範圍）

```bash
# 上傳單日
./run.sh python upload.py --start_date 2024-01-02 --dbname TWSE

# 上傳日期範圍
./run.sh python upload.py --start_date 2024-01-02 --end_date 2024-01-31 --dbname TWSE
```

### 3. 啟動每日排程上傳

```bash
./run.sh
```

### 4. 使用 docker-compose 啟動服務

```bash
docker compose -f docker/docker-compose.yaml up -d
```

### 5. 執行單元測試

```bash
docker run --rm tw_stock_db_operating:1.0.0 python -m pytest test/
```

## 命令列參數（upload.py）

| 參數 | 說明 | 預設值 |
|------|------|--------|
| `--start_date` | 起始日期（YYYY-MM-DD） | 必填 |
| `--end_date` | 結束日期（YYYY-MM-DD） | 同 start_date |
| `--host` | MySQL 主機位址 | `localhost:3306` |
| `--user` | MySQL 使用者名稱 | `root` |
| `--password` | MySQL 密碼 | `stock` |
| `--dbname` | 資料庫名稱 | `TWSE` |
| `--crawlerhost` | 爬蟲服務主機位址 | `127.0.0.1:6738` |

## 環境需求

- Python 3.12.7
- Docker
- MySQL 資料庫（需先建立資料庫與資料表）

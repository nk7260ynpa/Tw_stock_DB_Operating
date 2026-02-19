# Tw_stock_DB_Operating

台股資料庫操作模組，提供資料上傳與 MySQL 連線管理功能。

## 功能說明

- **DB 存取層**：透過 SQLAlchemy 建立 MySQL 連線（`clients.py`、`routers.py`）
- **DB 上傳層**：爬蟲資料預處理、schema 驗證與批次上傳（`data_upload/`）
- **批次上傳**：支援日期範圍批次上傳（`upload.py`）
- **每日排程**：自動檢查過去 30 天，補抓缺漏資料（`DailyUpload.py`）
- **Web 管理介面**：透過瀏覽器手動觸發上傳、修改排程時間（`web_server.py`）

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
├── web_server.py             # Web 管理介面（FastAPI）
├── data_upload/              # 資料上傳模組
│   ├── __init__.py
│   ├── base.py               # DataUploadBase 抽象基類
│   ├── twse.py
│   ├── tpex.py
│   ├── taifex.py
│   ├── faoi.py
│   └── mgts.py
├── frontend/                 # React 前端原始碼（Vite）
│   ├── package.json
│   ├── vite.config.js
│   ├── index.html
│   └── src/
│       ├── App.jsx
│       └── components/
│           ├── ManualUpload.jsx
│           └── ScheduleManager.jsx
├── docker/                   # Docker 設定
│   ├── build.sh              # 建立 Docker image 腳本
│   ├── Dockerfile            # Multi-stage build（Node + Python）
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

### 3. 啟動服務（含 Web 管理介面與每日排程）

啟動後可透過瀏覽器開啟 `http://localhost:8080` 存取管理介面。

```bash
# 透過 run.sh 啟動
./run.sh

# 或背景啟動
docker run -d --name tw_stock_db_operating \
  --network db_network \
  -p 8080:8080 \
  -v $(pwd)/logs:/workspace/logs \
  nk7260ynpa/tw_stock_db_operating:2.0.0
```

### 4. 使用 docker-compose 啟動服務

```bash
docker compose -f docker/docker-compose.yaml up -d
```

### 5. 執行單元測試

```bash
docker run --rm nk7260ynpa/tw_stock_db_operating:2.0.0 python -m pytest test/
```

## 命令列參數（upload.py）

| 參數 | 說明 | 預設值 |
|------|------|--------|
| `--start_date` | 起始日期（YYYY-MM-DD） | 必填 |
| `--end_date` | 結束日期（YYYY-MM-DD） | 同 start_date |
| `--host` | MySQL 主機位址 | `tw_stock_database:3306` |
| `--user` | MySQL 使用者名稱 | `root` |
| `--password` | MySQL 密碼 | `stock` |
| `--dbname` | 資料庫名稱 | `TWSE` |
| `--crawlerhost` | 爬蟲服務主機位址 | `tw_stocker_crawler:6738` |

## Web 管理介面

啟動服務後開啟 `http://localhost:8080`，提供以下功能：

- **手動上傳**：選擇日期範圍與資料庫，直接觸發資料上傳
- **排程設定**：檢視與修改每日自動上傳的排程時間

排程設定會儲存至 `logs/config.json`，容器重啟後自動套用。

## 環境需求

- Python 3.12.7
- Docker
- MySQL 資料庫（需先建立資料庫與資料表）

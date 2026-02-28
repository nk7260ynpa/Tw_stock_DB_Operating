# Tw_stock_DB_Operating

台股資料庫操作模組，提供資料上傳與 MySQL 連線管理功能。

## 功能說明

- **DB 存取層**：透過 SQLAlchemy 建立 MySQL 連線（`clients.py`、`routers.py`）
- **DB 上傳層**：爬蟲資料預處理、schema 驗證與批次上傳（`data_upload/`）
- **批次上傳**：支援日期範圍批次上傳（`upload.py`）
- **每日排程**：自動檢查過去 30 天，補抓缺漏資料（`DailyUpload.py`）
- **Web 管理介面**：透過瀏覽器手動觸發上傳、修改排程時間（`web_server.py`）
- **季度營業收入**：從公開資訊觀測站 (MOPS) 抓取上市公司季度營業收入（`data_upload/quarter_revenue.py`）
- **CTEE 新聞**：從爬蟲取得工商時報新聞，metadata 存入 MySQL，全文存為 txt 檔（`data_upload/ctee_news.py`）
- **CNYES 新聞**：從爬蟲取得鉅亨網新聞，metadata 存入 MySQL，全文存為 md 檔（`data_upload/cnyes_news.py`）
- **PTT 新聞**：從爬蟲取得 PTT 股版新聞，metadata 存入 MySQL，全文存為 md 檔（`data_upload/ptt_news.py`）
- **MoneyUDN 新聞**：從爬蟲取得經濟日報（聯合新聞網）新聞，metadata 存入 MySQL，全文存為 md 檔（`data_upload/moneyudn_news.py`）

## 支援的資料來源

| 模組 | 說明 |
|------|------|
| TWSE | 台灣證券交易所每日行情 |
| TPEX | 證券櫃檯買賣中心每日行情 |
| TAIFEX | 台灣期貨交易所每日行情 |
| FAOI | 三大法人買賣超（存放於 TWSE 資料庫的 FAOIDailyPrice 表） |
| MGTS | 融資融券（存放於 TWSE 資料庫的 MGTSDailyPrice 表） |
| QuarterRevenue | 上市公司季度營業收入（MOPS，存放於 TWSE 資料庫） |
| TDCC | 集保庫存分級（每日排程檢查並上傳新資料，存放於 TWSE 資料庫） |
| CTEE News | 工商時報新聞（metadata 存於 NEWS.CTEE，全文存為 txt 檔） |
| CNYES News | 鉅亨網新聞（metadata 存於 NEWS.CNYES，全文存為 md 檔） |
| PTT News | PTT 股版新聞（metadata 存於 NEWS.PTT，全文存為 md 檔） |
| MoneyUDN News | 經濟日報新聞（metadata 存於 NEWS.MoneyUDN，全文存為 md 檔） |

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
│   ├── mgts.py
│   ├── quarter_revenue.py    # 季度營業收入（MOPS）
│   ├── tdcc.py               # TDCC 集保庫存分級
│   ├── ctee_news.py          # CTEE 工商時報新聞
│   ├── cnyes_news.py         # CNYES 鉅亨網新聞
│   ├── ptt_news.py           # PTT 股版新聞
│   └── moneyudn_news.py     # MoneyUDN 經濟日報新聞
├── frontend/                 # React 前端原始碼（Vite）
│   ├── package.json
│   ├── vite.config.js
│   ├── index.html
│   └── src/
│       ├── App.jsx
│       ├── App.css
│       └── components/
│           ├── ManualUpload.jsx
│           ├── ScheduleManager.jsx
│           ├── QuarterRevenueUpload.jsx
│           ├── TDCCUpload.jsx
│           ├── CTEENewsUpload.jsx
│           ├── CNYESNewsUpload.jsx
│           ├── PTTNewsUpload.jsx
│           └── MoneyUDNNewsUpload.jsx
├── docker/                   # Docker 設定
│   ├── build.sh              # 建立 Docker image 腳本
│   ├── Dockerfile            # Multi-stage build（Node + Python）
│   └── docker-compose.yaml
├── test/                     # 單元測試
│   ├── test_base.py
│   ├── test_clients.py
│   ├── test_daily_upload.py
│   ├── test_faoi.py
│   ├── test_mgts.py
│   ├── test_quarter_revenue.py
│   ├── test_routers.py
│   ├── test_taifex.py
│   ├── test_tpex.py
│   ├── test_twse.py
│   ├── test_upload.py
│   ├── test_web_server.py
│   ├── test_web_server_revenue.py
│   ├── test_tdcc.py
│   ├── test_web_server_tdcc.py
│   ├── test_ctee_news.py
│   ├── test_web_server_ctee.py
│   ├── test_cnyes_news.py
│   ├── test_web_server_cnyes.py
│   ├── test_ptt_news.py
│   ├── test_web_server_ptt.py
│   ├── test_moneyudn_news.py
│   └── test_web_server_moneyudn.py
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
  nk7260ynpa/tw_stock_db_operating:2.2.0
```

### 4. 使用 docker-compose 啟動服務

```bash
docker compose -f docker/docker-compose.yaml up -d
```

### 5. 執行單元測試

```bash
docker run --rm nk7260ynpa/tw_stock_db_operating:2.2.0 python -m pytest test/
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
- **季度營業收入**：選擇民國年與季度，從 MOPS 抓取上市公司營業收入
- **TDCC 集保庫存**：一鍵取得最新集保庫存分級資料，每日排程自動檢查並上傳新資料
- **CTEE 新聞**：選擇日期範圍上傳工商時報新聞，metadata 寫入 MySQL，全文存為 txt 檔，每日排程自動抓取當日新聞
- **CNYES 新聞**：選擇日期範圍上傳鉅亨網新聞，metadata 寫入 MySQL，全文存為 md 檔，每日排程自動抓取當日新聞
- **PTT 新聞**：選擇日期範圍上傳 PTT 股版新聞，metadata 寫入 MySQL，全文存為 md 檔，每日排程自動抓取當日新聞
- **MoneyUDN 新聞**：選擇日期範圍上傳經濟日報新聞，metadata 寫入 MySQL，全文存為 md 檔，每日排程自動抓取當日新聞

排程設定會儲存至 `logs/config.json`，容器重啟後自動套用。

## 環境需求

- Python 3.12.7
- Docker
- MySQL 資料庫（需先建立資料庫與資料表）

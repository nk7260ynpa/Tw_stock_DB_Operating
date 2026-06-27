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
- **公司產業對照**：從爬蟲取得 TWSE/TPEX 公司基本資料與產業對照表，寫入 TWSE 資料庫（`data_upload/company_info.py`）
- **YT 逐字稿**：從「游庭皓的財經皓角」YouTube 頻道取得直播影片，透過 yt-dlp 下載自動字幕並解析為 Markdown 逐字稿，metadata 存入 MySQL，全文存為 md 檔（`data_upload/yt_transcript.py`）
- **原油價格**：從爬蟲取得國際原油價格（WTI/Brent），metadata 存入 SPECIAL_INFO 資料庫的 OilPrice 表（`data_upload/oil_price.py`）
- **黃金價格**：從爬蟲取得國際黃金期貨價格，metadata 存入 SPECIAL_INFO 資料庫的 GoldPrice 表（`data_upload/gold_price.py`）
- **比特幣價格**：從爬蟲取得比特幣價格，metadata 存入 SPECIAL_INFO 資料庫的 BitcoinPrice 表（`data_upload/bitcoin_price.py`）
- **匯率**：從爬蟲取得匯率資料（USDTWD/JPYTWD），metadata 存入 SPECIAL_INFO 資料庫的 CurrencyPrice 表（`data_upload/currency_price.py`）
- **股市指數**：從爬蟲取得國際股市指數價格（道瓊工業指數/納斯達克指數），資料存入 SPECIAL_INFO 資料庫的 IndicesPrice 表（`data_upload/indices_price.py`）
- **失敗重試佇列**：排程任務失敗時自動加入重試佇列。網路中斷每小時檢查網路並重試，最多 5 次；非網路錯誤（如「資料尚未發布」）標為 exhausted 後，每日（預設 06:30）「隔日重排」重設為 pending 再試一輪，最多 3 次，避免永久放棄隔日才會出現的資料（`retry_queue.py`）

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
| CompanyInfo | 公司產業對照（存放於 TWSE 資料庫的 CompanyInfo 和 IndustryMap 表） |
| YT Transcript | YouTube 逐字稿（metadata 存於 NEWS.YTTranscript，全文存為 md 檔） |
| OilPrice | 國際原油價格（WTI/Brent，存放於 SPECIAL_INFO 資料庫的 OilPrice 表） |
| GoldPrice | 國際黃金期貨價格（存放於 SPECIAL_INFO 資料庫的 GoldPrice 表） |
| BitcoinPrice | 比特幣價格（存放於 SPECIAL_INFO 資料庫的 BitcoinPrice 表） |
| CurrencyPrice | 匯率資料（USDTWD/JPYTWD，存放於 SPECIAL_INFO 資料庫的 CurrencyPrice 表） |
| IndicesPrice | 股市指數價格（DowJones/Nasdaq，存放於 SPECIAL_INFO 資料庫的 IndicesPrice 表） |

## 專案結構

```
Tw_stock_DB_Operating/
├── clients.py                # MySQL 連線函式
├── routers.py                # MySQLRouter 路由類別
├── upload.py                 # 批次上傳入口程式
├── DailyUpload.py            # 每日排程上傳
├── retry_queue.py            # 網路失敗重試佇列
├── job_queue.py              # 任務佇列（FIFO 排隊機制）
├── pyproject.toml            # Python 專案定義（PEP 621）
├── requirements.txt          # Docker 環境釘版依賴
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
│   ├── moneyudn_news.py     # MoneyUDN 經濟日報新聞
│   ├── company_info.py      # 公司產業對照
│   ├── yt_transcript.py     # YouTube 逐字稿（yt-dlp 自動字幕）
│   ├── oil_price.py         # 國際原油價格（WTI/Brent）
│   ├── gold_price.py        # 國際黃金期貨價格
│   ├── bitcoin_price.py     # 比特幣價格
│   ├── currency_price.py    # 匯率資料（USDTWD/JPYTWD）
│   └── indices_price.py    # 股市指數價格（DowJones/Nasdaq）
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
│           ├── MoneyUDNNewsUpload.jsx
│           ├── CompanyInfoUpload.jsx
│           ├── RetryQueue.jsx
│           ├── YTTranscriptUpload.jsx
│           ├── OilPriceUpload.jsx
│           └── IndicesPriceUpload.jsx
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
│   ├── test_web_server_moneyudn.py
│   ├── test_company_info.py
│   ├── test_web_server_company_info.py
│   ├── test_yt_transcript.py
│   ├── test_web_server_yt_transcript.py
│   ├── test_oil_price.py
│   ├── test_web_server_oil_price.py
│   ├── test_gold_price.py
│   ├── test_web_server_gold_price.py
│   ├── test_bitcoin_price.py
│   ├── test_web_server_bitcoin_price.py
│   ├── test_currency_price.py
│   ├── test_web_server_currency_price.py
│   ├── test_indices_price.py
│   ├── test_web_server_indices_price.py
│   ├── test_retry_queue.py
│   └── test_job_queue.py
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
  nk7260ynpa/tw_stock_db_operating:latest
```

### 4. 使用 docker-compose 啟動服務

```bash
docker compose -f docker/docker-compose.yaml up -d
```

### 5. 執行單元測試

```bash
docker run --rm nk7260ynpa/tw_stock_db_operating:latest python -m pytest test/
```

## 版本管理（單一版本軸 = git tag）

本專案以 **git tag（`vX.Y.Z`）為唯一版本真實來源**，避免「手動部署」與「CI 部署」跑出
矛盾或倒退的版本：

- **CI deploy**（`.gitlab-ci.yml`）：以 `${CI_COMMIT_TAG#v}` 為 `$VERSION`，build
  `:$VERSION` 與 `:latest`，並以 `:$VERSION` 啟動容器。
- **手動部署**（`run.sh`）：以 `git describe --tags --abbrev=0`（去開頭 `v`）推導
  `IMAGE_TAG`，與 CI 同源；取不到 git／tag 時退回 `:latest`（CI build 一併打的 latest）。
- **本機建置**（`docker/build.sh`）：同樣由 git tag 推導，並同時打 `:$VERSION` 與 `:latest`。
- **`pyproject.toml` 的 `version`**：發版時需手動對齊到所打的 tag（例如打 `v2.9.0` 則設
  `version = "2.9.0"`）。
- **`docker-compose.yaml`**：開發便利用途，固定使用 `:latest`，不持有獨立版本號。

> 發版規則：feature 分支合併進 `main` 後，於 `main` 最新 commit 打 annotated tag
> `vX.Y.Z`（依變更幅度遞增），該 tag 觸發 CI build + deploy；手動 `./run.sh` 在已同步該
> tag 的 host 上會自動取到相同版本。**請勿**再於 `run.sh`／`build.sh`／compose 硬編版本號。

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
- **公司產業對照**：一鍵從爬蟲取得最新 TWSE/TPEX 公司基本資料與產業對照表，寫入 TWSE 資料庫
- **YT 逐字稿**：選擇日期抓取「游庭皓的財經皓角」YouTube 直播影片逐字稿（透過 yt-dlp 下載自動字幕），metadata 寫入 MySQL，全文存為 md 檔，每日排程自動抓取當日逐字稿
- **原油價格**：選擇日期範圍上傳國際原油價格（WTI/Brent），資料寫入 SPECIAL_INFO 資料庫，每日排程 07:00 自動抓取（美國市場收盤後台灣早上資料已可取得）
- **黃金價格**：選擇日期範圍上傳國際黃金期貨價格，資料寫入 SPECIAL_INFO 資料庫，每日排程 07:05 自動抓取
- **比特幣價格**：選擇日期範圍上傳比特幣價格，資料寫入 SPECIAL_INFO 資料庫，每日排程 07:10 自動抓取
- **匯率**：選擇日期範圍上傳匯率資料（USDTWD/JPYTWD），資料寫入 SPECIAL_INFO 資料庫，每日排程 07:15 自動抓取
- **股市指數**：選擇日期範圍上傳股市指數價格（道瓊工業指數/納斯達克指數），資料寫入 SPECIAL_INFO 資料庫，每日排程 07:20 自動抓取
- **重試佇列**：檢視進入重試佇列的任務，可手動觸發重試、重設已耗盡（exhausted）任務、隔日重排、清除已完成或已放棄任務（exhausted 任務每日自動隔日重排，最多 3 次）
- **任務佇列**：所有上傳任務透過 FIFO 佇列管理，同一時間只執行一個任務，其餘排隊等待，前端顯示排隊位置

排程設定會儲存至 `logs/config.json`，重試佇列持久化至 `logs/retry_queue.json`，容器重啟後自動套用。

## 安裝方式

本專案使用 `pyproject.toml`（PEP 621）定義套件元資料與抽象依賴，
`requirements.txt` 負責 Docker 環境的完整釘版依賴。

```bash
# Docker 環境（由 Dockerfile 自動處理）
pip install -r requirements.txt  # 安裝釘版依賴
pip install --no-deps .          # 註冊專案模組

# 本機開發（含測試套件）
pip install -e ".[dev]"
```

## CI/CD

本專案使用 GitHub Actions 自動建置與發布 Docker image。

### 自動發布流程

推送版本 tag 時自動觸發：

```bash
git tag v2.3.0
git push origin v2.3.0
```

Pipeline 會自動：

1. 建置 Docker image（multi-stage build：Node 前端 + Python 後端）
2. 推送至 DockerHub，同時標記版本號 tag 和 `latest` tag

### 所需 GitHub Secrets

| Secret | 說明 |
|--------|------|
| `DOCKER_USERNAME` | DockerHub 使用者名稱 |
| `DOCKER_PASSWORD` | DockerHub 密碼或 Access Token |

### Workflow 檔案

- `.github/workflows/docker-publish.yml`：Docker image 建置與發布

### Git Remote 與鏡像

本專案以自架 GitLab 為開發主線，GitHub 作為對外鏡像：

- **雙 remote**：`origin` 指向 GitLab（預設推送目標），`github` 指向 GitHub。
- **鏡像管線**（`.gitlab-ci.yml`）：feature 分支開 Merge Request 合併進 `main` 後
  （合併進 `main` 當下不鏡像），於 `main` 最新 commit 打上 `vX.Y.Z` 版本 tag，
  該 tag 觸發 `mirror-to-github`，將 `main` 與該 tag 一併推送（鏡像）到 GitHub。
- **認證**：SSH 私鑰由 GitLab Runner 注入，名稱 `GITHUB_SSH_KEY`（其值可為金鑰檔路徑
  或金鑰內容，管線兩者皆支援）。

### GitLab CI 自動部署（deploy）

`.gitlab-ci.yml` 同時提供 `deploy` job，於打上 `vX.Y.Z` 版本 tag 時與 `mirror` 並行觸發
（兩 job 皆 `needs: []`），自動 build image 並以新 image 重啟本機服務容器：

- **觸發條件**：`CI_COMMIT_TAG` 符合 `^v\d+\.\d+\.\d+$`；`resource_group: deploy`
  序列化部署，避免多個 tag 同時互相覆蓋。
- **執行環境**：GitLab Runner 為 docker executor 並掛載 `/var/run/docker.sock`，job 內
  `docker` 指令直接作用在 host daemon，故 `docker run -v <絕對 host 路徑>` 由 host daemon
  解析、指向 host 真實目錄；服務容器接 `db_network`。
- **嚴格順序**：`docker build`（同時打 `:$VERSION` 與 `:latest`）→ `docker rm -f`
  舊容器 → `docker run` 新容器。先 build 成功才停舊容器，縮短服務中斷並避免 build 失敗時
  服務消失。
- **以版本化 tag 跑容器**：`docker run` 啟動的是 `$IMAGE:$VERSION`（**非 `:latest`**），
  `$VERSION` 為 tag 去掉開頭 `v`。`run.sh` 也由 git tag 推導出相同 `IMAGE_TAG`（同源，
  見「版本管理」），故手動部署與 CI 部署為同一版本、不再分岔。
- **NewsContents 綁絕對 host 路徑**：`-v /Users/chen/AI/Tw_stock/Tw_stock_DB/NewsContents:/workspace/NewsContents`
  （**bind mount，非具名 volume**）。此目錄由 db-operating 寫入、Tw_stock_news 讀取，必須
  與 host／其他容器看到同一份真實目錄。
- **logs 用具名 volume**：`tw_stock_db_operating_logs:/workspace/logs`（只有本服務寫入，
  不需與外部共享）。
- **無對外 publish port**：Web 管理介面（容器內 8080）由 Dashboard 透過 `db_network`
  反向代理存取（`ROOT_PATH=/app/db-operating`，已烤入 image），故與 `run.sh` 一致不加 `-p`。
  查 log 改用 `docker logs tw_stock_db_operating`。

## 環境需求

- Python 3.12.7
- Docker
- MySQL 資料庫（需先建立資料庫與資料表）

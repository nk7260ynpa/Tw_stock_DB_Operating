# Tw_stock_DB_Operating

台股資料庫操作模組，提供資料上傳與 MySQL 連線管理功能。

## 功能說明

- **DB 存取層**：透過 SQLAlchemy 建立 MySQL 連線（`clients.py`、`routers.py`）。所有取用點一律經
  `routers.db_conn` context manager 取得連線，離開區塊（含拋例外）必定關閉；引擎以 `NullPool`
  建立，不留連線池，避免長期累積閒置連線
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
- **失敗重試佇列**：排程任務失敗時自動加入重試佇列（相同 `(task_type, params)` 已在 **pending** 時不重複排入，避免逐日隔離後一次整體失敗排進上百筆等效任務；去重刻意**不含 `retrying`**，且載入時把卡在 `retrying` 的任務復原為 `pending`，見[重試佇列去重與當機復原](#重試佇列去重與當機復原)）。網路中斷每小時檢查網路並重試，最多 5 次；非網路錯誤（如「資料尚未發布」）標為 exhausted 後，每日（預設 06:30）「隔日重排」重設為 pending 再試一輪，最多 3 次，避免永久放棄隔日才會出現的資料（`retry_queue.py`）。整輪重試於**背景執行緒**執行（`run_retry_queue_scheduled`），不阻塞排程執行緒
- **重抓決策依爬蟲 `meta.retryable`**：新聞抓取不完整時，以爬蟲 v2.14.0 的
  `meta.retryable` 為單一判準決定「重抓有沒有機會補回來」，並以
  `detail_failed_ratio ≥ 0.2` 決定「值不值得付出整批重跑的成本」；舊版爬蟲回應
  （不帶 `retryable`）維持預設重抓（`data_upload/base.py`）
- **新聞 partial 如實回報筆數**：新聞抓取不完整（`status=partial`）時，已取得的部分**先寫入**
  MySQL 與 `NewsContents/` 才拋 `SourceError` 排入重試；例外會帶上已落地統計
  （`SourceError.partial_result`），Web 介面的任務因此顯示實際筆數而非固定 0，避免誤判成
  「完全沒抓到」而做多餘的人工補抓
- **行情類 empty-crawl 孤兒帳本自我修復**：TWSE/TPEX/TAIFEX/FAOI/MGTS 五個行情來源的每日排程（21:00 `daily_craw`）在補抓前，會先清除「近 7 天、落在平日、帳本標記 Open=False」的孤兒帳本並重新查詢，修復「交易日但當時資料尚未發布→爬空→被誤標非交易日而永久遮蔽」的真實行情；週末與更早於視窗的日期保留標記不重試。歷史（超出日常視窗）孤兒帳本以 `backfill_price.py` 較大視窗一次性 deep 修復（`DailyUpload.clear_price_orphans`）
- **SPECIAL_INFO 缺漏自我修復**：每日（預設 21:27，排在各商品 21:06~21:14 抓取之後補齊）對原油／黃金／比特幣／匯率／股市指數掃描近 30 天缺漏並自動補回，以「問爬蟲」為交易日／休市的唯一真相來源（回傳該日自身 K 棒即補上，只回更早日期且來源確認請求日無報價才視為非交易日），並一併清除最近 7 天的孤兒帳本重驗。掃描窗遠大於各商品排程的「過去 7 天」回補窗，足以自癒管線停擺數週造成的缺漏。共用邏輯於 `data_upload/special_info_common.py`，一次性修復與孤兒帳本清理入口為 `backfill_special_info.py`

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
├── clients.py                # MySQL 連線函式（NullPool，不留連線池）
├── routers.py                # MySQLRouter 路由類別 + db_conn context manager
├── upload.py                 # 批次上傳入口程式
├── DailyUpload.py            # 每日排程上傳（含行情類 empty-crawl 孤兒帳本每日重驗）
├── backfill_special_info.py  # SPECIAL_INFO 缺漏一次性回補與孤兒帳本清理入口
├── backfill_price.py         # 行情類 empty-crawl 孤兒帳本一次性 deep 修復入口
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
│   ├── indices_price.py    # 股市指數價格（DowJones/Nasdaq）
│   └── special_info_common.py # SPECIAL_INFO 價格共用邏輯（帳本語意 + 缺漏偵測補抓）
├── frontend/                 # React 前端原始碼（Vite）
│   ├── package.json
│   ├── vite.config.js
│   ├── index.html
│   └── src/
│       ├── App.jsx
│       ├── App.css
│       ├── api.js               # API 呼叫工具（套用 Vite base 前綴）
│       ├── constants.js         # 前端共用常數（如 MAX_UPLOADED_SHOWN）
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
│   ├── test_db_conn.py                   # 連線生命週期（含 AST 掃描防止裸連線復發）
│   ├── test_partial_result_reporting.py  # partial 已落地筆數如實回報
│   ├── test_crawler_meta_contract.py     # 爬蟲 v2.14.0 meta 契約重抓決策
│   ├── test_web_server_retry_queue_scheduled.py  # 重試佇列不阻塞排程
│   ├── test_daily_upload.py
│   ├── test_backfill_price.py            # 行情類孤兒帳本 deep 修復入口
│   ├── test_clear_price_orphans_db.py    # 清孤兒三條安全邊界（真實 SQL 引擎）
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
│   ├── test_special_info_common.py       # 共用邏輯（帳本語意 + 缺漏偵測補抓）
│   ├── test_web_server_special_info_backfill.py  # 缺漏自我修復 API 與作業
│   ├── test_retry_queue.py
│   ├── test_job_queue.py
│   └── test_config_persistence.py        # 設定位置、舊位置遷移與掛載一致性
├── logs/                     # 日誌資料夾（**不放設定**，見「設定持久化」）
└── config/                   # 持久化設定資料夾（config.json）
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

# 或背景啟動（設定目錄務必一併掛載，否則排程自訂會隨容器消失，
# 見「設定持久化」）
docker run -d --name tw_stock_db_operating \
  --network db_network \
  -p 8080:8080 \
  -v $(pwd)/logs:/workspace/logs \
  -v $(pwd)/config:/workspace/config \
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

啟動服務後開啟 `http://localhost:8080`，提供以下功能。

> **版面**：左側為常駐的「手動上傳」面板；右側採**標籤頁（Tabs）**呈現，一次只顯示一組
> 卡片（未切到的分頁不掛載，縮短初次載入），預設顯示「排程/重試」。四個分頁分組如下：
>
> | 分頁 | 內含卡片 |
> |------|----------|
> | 排程/重試 | 排程設定、重試佇列 |
> | 行情·公司 | 季度營業收入、TDCC 集保庫存、公司產業對照 |
> | 新聞 | CTEE、CNYES、PTT、MoneyUDN 新聞、YT 逐字稿 |
> | 商品·匯率·指數 | 原油價格、股市指數 |
>
> 分頁切換採 React 本地 state（不使用 URL routing），以維持反向代理（`root_path=/app/db-operating`）相容性。
>
> 各卡片的「已上傳日期 / 已上傳季度」清單僅顯示**最新 5 筆**（依日期由新到舊，常數
> `MAX_UPLOADED_SHOWN`，定義於 `frontend/src/constants.js`），以縮短卡片高度。此為純前端
> 顯示限制，後端 `/api/<source>/uploaded` 與防重複上傳的 `*Uploaded` 紀錄表皆不受影響。

- **手動上傳**：選擇日期範圍與資料庫，直接觸發資料上傳
- **排程設定**：檢視與修改每日自動上傳的排程時間
- **季度營業收入**：選擇民國年與季度，從 MOPS 抓取上市公司營業收入
- **TDCC 集保庫存**：一鍵取得最新集保庫存分級資料，每日排程自動檢查並上傳新資料
- **CTEE 新聞**：選擇日期範圍上傳工商時報新聞，metadata 寫入 MySQL，全文存為 txt 檔，每日排程（21:16）回溯過去 48 小時抓取，涵蓋昨日整天
- **CNYES 新聞**：選擇日期範圍上傳鉅亨網新聞，metadata 寫入 MySQL，全文存為 md 檔，每日排程（21:18）回溯過去 48 小時抓取，涵蓋昨日整天
- **PTT 新聞**：選擇日期範圍上傳 PTT 股版新聞，metadata 寫入 MySQL，全文存為 md 檔，每日排程（21:20）回溯過去 48 小時抓取，涵蓋昨日整天
- **MoneyUDN 新聞**：選擇日期範圍上傳經濟日報新聞，metadata 寫入 MySQL，全文存為 md 檔，每日排程（21:22）回溯過去 48 小時抓取，涵蓋昨日整天
- **公司產業對照**：一鍵從爬蟲取得最新 TWSE/TPEX 公司基本資料與產業對照表，寫入 TWSE 資料庫
- **YT 逐字稿**：選擇日期抓取「游庭皓的財經皓角」YouTube 直播影片逐字稿（透過 yt-dlp 下載自動字幕），metadata 寫入 MySQL，全文存為 md 檔，每日排程（21:24）自動抓取**昨日**已完成直播的逐字稿（當日直播多半尚未結束或字幕未產生）
- **原油價格**：選擇日期範圍上傳國際原油價格（WTI/Brent），資料寫入 SPECIAL_INFO 資料庫，每日排程 21:06 自動抓取（區間上界固定為**昨日**，理由見[排程只抓到昨日](#排程只抓到昨日不寫尚未定案的日-k)）
- **黃金價格**：選擇日期範圍上傳國際黃金期貨價格，資料寫入 SPECIAL_INFO 資料庫，每日排程 21:08 自動抓取（區間上界固定為**昨日**）
- **比特幣價格**：選擇日期範圍上傳比特幣價格，資料寫入 SPECIAL_INFO 資料庫，每日排程 21:10 自動抓取（區間上界固定為**昨日**）
- **匯率**：選擇日期範圍上傳匯率資料（USDTWD/JPYTWD），資料寫入 SPECIAL_INFO 資料庫，每日排程 21:12 自動抓取（區間上界固定為**昨日**）
- **股市指數**：選擇日期範圍上傳股市指數價格（道瓊工業指數/納斯達克指數），資料寫入 SPECIAL_INFO 資料庫，每日排程 21:14 自動抓取（區間上界固定為**昨日**）
- **重試佇列**：檢視進入重試佇列的任務，可手動觸發重試、重設已耗盡（exhausted）任務、隔日重排、清除已完成或已放棄任務（exhausted 任務每日自動隔日重排，最多 3 次）
- **任務佇列**：所有上傳任務透過 FIFO 佇列管理，同一時間只執行一個任務，其餘排隊等待，前端顯示排隊位置

排程設定會儲存至 `config/config.json`（**與 log 目錄分離**，理由見[設定持久化](#設定持久化設定為什麼不能放在-log-目錄)），重試佇列持久化至 `logs/retry_queue.json`，容器重啟後自動套用。

## 每日排程時間表（爬蟲抓取集中於 21:00~21:30）

所有「爬蟲抓取類」排程集中於 **21:00~21:30** 之間並彼此錯開，避免同時併發搶爬蟲
資源；抓取的都是「前一交易日／昨日」已底定的資料。

> **為什麼是晚上 21:00**（v2.19.0 起；先前為 07:30~08:00）：宿主 Mac 長期處於睡眠
> 循環，Docker VM 隨之凍結、容器內進程停止執行，排程到點也不會觸發。以 Prometheus
> self-scrape 的逐分鐘樣本統計 7 天，容器「實際醒著」的比率為 07:00 僅 **36.9%**、
> 08:00 為 50.5%（早上逐日極不穩定：4%、8%、33%、37%、46%、78%、100%），而
> **21:00 為 93.8%（全日最高）**、20:00 為 91.9%、22:00 為 91.4%，晚間穩定在
> 71~100%。故整批平移 13.5 小時到 21:00 窗，**各排程的相對間隔完全不變**。

| 時間 | 排程 | 說明 |
|------|------|------|
| 21:00 | daily_craw | 台股行情（TWSE/TPEX/TAIFEX/三大法人/融資融券），補抓過去 30 天缺漏，**排除今日**（尚未收盤），並先清除近 7 天平日 empty-crawl 孤兒帳本重驗 |
| 21:03 | TDCC 集保 | 檢查並上傳最新集保庫存分級 |
| 21:06 | 原油 | SPECIAL_INFO 原油價格（回補至**昨日**，往前 7 天） |
| 21:08 | 黃金 | SPECIAL_INFO 黃金價格（回補至**昨日**，往前 7 天） |
| 21:10 | 比特幣 | SPECIAL_INFO 比特幣價格（回補至**昨日**，往前 7 天） |
| 21:12 | 匯率 | SPECIAL_INFO 匯率（回補至**昨日**，往前 7 天） |
| 21:14 | 股市指數 | SPECIAL_INFO 道瓊/納斯達克（回補至**昨日**，往前 7 天） |
| 21:16 | CTEE 新聞 | 回溯過去 48 小時 |
| 21:18 | CNYES 新聞 | 回溯過去 48 小時 |
| 21:20 | PTT 新聞 | 回溯過去 48 小時 |
| 21:22 | MoneyUDN 新聞 | 回溯過去 48 小時 |
| 21:24 | YT 逐字稿 | 抓**昨日**已完成直播的逐字稿 |
| 21:27 | SPECIAL_INFO 缺漏自我修復 | 排在各商品之後補齊至**昨日**為止的近 30 天缺漏 |

不受此集中政策約束的維護型排程：**06:30** exhausted 隔日重排（仍排在當日 21:00
抓取窗之前，把前一晚放棄的任務重設為 pending，供白天每小時的重試佇列補回）、
重試佇列**每小時**檢查。

**睡過頭會自動補跑**：21:00 的醒著比率是 93.8% 而非 100%，仍有睡過頭的時候。
`schedule` 套件的 `every().day.at()` 是以「下次執行時間」判定，宿主睡眠期間容器
進程只是被凍結、並未結束，醒來後第一次 `run_pending()` 就會補觸發已到期的任務
（`scheduler_thread` 每秒呼叫一次）。此自癒能力靠的是「進程不中斷」，故**請勿**
在排程時間前後重啟容器——重啟會重新註冊排程並把 next_run 推到下一次發生時間。

### 幾個正確性設計

- **排程只抓到昨日：不寫尚未定案的日 K**：SPECIAL_INFO 五商品的日 K 由 yfinance 供應，
  「當日」那一根在該市場收盤前一直是**進行中的半根 K**。舊排程 07:3x（＝前一日
  23:3x UTC／19:3x ET）時 UTC 尚未跨日、美股現貨也尚未開盤，靠「爬蟲 fallback 回上一
  交易日」恰好只會取到已定案的日 K；搬到 21:0x（＝13:0x UTC／09:0x ET）後，比特幣與
  匯率當日的 UTC 日 K 已存在且僅完成約一半，會被 `REPLACE INTO` 寫進價格表並記帳，
  而**帳本與價格表雙重跳過**（`check_uploaded` 看帳本、`find_missing_dates` 看價格表）
  使該日永遠不會被重驗，等於把半根 K 永久凍結。原油／黃金為 CME Globex 期貨（前一日
  18:00 ET 即開盤），此問題在舊排程就已存在，v2.19.0 一併修掉。故五個行情排程與
  21:27 的缺漏自我修復一律以 `settled_end_date()`（＝昨日）為區間上界／掃描基準日，
  當日資料於次日排程自然補上。**資料新鮮度與舊制相同**——舊制在 D 日請求 D、實際
  寫進價格表的也是 D-1 那一根。手動觸發的補抓端點不受此限（由操作者自行決定範圍）。
- **新聞回溯 48 小時**：回溯窗須涵蓋「昨日整天到現在」。設為 48 小時（爬蟲支援
  1~72），可補回偶發漏抓一日；重複抓到的記錄由各上傳器以 URL 去重（同日已存在的
  URL 會跳過），不會重複寫入。48 小時是**往回推的相對窗**，21:16 執行時涵蓋前日
  21:16 至今，昨日整天必然完整包含在內（早上 07:46 執行時亦然），故搬窗後涵蓋範圍
  只增不減、不需調整時數。
- **daily_craw 排除今日（刻意保留的保守設定）**：原始理由是排程在早上 07:30 執行、
  台股尚未收盤，爬取今日會取得空資料並被 `base.upload_date` 標記為「非交易日」而
  永久跳過。排程移到 **21:00 後台股（13:30 收盤）當日資料其實已可抓取**，理論上可
  不再排除今日，但這是刻意維持的現狀，**未隨搬窗一併變更**：代價只是今日資料延到
  隔日排程以「昨日」身分補回（資料不會遺失），換取零誤標風險。
- **行情類 empty-crawl 孤兒帳本每日重驗**：`daily_craw` 在補抓前先 `clear_price_orphans`
  清除「近 7 天、平日、Open=False」的孤兒帳本，使被誤標的真實交易日重新成為缺漏候選
  並重抓（詳見下方「行情類 empty-crawl 帳本語意與自我修復」）。
- **YT 抓昨日（跨午夜補跑仍取排程日前一日）**：21:24 執行時「當日」直播多半尚未結束
  或自動字幕尚未產生，故排程抓「昨日」已完成的直播影片。搬窗後距午夜只剩 2 小時
  36 分（舊制 07:54 距午夜 16 小時），睡過頭補跑很可能落到隔日凌晨；若沿用
  `datetime.now()` 減一天，會算成「排程日當天」而少抓一天，而 YT 排程只吃單一日期、
  **沒有多日回補路徑**，漏掉就永久漏掉（行情有 30 天補抓窗、新聞有 48 小時窗可自癒）。
  故改由 `yt_transcript_target_date()` 以「現在時刻是否已過排定時刻」還原排程日再減
  一天；準時執行時結果與舊算法完全相同，與 `Tw_stock_crawer` 的「YT 抓昨天」契約
  維持一致。
- **daily_craw 於背景執行緒執行（不阻塞排程）**：`daily_craw` 需逐日向爬蟲請求缺漏
  日期，耗時隨缺漏天數成長。若直接在 `scheduler_thread` 內同步執行，會在
  `run_pending()` 期間持有 `schedule_lock`，使當日 21:03~21:27 的後續排程全部延後。
  故排程註冊的是 `run_daily_craw_scheduled` 包裝函式：它以背景執行緒啟動 `daily_craw`
  後立即返回，並以重入旗標確保同時只有一輪在跑（上一輪未結束則記錄警告並略過本次）。
- **重試佇列同樣於背景執行緒執行**：`process_retry_queue` 逐一**同步**重跑佇列任務，
  新聞類是整個 48 小時窗重抓，一輪可達數分鐘以上；若直接註冊為每小時排程的 callback，
  同樣會在 `run_pending()` 期間持有 `schedule_lock` 而拖累其後所有排程。故排程與 Web
  介面的「立即重試」都改走 `run_retry_queue_scheduled` 包裝（背景執行緒 + 重入旗標），
  兩者共用同一面旗標，避免同一任務被兩輪並行執行而重複寫入。
- **爬蟲請求一律設 timeout**：`data_upload/base.py` 的 `CRAW_TIMEOUT`（預設 120 秒，
  可用同名環境變數覆寫）套用於所有行情類爬取請求。未設 timeout 時 `requests` 會無限期
  等待，爬蟲一旦 hang 住即會卡住 `daily_craw`。逾時歸類為**可重試**的 `NetworkError`
  並進入重試佇列，而非直接放棄。

> **2026-08 事故背景**：上述兩點源自一次連續多日的資料缺漏。爬蟲間歇性回傳缺少 `data`
> 鍵的 payload，使該日期不寫帳本而每日重抓、缺漏數逐日累積；又因請求未設 timeout，
> `daily_craw` 曾單日執行逾 20 小時並持有 `schedule_lock`，導致新聞等排程被延後到深夜
> 才觸發。新聞來源的日期回溯僅約 3 天，錯過抓取窗即**永久無法補回**。

### 依爬蟲 `status` 判讀成敗（`Tw_stock_crawer` v2.13.0 起）

爬蟲新契約**保證 `data` 鍵永遠存在**（失敗時為 `[]`，`/company_info` 為 `{}`），並新增
`status`（`ok`／`empty`／`partial`／`out_of_range`／`error`）、`message`、`meta`。

這對本專案是**必須主動適配的破壞性變更**：`data_upload/base.py` 原本正是靠
`response.json()["data"]` 的 `KeyError` 得知爬取失敗（→ `CrawlError` → 不寫帳本 →
次日重抓）。新契約下失敗不再拋 `KeyError`，若沿用「有 `data` 就當成功」的邏輯，失敗日
會被 `upload_date()` 寫成 `Open=False`（非交易日）而**永久跳過**——與上述事故「帳本
誤標 → 永久遮蔽」是同一種死法，只是換一個入口。

故 `base.check_crawl_status()` 於**取用 `data` 之前**先判讀狀態：

| `status` | 行為 | 是否寫帳本 | 是否重試 |
|---|---|---|---|
| `ok` | 正常寫入 | 依資料筆數 | — |
| `empty` | 該日確實無資料 | `Open=False` | 否 |
| `partial` | 資料不完整 | **不寫** | 是（`SourceError`） |
| `error` | 抓取失敗，0 筆不代表沒有 | **不寫** | 是（`SourceError`） |
| `out_of_range` | 來源不再提供，重抓無用 | **不寫** | 否（`OutOfRangeError`） |

- `OutOfRangeError` 刻意繼承 `CrawlError` 而非 `NetworkError`，故**不進** retry queue
  （本專案以「是否為 `NetworkError`」作為可重試判準），避免對來源根本拿不到的日期反覆重抓。
- **未知狀態**保守視為可重試失敗。寧可多重試，也不可把失敗誤記成「當日無資料」。
- **`SourceError` 與 `NetworkError` 的批次策略相反**：`SourceError`（繼承 `NetworkError`，
  故仍可重試）代表「爬蟲可達、只有這一筆抓不到」，`daily_craw` 與 `process_retry_queue`
  **只跳過該筆並繼續**；`NetworkError`（連不上爬蟲）則整批排入重試並**中止本輪**。若兩者
  不分，`missing_dates` 昇冪排序下最舊的「毒日期」會每天在同一處中斷補抓，其後日期永遠
  不被嘗試，直到滑出 30 天視窗即永久遺失。
- **`status` 缺席時放行**，維持既有行為以相容舊版爬蟲。
- **新聞類的 `partial` 例外**：新聞以 URL 去重、重抓為冪等，故 `partial` 的資料**先落地**
  再依 `meta` 決定是否重試（判準見下節）。行情類則相反：`DailyPrice` 為 append 寫入且
  **無去重**，存入部分資料會在重抓時產生重複列，故一律丟棄重抓。
- **`partial` 但 `data` 為空時絕不寫帳本**：「重抓也拿不到」不等於「當日確實沒有新聞」
  ——來源硬上限截斷時，被截掉的部分是真實存在的資料。四支上傳器的 0 筆早退路徑都在
  `record_uploaded_date()` **之前**返回，故 `*Uploaded` 帳本不會被寫入，該日仍是缺漏
  候選；若寫了，該日就永久宣告處理完畢、再也不會被檢查。
- **新聞爬取的 `timeout=600` 不可調低**：爬蟲端 `MAX_RUNTIME_SECONDS=480`（最壞含重試約
  573 秒）刻意設計為低於本端 600 秒。CTEE 正常耗時約 210~230 秒、尖峰更久，調低會提前中斷。

### 新聞 `partial` 的重抓決策（`Tw_stock_crawer` v2.14.0 起）

爬蟲 v2.14.0 起在 `meta` 增加下列欄位（全為增量，既有欄位語意不變）：

| 欄位 | 型別 | 出現時機 | 語意 |
|---|---|---|---|
| `retryable` | bool | `partial`／`out_of_range`／`error` | 重抓有沒有機會補回來（**單一判準**） |
| `retryable_reasons` | list | 有暫時性成因時 | `list_failed`／`detail_failed`／`deadline`／`crawl_failed` |
| `non_retryable_reasons` | list | 有硬限制成因時 | `source_truncated`／`out_of_range` |
| `detail_total`／`detail_failed_ratio` | int／float | 有嘗試抓全文時 | 全文抓取的總篇數與失敗率 |
| `list_failed` | bool | 列表／分頁抓取中途失敗 | 缺的是「哪些文章存在」 |
| `pages` | int | CNYES | 翻頁停在第幾頁（診斷用） |

`data_upload/base.py` 的 `partial_retry_reason()` 據此分兩層決策：

1. **能不能補回來** → 以 `meta.retryable` 為準。爬蟲已彙整所有成因，
   `retryable=False`（如 CNYES 翻頁上限 `source_truncated`）一律只告警不重抓。
2. **值不值得重抓** → 僅當成因**只有** `detail_failed` 時，看
   `detail_failed_ratio` 是否達 `DETAIL_FAILED_RETRY_RATIO`（**0.2**）；
   `list_failed`／`deadline`／`crawl_failed` 則無視門檻一律重抓。

**門檻取 0.2 的理由**：重試佇列是**同步**重跑整個 48 小時窗（CTEE 約 210~230 秒）
且每小時觸發，成本高；而全文抓失敗的文章被爬蟲**整篇排除**在 `data` 之外，不會寫進
MySQL，隔日排程的 48 小時窗重抓時仍是「新記錄」而會被補上——等於每篇本來就有第二次
免費機會（界線：視窗只往前推 48 小時，今天視窗中較舊的 24 小時不會被明天涵蓋，
每篇合計兩次機會；要真的漏掉須連兩天同一篇都失敗）。門檻要攔的是「來源擋人／全文頁
改版」這類系統性異常（失敗率高到五分之一，明天多半也修不好），零星幾篇交給隔日自然
補抓即可。反之 `list_failed`／`deadline` 連「有哪些文章」都不知道，損失無上限且
CTEE 來源僅保留約 3 天，等不起。

> 若無此門檻：PTT／MoneyUDN 過去抓漏也回 `ok`，新契約改回 `partial` + `detail_failed`，
> 沿用舊的「1 篇失敗就重抓」會讓它們**天天**排一次同步重跑，把 21:00~21:30 的
> 抓取窗整批往後推——與 2026-08 的排程連鎖延遲事故同型。

**向後相容**：`meta` 缺席或不帶 `retryable`（舊版爬蟲、非制式回應）時走舊邏輯，
維持**預設重抓**；只有明確標示 `source_truncated` 才不重抓。絕不可因為「沒有
`retryable`」就當成不重抓——那正是「把失敗誤記成空」而永久遮蔽該日的老毛病。
同理，`retryable=False` 但 `non_retryable_reasons` 與 `source_truncated` 皆空的
退化回應（爬蟲現行邏輯不會產生）也保守重抓；此防線只會把「不重抓」翻成「重抓」。

### 排程時間的一次性遷移（config_version）

排程時間有兩個來源：程式碼預設值（`load_config()`）與持久化的 `config/config.json`
（Web 介面修改後寫回、實際生效以此為準，位置見
[設定持久化](#設定持久化設定為什麼不能放在-log-目錄)）。既有的持久化設定會在重啟後
覆蓋新的程式碼預設值，為此 `load_config()` 內建**版本控管的一次性遷移**
（`config_version`）：

- 讀取到 `config_version` 低於現行版本（或缺少）的舊設定時，將**落在 21:00~21:30
  窗外**的爬蟲抓取排程收斂到新預設並寫回 `config.json`，接著把版本標記為現行版本。
- 已落在窗內的自訂值會被**保留**（不覆寫為預設）。
- 例外清單 `_SUPERSEDED_IN_WINDOW_DEFAULTS`：**恰好落在新窗內的舊版預設值**會被上一條
  誤認為使用者自訂而保留，故需逐一列出強制收斂。v3 有兩個——v1 的 CTEE 預設 `21:00`
  （等於新窗起點，會與 daily_craw 撞在同一分鐘）與 CNYES 預設 `21:30`（等於新窗終點）。
  日後再搬窗時務必重新盤點歷史預設值，漏列的鍵會靜默停在舊值。
- 版本標記完成後**不再重跑**，故使用者日後仍可經 Web 介面自由微調排程時間（含窗外），
  重啟不會被再次覆蓋。

版本沿革：**v2** 把爬蟲抓取排程集中至 07:30~08:00；**v3**（v2.19.0）整窗平移至
21:00~21:30。

因此升版部署後，服務**首次啟動即自動**把持久化的舊排程遷移到新的 21:00~21:30 窗，
無需手動編輯 `config.json`。

## 設定持久化：設定為什麼不能放在 log 目錄

**設定檔位置為 `config/config.json`（容器內 `/workspace/config/config.json`），
與 `logs/` 完全分離。** 這是踩過坑後的規定，改動前務必先讀完本節。

### 曾經的坑

設定檔原本寄生在 log 目錄（`logs/config.json`）。後來部署方式演進成：

| 啟動路徑 | `logs/` 掛載方式 |
|----------|------------------|
| CI deploy（打 tag 自動部署） | 具名 volume `tw_stock_db_operating_logs` |
| 手動 `./run.sh` | host bind `<repo>/logs` |

兩者的 log 目錄**是不同的兩份資料**。設定寄生其中的後果：

- 具名 volume 內從未有 `config.json`，容器實際跑的是**程式碼預設值**，host 上那份
  使用者設定**再也不會被讀到**——而且**不會有任何錯誤訊息**。
- 經 Web 介面改排程只會寫進當下掛載的那一份，換個啟動方式就「改動消失」。

### 現在的規則

- **設定**（`config.json`）放 `config/`；**log 與執行期狀態**（`*.log`、
  `retry_queue.json`）放 `logs/`。程式端 `CONFIG_PATH` 不依附 `LOG_DIR`，
  可用環境變數 `CONFIG_DIR` 覆寫（預設 `<專案根>/config`）。
- `run.sh` 與 `.gitlab-ci.yml` 的 deploy job **掛載同一個 host 絕對路徑**
  （`/Users/chen/AI/Tw_stock/Tw_stock_DB_Operating/config` → `/workspace/config`），
  兩條啟動路徑共用同一份設定，不再分岔。設定用 bind mount（非具名 volume），
  好處是 host 上可直接 `cat`／備份／比對。
- `config/` 內容不進版控（`.gitignore`），且以 `.dockerignore` 排除於 build context
  之外，避免把某次 build 當下的設定烤進 image。
- **設定寫入為原子操作**：`save_config` 先寫同目錄的暫存檔（檔名帶 pid 與遞增序號，
  避免併發寫入互相覆蓋），再以 `Path.replace()`（即 `os.replace`）換上去。直接覆寫會
  先截斷舊檔，寫到一半失敗（磁碟滿、容器被 kill）就留下毀損 JSON——而毀損的新檔會讓
  遷移邏輯認定「新位置已有設定」，把舊位置的備援一併遮蔽掉。任何寫入失敗都會清掉
  暫存檔，既有設定不受影響。
  **邊界**：未做 `fsync`，正常的容器重啟／`docker rm -f` 不受影響，但主機層突然斷電或
  VM 崩潰時仍可能丟失最後一次寫入（最壞情況由下面的隔離＋舊設定 fallback 接住）。
- **「讀不到」與「內容壞掉」分開處理**（兩者的正確處置相反）：
  - 讀取失敗（`OSError`：權限、暫時性 IO 錯誤）→ 只記 error、**原檔完全不動**，本輪
    退回預設值，下次啟動仍可讀回。把內容完好的設定靜默改名走，正是本節要杜絕的事。
  - 內容毀損（`ValueError`：JSON 壞掉、非 UTF-8、頂層不是 JSON 物件）→ 改名為
    `config.json.corrupt` 隔離（保留現場供人工檢視），記 error，接著再給舊設定一次
    搬遷機會，都沒有才退回程式碼預設值。服務不會因為一個壞掉的設定檔卡在重啟迴圈。
- **欄位型別／格式錯就地修復（不隔離整份設定）**：頂層雖是 JSON 物件，但像
  `"tdcc_schedule": null`、`"ctee_schedule": 5`、`"schedule_time": "sunday"` 這種值一路
  傳到 `setup_schedule` 才爆炸的話，在 `--restart always` 下同樣是重啟迴圈。故
  `_normalize_config` 做**與版本無關**的形狀正規化：缺鍵靜默補上預設（舊版設定沒有新
  欄位屬正常），型別或 `HH:MM` 格式不符者換成預設值並記 warning，同一份設定中其餘正常
  的自訂則保留。修復結果**不寫回**檔案（保留使用者原檔供人工檢視，每次啟動會再警告一
  次）。`config_version` 無法判讀時視同最舊版本，讓一次性遷移有機會補跑。
  `load_config` 另留一道保險絲：正規化仍拋出預期外例外（`AttributeError`、`KeyError`、
  `TypeError`、`ValueError`、`RecursionError`）時，處置與頂層毀損一致（隔離 → 預設值）。
  同一組壞欄位的 warning 只記一次（其後降 debug），壞欄位換一組時會再警告一次。
- **端點驗證與正規化用同一判準**：所有排程 PUT 端點改用 `_is_valid_time` 檢查
  `HH:MM`。先前端點自行 `split(":")` 較寬鬆，`"07:30:00"`、`"7:30"` 會被存進設定檔、
  重啟後又被正規化換回預設值——設定等於被靜默丟棄，正是本節要杜絕的失敗模式。
  `test/test_config_persistence.py` 有守門測試盯住這點。
- **設定寫回失敗不擋啟動**：一次性遷移（TDCC 週排程改每日、爬蟲時間窗收斂）需把結果
  寫回設定檔，但設定目錄唯讀／磁碟滿時不該讓服務起不來。寫回採 best-effort：失敗只記
  error，本輪仍以**記憶體內已遷移**的設定繼續執行，下次啟動會再試一次寫回。寫回統一在
  正規化全部完成後執行一次，避免半套結果落地。
- 守門測試在 `test/test_config_persistence.py`：包含「`CONFIG_PATH` 不得位於
  `LOG_DIR` 之下」與「`run.sh` 與 CI deploy 掛載到容器內同一設定路徑」的結構性檢查
  （後者斷言的是**掛載效果**而非變數名稱），有人改回舊做法就會紅燈。

### 舊設定的一次性遷移

服務啟動讀設定前會呼叫 `migrate_legacy_config()`：

1. 新位置有設定 → 直接使用，**新位置永遠優先**；此時若舊位置也還在，記一筆
   warning 提示舊檔已失效（不覆寫、不刪除，保留人工比對機會）。
2. 新位置沒有、舊位置（`logs/config.json`）有 → **原樣**搬遷（含 `config_version`，
   確保後續的排程時間窗一次性遷移語意不變），並把舊檔改名為 `config.json.migrated`
   保留備份，避免每次啟動重複判讀。
3. 兩邊都沒有 → 回傳程式碼預設值，不憑空寫出設定檔。
4. 舊檔毀損（JSON 壞掉、非 UTF-8、頂層非物件）或讀取失敗 → 記 warning 後退回預設值，
   **舊檔原樣保留不動**（不隔離、不搬遷，避免把垃圾內容搬進新位置），不讓服務啟動失敗；
   同一支 warning 也只記第一次。
5. 新位置寫不進去（權限／唯讀）→ 記 error 但**保留舊檔不改名**，下次啟動可再試，
   設定不會消失。

新舊並存的 warning **只在第一次記錄**（其後降為 debug）：`load_config` 幾乎每個 API
端點都會呼叫，而舊檔依設計不會自動刪除，每次都記會把 log 洗掉。

`run.sh` 另於 host 端做同語意的搬遷（`logs/config.json` → `config/config.json`），
一樣是「先複製、成功後才把舊檔改名為 `config.json.migrated`」而非 `mv` 直接搬走，
讓 host 上的舊設定也能被具名 volume 部署沿用，且複製失敗時舊檔仍在、下次啟動可重試。

## SPECIAL_INFO 帳本語意與缺漏自我修復

原油／黃金／比特幣／匯率／股市指數五個 SPECIAL_INFO 商品的上傳器共用
`data_upload/special_info_common.py`，重點如下。

### 爬蟲狀態契約（判斷「失敗」還是「真的沒有」）

`*Uploaded` 帳本是「這天已處理完畢，以後別再看了」的**永久**標記，
`upload()` 只憑帳本決定是否跳過。因此**把抓取失敗寫進帳本 = 永久遮蔽該日**，
且會自我強化：重試被失敗自己寫下的帳本滿足，log 顯示「資料已存在，跳過上傳」
→「重試任務成功」，實際上什麼都沒補。

判讀一律走 `base.check_crawl_status()`（爬蟲 v2.15.0 契約），**不得**再用錯誤
訊息字串（舊版靠「無法取得任何」判非交易日，該字串已不存在，且字串啟發式本身
就是把失敗誤記成空的成因）：

| `status` | 意義 | 是否寫帳本 | 是否重試 |
|---|---|---|---|
| `ok` | 全數取得 | 記**實際交易日** | — |
| `empty` | 探測確認該期間無報價 | 可記請求日 | — |
| `out_of_range` | 早於來源涵蓋起點 | 可記請求日 | 否 |
| `partial` | 部分商品失敗 | **絕不寫** | 是 |
| `error`／未知 | 抓取失敗，0 筆不代表沒有 | **絕不寫** | 是 |

- 行情類 `partial` **整批丟棄重抓**（`allow_partial=False`）：缺商品即為不完整
  的一天，與新聞類（可累積補齊）不同。
- `ok` 卻 0 筆屬自相矛盾，一律當失敗處理——寧可多重試，也不可誤記成無資料。
- 單日的來源端失敗（`SourceError`）與該日的資料格式異常（`CrawlError`）都只跳過
  該日並各自排入重試佇列，**不中斷**整個日期區間；只有連不上爬蟲
  （`NetworkError`，後續日期必然同樣失敗）才整批中止。攔截順序必須是
  「`SourceError` → `NetworkError`（重拋）→ `CrawlError`」，因為
  `SourceError ⊂ NetworkError ⊂ CrawlError`，父類別寫在前面會把子類別吃掉。
  任務有失敗日時狀態記為 `completed_with_errors`，避免部分成功被讀成全數成功。
- **寫帳本需要正面證據，「不知道」不等於「沒有」**：回空只有在
  `status == "empty"`（探測確認該期間無報價）時才記帳；fallback 只有在
  `meta.target_date_available` **明確為 `false`** 時才把請求日標為非交易日。
  `status` 缺席、`meta` 沒有該欄位、或回應根本不是 JSON 物件（拋 `SourceError`），
  一律留白待重驗——多問幾次的成本，遠低於永久遮蔽一天的行情。

> **狀態欄位攔不住的殘缺資料**：來源（yfinance）偶爾回傳「有 `volume` 但
> OHLC 全為 `null`」的殘缺 K 棒，爬蟲仍標記 `status=ok`、
> `meta.target_date_available=true`，狀態欄位完全無從察覺（2026-08-17／08-18
> 道瓊、納斯達克即為此類）。故資料層再守一道：**必要欄位含空值一律拋
> `SourceError`、整批丟棄重抓、絕不寫帳本**（數值 `0` 不算空值，匯率
> `volume=0` 為正常值）。若放行，`check_schema` 會拋出未歸類的 pydantic
> `ValidationError`，直接炸掉整個多商品補抓作業。

### 帳本記帳語意（防止資料掉列）

`*Uploaded` 帳本只記錄「爬蟲回傳 DataFrame 內每一筆的**實際交易日**」；
**請求日**僅在下列情況才額外標記已完成：

1. 取得的最新實際日期 == 請求日（真的拿到當日資料）；或
2. 該商品**非 24/7**（原油／黃金／匯率／股市指數）、請求日**已定案**（早於
   今日），且爬蟲確認該日無報價（`empty`，或 fallback 到更早日期且
   `meta.target_date_available` 非真）。

各上傳器以類別屬性 `is_continuous_market` 區分兩種行為：**比特幣為 True**
（24/7 連續市場），實際日期 < 請求日時**不**標記請求日，留待次日 UTC 日 K
完成後回補，避免帳本謊報造成 `check_uploaded` 日後永久跳過該日。

> **「已定案」守衛**：請求日不早於今日時，一律不標記非交易日。盤前（或美股
> 開盤前）去問「今天」，來源當然只給得出昨天的日 K，舊碼會就此把今天記成非
> 交易日而永久遮蔽——2026-08-17／08-18 四商品的孤兒帳本即為此類。未定案的
> 日期一律留待次日重驗。

### 每日缺漏自我修復（排程 21:27）

每日對五個商品掃描近 30 天缺漏並自動補回，以「問爬蟲」為交易日／休市的唯一
真相來源（免維護各市場假日表）。同時**清除最近 7 天的孤兒帳本並重驗**
（`SPECIAL_INFO_REVERIFY_DAYS`）：誤標一旦寫入，`find_missing_dates` 就永遠
不會再把該日列為候選，若只有人工 `deep` 才清，誤標就會像 2026-08 那樣持續
累積。重驗窗刻意取小（非整個 30 天），以免每天重問幾十個早已確認的非交易日。
REST 端點：

- `GET /api/special-info-backfill/schedule`、`PUT /api/special-info-backfill/schedule`：檢視／修改排程時間。
- `POST /api/special-info-backfill/run`：手動觸發，body 可帶 `{"days": 30, "deep": false}`。
  - `deep=false`（日常）：跳過帳本已標記的日期（避免反覆詢問已確認的非交易日），
    但仍會重驗最近 7 天的孤兒帳本。
  - `deep=true`（人工修復歷史缺漏）：先清除**整個窗**的孤兒帳本再交由爬蟲重驗，
    救回舊 bug／管線停擺期間被誤標為已完成的「真實交易日」。

### 一次性回補與孤兒帳本清理

`backfill_special_info.py` 以 deep 模式對五個商品執行「清孤兒帳本 → 逐日重驗
補回」，冪等可重跑：

```bash
docker run --rm --network db_network \
  nk7260ynpa/tw_stock_db_operating:latest \
  python backfill_special_info.py --days 160
```

失敗一律逐層隔離、絕不中斷整批：單日 `NetworkError`／`SourceError` 記入
`network_errors`、`CrawlError` 記入 `crawl_errors`，單一商品的未預期例外只讓該商品
中止。結束碼 0 表示全數成功，1 表示尚有日期／商品待再跑一次。

> **2026-08-18 一次性修復紀錄**：以 `--days 160` 重驗 2026-03-12 起的窗，補回 **35 個
> 被誤標為「已處理」的真實交易日**（原油 7、黃金 7、比特幣 8、匯率 6、股市指數 7，
> 主要落在 2026-03-23~03-31 管線停擺期與 05-05、06-02、07-09）。修復後全時段「非
> 週末孤兒帳本」由 53 筆降至 18 筆，且 18 筆全部是美股假日（元旦、MLK、總統日、
> 耶穌受難日、陣亡將士紀念日、六月節、國慶補假），比特幣（24/7）孤兒為 0。

### 重試佇列去重與當機復原

補抓改為「逐日隔離」後，一次來源端整體失敗會讓每個失敗日期各排一筆重試
（30 天 × 5 商品 ≈ 150 筆），且多數在下一輪掃描仍會失敗、再排一次。故
`RetryQueue.add()` 對相同 `(task_type, params)` 去重。

> **去重範圍刻意只含 `pending`，不含 `retrying`。** `process_retry_queue`
> 會先把任務標成 `retrying` 才執行；若行程在這期間結束（CI deploy 的
> `rm -f`、當機），該任務就永遠停在 `retrying`——`get_pending()` 只回
> `pending`、隔日重排只處理 `exhausted`，沒有任何機制會把它撿回來。若把
> `retrying` 也納入去重，其後所有同名失敗都會被吞進這筆永遠不會執行的
> 任務。對 `params` 為常數的 `tdcc`（`{}`）與新聞類（`{"hours": 48}`），
> 等於該來源的重試佇列從此永久失效且無告警——正是本專案再三要消滅的
> 「失敗被靜默永久遮蔽」模式。
>
> 另於 `RetryQueue._load()` 把載入時仍是 `retrying` 的任務復原為
> `pending`，順帶清掉既有的孤兒殘留。

## 行情類 empty-crawl 帳本語意與自我修復

TWSE／TPEX／TAIFEX／FAOI／MGTS 五個行情來源共用 `data_upload/base.py`，以各自的
`*UploadDate` 帳本表判斷某日是否已上傳（避免重抓）。

### 風險：empty-crawl 誤記帳本 → 孤兒帳本永久遮蔽交易日

> **定位：預防性防呆，非修復現存故障。** 查核當下（2026-08-15）五個行情來源自
> 2026-06-01 起孤兒帳本數皆為 **0**，本機制是為避免下述情境日後發生而預先設置。

`base.upload_date` 在爬蟲**回空資料**時一律以 `Open=False` 記入帳本，而 `base.upload`
見帳本已有該日即**永久跳過**。因此「交易日但當時資料尚未發布」若被爬空，會被誤標為
非交易日而**永久遮蔽**——形成「帳本有列、價格表卻無資料」的**孤兒帳本**，真實行情
再也補不回（同型問題曾在 SPECIAL_INFO 發生，見 v2.11.0）。行情類的爬蟲**不像
SPECIAL_INFO 有 fallback**（回更早交易日）機制，無法單以「問爬蟲」區分「非交易日」與
「交易日但資料未發布」，故改以「交易日曆常識（週末必為非交易日）＋近期時間視窗重驗」
自我修復。

### 修法：每日近期重驗 + 一次性 deep 修復

- **每日近期重驗（`DailyUpload.clear_price_orphans`，內建於 21:00 `daily_craw`）**：
  補抓前先清除「近 `REVERIFY_DAYS`（預設 7）天、落在**平日**、帳本 `Open=False`」的
  孤兒帳本，使其重新成為缺漏候選並於同一輪重抓——資料已發布→補回並標 `Open=True`；
  仍為空→重標 `Open=False`。**週末**為確定非交易日、**更早於視窗**的日期一律保留
  標記，不清、不重試，避免對已確定的非交易日反覆重試同一天。台股行情最遲於盤後隔日
  清晨發布，7 天視窗足以涵蓋發布延遲。
- **一次性 deep 修復（`backfill_price.py`）**：對超出日常視窗的歷史孤兒帳本，以較大
  視窗清孤兒→逐日重抓，冪等可重跑（等同 SPECIAL_INFO 的 `backfill_special_info.py`）：

  ```bash
  docker run --rm --network db_network \
    nk7260ynpa/tw_stock_db_operating:latest \
    python backfill_price.py --days 30
  ```

  每次重抓前與 `daily_craw` 一樣隨機暫停 3~15 秒節流，避免大視窗一次清出數十個日期時
  背靠背打爆上游爬蟲。結束碼 0 代表全數完成；1 代表有網路失敗、或有日期重抓後帳本未
  回填（爬取失敗），兩者皆可直接重跑。若拋出 `NetworkError` 以外的例外（如 DB 連線、
  schema 錯誤）會中止整輪，該來源已清除但尚未重抓的日期會暫時失去帳本標記——價格資料
  不受影響、重跑即可補回，但視窗超過 30 天時 `daily_craw` 不會自動涵蓋，需人工重跑。

> **既有行為不受影響**：清孤兒只針對 `Open=False`（等同該日無價格資料）的帳本；
> `Open=True`（已成功上傳、已有價格）的日期一律不清、不重抓，維持「已上傳日正確跳過、
> 避免重抓」的既有保證。

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
- **持久化設定綁絕對 host 路徑**：`-v /Users/chen/AI/Tw_stock/Tw_stock_DB_Operating/config:/workspace/config`
  （**bind mount，非具名 volume**），與 `run.sh` 掛載同一份，兩條啟動路徑共用同一份設定。
  設定**不可**放進 logs 具名 volume，理由見
  [設定持久化](#設定持久化設定為什麼不能放在-log-目錄)。
- **無對外 publish port**：Web 管理介面（容器內 8080）由 Dashboard 透過 `db_network`
  反向代理存取（`ROOT_PATH=/app/db-operating`，已烤入 image），故與 `run.sh` 一致不加 `-p`。
  查 log 改用 `docker logs tw_stock_db_operating`。

## 環境需求

- Python 3.12.7
- Docker
- MySQL 資料庫（需先建立資料庫與資料表）

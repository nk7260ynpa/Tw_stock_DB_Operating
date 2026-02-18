# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 專案概述

台股資料庫操作模組，從爬蟲服務取得台股資料，經預處理與 schema 驗證後上傳至 MySQL 資料庫。支援 TWSE、TPEX、TAIFEX、FAOI、MGTS 五個資料來源。

## 常用指令

```bash
# 建立 Docker image
bash docker/build.sh

# 啟動每日排程上傳（預設 CMD）
./run.sh

# 批次上傳指定日期範圍
./run.sh python upload.py --start_date 2024-01-02 --end_date 2024-01-31 --dbname TWSE

# 執行全部單元測試（修改程式碼後需先 build）
docker run --rm nk7260ynpa/tw_stock_db_operating:1.0.0 python -m pytest test/

# 執行單一測試檔案
docker run --rm nk7260ynpa/tw_stock_db_operating:1.0.0 python -m pytest test/test_base.py -v

# 啟動完整服務（含爬蟲）
docker compose -f docker/docker-compose.yaml up -d

# 背景啟動每日排程容器
docker run -d --name tw_stock_daily_upload \
  --network db_network \
  -v $(pwd)/logs:/workspace/logs \
  nk7260ynpa/tw_stock_db_operating:1.0.0
```

## 架構

### 分層設計

```
連線層: clients.py → routers.py
                        ↓
協調層: upload.py（批次） / DailyUpload.py（排程）
                        ↓
上傳層: data_upload/base.py（模板方法）
            ↓
        twse / tpex / taifex / faoi / mgts
```

- **連線層**：`clients.py` 提供原始 MySQL 連線函式，`routers.py` 的 `MySQLRouter` 封裝連線邏輯
- **上傳層**：`data_upload/base.py` 定義 `DataUploadBase` 抽象基類（模板方法模式），子模組實作 `preprocess()` 和 `UploadType`（Pydantic model）
- **協調層**：`upload.py` 透過 `data_upload.__dict__[dbname.lower()].Uploader` 動態載入對應上傳器；`DailyUpload.py` 使用 schedule 每日 20:07 排程

### 資料流

```
爬蟲服務 HTTP GET → craw_data() → register_stock_names() → preprocess()
→ check_schema() (Pydantic) → upload_df() (to_sql DailyPrice) → upload_date() (UploadDate 記錄)
```

### 動態載入機制

`upload.py` 透過 `data_upload.__dict__[dbname.lower()].Uploader` 動態載入對應上傳器，無需手動維護 if/else 分支。資料源模組名稱小寫（twse），資料庫名稱大寫（TWSE），透過 `dbname.lower()` 對應。

### 新增資料來源

1. 在 `data_upload/` 建立新模組，定義 `UploadType`（Pydantic BaseModel）和 `Uploader`（繼承 `DataUploadBase`，實作 `preprocess()`）
2. 若有 StockName 表，在 `__init__` 設定 `self.stock_code_col` 和 `self.stock_name_col`
3. 在 `data_upload/__init__.py` 加入 `from .新模組 import *`
4. 在 `upload.py` 的 `--dbname` choices 加入新名稱
5. 在 `DailyUpload.py` 的 `DB_NAMES` 加入新名稱
6. 建立對應的單元測試 `test/test_新模組.py`

### StockName 註冊機制

上傳前會自動比對 StockName 表，將不存在的股票代碼與名稱插入。各資料源的欄位對應：
- TWSE/FAOI/MGTS：`SecurityCode` + `StockName`
- TPEX：`Code` + `Name`
- TAIFEX：無 StockName 表（`stock_code_col = None`，自動跳過）

### 關鍵約定

- 每次上傳前以 `check_date()` 檢查 UploadDate 避免重複
- 爬蟲請求間隨機暫停 3-15 秒（避免限流）
- 爬蟲 API：`GET http://{host}/{source_name}?date=YYYY-MM-DD`，回傳 `{"data": [...]}`
- 日誌輸出至 `logs/` 資料夾（upload.log、daily_upload.log）

## 程式碼風格

- Python 遵循 Google Python Style Guide，Docstring 使用繁體中文
- Docstring 的 Args 須標註型別，如 `host (str): MySQL 主機位址。`
- 每個 Python 檔案須有 module-level docstring
- Shell 腳本遵循 Google Shell Style Guide

## Docker 環境

- Image 名稱：`nk7260ynpa/tw_stock_db_operating:1.0.0`
- 基於 `python:3.12.7`，時區 `Asia/Taipei`
- 預設 CMD：`python DailyUpload.py`
- 需要外部 `db_network` Docker 網路，容器間透過名稱通訊：
  - MySQL：`tw_stock_database:3306`
  - 爬蟲服務：`tw_stocker_crawler:6738`
- 所有 Python 程式與單元測試皆在 Docker container 中執行
- 修改程式碼後需重新 `bash docker/build.sh` 才能生效

## 測試

使用 `unittest` + `unittest.mock`，mock 外部 MySQL 連線進行測試。測試檔案放在 `test/` 目錄下，共 62 個測試案例涵蓋所有模組。

測試結構對應：每個 `*.py` 對應 `test/test_*.py`，`data_upload/base.py` 對應 `test/test_base.py`（含 20 個測試，覆蓋 craw_data、check_schema、check_date、upload_df、upload_date、upload、register_stock_names）。

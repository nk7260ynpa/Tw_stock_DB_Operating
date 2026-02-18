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

# 執行全部單元測試
docker run --rm nk7260ynpa/tw_stock_db_operating:1.0.0 python -m pytest test/

# 執行單一測試檔案
docker run --rm nk7260ynpa/tw_stock_db_operating:1.0.0 python -m pytest test/test_routers.py -v

# 啟動完整服務（含爬蟲）
docker compose -f docker/docker-compose.yaml up -d
```

## 架構

### 分層設計

```
clients.py → routers.py → upload.py / DailyUpload.py
                              ↓
                         data_upload/
                    (base.py + 各資料源模組)
```

- **連線層**：`clients.py` 提供原始 MySQL 連線函式，`routers.py` 的 `MySQLRouter` 封裝連線邏輯
- **上傳層**：`data_upload/base.py` 定義 `DataUploadBase` 抽象基類（模板方法模式），子模組實作 `preprocess()` 和 `UploadType`（Pydantic model）
- **協調層**：`upload.py` 透過 `data_upload.__dict__[dbname.lower()].Uploader` 動態載入對應上傳器；`DailyUpload.py` 使用 schedule 排程

### 資料流

爬蟲服務 HTTP POST → `craw_data()` → `preprocess()` → `check_schema()` (Pydantic) → `upload_df()` (to_sql DailyPrice) → `upload_date()` (UploadDate 記錄)

### 新增資料來源

1. 在 `data_upload/` 建立新模組，定義 `UploadType`（Pydantic BaseModel）和 `Uploader`（繼承 `DataUploadBase`，實作 `preprocess()`）
2. 在 `data_upload/__init__.py` 加入 `from .新模組 import *`
3. 在 `upload.py` 的 `--dbname` choices 加入新名稱
4. 在 `DailyUpload.py` 加入排程時間

### 關鍵約定

- 資料源模組名稱小寫（twse），資料庫名稱大寫（TWSE），透過 `dbname.lower()` 對應
- 每次上傳前以 `check_date()` 檢查 UploadDate 避免重複；爬蟲請求間隨機暫停 3-15 秒
- 日誌輸出至 `logs/` 資料夾（upload.log、daily_upload.log）

## 程式碼風格

- Python 遵循 Google Python Style Guide，Docstring 使用繁體中文
- Docstring 的 Args 須標註型別，如 `host (str): MySQL 主機位址。`
- 每個 Python 檔案須有 module-level docstring
- Shell 腳本遵循 Google Shell Style Guide

## Docker 環境

- 基於 `python:3.12.7`，時區 `Asia/Taipei`
- 需要外部 `db_network` Docker 網路（連接 `tw_stock_database:3306` 和 `tw_stocker_crawler:6738`）
- 所有 Python 程式與單元測試皆在 Docker container 中執行

## 測試

使用 `unittest` + `unittest.mock`，mock 外部 MySQL 連線進行測試。測試檔案放在 `test/` 目錄下。

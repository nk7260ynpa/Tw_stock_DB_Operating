"""Web 管理介面模組。

提供 FastAPI Web 伺服器，整合每日排程上傳與手動上傳功能。
支援透過網頁操作手動上傳指定日期的資料，以及修改每日排程時間。
"""

import os
import json
import uuid
import time
import random
import threading
import logging
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager

import schedule as schedule_lib
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from easydict import EasyDict
from sqlalchemy import text

from DailyUpload import daily_craw, DB_NAMES, HOST, USER, PASSWORD, CRAWLERHOST
from upload import day_upload
from data_upload.quarter_revenue import QuarterRevenueUploader
from routers import MySQLRouter

# 路徑設定
BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
LOG_DIR = BASE_DIR / "logs"
CONFIG_PATH = LOG_DIR / "config.json"

# 確保 logs 資料夾存在
os.makedirs(LOG_DIR, exist_ok=True)

# Logging 設定
log_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
file_handler = logging.FileHandler(LOG_DIR / "web_server.log")
file_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 上傳任務追蹤
upload_jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()

# 排程管理
schedule_lock = threading.Lock()


def load_config():
    """讀取設定檔。

    Returns:
        dict: 設定內容，包含 schedule_time 欄位。
    """
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"schedule_time": "20:07"}


def save_config(config):
    """儲存設定檔。

    Args:
        config (dict): 設定內容。
    """
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def setup_schedule(schedule_time):
    """設定每日排程。

    Args:
        schedule_time (str): 排程時間，格式為 HH:MM。
    """
    with schedule_lock:
        schedule_lib.clear()
        schedule_lib.every().day.at(schedule_time).do(daily_craw)
        logger.info("排程已設定為每日 %s", schedule_time)


def scheduler_thread():
    """排程執行緒，持續檢查並執行待處理的排程任務。"""
    while True:
        with schedule_lock:
            schedule_lib.run_pending()
        time.sleep(1)


def run_upload_job(job_id, start_date, end_date, databases):
    """執行上傳任務（背景執行緒）。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期，格式為 YYYY-MM-DD。
        end_date (str): 結束日期，格式為 YYYY-MM-DD。
        databases (list[str]): 資料庫名稱清單。
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    total_tasks = len(dates) * len(databases)

    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"
        upload_jobs[job_id]["total"] = total_tasks
        upload_jobs[job_id]["completed"] = 0

    completed = 0

    try:
        for db_name in databases:
            opt = EasyDict({
                "host": HOST,
                "user": USER,
                "password": PASSWORD,
                "dbname": db_name,
                "crawlerhost": CRAWLERHOST,
            })

            for date in dates:
                with jobs_lock:
                    upload_jobs[job_id]["current_date"] = date
                    upload_jobs[job_id]["current_db"] = db_name

                try:
                    pause_duration = random.uniform(3, 15)
                    time.sleep(pause_duration)
                    day_upload(date, opt)
                except Exception as e:
                    logger.error("上傳失敗 %s %s: %s", db_name, date, e)
                    with jobs_lock:
                        upload_jobs[job_id]["errors"].append(
                            f"{db_name} {date}: {str(e)}"
                        )

                completed += 1
                with jobs_lock:
                    upload_jobs[job_id]["completed"] = completed

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info("上傳任務完成 %s", job_id)

    except Exception as e:
        logger.error("上傳任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


# Pydantic 請求模型
class UploadRequest(BaseModel):
    """手動上傳請求。"""
    start_date: str
    end_date: str
    databases: list[str]


class QuarterRevenueRequest(BaseModel):
    """季度營業收入抓取請求。"""
    year: int
    season: int


class ScheduleRequest(BaseModel):
    """排程時間更新請求。"""
    time: str


# FastAPI 應用
@asynccontextmanager
async def lifespan(app: FastAPI):
    """應用程式生命週期管理。"""
    config = load_config()
    setup_schedule(config["schedule_time"])

    t = threading.Thread(target=scheduler_thread, daemon=True)
    t.start()
    logger.info("Web 伺服器與排程服務已啟動。")

    yield


app = FastAPI(title="台股資料管理介面", lifespan=lifespan)


@app.post("/api/upload")
def create_upload(req: UploadRequest):
    """建立手動上傳任務。

    Args:
        req: 包含起始日期、結束日期、資料庫清單的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 檢查是否有正在執行的任務
    with jobs_lock:
        running_jobs = [
            j for j in upload_jobs.values() if j["status"] == "running"
        ]
        if running_jobs:
            raise HTTPException(
                409, "已有上傳任務正在執行中，請等待完成後再提交"
            )

    # 驗證資料庫名稱
    for db in req.databases:
        if db not in DB_NAMES:
            raise HTTPException(400, f"不支援的資料庫: {db}")

    if not req.databases:
        raise HTTPException(400, "請至少選擇一個資料庫")

    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "status": "pending",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "databases": req.databases,
            "total": 0,
            "completed": 0,
            "current_date": "",
            "current_db": "",
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    t = threading.Thread(
        target=run_upload_job,
        args=(job_id, req.start_date, req.end_date, req.databases),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "status": "pending"}


@app.get("/api/upload/jobs")
def list_upload_jobs():
    """列出所有上傳任務。

    Returns:
        list[dict]: 所有任務的狀態資訊。
    """
    with jobs_lock:
        return list(upload_jobs.values())


@app.get("/api/upload/status/{job_id}")
def get_upload_status(job_id: str):
    """查詢上傳任務狀態。

    Args:
        job_id: 任務 ID。

    Returns:
        dict: 任務狀態資訊。
    """
    with jobs_lock:
        if job_id not in upload_jobs:
            raise HTTPException(404, "任務不存在")
        return upload_jobs[job_id]


@app.get("/api/schedule")
def get_schedule():
    """取得目前排程時間。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    return {"time": config["schedule_time"]}


@app.put("/api/schedule")
def update_schedule(req: ScheduleRequest):
    """更新排程時間。

    Args:
        req: 包含新排程時間的請求。

    Returns:
        dict: 更新後的排程時間與訊息。
    """
    try:
        time_parts = req.time.split(":")
        hour = int(time_parts[0])
        minute = int(time_parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
    except (ValueError, IndexError):
        raise HTTPException(400, "時間格式錯誤，請使用 HH:MM")

    config = load_config()
    config["schedule_time"] = req.time
    save_config(config)
    setup_schedule(req.time)

    logger.info("排程時間已更新為 %s", req.time)
    return {"time": req.time, "message": f"排程時間已更新為 {req.time}"}


@app.get("/api/databases")
def list_databases():
    """列出可用的資料庫。

    Returns:
        dict: 包含 databases 欄位的資料庫清單。
    """
    return {"databases": DB_NAMES}


def run_quarter_revenue_job(job_id, year, season):
    """執行季度營業收入抓取任務（背景執行緒）。

    Args:
        job_id (str): 任務 ID。
        year (int): 民國年。
        season (int): 季度（1-4）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "TWSE").mysql_conn
        uploader = QuarterRevenueUploader(conn)
        record_count = uploader.upload(year, season)
        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = record_count
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info("季度營業收入任務完成 %s", job_id)

    except Exception as e:
        logger.error("季度營業收入任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


@app.post("/api/quarter-revenue/upload")
def create_quarter_revenue_upload(req: QuarterRevenueRequest):
    """建立季度營業收入抓取任務。

    Args:
        req: 包含年份與季度的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    if req.season not in (1, 2, 3, 4):
        raise HTTPException(400, "季度必須為 1-4")

    if not (80 <= req.year <= 200):
        raise HTTPException(400, "年份必須為 80-200（民國年）")

    with jobs_lock:
        running_jobs = [
            j for j in upload_jobs.values()
            if j["status"] == "running"
        ]
        if running_jobs:
            raise HTTPException(
                409, "已有任務正在執行中，請等待完成後再提交"
            )

    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "quarter_revenue",
            "status": "pending",
            "year": req.year,
            "season": req.season,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    t = threading.Thread(
        target=run_quarter_revenue_job,
        args=(job_id, req.year, req.season),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "status": "pending"}


@app.get("/api/quarter-revenue/uploaded")
def list_uploaded_quarters():
    """列出已上傳的季度營業收入記錄。

    Returns:
        dict: 包含 uploaded 欄位的已上傳記錄清單。
    """
    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "TWSE").mysql_conn

        # 檢查並移除不相容的舊表結構
        try:
            cols = conn.execute(
                text("DESCRIBE QuarterRevenueUploaded")
            ).fetchall()
            col_names = {row[0] for row in cols}
            if "Season" not in col_names:
                conn.execute(text("DROP TABLE QuarterRevenueUploaded"))
                conn.commit()
        except Exception:
            pass

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS QuarterRevenueUploaded (
                Year INT,
                Season INT,
                UploadedAt DATETIME,
                RecordCount INT,
                UNIQUE KEY uq_quarter_uploaded (Year, Season)
            )
        """))
        conn.commit()

        rows = conn.execute(
            text(
                "SELECT Year, Season, UploadedAt, RecordCount "
                "FROM QuarterRevenueUploaded "
                "ORDER BY Year DESC, Season DESC"
            )
        ).fetchall()
        conn.close()

        uploaded = [
            {
                "year": row[0],
                "season": row[1],
                "uploaded_at": row[2].isoformat() if row[2] else None,
                "record_count": row[3],
            }
            for row in rows
        ]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳季度失敗：%s", e)
        return {"uploaded": []}


# Serve React 前端靜態檔案
@app.get("/{full_path:path}")
async def serve_frontend(full_path: str):
    """Serve React 前端頁面與靜態資源。

    Args:
        full_path: 請求路徑。

    Returns:
        FileResponse: 靜態檔案或 index.html（SPA fallback）。
    """
    if not STATIC_DIR.exists():
        raise HTTPException(404, "前端頁面尚未建構")

    # 防止路徑穿越攻擊
    if full_path:
        file_path = (STATIC_DIR / full_path).resolve()
        if not str(file_path).startswith(str(STATIC_DIR.resolve())):
            raise HTTPException(403, "禁止存取")
        if file_path.is_file():
            return FileResponse(file_path)

    # SPA fallback：回傳 index.html
    index_file = STATIC_DIR / "index.html"
    if index_file.is_file():
        return FileResponse(index_file)

    raise HTTPException(404, "頁面不存在")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

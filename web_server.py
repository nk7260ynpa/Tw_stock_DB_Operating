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

from dataclasses import asdict

from DailyUpload import daily_craw, set_retry_queue, DB_NAMES, HOST, USER, PASSWORD, CRAWLERHOST
from upload import day_upload
from data_upload.base import NetworkError
from data_upload.quarter_revenue import QuarterRevenueUploader
from data_upload.tdcc import TDCCUploader
from data_upload.ctee_news import CTEENewsUploader
from data_upload.cnyes_news import CNYESNewsUploader
from data_upload.ptt_news import PTTNewsUploader
from data_upload.moneyudn_news import MoneyUDNNewsUploader
from data_upload.company_info import CompanyInfoUploader
from retry_queue import RetryQueue, is_network_error, check_network_available
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

# 設定 root logger 讓所有子模組的 log 都能輸出
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

# 上傳任務追蹤
upload_jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()

# 排程管理
schedule_lock = threading.Lock()

# 網路失敗重試佇列
retry_queue: RetryQueue | None = None


def load_config():
    """讀取設定檔。

    Returns:
        dict: 設定內容，包含 schedule_time、tdcc_schedule、
            ctee_schedule、cnyes_schedule、ptt_schedule
            和 moneyudn_schedule 欄位。
    """
    default = {
        "schedule_time": "20:07",
        "tdcc_schedule": {"time": "10:00"},
        "ctee_schedule": {"time": "21:00"},
        "cnyes_schedule": {"time": "21:30"},
        "ptt_schedule": {"time": "22:00"},
        "moneyudn_schedule": {"time": "22:30"},
    }
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
        # 向後相容：舊 config 可能沒有 tdcc_schedule
        if "tdcc_schedule" not in config:
            config["tdcc_schedule"] = default["tdcc_schedule"]
        # 向後相容：舊格式含 day 欄位，遷移為新格式（僅保留 time）
        elif "day" in config["tdcc_schedule"]:
            config["tdcc_schedule"] = {
                "time": config["tdcc_schedule"].get("time", "10:00"),
            }
            save_config(config)
            logger.info("已將 TDCC 排程設定從週排程遷移為每日排程。")
        # 向後相容：舊 config 可能沒有 ctee_schedule
        if "ctee_schedule" not in config:
            config["ctee_schedule"] = default["ctee_schedule"]
        # 向後相容：舊 config 可能沒有 cnyes_schedule
        if "cnyes_schedule" not in config:
            config["cnyes_schedule"] = default["cnyes_schedule"]
        # 向後相容：舊 config 可能沒有 ptt_schedule
        if "ptt_schedule" not in config:
            config["ptt_schedule"] = default["ptt_schedule"]
        # 向後相容：舊 config 可能沒有 moneyudn_schedule
        if "moneyudn_schedule" not in config:
            config["moneyudn_schedule"] = default["moneyudn_schedule"]
        return config
    return default


def save_config(config):
    """儲存設定檔。

    Args:
        config (dict): 設定內容。
    """
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def setup_schedule(
    schedule_time, tdcc_schedule=None, ctee_schedule=None,
    cnyes_schedule=None, ptt_schedule=None, moneyudn_schedule=None,
):
    """設定每日排程（含 TDCC、CTEE、CNYES、PTT、MoneyUDN 每日檢查）。

    Args:
        schedule_time (str): 每日資料上傳排程時間，格式為 HH:MM。
        tdcc_schedule (dict | None): TDCC 每日排程設定，
            包含 time（HH:MM）。
        ctee_schedule (dict | None): CTEE 新聞每日排程設定，
            包含 time（HH:MM）。
        cnyes_schedule (dict | None): CNYES 新聞每日排程設定，
            包含 time（HH:MM）。
        ptt_schedule (dict | None): PTT 新聞每日排程設定，
            包含 time（HH:MM）。
        moneyudn_schedule (dict | None): MoneyUDN 新聞每日排程設定，
            包含 time（HH:MM）。
    """
    with schedule_lock:
        schedule_lib.clear()
        schedule_lib.every().day.at(schedule_time).do(daily_craw)
        logger.info("每日排程已設定為 %s", schedule_time)

        if tdcc_schedule:
            tdcc_time = tdcc_schedule.get("time", "10:00")
            schedule_lib.every().day.at(tdcc_time).do(
                run_tdcc_scheduled
            )
            logger.info(
                "TDCC 每日排程已設定為 %s", tdcc_time
            )

        if ctee_schedule:
            ctee_time = ctee_schedule.get("time", "21:00")
            schedule_lib.every().day.at(ctee_time).do(
                run_ctee_news_scheduled
            )
            logger.info(
                "CTEE 新聞每日排程已設定為 %s", ctee_time
            )

        if cnyes_schedule:
            cnyes_time = cnyes_schedule.get("time", "21:30")
            schedule_lib.every().day.at(cnyes_time).do(
                run_cnyes_news_scheduled
            )
            logger.info(
                "CNYES 新聞每日排程已設定為 %s", cnyes_time
            )

        if ptt_schedule:
            ptt_time = ptt_schedule.get("time", "22:00")
            schedule_lib.every().day.at(ptt_time).do(
                run_ptt_news_scheduled
            )
            logger.info(
                "PTT 新聞每日排程已設定為 %s", ptt_time
            )

        if moneyudn_schedule:
            moneyudn_time = moneyudn_schedule.get("time", "22:30")
            schedule_lib.every().day.at(moneyudn_time).do(
                run_moneyudn_news_scheduled
            )
            logger.info(
                "MoneyUDN 新聞每日排程已設定為 %s", moneyudn_time
            )

        # 每小時執行重試佇列
        schedule_lib.every(1).hours.do(process_retry_queue)
        logger.info("重試佇列每小時排程已設定。")


def process_retry_queue():
    """處理重試佇列中的 pending 任務。

    檢查網路連通後，逐一執行 pending 任務。
    成功則標為 success，NetworkError 則 retry_count+1 並中斷，
    非網路錯誤或超過重試上限則標為 exhausted。
    """
    global retry_queue
    if retry_queue is None:
        return

    pending = retry_queue.get_pending()
    if not pending:
        return

    logger.info("開始處理重試佇列，共 %d 筆 pending 任務。", len(pending))

    if not check_network_available(CRAWLERHOST):
        logger.warning("爬蟲服務不可達，跳過本次重試。")
        return

    for task in pending:
        if task.retry_count >= task.max_retries:
            retry_queue.update_status(task.task_id, "exhausted")
            logger.warning(
                "重試任務 %s 已達上限 %d 次，標為 exhausted。",
                task.task_id, task.max_retries,
            )
            continue

        retry_queue.update_status(task.task_id, "retrying")
        logger.info(
            "重試任務 %s（%s），第 %d 次重試。",
            task.task_id, task.task_type, task.retry_count,
        )

        try:
            _execute_retry_task(task)
            retry_queue.update_status(task.task_id, "success")
            logger.info("重試任務 %s 成功。", task.task_id)
        except NetworkError as e:
            logger.warning(
                "重試任務 %s 仍然網路失敗：%s，中斷本輪重試。",
                task.task_id, e,
            )
            retry_queue.update_status(
                task.task_id, "pending", str(e)
            )
            break
        except Exception as e:
            logger.error(
                "重試任務 %s 非網路錯誤：%s，標為 exhausted。",
                task.task_id, e,
            )
            retry_queue.update_status(
                task.task_id, "exhausted", str(e)
            )


def _execute_retry_task(task):
    """根據任務類型分發執行重試任務。

    Args:
        task (RetryTask): 要重試的任務。

    Raises:
        NetworkError: 網路連線失敗。
        Exception: 其他執行錯誤。
    """
    if task.task_type == "daily_upload":
        db_name = task.params["db_name"]
        dates = task.params["dates"]
        opt = EasyDict({
            "host": HOST,
            "user": USER,
            "password": PASSWORD,
            "dbname": db_name,
            "crawlerhost": CRAWLERHOST,
        })
        for date in sorted(dates):
            pause_duration = random.uniform(3, 15)
            time.sleep(pause_duration)
            day_upload(date, opt)

    elif task.task_type == "ctee_news":
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = CTEENewsUploader(conn, CRAWLERHOST)
        uploader.upload_by_hours(task.params["hours"])
        conn.close()

    elif task.task_type == "cnyes_news":
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = CNYESNewsUploader(conn, CRAWLERHOST)
        uploader.upload_by_hours(task.params["hours"])
        conn.close()

    elif task.task_type == "ptt_news":
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = PTTNewsUploader(conn, CRAWLERHOST)
        uploader.upload_by_hours(task.params["hours"])
        conn.close()

    elif task.task_type == "moneyudn_news":
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = MoneyUDNNewsUploader(conn, CRAWLERHOST)
        uploader.upload_by_hours(task.params["hours"])
        conn.close()

    elif task.task_type == "tdcc":
        conn = MySQLRouter(HOST, USER, PASSWORD, "TWSE").mysql_conn
        uploader = TDCCUploader(conn, CRAWLERHOST)
        uploader.upload()
        conn.close()

    else:
        raise ValueError(f"不支援的重試任務類型：{task.task_type}")


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


def run_ctee_news_scheduled():
    """排程觸發的 CTEE 新聞上傳（過去 24 小時）。"""
    today = datetime.now().strftime("%Y-%m-%d")
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "ctee_news",
            "status": "pending",
            "date": today,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    t = threading.Thread(
        target=run_ctee_news_hours_job,
        args=(job_id, 24),
        daemon=True,
    )
    t.start()
    logger.info("CTEE 新聞排程任務已建立 %s（hours=24）", job_id)


def run_ctee_news_upload_job(job_id, start_date, end_date):
    """執行 CTEE 新聞上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的新聞。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = CTEENewsUploader(conn, CRAWLERHOST)

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        total_records = 0
        total_files = 0
        current = start_dt

        while current <= end_dt:
            date_str = current.strftime("%Y-%m-%d")
            with jobs_lock:
                upload_jobs[job_id]["date"] = date_str

            result = uploader.upload(date_str)
            total_records += result["record_count"]
            total_files += result["file_count"]
            current += timedelta(days=1)

        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = total_records
            upload_jobs[job_id]["file_count"] = total_files
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "CTEE 新聞任務完成 %s（%d 筆 metadata，%d 個檔案）",
            job_id, total_records, total_files,
        )

    except Exception as e:
        logger.error("CTEE 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_ctee_news_hours_job(job_id, hours):
    """執行 CTEE 新聞時數模式上傳任務（背景執行緒）。

    使用 hours 參數呼叫爬蟲 API，取得過去指定小時數的新聞，
    自動處理跨日資料的去重與上傳。

    Args:
        job_id (str): 任務 ID。
        hours (int): 要回溯的小時數。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = CTEENewsUploader(conn, CRAWLERHOST)

        result = uploader.upload_by_hours(hours)
        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = result["record_count"]
            upload_jobs[job_id]["file_count"] = result["file_count"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "CTEE 新聞任務完成 %s（hours=%d，%d 筆 metadata，%d 個檔案）",
            job_id, hours, result["record_count"], result["file_count"],
        )

    except NetworkError as e:
        logger.warning("CTEE 新聞任務網路失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "ctee_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("CTEE 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_cnyes_news_scheduled():
    """排程觸發的 CNYES 新聞上傳（過去 24 小時）。"""
    today = datetime.now().strftime("%Y-%m-%d")
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "cnyes_news",
            "status": "pending",
            "date": today,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    t = threading.Thread(
        target=run_cnyes_news_hours_job,
        args=(job_id, 24),
        daemon=True,
    )
    t.start()
    logger.info("CNYES 新聞排程任務已建立 %s（hours=24）", job_id)


def run_cnyes_news_upload_job(job_id, start_date, end_date):
    """執行 CNYES 新聞上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的新聞。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = CNYESNewsUploader(conn, CRAWLERHOST)

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        total_records = 0
        total_files = 0
        current = start_dt

        while current <= end_dt:
            date_str = current.strftime("%Y-%m-%d")
            with jobs_lock:
                upload_jobs[job_id]["date"] = date_str

            result = uploader.upload(date_str)
            total_records += result["record_count"]
            total_files += result["file_count"]
            current += timedelta(days=1)

        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = total_records
            upload_jobs[job_id]["file_count"] = total_files
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "CNYES 新聞任務完成 %s（%d 筆 metadata，%d 個檔案）",
            job_id, total_records, total_files,
        )

    except Exception as e:
        logger.error("CNYES 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_cnyes_news_hours_job(job_id, hours):
    """執行 CNYES 新聞時數模式上傳任務（背景執行緒）。

    使用 hours 參數呼叫爬蟲 API，取得過去指定小時數的新聞，
    自動處理跨日資料的去重與上傳。

    Args:
        job_id (str): 任務 ID。
        hours (int): 要回溯的小時數。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = CNYESNewsUploader(conn, CRAWLERHOST)

        result = uploader.upload_by_hours(hours)
        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = result["record_count"]
            upload_jobs[job_id]["file_count"] = result["file_count"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "CNYES 新聞任務完成 %s（hours=%d，%d 筆 metadata，%d 個檔案）",
            job_id, hours, result["record_count"], result["file_count"],
        )

    except NetworkError as e:
        logger.warning("CNYES 新聞任務網路失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "cnyes_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("CNYES 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_ptt_news_scheduled():
    """排程觸發的 PTT 新聞上傳（過去 24 小時）。"""
    today = datetime.now().strftime("%Y-%m-%d")
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "ptt_news",
            "status": "pending",
            "date": today,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    t = threading.Thread(
        target=run_ptt_news_hours_job,
        args=(job_id, 24),
        daemon=True,
    )
    t.start()
    logger.info("PTT 新聞排程任務已建立 %s（hours=24）", job_id)


def run_ptt_news_upload_job(job_id, start_date, end_date):
    """執行 PTT 新聞上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的新聞。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = PTTNewsUploader(conn, CRAWLERHOST)

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        total_records = 0
        total_files = 0
        current = start_dt

        while current <= end_dt:
            date_str = current.strftime("%Y-%m-%d")
            with jobs_lock:
                upload_jobs[job_id]["date"] = date_str

            result = uploader.upload(date_str)
            total_records += result["record_count"]
            total_files += result["file_count"]
            current += timedelta(days=1)

        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = total_records
            upload_jobs[job_id]["file_count"] = total_files
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "PTT 新聞任務完成 %s（%d 筆 metadata，%d 個檔案）",
            job_id, total_records, total_files,
        )

    except Exception as e:
        logger.error("PTT 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_ptt_news_hours_job(job_id, hours):
    """執行 PTT 新聞時數模式上傳任務（背景執行緒）。

    使用 hours 參數呼叫爬蟲 API，取得過去指定小時數的新聞，
    自動處理跨日資料的去重與上傳。

    Args:
        job_id (str): 任務 ID。
        hours (int): 要回溯的小時數。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = PTTNewsUploader(conn, CRAWLERHOST)

        result = uploader.upload_by_hours(hours)
        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = result["record_count"]
            upload_jobs[job_id]["file_count"] = result["file_count"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "PTT 新聞任務完成 %s（hours=%d，%d 筆 metadata，%d 個檔案）",
            job_id, hours, result["record_count"], result["file_count"],
        )

    except NetworkError as e:
        logger.warning("PTT 新聞任務網路失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "ptt_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("PTT 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_moneyudn_news_scheduled():
    """排程觸發的 MoneyUDN 新聞上傳（過去 24 小時）。"""
    today = datetime.now().strftime("%Y-%m-%d")
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "moneyudn_news",
            "status": "pending",
            "date": today,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    t = threading.Thread(
        target=run_moneyudn_news_hours_job,
        args=(job_id, 24),
        daemon=True,
    )
    t.start()
    logger.info("MoneyUDN 新聞排程任務已建立 %s（hours=24）", job_id)


def run_moneyudn_news_upload_job(job_id, start_date, end_date):
    """執行 MoneyUDN 新聞上傳任務（背景執行緒）。

    支援日期範圍上傳，依序處理每一天的新聞。

    Args:
        job_id (str): 任務 ID。
        start_date (str): 起始日期（YYYY-MM-DD）。
        end_date (str): 結束日期（YYYY-MM-DD）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = MoneyUDNNewsUploader(conn, CRAWLERHOST)

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        total_records = 0
        total_files = 0
        current = start_dt

        while current <= end_dt:
            date_str = current.strftime("%Y-%m-%d")
            with jobs_lock:
                upload_jobs[job_id]["date"] = date_str

            result = uploader.upload(date_str)
            total_records += result["record_count"]
            total_files += result["file_count"]
            current += timedelta(days=1)

        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = total_records
            upload_jobs[job_id]["file_count"] = total_files
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "MoneyUDN 新聞任務完成 %s（%d 筆 metadata，%d 個檔案）",
            job_id, total_records, total_files,
        )

    except Exception as e:
        logger.error("MoneyUDN 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_moneyudn_news_hours_job(job_id, hours):
    """執行 MoneyUDN 新聞時數模式上傳任務（背景執行緒）。

    使用 hours 參數呼叫爬蟲 API，取得過去指定小時數的新聞，
    自動處理跨日資料的去重與上傳。

    Args:
        job_id (str): 任務 ID。
        hours (int): 要回溯的小時數。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        uploader = MoneyUDNNewsUploader(conn, CRAWLERHOST)

        result = uploader.upload_by_hours(hours)
        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["record_count"] = result["record_count"]
            upload_jobs[job_id]["file_count"] = result["file_count"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info(
            "MoneyUDN 新聞任務完成 %s（hours=%d，%d 筆 metadata，%d 個檔案）",
            job_id, hours, result["record_count"], result["file_count"],
        )

    except NetworkError as e:
        logger.warning("MoneyUDN 新聞任務網路失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "moneyudn_news", {"hours": hours}, str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("MoneyUDN 新聞任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_tdcc_scheduled():
    """排程觸發的 TDCC 上傳。"""
    job_id = str(uuid.uuid4())[:8]

    with jobs_lock:
        upload_jobs[job_id] = {
            "job_id": job_id,
            "type": "tdcc",
            "status": "pending",
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
            "scheduled": True,
        }

    t = threading.Thread(
        target=run_tdcc_upload_job,
        args=(job_id,),
        daemon=True,
    )
    t.start()
    logger.info("TDCC 排程任務已建立 %s", job_id)


def run_tdcc_upload_job(job_id):
    """執行 TDCC 上傳任務（背景執行緒）。

    Args:
        job_id (str): 任務 ID。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "TWSE").mysql_conn
        uploader = TDCCUploader(conn, CRAWLERHOST)
        result = uploader.upload()
        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["date"] = result["date"]
            upload_jobs[job_id]["record_count"] = result["record_count"]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info("TDCC 任務完成 %s", job_id)

    except NetworkError as e:
        logger.warning("TDCC 任務網路失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        if retry_queue is not None:
            retry_queue.add(
                "tdcc", {}, str(e),
                created_by_job_id=job_id,
            )

    except Exception as e:
        logger.error("TDCC 任務失敗 %s: %s", job_id, e)
        with jobs_lock:
            upload_jobs[job_id]["status"] = "failed"
            upload_jobs[job_id]["error"] = str(e)
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()


def run_company_info_upload_job(job_id):
    """執行公司產業對照上傳任務（背景執行緒）。

    Args:
        job_id (str): 任務 ID。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "TWSE").mysql_conn
        uploader = CompanyInfoUploader(conn, CRAWLERHOST)
        result = uploader.upload()
        conn.close()

        with jobs_lock:
            upload_jobs[job_id]["status"] = "completed"
            upload_jobs[job_id]["company_info_count"] = result[
                "company_info_count"
            ]
            upload_jobs[job_id]["industry_map_count"] = result[
                "industry_map_count"
            ]
            upload_jobs[job_id]["finished_at"] = datetime.now().isoformat()
        logger.info("公司產業對照任務完成 %s", job_id)

    except Exception as e:
        logger.error("公司產業對照任務失敗 %s: %s", job_id, e)
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
    quarter: int


class ScheduleRequest(BaseModel):
    """排程時間更新請求。"""
    time: str


class TDCCScheduleRequest(BaseModel):
    """TDCC 每日排程更新請求。"""
    time: str


class CTEENewsUploadRequest(BaseModel):
    """CTEE 新聞上傳請求。"""
    start_date: str
    end_date: str


class CTEENewsScheduleRequest(BaseModel):
    """CTEE 新聞每日排程更新請求。"""
    time: str


class CNYESNewsUploadRequest(BaseModel):
    """CNYES 新聞上傳請求。"""
    start_date: str
    end_date: str


class CNYESNewsScheduleRequest(BaseModel):
    """CNYES 新聞每日排程更新請求。"""
    time: str


class PTTNewsUploadRequest(BaseModel):
    """PTT 新聞上傳請求。"""
    start_date: str
    end_date: str


class PTTNewsScheduleRequest(BaseModel):
    """PTT 新聞每日排程更新請求。"""
    time: str


class MoneyUDNNewsUploadRequest(BaseModel):
    """MoneyUDN 新聞上傳請求。"""
    start_date: str
    end_date: str


class MoneyUDNNewsScheduleRequest(BaseModel):
    """MoneyUDN 新聞每日排程更新請求。"""
    time: str


# FastAPI 應用
@asynccontextmanager
async def lifespan(app: FastAPI):
    """應用程式生命週期管理。"""
    global retry_queue
    retry_queue = RetryQueue(LOG_DIR / "retry_queue.json")
    set_retry_queue(retry_queue)
    logger.info("重試佇列已初始化。")

    config = load_config()
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
    )

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
    setup_schedule(
        req.time,
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
    )

    logger.info("排程時間已更新為 %s", req.time)
    return {"time": req.time, "message": f"排程時間已更新為 {req.time}"}


@app.get("/api/databases")
def list_databases():
    """列出可用的資料庫。

    Returns:
        dict: 包含 databases 欄位的資料庫清單。
    """
    return {"databases": DB_NAMES}


def run_quarter_revenue_job(job_id, year, quarter):
    """執行季度營業收入抓取任務（背景執行緒）。

    Args:
        job_id (str): 任務 ID。
        year (int): 民國年。
        quarter (int): 季度（1-4）。
    """
    with jobs_lock:
        upload_jobs[job_id]["status"] = "running"

    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "TWSE").mysql_conn
        uploader = QuarterRevenueUploader(conn)
        record_count = uploader.upload(year, quarter)
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
    if req.quarter not in (1, 2, 3, 4):
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
            "quarter": req.quarter,
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    t = threading.Thread(
        target=run_quarter_revenue_job,
        args=(job_id, req.year, req.quarter),
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

        rows = conn.execute(
            text(
                "SELECT Year, Quarter "
                "FROM QuarterRevenueUploaded "
                "ORDER BY Year DESC, Quarter DESC"
            )
        ).fetchall()
        conn.close()

        uploaded = [
            {
                "year": row[0],
                "quarter": row[1],
            }
            for row in rows
        ]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳季度失敗：%s", e)
        return {"uploaded": []}


@app.post("/api/tdcc/upload")
def create_tdcc_upload():
    """建立 TDCC 上傳任務。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
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
            "type": "tdcc",
            "status": "pending",
            "record_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    t = threading.Thread(
        target=run_tdcc_upload_job,
        args=(job_id,),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "status": "pending"}


@app.get("/api/tdcc/uploaded")
def list_uploaded_tdcc():
    """列出已上傳的 TDCC 日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 20 筆）。
    """
    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "TWSE").mysql_conn

        rows = conn.execute(
            text(
                "SELECT DISTINCT Date FROM TDCC "
                "ORDER BY Date DESC LIMIT 20"
            )
        ).fetchall()
        conn.close()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳 TDCC 日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/tdcc/schedule")
def get_tdcc_schedule():
    """取得 TDCC 每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    tdcc = config.get("tdcc_schedule", {"time": "10:00"})
    return {"time": tdcc["time"]}


@app.put("/api/tdcc/schedule")
def update_tdcc_schedule(req: TDCCScheduleRequest):
    """更新 TDCC 每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
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
    config["tdcc_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config["tdcc_schedule"],
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
    )

    logger.info("TDCC 每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"TDCC 每日排程已更新為 {req.time}",
    }


# CTEE 新聞 API 端點
@app.post("/api/ctee-news/upload")
def create_ctee_news_upload(req: CTEENewsUploadRequest):
    """建立 CTEE 新聞上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

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
            "type": "ctee_news",
            "status": "pending",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    t = threading.Thread(
        target=run_ctee_news_upload_job,
        args=(job_id, req.start_date, req.end_date),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "status": "pending"}


@app.get("/api/ctee-news/uploaded")
def list_uploaded_ctee_news():
    """列出已上傳的 CTEE 新聞日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn

        rows = conn.execute(
            text(
                "SELECT Date FROM CTEEUploaded "
                "ORDER BY Date DESC LIMIT 50"
            )
        ).fetchall()
        conn.close()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳 CTEE 新聞日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/ctee-news/schedule")
def get_ctee_news_schedule():
    """取得 CTEE 新聞每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    ctee = config.get("ctee_schedule", {"time": "21:00"})
    return {"time": ctee["time"]}


@app.put("/api/ctee-news/schedule")
def update_ctee_news_schedule(req: CTEENewsScheduleRequest):
    """更新 CTEE 新聞每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
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
    config["ctee_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config["ctee_schedule"],
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
    )

    logger.info("CTEE 新聞每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"CTEE 新聞每日排程已更新為 {req.time}",
    }


# CNYES 新聞 API 端點
@app.post("/api/cnyes-news/upload")
def create_cnyes_news_upload(req: CNYESNewsUploadRequest):
    """建立 CNYES 新聞上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

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
            "type": "cnyes_news",
            "status": "pending",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    t = threading.Thread(
        target=run_cnyes_news_upload_job,
        args=(job_id, req.start_date, req.end_date),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "status": "pending"}


@app.get("/api/cnyes-news/uploaded")
def list_uploaded_cnyes_news():
    """列出已上傳的 CNYES 新聞日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn

        rows = conn.execute(
            text(
                "SELECT Date FROM CNYESUploaded "
                "ORDER BY Date DESC LIMIT 50"
            )
        ).fetchall()
        conn.close()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳 CNYES 新聞日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/cnyes-news/schedule")
def get_cnyes_news_schedule():
    """取得 CNYES 新聞每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    cnyes = config.get("cnyes_schedule", {"time": "21:30"})
    return {"time": cnyes["time"]}


@app.put("/api/cnyes-news/schedule")
def update_cnyes_news_schedule(req: CNYESNewsScheduleRequest):
    """更新 CNYES 新聞每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
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
    config["cnyes_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config["cnyes_schedule"],
        config.get("ptt_schedule"),
        config.get("moneyudn_schedule"),
    )

    logger.info("CNYES 新聞每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"CNYES 新聞每日排程已更新為 {req.time}",
    }


# PTT 新聞 API 端點
@app.post("/api/ptt-news/upload")
def create_ptt_news_upload(req: PTTNewsUploadRequest):
    """建立 PTT 新聞上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

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
            "type": "ptt_news",
            "status": "pending",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    t = threading.Thread(
        target=run_ptt_news_upload_job,
        args=(job_id, req.start_date, req.end_date),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "status": "pending"}


@app.get("/api/ptt-news/uploaded")
def list_uploaded_ptt_news():
    """列出已上傳的 PTT 新聞日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn

        rows = conn.execute(
            text(
                "SELECT Date FROM PTTUploaded "
                "ORDER BY Date DESC LIMIT 50"
            )
        ).fetchall()
        conn.close()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳 PTT 新聞日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/ptt-news/schedule")
def get_ptt_news_schedule():
    """取得 PTT 新聞每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    ptt = config.get("ptt_schedule", {"time": "22:00"})
    return {"time": ptt["time"]}


@app.put("/api/ptt-news/schedule")
def update_ptt_news_schedule(req: PTTNewsScheduleRequest):
    """更新 PTT 新聞每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
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
    config["ptt_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config["ptt_schedule"],
        config.get("moneyudn_schedule"),
    )

    logger.info("PTT 新聞每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"PTT 新聞每日排程已更新為 {req.time}",
    }


# MoneyUDN 新聞 API 端點
@app.post("/api/moneyudn-news/upload")
def create_moneyudn_news_upload(req: MoneyUDNNewsUploadRequest):
    """建立 MoneyUDN 新聞上傳任務。

    Args:
        req: 包含起始日期與結束日期的請求。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
    # 驗證日期格式
    try:
        start = datetime.strptime(req.start_date, "%Y-%m-%d")
        end = datetime.strptime(req.end_date, "%Y-%m-%d")
        if end < start:
            raise HTTPException(400, "結束日期不能早於起始日期")
    except ValueError:
        raise HTTPException(400, "日期格式錯誤，請使用 YYYY-MM-DD")

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
            "type": "moneyudn_news",
            "status": "pending",
            "start_date": req.start_date,
            "end_date": req.end_date,
            "date": req.start_date,
            "record_count": 0,
            "file_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    t = threading.Thread(
        target=run_moneyudn_news_upload_job,
        args=(job_id, req.start_date, req.end_date),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "status": "pending"}


@app.get("/api/moneyudn-news/uploaded")
def list_uploaded_moneyudn_news():
    """列出已上傳的 MoneyUDN 新聞日期。

    Returns:
        dict: 包含 uploaded 欄位的已上傳日期清單（最近 50 筆）。
    """
    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn

        rows = conn.execute(
            text(
                "SELECT Date FROM MoneyUDNUploaded "
                "ORDER BY Date DESC LIMIT 50"
            )
        ).fetchall()
        conn.close()

        uploaded = [str(row[0]) for row in rows]
        return {"uploaded": uploaded}

    except Exception as e:
        logger.error("查詢已上傳 MoneyUDN 新聞日期失敗：%s", e)
        return {"uploaded": []}


@app.get("/api/moneyudn-news/schedule")
def get_moneyudn_news_schedule():
    """取得 MoneyUDN 新聞每日排程設定。

    Returns:
        dict: 包含 time 欄位的排程資訊。
    """
    config = load_config()
    moneyudn = config.get("moneyudn_schedule", {"time": "22:30"})
    return {"time": moneyudn["time"]}


@app.put("/api/moneyudn-news/schedule")
def update_moneyudn_news_schedule(req: MoneyUDNNewsScheduleRequest):
    """更新 MoneyUDN 新聞每日排程設定。

    Args:
        req: 包含 time 的請求。

    Returns:
        dict: 更新後的排程設定與訊息。
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
    config["moneyudn_schedule"] = {"time": req.time}
    save_config(config)
    setup_schedule(
        config["schedule_time"],
        config.get("tdcc_schedule"),
        config.get("ctee_schedule"),
        config.get("cnyes_schedule"),
        config.get("ptt_schedule"),
        config["moneyudn_schedule"],
    )

    logger.info("MoneyUDN 新聞每日排程已更新為 %s", req.time)
    return {
        "time": req.time,
        "message": f"MoneyUDN 新聞每日排程已更新為 {req.time}",
    }


# 公司產業對照 API 端點
@app.post("/api/company-info/upload")
def create_company_info_upload():
    """建立公司產業對照上傳任務。

    Returns:
        dict: 任務 ID 與初始狀態。
    """
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
            "type": "company_info",
            "status": "pending",
            "company_info_count": 0,
            "industry_map_count": 0,
            "errors": [],
            "created_at": datetime.now().isoformat(),
            "finished_at": None,
        }

    t = threading.Thread(
        target=run_company_info_upload_job,
        args=(job_id,),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "status": "pending"}


@app.get("/api/company-info/status")
def get_company_info_status():
    """取得 CompanyInfo 和 IndustryMap 表的資料筆數。

    Returns:
        dict: 包含 company_info_count 和 industry_map_count。
    """
    try:
        conn = MySQLRouter(HOST, USER, PASSWORD, "TWSE").mysql_conn

        company_count = conn.execute(
            text("SELECT COUNT(*) FROM CompanyInfo")
        ).scalar()
        industry_count = conn.execute(
            text("SELECT COUNT(*) FROM IndustryMap")
        ).scalar()
        conn.close()

        return {
            "company_info_count": company_count,
            "industry_map_count": industry_count,
        }

    except Exception as e:
        logger.error("查詢公司產業對照狀態失敗：%s", e)
        return {
            "company_info_count": 0,
            "industry_map_count": 0,
        }


# 重試佇列 API 端點
@app.get("/api/retry-queue")
def get_retry_queue():
    """取得所有重試任務與網路狀態。

    Returns:
        dict: 包含 tasks、network_available 和 summary。
    """
    tasks = retry_queue.get_all()
    network_ok = check_network_available(CRAWLERHOST)
    return {
        "tasks": [asdict(t) for t in tasks],
        "network_available": network_ok,
        "summary": {
            "pending": sum(1 for t in tasks if t.status == "pending"),
            "retrying": sum(1 for t in tasks if t.status == "retrying"),
            "success": sum(1 for t in tasks if t.status == "success"),
            "exhausted": sum(1 for t in tasks if t.status == "exhausted"),
        },
    }


@app.post("/api/retry-queue/retry-all")
def retry_all_pending():
    """手動立即觸發重試所有 pending 任務。

    Returns:
        dict: 操作結果訊息。
    """
    t = threading.Thread(target=process_retry_queue, daemon=True)
    t.start()
    return {"message": "已觸發重試所有 pending 任務"}


@app.post("/api/retry-queue/reset-exhausted")
def reset_exhausted_tasks():
    """將所有 exhausted 任務重設為 pending。

    Returns:
        dict: 操作結果訊息與重設數量。
    """
    count = retry_queue.reset_exhausted()
    return {
        "message": f"已重設 {count} 筆 exhausted 任務",
        "reset_count": count,
    }


@app.delete("/api/retry-queue/clear")
def clear_completed_retry_tasks():
    """清除所有已完成的重試任務。

    Returns:
        dict: 操作結果訊息與清除數量。
    """
    count = retry_queue.clear_completed()
    return {
        "message": f"已清除 {count} 筆已完成任務",
        "cleared_count": count,
    }


@app.delete("/api/retry-queue/{task_id}")
def remove_retry_task(task_id: str):
    """移除單一重試任務。

    Args:
        task_id: 任務 ID。

    Returns:
        dict: 操作結果訊息。

    Raises:
        HTTPException: 任務不存在時拋出 404。
    """
    if not retry_queue.remove(task_id):
        raise HTTPException(404, "任務不存在")
    return {"message": "任務已移除"}


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

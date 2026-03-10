"""每日排程上傳模組。"""

import os
import time
import random
import logging
import datetime

from easydict import EasyDict
from sqlalchemy import text
import schedule

import upload
from routers import MySQLRouter
from data_upload.base import NetworkError

# 設定 logging，輸出至 logs/ 資料夾
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(log_dir, exist_ok=True)

log_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log_handler = logging.FileHandler(os.path.join(log_dir, "daily_upload.log"))
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# 全域重試佇列引用，由 web_server 注入
_retry_queue = None

DB_NAMES = ["TWSE", "TPEX", "TAIFEX", "FAOI", "MGTS"]
HOST = "tw_stock_database:3306"
USER = "root"
PASSWORD = "stock"
CRAWLERHOST = "tw_stocker_crawler:6738"

# MGTS、FAOI 已合併至 TWSE 資料庫，連線時需對應至 TWSE
DB_MAPPING = {
    "TWSE": "TWSE",
    "TPEX": "TPEX",
    "TAIFEX": "TAIFEX",
    "FAOI": "TWSE",
    "MGTS": "TWSE",
}

# 各資料來源對應的 UploadDate 表名
UPLOAD_DATE_TABLE = {
    "TWSE": "UploadDate",
    "TPEX": "UploadDate",
    "TAIFEX": "UploadDate",
    "FAOI": "FAOIUploadDate",
    "MGTS": "MGTSUploadDate",
}


def set_retry_queue(queue):
    """設定全域重試佇列引用。

    Args:
        queue (RetryQueue): 重試佇列實例。
    """
    global _retry_queue
    _retry_queue = queue


def _add_to_retry_queue(task_type, params, error_message):
    """將失敗任務加入重試佇列。

    若全域重試佇列尚未初始化則僅記錄日誌。

    Args:
        task_type (str): 任務類型。
        params (dict): 任務參數。
        error_message (str): 錯誤訊息。
    """
    if _retry_queue is None:
        logger.warning("重試佇列尚未初始化，無法加入重試任務。")
        return
    _retry_queue.add(task_type, params, error_message)


def get_missing_dates(db_name, days=30):
    """查詢過去指定天數內尚未上傳的日期。

    Args:
        db_name (str): 資料來源名稱（TWSE/TPEX/TAIFEX/FAOI/MGTS）。
        days (int): 往回檢查的天數，預設為 30。

    Returns:
        list[str]: 尚未上傳的日期清單，格式為 YYYY-MM-DD。
    """
    actual_db = DB_MAPPING.get(db_name, db_name)
    upload_date_table = UPLOAD_DATE_TABLE.get(db_name, "UploadDate")
    conn = MySQLRouter(HOST, USER, PASSWORD, actual_db).mysql_conn

    date_list = [
        (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(days)
    ]

    uploaded_dates = conn.execute(
        text(
            f"SELECT Date FROM {upload_date_table} "
            f"WHERE Date >= '{date_list[-1]}'"
        )
    ).fetchall()
    conn.close()

    uploaded_set = {row[0].strftime("%Y-%m-%d") for row in uploaded_dates}
    missing_dates = [d for d in date_list if d not in uploaded_set]

    return missing_dates


def daily_craw():
    """每日排程爬取資料並上傳至 MySQL 資料庫。

    檢查過去 30 天內所有資料來源是否有未上傳的日期，
    若有則依序爬取並上傳。
    """
    for db_name in DB_NAMES:
        opt = EasyDict({
            "host": HOST,
            "user": USER,
            "password": PASSWORD,
            "dbname": db_name,
            "crawlerhost": CRAWLERHOST,
        })

        missing_dates = get_missing_dates(db_name, days=30)

        if not missing_dates:
            logger.info(f"{db_name}: 過去 30 天資料皆已上傳，無需補抓。")
            continue

        logger.info(
            f"{db_name}: 發現 {len(missing_dates)} 個未上傳日期，開始補抓。"
        )

        for date in sorted(missing_dates):
            pause_duration = random.uniform(3, 15)
            time.sleep(pause_duration)
            try:
                upload.day_upload(date, opt)
            except NetworkError as e:
                logger.warning(
                    f"{db_name}: 日期 {date} 網路連線失敗，"
                    f"跳過後續日期：{e}"
                )
                _add_to_retry_queue(
                    "daily_upload",
                    {"db_name": db_name, "dates": sorted(missing_dates)},
                    str(e),
                )
                break

        logger.info(f"{db_name}: 補抓完成。")


if __name__ == "__main__":
    schedule.every().day.at("20:07").do(daily_craw)

    logger.info("每日排程上傳服務已啟動，排程時間 20:07。")

    while True:
        schedule.run_pending()
        time.sleep(1)

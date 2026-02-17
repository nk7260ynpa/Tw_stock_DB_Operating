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

DB_NAMES = ["TWSE", "TPEX", "TAIFEX", "FAOI", "MGTS"]
HOST = "localhost:3306"
USER = "root"
PASSWORD = "stock"
CRAWLERHOST = "127.0.0.1:6738"


def get_missing_dates(db_name, days=30):
    """查詢過去指定天數內尚未上傳的日期。

    Args:
        db_name (str): 資料庫名稱。
        days (int): 往回檢查的天數，預設為 30。

    Returns:
        list[str]: 尚未上傳的日期清單，格式為 YYYY-MM-DD。
    """
    conn = MySQLRouter(HOST, USER, PASSWORD, db_name).mysql_conn

    date_list = [
        (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(days)
    ]

    uploaded_dates = conn.execute(
        text(
            f"SELECT Date FROM UploadDate "
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
            upload.day_upload(date, opt)

        logger.info(f"{db_name}: 補抓完成。")


if __name__ == "__main__":
    schedule.every().day.at("20:07").do(daily_craw)

    logger.info("每日排程上傳服務已啟動，排程時間 20:07。")

    while True:
        schedule.run_pending()
        time.sleep(1)

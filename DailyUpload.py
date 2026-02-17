import os
import time
import random
import logging
import datetime

from easydict import EasyDict
import schedule

import upload

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


def daily_craw(db_name):
    """每日排程爬取資料並上傳至 MySQL 資料庫。

    自動上傳最近 7 天的資料，確保資料完整性。

    Args:
        db_name: 資料庫名稱，可選 "TWSE"、"TPEX"、"TAIFEX"、"FAOI" 或 "MGTS"。
    """
    HOST = "tw_stock_database:3306"
    USER = "root"
    PASSWORD = "stock"
    DBNAME = db_name
    CRAWLERHOST = "tw_stocker_crawler:6738"
    opt = EasyDict({
        "host": HOST,
        "user": USER,
        "password": PASSWORD,
        "dbname": DBNAME,
        "crawlerhost": CRAWLERHOST,
    })

    date_list = [
        (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(7)
    ]
    for date in date_list:
        pause_duration = random.uniform(3, 15)
        time.sleep(pause_duration)
        upload.day_upload(date, opt)


if __name__ == "__main__":
    schedule.every().day.at("20:10").do(daily_craw, "TWSE")
    schedule.every().day.at("20:18").do(daily_craw, "TPEX")
    schedule.every().day.at("20:23").do(daily_craw, "TAIFEX")
    schedule.every().day.at("20:33").do(daily_craw, "FAOI")
    schedule.every().day.at("20:45").do(daily_craw, "MGTS")

    logger.info("每日排程上傳服務已啟動。")

    while True:
        schedule.run_pending()
        time.sleep(1)

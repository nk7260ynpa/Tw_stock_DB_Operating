"""台股資料批次上傳入口模組。"""

import os
import time
import random
import argparse
import logging

from datetime import datetime, timedelta

import data_upload
from routers import MySQLRouter

# 設定 logging，輸出至 logs/ 資料夾
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(log_dir, exist_ok=True)

log_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log_handler = logging.FileHandler(os.path.join(log_dir, "upload.log"))
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)


def day_upload(date, opt):
    """執行單日資料上傳至 MySQL 資料庫。

    Args:
        date (str): 日期字串，格式為 YYYY-MM-DD。
        opt (argparse.Namespace | EasyDict): 命令列參數，包含以下屬性：
            - host (str): MySQL 主機位址。
            - user (str): MySQL 使用者名稱。
            - password (str): MySQL 密碼。
            - dbname (str): MySQL 資料庫名稱。
            - crawlerhost (str): 爬蟲服務主機位址。
    """
    HOST = opt.host
    USER = opt.user
    PASSWORD = opt.password
    DBNAME = opt.dbname
    CRAWLERHOST = opt.crawlerhost

    logger.info(f"連線至 MySQL 資料庫 {DBNAME}，主機 {HOST}，使用者 {USER}")
    conn = MySQLRouter(HOST, USER, PASSWORD, DBNAME).mysql_conn
    package_name = DBNAME.lower()

    logger.info(f"上傳資料：模組 {package_name}，日期 {date}")
    uploader = data_upload.__dict__[package_name].Uploader(conn, CRAWLERHOST)
    uploader.upload(date)
    conn.close()

    logger.info("資料上傳完成。")


def main(opt):
    """主函式，處理命令列參數並執行批次上傳。

    Args:
        opt (argparse.Namespace): 命令列參數。
    """
    if not opt.end_date:
        opt.end_date = opt.start_date

    start_date = opt.start_date
    end_date = opt.end_date

    logger.info(f"開始上傳，日期範圍 {start_date} 至 {end_date}")

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    current_dt = start_dt
    while current_dt <= end_dt:
        pause_duration = random.uniform(3, 15)
        logger.info(
            f"暫停 {pause_duration:.1f} 秒後處理日期：{current_dt.strftime('%Y-%m-%d')}"
        )
        time.sleep(pause_duration)
        date_str = current_dt.strftime("%Y-%m-%d")
        day_upload(date_str, opt)
        current_dt += timedelta(days=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="上傳台股資料至 MySQL 資料庫。"
    )
    parser.add_argument(
        "--start_date", type=str, required=True,
        help="起始日期，格式為 YYYY-MM-DD"
    )
    parser.add_argument(
        "--end_date", type=str, default="",
        help="結束日期，格式為 YYYY-MM-DD"
    )
    parser.add_argument(
        "--host", type=str, default="tw_stock_database:3306",
        help="MySQL 主機位址"
    )
    parser.add_argument(
        "--user", type=str, default="root",
        help="MySQL 使用者名稱"
    )
    parser.add_argument(
        "--password", type=str, default="stock",
        help="MySQL 密碼"
    )
    parser.add_argument(
        "--dbname", type=str,
        choices=["TWSE", "TPEX", "TAIFEX", "FAOI", "MGTS"],
        default="TWSE",
        help="MySQL 資料庫名稱"
    )
    parser.add_argument(
        "--crawlerhost", type=str, default="tw_stocker_crawler:6738",
        help="爬蟲服務主機位址"
    )
    opt = parser.parse_args()

    main(opt)

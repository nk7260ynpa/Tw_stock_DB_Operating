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
from routers import db_conn
from data_upload.base import NetworkError, SourceError

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

# 行情類 empty-crawl 孤兒帳本每日重驗的近期視窗天數。
#
# 潛在風險（預防性防呆，非修復現存故障）：`base.upload_date` 在爬蟲回空時一律以
# `Open=False` 記入帳本，`upload` 見帳本有該日即永久跳過。故「交易日但當時資料尚未
# 發布」若被爬空，該日會被誤標為非交易日而永久遮蔽（孤兒帳本＝帳本有列、價格表無
# 資料），真實行情再也補不回。查核時（2026-08-15）五個行情來源自 2026-06-01 起孤兒
# 帳本數皆為 0，本機制為避免日後發生而設。
#
# 防呆：每日排程先清除「近 REVERIFY_DAYS 天、落在平日、標記 Open=False」的孤兒
# 帳本，使其重新成為缺漏候選並於同一輪重新向爬蟲查詢——資料已發布則補回並標
# Open=True，仍為空（真正的非交易日）則重標 Open=False。台股行情最遲於當日盤後
# 隔日清晨即發布，7 天視窗足以涵蓋發布延遲；週末為確定非交易日（不清、不重試），
# 更早於視窗的日期亦保留標記，避免對已確定的非交易日反覆重試同一天。歷史（超出
# 視窗）孤兒帳本的一次性修復請用 `backfill_price.py` 以較大視窗執行。
REVERIFY_DAYS = 7


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

    date_list = [
        (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(days)
    ]

    with db_conn(HOST, USER, PASSWORD, actual_db) as conn:
        uploaded_dates = conn.execute(
            text(
                f"SELECT Date FROM {upload_date_table} "
                f"WHERE Date >= '{date_list[-1]}'"
            )
        ).fetchall()

    uploaded_set = {row[0].strftime("%Y-%m-%d") for row in uploaded_dates}
    missing_dates = [d for d in date_list if d not in uploaded_set]

    return missing_dates


def _to_date(value):
    """將帳本 Date 欄位的查詢結果正規化為 `datetime.date`。

    MySQL 的 DATE 欄位經 pymysql 回傳 `datetime.date`，但不同驅動（或測試用的
    SQLite）可能回傳 `datetime.datetime` 或 `YYYY-MM-DD` 字串，故統一正規化，
    避免後續 `weekday()` 判斷因型別差異而誤判。

    Args:
        value (datetime.date | datetime.datetime | str): 帳本日期欄位值。

    Returns:
        datetime.date: 正規化後的日期。

    Raises:
        TypeError: 無法辨識的日期型別。
    """
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        return datetime.datetime.strptime(value[:10], "%Y-%m-%d").date()
    raise TypeError(f"無法辨識的帳本日期型別：{type(value)!r}")


def clear_price_orphans(
    db_name,
    days=REVERIFY_DAYS,
    today=None,
    host=None,
    user=None,
    password=None,
):
    """清除近 N 天內平日的 empty-crawl 孤兒帳本（Open=False），回傳被清除的日期。

    孤兒帳本＝帳本標記為非交易日（`Open=False`，行情類此標記等同「該日無價格
    資料」）的日期。其中「交易日但當時資料尚未發布」被爬空而誤標者，一經標記便
    被 `upload` 永久跳過而遮蔽真實行情。本函式僅重驗**近期平日**的此類孤兒：

        - 視窗為以 `today` 往回推算 `days` 個日曆天並排除 `today` 本身，即
          `today - days + 1` ～ `today - 1`（與 `get_missing_dates` 的「近 N 天
          含今日」同義，再排除今日）。排除今日與 `daily_craw` 的「排除今日」
          是同一個保守設定：v3 把排程搬到 21:00 後台股其實已收盤，但兩處都
          刻意維持現狀，日後若要改須兩處一併重新評估。落在視窗內且為平日
          （週一～週五）者才清除；清除後該日重新成為 `get_missing_dates` 的缺漏候選，由呼叫端
          （daily_craw／一次性修復）重新向爬蟲查詢。資料已發布→補回並標
          Open=True；仍為空→重標 Open=False。
        - 週末為確定非交易日、更早於視窗的日期亦保留其 Open=False 標記，不清除、
          不重試，避免對已確定的非交易日反覆重試同一天。

    僅清除 `Open=False` 的帳本，`Open=True`（已成功上傳、已有價格）的日期一律
    不受影響，確保「已上傳日正確跳過、避免重抓」的既有行為不被破壞。

    Args:
        db_name (str): 資料來源名稱（TWSE/TPEX/TAIFEX/FAOI/MGTS）。
        days (int): 重驗的近期視窗天數（含今日往回推算的日曆天數）。
        today (datetime.date | None): 視窗基準日，預設為當日；供測試注入。
        host (str | None): MySQL 主機位址，預設取模組層 `HOST`。
        user (str | None): MySQL 使用者名稱，預設取模組層 `USER`。
        password (str | None): MySQL 密碼，預設取模組層 `PASSWORD`。

    Returns:
        list[str]: 由舊到新排序、被清除的孤兒帳本日期字串（YYYY-MM-DD）。
    """
    actual_db = DB_MAPPING.get(db_name, db_name)
    upload_date_table = UPLOAD_DATE_TABLE.get(db_name, "UploadDate")
    if today is None:
        today = datetime.datetime.now().date()

    start = today - datetime.timedelta(days=days - 1)
    with db_conn(
        host if host is not None else HOST,
        user if user is not None else USER,
        password if password is not None else PASSWORD,
        actual_db,
    ) as conn:
        rows = conn.execute(
            text(
                f"SELECT Date FROM {upload_date_table} "
                f"WHERE Date >= :start AND Date < :today AND `Open` = 0"
            ),
            {
                "start": start.strftime("%Y-%m-%d"),
                "today": today.strftime("%Y-%m-%d"),
            },
        ).fetchall()

        # 僅重驗平日；週末（weekday() >= 5）為確定非交易日，保留標記不清除。
        orphans = sorted(
            _to_date(row[0]).strftime("%Y-%m-%d")
            for row in rows
            if _to_date(row[0]).weekday() < 5
        )

        for date_str in orphans:
            conn.execute(
                text(
                    f"DELETE FROM {upload_date_table} "
                    f"WHERE Date = :date AND `Open` = 0"
                ),
                {"date": date_str},
            )
        if orphans:
            conn.commit()

    return orphans


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

        # 先重驗近期平日的 empty-crawl 孤兒帳本（可能為「交易日但當時資料尚未
        # 發布」而被誤標 Open=False），清除後它們會重新成為下方缺漏候選並重抓，
        # 避免真實交易日被永久遮蔽。
        try:
            cleared = clear_price_orphans(db_name, days=REVERIFY_DAYS)
            if cleared:
                logger.info(
                    f"{db_name}: 清除近 {REVERIFY_DAYS} 天平日孤兒帳本 "
                    f"{len(cleared)} 筆並將重新查詢：{cleared}"
                )
        except Exception as e:  # noqa: BLE001 - 重驗失敗不應中斷當日補抓
            logger.warning(f"{db_name}: 清除孤兒帳本失敗，略過重驗：{e}")

        missing_dates = get_missing_dates(db_name, days=30)

        # 排除今日（**刻意保留的保守設定，v3 搬窗後未變更**）。
        #
        # 原始理由：排程原在早上 07:30 執行，當日台股尚未收盤、行情資料尚未發布，
        # 貿然爬取今日會取得空資料，經 base.upload_date 標記為「非交易日」
        # (Open=False) 後將永久跳過，導致今日真實行情永遠不會補上。
        #
        # 排程於 v3 移到 21:00 後，台股（13:30 收盤）當日資料其實已可抓取，理論上
        # 可以不再排除今日；但這是使用者明確選擇維持現狀的保守設定，故**不隨搬窗
        # 一併變更**。代價僅是「今日資料延到明日排程以昨日身分補回」，資料不會遺失，
        # 且排除今日在任何時段都安全（多一天延遲，換取零誤標風險）。
        # 若日後要改為納入今日，須連同 clear_price_orphans 的視窗上界一併評估。
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        missing_dates = [d for d in missing_dates if d < today_str]

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
            except SourceError as e:
                # 爬蟲仍可達，只是這一天在來源端抓不到：僅該日排入重試並
                # 繼續補後續日期。missing_dates 為昇冪排序，若比照網路失敗
                # 直接 break，最舊的「毒日期」會每天在同一處中斷排程，
                # 其後日期永遠不會被嘗試，直到滑出 30 天視窗即永久遺失。
                logger.warning(
                    f"{db_name}: 日期 {date} 來源端抓取失敗，"
                    f"排入重試並繼續後續日期：{e}"
                )
                _add_to_retry_queue(
                    "daily_upload",
                    {"db_name": db_name, "dates": [date]},
                    str(e),
                )
                continue
            except NetworkError as e:
                # 連不上爬蟲：後續日期必然同樣失敗，整批排入重試並中止。
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

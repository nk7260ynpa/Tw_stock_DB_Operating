"""行情類 empty-crawl 孤兒帳本一次性 deep 修復入口程式。

對 TWSE／TPEX／TAIFEX／FAOI／MGTS 五個行情來源，於近 N 天（預設 30）以 deep
（深度重驗）模式修復被 empty-crawl 誤標而永久遮蔽的真實交易日：

1. 清除窗內「落在平日、帳本標記 Open=False（等同該日無價格資料）」的孤兒帳本
   ——包含舊 bug／管線停擺期間「交易日但當時資料尚未發布」被爬空而誤標者，
   以及合法的平日非交易日（國定假日）。
2. 清除後這些日期重新成為缺漏候選，交由爬蟲（唯一真相來源）逐日重驗：資料已
   發布 → 補回價格並記帳 Open=True；仍為空 → 重標 Open=False（真正非交易日）。

與 `DailyUpload.clear_price_orphans` 共用同一組清孤兒邏輯；重抓沿用
`upload.day_upload`，冪等、可安全重跑。日常每日排程（daily_craw）已內建近
`REVERIFY_DAYS` 天的自我重驗，本作業則供「歷史缺漏超出日常視窗」時以較大視窗
一次性修復（等同 SPECIAL_INFO 的 `backfill_special_info.py` deep 修復）。

用法（於 db_network 上以既有 image 執行）：

    docker run --rm --network db_network \\
      nk7260ynpa/tw_stock_db_operating:latest \\
      python backfill_price.py --days 30
"""

import argparse
import logging
import sys

from easydict import EasyDict
from sqlalchemy import text

import upload
from DailyUpload import (
    CRAWLERHOST,
    DB_MAPPING,
    DB_NAMES,
    HOST,
    PASSWORD,
    UPLOAD_DATE_TABLE,
    USER,
    clear_price_orphans,
)
from data_upload.base import NetworkError
from routers import MySQLRouter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args(argv=None):
    """解析命令列參數。

    Args:
        argv (list[str] | None): 參數清單，預設取 sys.argv。

    Returns:
        argparse.Namespace: 解析後的參數。
    """
    parser = argparse.ArgumentParser(
        description="行情類 empty-crawl 孤兒帳本一次性 deep 修復。",
    )
    parser.add_argument(
        "--days", type=int, default=30, help="重驗視窗天數（預設 30）。",
    )
    parser.add_argument(
        "--host", default=HOST, help="MySQL 主機位址。",
    )
    parser.add_argument("--user", default=USER, help="MySQL 使用者名稱。")
    parser.add_argument("--password", default=PASSWORD, help="MySQL 密碼。")
    parser.add_argument(
        "--crawlerhost", default=CRAWLERHOST, help="爬蟲服務主機位址。",
    )
    return parser.parse_args(argv)


def _classify_dates(db_name, dates, host, user, password):
    """重讀帳本，將重驗後的日期分類為已補回／仍非交易日。

    Args:
        db_name (str): 資料來源名稱。
        dates (list[str]): 重驗過的日期字串清單。
        host (str): MySQL 主機位址。
        user (str): MySQL 使用者名稱。
        password (str): MySQL 密碼。

    Returns:
        tuple[int, int]: (已補回筆數 Open=True, 仍非交易日筆數 Open=False)。
    """
    if not dates:
        return 0, 0
    actual_db = DB_MAPPING.get(db_name, db_name)
    table = UPLOAD_DATE_TABLE.get(db_name, "UploadDate")
    conn = MySQLRouter(host, user, password, actual_db).mysql_conn
    filled = 0
    non_trading = 0
    try:
        for date_str in dates:
            open_val = conn.execute(
                text(
                    f"SELECT `Open` FROM {table} WHERE Date = :date"
                ),
                {"date": date_str},
            ).scalar()
            if open_val:
                filled += 1
            elif open_val is not None:
                non_trading += 1
    finally:
        conn.close()
    return filled, non_trading


def run_backfill(days, host, user, password, crawlerhost):
    """對五個行情來源執行孤兒帳本清理與缺漏重驗補回。

    Args:
        days (int): 重驗視窗天數。
        host (str): MySQL 主機位址。
        user (str): MySQL 使用者名稱。
        password (str): MySQL 密碼。
        crawlerhost (str): 爬蟲服務主機位址。

    Returns:
        list[dict]: 各來源的修復摘要。
    """
    summaries = []
    for db_name in DB_NAMES:
        opt = EasyDict({
            "host": host,
            "user": user,
            "password": password,
            "dbname": db_name,
            "crawlerhost": crawlerhost,
        })

        cleared = clear_price_orphans(
            db_name, days=days, host=host, user=user, password=password,
        )
        network_errors = []
        requeried = []
        for date_str in sorted(cleared):
            try:
                upload.day_upload(date_str, opt)
                requeried.append(date_str)
            except NetworkError as e:
                logger.warning(
                    "%s：重驗 %s 網路失敗，稍後可重跑：%s",
                    db_name, date_str, e,
                )
                network_errors.append(date_str)

        filled, non_trading = _classify_dates(
            db_name, requeried, host, user, password,
        )
        summaries.append({
            "db_name": db_name,
            "cleared": len(cleared),
            "filled": filled,
            "non_trading": non_trading,
            "network_errors": network_errors,
        })
    return summaries


def main(argv=None):
    """主程式進入點。

    Args:
        argv (list[str] | None): 參數清單，預設取 sys.argv。

    Returns:
        int: 結束碼（0 成功、1 有網路失敗待重試）。
    """
    args = parse_args(argv)
    logger.info("開始行情類孤兒帳本 deep 修復（近 %d 天）。", args.days)

    summaries = run_backfill(
        args.days, args.host, args.user, args.password, args.crawlerhost,
    )

    total_filled = 0
    total_network_errors = 0
    for summary in summaries:
        total_filled += summary["filled"]
        total_network_errors += len(summary["network_errors"])
        logger.info(
            "%s：清除孤兒 %d、補回交易日 %d、仍非交易日 %d、網路失敗 %d。",
            summary["db_name"], summary["cleared"], summary["filled"],
            summary["non_trading"], len(summary["network_errors"]),
        )

    logger.info(
        "行情類孤兒帳本 deep 修復完成：共補回 %d 個交易日，網路失敗 %d 筆。",
        total_filled, total_network_errors,
    )
    return 1 if total_network_errors else 0


if __name__ == "__main__":
    sys.exit(main())

"""SPECIAL_INFO 缺漏一次性回補與孤兒帳本清理入口程式。

對原油／黃金／比特幣／匯率／股市指數五個商品，於近 N 天（預設 30）以
deep（深度重驗）模式執行：

1. 先清除窗內「帳本有列但價格表無對應列」的孤兒帳本（涵蓋全部商品）——
   包含舊 bug／管線停擺期間被誤標為已完成的「真實交易日」與合法非交易日。
2. 清除後這些日期重新成為候選，交由爬蟲（唯一真相來源）逐日重驗：實際==
   請求 → REPLACE INTO 補回價格並記帳；只回更早日期／空 → 非交易日，重新記帳。

本作業使用 REPLACE INTO／INSERT IGNORE，冪等、可安全重跑。缺漏偵測與補抓
邏輯與 web_server 每日排程的「缺漏自我修復」共用同一組 Uploader 方法。

用法（於 db_network 上以既有 image 執行）：

    docker run --rm --network db_network \\
      nk7260ynpa/tw_stock_db_operating:latest \\
      python backfill_special_info.py --days 30
"""

import argparse
import logging
import sys

from data_upload.bitcoin_price import BitcoinPriceUploader
from data_upload.currency_price import CurrencyPriceUploader
from data_upload.gold_price import GoldPriceUploader
from data_upload.indices_price import IndicesPriceUploader
from data_upload.oil_price import OilPriceUploader
from routers import db_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# 五個商品 Uploader 類別（順序與每日排程一致）。
UPLOADER_CLASSES = [
    OilPriceUploader,
    GoldPriceUploader,
    BitcoinPriceUploader,
    CurrencyPriceUploader,
    IndicesPriceUploader,
]


def parse_args(argv=None):
    """解析命令列參數。

    Args:
        argv (list[str] | None): 參數清單，預設取 sys.argv。

    Returns:
        argparse.Namespace: 解析後的參數。
    """
    parser = argparse.ArgumentParser(
        description="SPECIAL_INFO 缺漏一次性回補與孤兒帳本清理。",
    )
    parser.add_argument(
        "--days", type=int, default=30, help="掃描天數（預設 30）。",
    )
    parser.add_argument(
        "--host", default="tw_stock_database:3306", help="MySQL 主機位址。",
    )
    parser.add_argument("--user", default="root", help="MySQL 使用者名稱。")
    parser.add_argument("--password", default="stock", help="MySQL 密碼。")
    parser.add_argument(
        "--crawlerhost", default="tw_stocker_crawler:6738",
        help="爬蟲服務主機位址。",
    )
    return parser.parse_args(argv)


def run_backfill(days, host, user, password, crawlerhost):
    """對五個商品執行孤兒帳本清理與缺漏回補。

    Args:
        days (int): 掃描天數。
        host (str): MySQL 主機位址。
        user (str): MySQL 使用者名稱。
        password (str): MySQL 密碼。
        crawlerhost (str): 爬蟲服務主機位址。

    單一商品若拋出未預期例外（如資料庫連線中斷），僅記錄該商品失敗並繼續
    處理其餘商品：本作業冪等可重跑，讓四個健康商品的補抓成果落地，遠優於
    因一個商品中斷而整批作廢。

    Returns:
        list[dict]: 各商品的補抓摘要（失敗者含 fatal 鍵）。
    """
    summaries = []
    for uploader_cls in UPLOADER_CLASSES:
        try:
            with db_conn(host, user, password, "SPECIAL_INFO") as conn:
                uploader = uploader_cls(conn, crawlerhost)
                # deep=True：先清孤兒帳本再交由爬蟲重驗，救回被誤標的真實
                # 交易日。
                summaries.append(uploader.backfill_missing(
                    days=days, deep=True,
                ))
        except Exception as e:  # noqa: BLE001 - 批次作業需逐商品隔離
            logger.exception(
                "%s 補抓中止（未預期例外），略過該商品繼續其餘商品。",
                uploader_cls.asset_label,
            )
            summaries.append({
                "asset": uploader_cls.asset_label,
                "scanned": 0, "filled": 0, "filled_dates": [],
                "non_trading": 0, "still_pending": 0, "records": 0,
                "orphans_cleared": 0, "network_errors": [],
                "crawl_errors": [], "fatal": str(e),
            })
    return summaries


def main(argv=None):
    """主程式進入點。

    Args:
        argv (list[str] | None): 參數清單，預設取 sys.argv。

    Returns:
        int: 結束碼（0 全數成功、1 有日期或商品失敗需再跑一次）。
    """
    args = parse_args(argv)
    logger.info("開始 SPECIAL_INFO 缺漏回補（近 %d 天）。", args.days)

    summaries = run_backfill(
        args.days, args.host, args.user, args.password, args.crawlerhost,
    )

    total_filled = 0
    total_failures = 0
    for summary in summaries:
        total_filled += summary["filled"]
        total_failures += len(summary["network_errors"])
        total_failures += len(summary.get("crawl_errors", []))
        if summary.get("fatal"):
            total_failures += 1
        logger.info(
            "%s：掃描 %d、補回 %d（%s）、非交易日 %d、待次日 %d、"
            "清除孤兒 %d、抓取失敗 %d、格式異常 %d%s。",
            summary["asset"], summary["scanned"], summary["filled"],
            ", ".join(summary["filled_dates"]) or "無",
            summary["non_trading"], summary["still_pending"],
            summary.get("orphans_cleared", 0),
            len(summary["network_errors"]),
            len(summary.get("crawl_errors", [])),
            f"、商品中止（{summary['fatal']}）" if summary.get("fatal") else "",
        )

    logger.info(
        "SPECIAL_INFO 缺漏回補完成：共補回 %d 筆日期，失敗 %d 筆。",
        total_filled, total_failures,
    )
    return 1 if total_failures else 0


if __name__ == "__main__":
    sys.exit(main())

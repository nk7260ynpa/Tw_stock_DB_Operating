"""SPECIAL_INFO 價格上傳共用邏輯（帳本語意 + 缺漏偵測補抓）。

原油／黃金／比特幣／匯率／股市指數五個上傳器結構相同，共用「取得資料→
寫入→記帳」與「近 N 天缺漏偵測補抓」邏輯。各上傳器須提供下列屬性與方法：

    屬性：
        conn: SQLAlchemy 連線物件（SPECIAL_INFO 資料庫）。
        is_continuous_market (bool): 是否為 24/7 連續市場（僅 Bitcoin 為 True）。
        price_table (str): 價格表名稱（如 "BitcoinPrice"）。
        uploaded_table (str): 帳本表名稱（如 "BitcoinPriceUploaded"）。
        asset_label (str): 商品中文名稱（如 "比特幣價格"），供 log 使用。
    方法：
        crawl_data(date) -> pd.DataFrame
        check_schema(df) -> pd.DataFrame
        _replace_into(df)
        _record_uploaded_date(date)

帳本語意（關鍵）：`*Uploaded` 帳本只記錄「df 內每一筆的實際交易日」；
「請求日」僅在下列情況才額外標記已完成：

    (a) 取得的最新實際日期 == 請求日（真的拿到當日資料）；或
    (b) 該商品「非 24/7」且爬蟲 fallback（回更早日期）或回空
        （確定請求日為非交易日）。

24/7 商品（Bitcoin）在實際日期 < 請求日時「不」標記請求日，留待次日 UTC
日 K 完成後回補，避免帳本謊報導致 check_uploaded 之後永久跳過該日。

缺漏偵測補抓：以「問爬蟲」為交易日／休市的唯一真相來源——候選缺漏日呼叫
爬蟲，回傳該日自身 K 棒（實際==該日）→ REPLACE INTO 補上並記帳；只回更早
日期（fallback）或回空 → 該日為非交易日，記帳標記避免反覆檢查。
"""

import logging
from datetime import datetime, timedelta

from sqlalchemy import text

from data_upload.base import NetworkError

logger = logging.getLogger(__name__)


def record_uploaded_dates(uploader, dates):
    """批次記錄多個實際交易日至 *Uploaded 帳本表（INSERT IGNORE）。

    Args:
        uploader: 上傳器實例。
        dates (list[str]): 實際交易日字串清單（YYYY-MM-DD）。
    """
    if not dates:
        return
    sql = text(
        f"INSERT IGNORE INTO {uploader.uploaded_table} (Date) VALUES (:date)"
    )
    for date in dates:
        uploader.conn.execute(sql, {"date": date})
    uploader.conn.commit()


def fetch_and_store(uploader, date):
    """取得指定日期資料並依帳本語意寫入（供 upload 與缺漏補抓共用）。

    不檢查 check_uploaded，由呼叫端自行決定是否先檢查帳本。

    Args:
        uploader: 上傳器實例。
        date (str): 請求日期字串（YYYY-MM-DD）。

    Returns:
        dict: 包含 date、record_count 與 filled 的結果字典。
            filled 表示是否取得「請求日自身」的資料（實際==請求）。

    Raises:
        NetworkError: 網路連線失敗（供重試機制使用）。
    """
    df = uploader.crawl_data(date)

    if df.empty:
        # 爬蟲回空（error「無法取得任何」或 data 空）：非交易日或當日尚未定案。
        if not uploader.is_continuous_market:
            uploader._record_uploaded_date(date)
            logger.info(
                "%s %s 無資料（非交易日），已記帳。", uploader.asset_label, date
            )
        else:
            logger.info(
                "%s %s 為 24/7 商品且當日資料尚未定案，暫不記帳，留待次日回補。",
                uploader.asset_label, date,
            )
        return {"date": date, "record_count": 0, "filled": False}

    df = uploader.check_schema(df)
    record_count = len(df)
    uploader._replace_into(df)

    # 帳本只記 df 內每一筆的實際交易日（而非盲目記請求日）。
    actual_dates = sorted({str(d) for d in df["Date"].tolist()})
    record_uploaded_dates(uploader, actual_dates)

    filled = date in actual_dates
    if not filled and not uploader.is_continuous_market:
        # 非 24/7 且 fallback 到更早交易日 → 請求日為非交易日，記帳避免反覆檢查。
        uploader._record_uploaded_date(date)

    logger.info(
        "%s %s 已上傳，共 %d 筆（實際交易日：%s，%s）。",
        uploader.asset_label, date, record_count, ", ".join(actual_dates),
        "含請求日" if filled else "未含請求日",
    )
    return {"date": date, "record_count": record_count, "filled": filled}


def _price_dates(uploader, start_date, end_date):
    """查詢價格表在 [start_date, end_date] 內已存在的日期集合。"""
    rows = uploader.conn.execute(
        text(
            f"SELECT DISTINCT Date FROM {uploader.price_table} "
            f"WHERE Date >= :start AND Date <= :end"
        ),
        {"start": start_date, "end": end_date},
    ).fetchall()
    return {str(r[0]) for r in rows}


def _ledger_dates(uploader, start_date, end_date):
    """查詢帳本表在 [start_date, end_date] 內已記錄的日期集合。"""
    rows = uploader.conn.execute(
        text(
            f"SELECT Date FROM {uploader.uploaded_table} "
            f"WHERE Date >= :start AND Date <= :end"
        ),
        {"start": start_date, "end": end_date},
    ).fetchall()
    return {str(r[0]) for r in rows}


def _resolve_today(today):
    """將 today 參數正規化為 date 物件（None 取當日）。"""
    if today is None:
        return datetime.now().date()
    if isinstance(today, str):
        return datetime.strptime(today, "%Y-%m-%d").date()
    return today


def find_missing_dates(uploader, days=30, today=None):
    """找出近 N 天在價格表缺漏、需補抓的候選日期。

    連續市場（24/7，Bitcoin）：候選 = 價格表沒有的日期（忽略帳本，
    使孤兒帳本日期仍可自我修復）。
    非連續市場：候選 = 價格表沒有「且」帳本也沒有的日期（帳本已標記者
    視為已檢查的交易日／非交易日，避免反覆詢問爬蟲）。

    Args:
        uploader: 上傳器實例。
        days (int): 掃描天數（含今日往回推算的日曆天數）。
        today (str | date | None): 掃描基準日，預設為當日。

    Returns:
        list[str]: 由舊到新排序的候選缺漏日期字串。
    """
    today = _resolve_today(today)
    start = today - timedelta(days=days - 1)
    start_str = start.strftime("%Y-%m-%d")
    end_str = today.strftime("%Y-%m-%d")

    price = _price_dates(uploader, start_str, end_str)
    if uploader.is_continuous_market:
        skip = price
    else:
        skip = price | _ledger_dates(uploader, start_str, end_str)

    missing = []
    cur = start
    while cur <= today:
        date_str = cur.strftime("%Y-%m-%d")
        if date_str not in skip:
            missing.append(date_str)
        cur += timedelta(days=1)
    return missing


def backfill_missing(uploader, days=30, today=None, deep=False):
    """掃描近 N 天缺漏並補抓（冪等、可重跑）。

    對每個候選缺漏日呼叫 fetch_and_store：

        - 取得請求日自身資料 → filled，計入 filled。
        - 只回更早日期／空（非連續市場）→ 非交易日（已記帳），計入 non_trading。
        - 24/7 商品當日尚未定案 → 計入 still_pending（留待次日）。

    NetworkError 逐日捕捉、記入 network_errors 供呼叫端交由 retry_queue，
    不中斷整體掃描。

    deep=True（深度重驗，供一次性修復／人工觸發使用）：先刪除窗內「帳本有列
    但價格表無列」的孤兒帳本，使這些日期重新成為候選，再交由爬蟲（唯一真相
    來源）重驗——舊 bug／管線停擺期間被誤標為已完成的「真實交易日」會被補回
    價格，真正的非交易日則重新記帳。日常排程用 deep=False 以避免反覆詢問已
    確認的非交易日。

    Args:
        uploader: 上傳器實例。
        days (int): 掃描天數。
        today (str | date | None): 掃描基準日，預設為當日。
        deep (bool): 是否先清除孤兒帳本再重驗，預設 False。

    Returns:
        dict: 補抓摘要。
    """
    orphans_cleared = 0
    if deep:
        orphans_cleared = _delete_ledger_orphans(uploader, days=days, today=today)

    missing = find_missing_dates(uploader, days=days, today=today)
    summary = {
        "asset": uploader.asset_label,
        "scanned": len(missing),
        "filled": 0,
        "filled_dates": [],
        "non_trading": 0,
        "still_pending": 0,
        "records": 0,
        "orphans_cleared": orphans_cleared,
        "network_errors": [],
    }
    for date_str in missing:
        try:
            result = fetch_and_store(uploader, date_str)
        except NetworkError as e:
            logger.warning(
                "%s 缺漏補抓 %s 網路失敗：%s（交由重試佇列）。",
                uploader.asset_label, date_str, e,
            )
            summary["network_errors"].append(date_str)
            continue
        if result["filled"]:
            summary["filled"] += 1
            summary["filled_dates"].append(date_str)
            summary["records"] += result["record_count"]
        elif uploader.is_continuous_market:
            # 24/7 商品仍抓不到請求日自身 → 當日尚未定案，留待次日。
            summary["still_pending"] += 1
        else:
            summary["non_trading"] += 1
    logger.info(
        "%s 缺漏補抓完成：清孤兒 %d、掃描 %d、補回 %d、非交易日 %d、"
        "待次日 %d、網路失敗 %d。",
        uploader.asset_label, summary["orphans_cleared"], summary["scanned"],
        summary["filled"], summary["non_trading"], summary["still_pending"],
        len(summary["network_errors"]),
    )
    return summary


def _delete_ledger_orphans(uploader, days=30, today=None):
    """刪除窗內「帳本有列但價格表無對應列」的孤兒帳本，回傳刪除筆數。

    供 deep 重驗使用。刪除後這些日期會重新成為缺漏候選，交由爬蟲重驗：

        - 24/7 連續市場（Bitcoin）：每日皆有資料，帳本日期理應都對應到價格
          列，孤兒必為舊 bug 造成，刪除後可補回。
        - 非連續市場：孤兒可能是「舊 bug／停擺誤標的真實交易日」（應補回），
          也可能是「合法非交易日標記」（重驗後會再次記帳）。一律刪除交由爬蟲
          重驗，才能救回被誤標的真實交易日。

    Args:
        uploader: 上傳器實例。
        days (int): 檢查天數。
        today (str | date | None): 檢查基準日，預設為當日。

    Returns:
        int: 刪除的孤兒帳本筆數。
    """
    today = _resolve_today(today)
    start = today - timedelta(days=days - 1)
    start_str = start.strftime("%Y-%m-%d")
    end_str = today.strftime("%Y-%m-%d")

    price = _price_dates(uploader, start_str, end_str)
    ledger = _ledger_dates(uploader, start_str, end_str)
    orphans = sorted(ledger - price)
    if not orphans:
        return 0

    sql = text(f"DELETE FROM {uploader.uploaded_table} WHERE Date = :date")
    for date_str in orphans:
        uploader.conn.execute(sql, {"date": date_str})
    uploader.conn.commit()
    logger.info(
        "%s 清除孤兒帳本 %d 筆：%s",
        uploader.asset_label, len(orphans), ", ".join(orphans),
    )
    return len(orphans)

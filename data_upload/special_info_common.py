"""SPECIAL_INFO 價格上傳共用邏輯（爬蟲狀態判讀 + 帳本語意 + 缺漏補抓）。

原油／黃金／比特幣／匯率／股市指數五個上傳器結構相同，共用「取得資料→
判讀狀態→寫入→記帳」與「近 N 天缺漏偵測補抓」邏輯。各上傳器須提供下列
屬性與方法：

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

狀態契約（爬蟲 v2.15.0 起，關鍵）：`*Uploaded` 帳本是「這天已處理完畢，
以後別再看了」的永久標記，`upload()` 只憑帳本決定是否跳過。因此**把抓取
失敗寫進帳本 = 永久遮蔽該日資料**，且會自我強化（重試被失敗自己寫下的帳本
滿足）。判讀一律走 `base.check_crawl_status()`，**不得**再用錯誤訊息字串
（爬蟲 v2.15.0 起訊息已不含「無法取得任何」，舊啟發式全數失效）：

    | status         | 意義                     | 帳本 | 重試 |
    |----------------|--------------------------|------|------|
    | `ok`           | 全數取得                 | 記實際交易日 | — |
    | `empty`        | 探測確認該期間無報價     | 可記請求日   | — |
    | `out_of_range` | 早於來源涵蓋起點         | 可記請求日   | 否 |
    | `partial`      | 部分商品失敗             | **不得記**   | 是 |
    | `error`／未知  | 抓取失敗，0 筆不代表沒有 | **不得記**   | 是 |

行情類 `partial` 一律整批丟棄重抓（`check_crawl_status(allow_partial=False)`）：
`*Price` 為 REPLACE INTO 但列鍵為 (Date, Product)，部分商品缺漏時若先存一半，
之後補抓仍需重跑整日，故不如整批重抓語意單純；且與新聞類（可累積補齊）
不同，行情缺商品即為不完整的一天。

帳本語意（關鍵）：`*Uploaded` 帳本只記錄「df 內每一筆的實際交易日」；
「請求日」僅在下列情況才額外標記已完成：

    (a) 取得的最新實際日期 == 請求日（真的拿到當日資料）；或
    (b) 該商品「非 24/7」、請求日**已定案**（早於今日），且爬蟲確認該日
        無報價（回空的 `empty`，或 fallback 到更早日期且
        `meta.target_date_available` 非真）。

「已定案」守衛（`_is_settled`）是 2026-08 孤兒帳本的直接成因防線：早上／盤中
去問「今天」，來源當然只給得出昨天的日 K，舊碼會就此把今天記成非交易日而
永久遮蔽（2026-08-17／08-18 四商品即為此類）。未定案的日期一律不記帳，留待
次日重來。

24/7 商品（Bitcoin）在實際日期 < 請求日時「不」標記請求日，留待次日 UTC
日 K 完成後回補，避免帳本謊報導致 check_uploaded 之後永久跳過該日。

缺漏偵測補抓：以「問爬蟲」為交易日／休市的唯一真相來源——候選缺漏日呼叫
爬蟲，回傳該日自身 K 棒（實際==該日）→ REPLACE INTO 補上並記帳；只回更早
日期（fallback）或探測確認無報價 → 該日為非交易日，記帳標記避免反覆檢查。
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
from pydantic import ValidationError
from sqlalchemy import text

from data_upload.base import (
    STATUS_EMPTY,
    STATUS_OK,
    CrawlError,
    NetworkError,
    OutOfRangeError,
    SourceError,
    check_crawl_status,
)

logger = logging.getLogger(__name__)

# 行情爬蟲回傳欄位（小寫）→ 資料庫欄位（大寫）。五個商品格式一致。
COLUMN_MAPPING = {
    "product": "Product",
    "date": "Date",
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "volume": "Volume",
}

# 價格表必要欄位，缺一即視為爬蟲回傳格式異常。
REQUIRED_COLUMNS = frozenset(
    {"Date", "Product", "Open", "High", "Low", "Close", "Volume"}
)

# fetch_and_store 的處理結果分類（供 backfill 統計與呼叫端判讀）。
OUTCOME_FILLED = "filled"          # 取得請求日自身資料
OUTCOME_NON_TRADING = "non_trading"  # 已定案且來源確認無報價
OUTCOME_PENDING = "pending"        # 尚未定案／24/7 當日未完成，留待次日
OUTCOME_OUT_OF_RANGE = "out_of_range"  # 早於來源涵蓋起點，重試無用


def reset_crawl_state(uploader):
    """清除上傳器上一次的爬蟲狀態記錄。

    Args:
        uploader: 上傳器實例。
    """
    uploader.last_crawl_status = None
    uploader.last_crawl_meta = {}


def record_crawl_state(uploader, status, meta):
    """記錄本次爬蟲回應的狀態與 meta，供帳本判讀使用。

    `crawl_data` 依既有介面只回傳 DataFrame，狀態資訊改掛在上傳器實例上
    傳遞，避免變更所有呼叫端與測試的簽章。

    Args:
        uploader: 上傳器實例。
        status (str | None): 爬蟲回傳的 `status`（舊版爬蟲缺席時為 None）。
        meta (dict | None): 爬蟲回傳的 `meta` 物件。
    """
    uploader.last_crawl_status = status
    uploader.last_crawl_meta = meta or {}


def crawl_state(uploader):
    """取得上傳器最近一次的爬蟲狀態記錄。

    Args:
        uploader: 上傳器實例。

    Returns:
        dict: 含 status（str | None）與 meta（dict）兩個鍵。
    """
    return {
        "status": getattr(uploader, "last_crawl_status", None),
        "meta": getattr(uploader, "last_crawl_meta", None) or {},
    }


def parse_price_response(uploader, result, date):
    """判讀行情爬蟲回應並轉為 DataFrame（五個商品共用）。

    先以 `check_crawl_status` 判讀狀態（失敗即拋例外，確保「抓取失敗」
    不會被當成「當日無資料」流入帳本），再做欄位正規化與必要欄位檢查。

    Args:
        uploader: 上傳器實例（提供 asset_label）。
        result (dict): 爬蟲回應的 JSON 物件。
        date (str): 請求日期字串（YYYY-MM-DD），供錯誤訊息使用。

    Returns:
        pd.DataFrame: 欄位已正規化的行情 DataFrame（無資料時為空）。

    Raises:
        SourceError: `partial`／`error`／未知狀態、回應非物件，或必要欄位
            含空值（可重試，不得記帳）。
        OutOfRangeError: `out_of_range`（不可重試，由呼叫端記帳）。
        CrawlError: 舊版格式錯誤回應，或回傳資料缺少必要欄位。
    """
    label = uploader.asset_label
    context = f"（{date}）"
    if not isinstance(result, dict):
        # 回應根本不是物件（如代理層回了字串／陣列）：這是抓取失敗，不是
        # 「當日無資料」。若放行會一路走到「回空 DataFrame → 記帳非交易日」
        # 而永久遮蔽該日，正是本模組要消滅的失敗誤記模式。
        raise SourceError(
            f"{label}{context} 爬蟲回應格式非預期（{type(result).__name__}），"
            "視為抓取失敗以免誤記為當日無資料。"
        )
    raw_status = result.get("status")

    # 行情類不接受 partial：缺商品即為不完整的一天，整批重抓語意較單純。
    status = check_crawl_status(result, label, context, allow_partial=False)
    record_crawl_state(uploader, raw_status, result.get("meta"))

    if raw_status is None and "error" in result:
        # 舊版爬蟲（無 status）僅以 error 表示失敗：一律視為失敗而非無資料。
        # 舊碼在此靠「無法取得任何」字串判非交易日，新契約下該字串已不存在，
        # 且字串判斷本身就是把失敗誤記成空的來源，故不再保留。
        raise CrawlError(
            f"{label}{context} 爬蟲回傳錯誤（舊版格式，無 status）："
            f"{result['error']}"
        )

    data = result.get("data")
    if not data:
        logger.info(
            "%s%s 爬蟲回報無資料（status=%s）。", label, context, raw_status
        )
        return pd.DataFrame()

    df = pd.DataFrame(data).rename(columns=COLUMN_MAPPING)
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise CrawlError(f"{label}{context} 爬蟲回傳資料缺少欄位：{missing}")
    _reject_null_rows(uploader, df, date)
    return df


def _reject_null_rows(uploader, df, date):
    """必要欄位含 null 時視為抓取失敗（可重試、不得記帳）。

    來源（yfinance）偶爾回傳「有 volume 但 OHLC 全為 null」的殘缺 K 棒，
    此時爬蟲仍標記 `status=ok`／`target_date_available=true`，狀態欄位無從
    察覺。若放行，check_schema 會拋 pydantic ValidationError（未被歸類的
    例外，會炸掉整批補抓）；若改為容忍寫入，等於把殘缺資料當成完整的一天
    記進帳本而永久遮蔽。故一律當成來源端暫時性失敗，整批丟棄重抓。

    Args:
        uploader: 上傳器實例（提供 asset_label）。
        df (pd.DataFrame): 欄位已正規化的行情 DataFrame。
        date (str): 請求日期字串（YYYY-MM-DD）。

    Raises:
        SourceError: 任一必要欄位含 null（可重試，一律不得寫入帳本）。
    """
    null_mask = df[sorted(REQUIRED_COLUMNS)].isna()
    if not null_mask.to_numpy().any():
        return

    bad_columns = sorted(null_mask.columns[null_mask.any()].tolist())
    bad_rows = df.loc[null_mask.any(axis=1)]
    products = sorted({str(p) for p in bad_rows.get("Product", [])})
    raise SourceError(
        f"{uploader.asset_label}（{date}）爬蟲回傳資料含空值欄位"
        f"{bad_columns}（商品：{', '.join(products) or '未知'}），"
        "視為來源端殘缺資料，整批丟棄重抓以免記入不完整的一天。"
    )


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


def _is_settled(date, now=None):
    """判斷請求日是否已定案（早於今日）。

    只有已定案的日期才可能被標記為「非交易日」；「今天」在盤前／盤中問來源
    本來就拿不到自己的日 K，若據此記帳即造成永久遮蔽的孤兒帳本。

    Args:
        date (str): 請求日期字串（YYYY-MM-DD）。
        now (datetime | None): 判斷基準時間，預設為現在（供測試注入）。

    Returns:
        bool: 已定案回傳 True。
    """
    base = (now or datetime.now()).date()
    return datetime.strptime(date, "%Y-%m-%d").date() < base


def fetch_and_store(uploader, date):
    """取得指定日期資料並依帳本語意寫入（供 upload 與缺漏補抓共用）。

    不檢查 check_uploaded，由呼叫端自行決定是否先檢查帳本。

    Args:
        uploader: 上傳器實例。
        date (str): 請求日期字串（YYYY-MM-DD）。

    Returns:
        dict: 包含 date、record_count、filled 與 outcome 的結果字典。
            filled 表示是否取得「請求日自身」的資料（實際==請求）；
            outcome 為 OUTCOME_* 之一，供補抓統計與呼叫端判讀。

    Raises:
        NetworkError: 網路連線失敗（供重試機制使用）。
        SourceError: 來源端抓取失敗、資料殘缺或狀態自相矛盾（可重試、
            未記帳）。
        CrawlError: 爬蟲回傳格式或型別異常（未記帳）。
    """
    reset_crawl_state(uploader)
    try:
        df = uploader.crawl_data(date)
    except OutOfRangeError as e:
        # 早於來源涵蓋起點：重試永遠不會有結果，記帳避免每日重複詢問。
        uploader._record_uploaded_date(date)
        logger.warning(
            "%s %s 超出來源可回溯範圍（重試無用），已記帳：%s",
            uploader.asset_label, date, e,
        )
        return {
            "date": date, "record_count": 0, "filled": False,
            "outcome": OUTCOME_OUT_OF_RANGE,
        }

    state = crawl_state(uploader)
    status = state["status"]
    settled = _is_settled(date)

    if df.empty:
        if status == STATUS_OK:
            # 契約上 ok 代表全數取得，0 筆自相矛盾；寧可重試也不可記帳。
            raise SourceError(
                f"{uploader.asset_label}（{date}）爬蟲回報 ok 卻 0 筆，"
                "狀態自相矛盾，視為抓取失敗以免誤記為當日無資料。"
            )
        return _handle_no_data(uploader, date, settled, status)

    try:
        df = uploader.check_schema(df)
    except ValidationError as e:
        # 欄位型別不符（如來源改格式）：歸類為格式異常，逐日隔離而非炸整批。
        raise CrawlError(
            f"{uploader.asset_label}（{date}）爬蟲回傳資料型別不符：{e}"
        ) from e
    record_count = len(df)
    uploader._replace_into(df)

    # 帳本只記 df 內每一筆的實際交易日（而非盲目記請求日）。
    actual_dates = sorted({str(d) for d in df["Date"].tolist()})
    record_uploaded_dates(uploader, actual_dates)

    filled = date in actual_dates
    outcome = OUTCOME_FILLED if filled else _mark_fallback(
        uploader, date, settled, state["meta"]
    )
    logger.info(
        "%s %s 已上傳，共 %d 筆（實際交易日：%s，%s）。",
        uploader.asset_label, date, record_count, ", ".join(actual_dates),
        "含請求日" if filled else f"未含請求日／{outcome}",
    )
    return {
        "date": date, "record_count": record_count, "filled": filled,
        "outcome": outcome,
    }


def _handle_no_data(uploader, date, settled, status):
    """處理「爬蟲回空」的記帳決策。

    **只有 `status == "empty"`（探測確認該期間確實無報價）才可能記帳**；
    `status` 缺席（舊版爬蟲／回應退化）時一律留白待重驗——「不知道」不等於
    「沒有」，把不確定寫進永久帳本即為本模組要消滅的失敗誤記模式。

    Args:
        uploader: 上傳器實例。
        date (str): 請求日期字串。
        settled (bool): 請求日是否已定案。
        status (str | None): 爬蟲回傳的狀態。

    Returns:
        dict: fetch_and_store 的結果字典。
    """
    result = {"date": date, "record_count": 0, "filled": False}
    if uploader.is_continuous_market:
        # 24/7 商品理應天天有資料，不記帳，留待次日 UTC 日 K 完成後回補。
        logger.info(
            "%s %s 為 24/7 商品且來源尚無資料，暫不記帳，留待次日回補。",
            uploader.asset_label, date,
        )
        return {**result, "outcome": OUTCOME_PENDING}
    if not settled:
        logger.info(
            "%s %s 尚未定案（非過去日期），不記帳，留待次日重驗。",
            uploader.asset_label, date,
        )
        return {**result, "outcome": OUTCOME_PENDING}
    if status != STATUS_EMPTY:
        # 只有 empty 是「探測確認無報價」，其餘（含 status 缺席）都只是
        # 「這次沒拿到」，不足以永久斷定該日無報價。
        logger.warning(
            "%s %s 回空但 status=%s（非 empty），不記帳以免誤標非交易日，"
            "留待重驗。",
            uploader.asset_label, date, status,
        )
        return {**result, "outcome": OUTCOME_PENDING}
    uploader._record_uploaded_date(date)
    logger.info(
        "%s %s 來源確認該期間無報價（非交易日，status=%s），已記帳。",
        uploader.asset_label, date, status,
    )
    return {**result, "outcome": OUTCOME_NON_TRADING}


def _mark_fallback(uploader, date, settled, meta):
    """處理「取得資料但未含請求日」（爬蟲 fallback 到更早交易日）的記帳決策。

    只有在 `meta.target_date_available` **明確為 false**（爬蟲確認請求日無
    報價）時才標記非交易日；欄位缺席或為真都留白待重驗。

    Args:
        uploader: 上傳器實例。
        date (str): 請求日期字串。
        settled (bool): 請求日是否已定案。
        meta (dict): 爬蟲回應的 meta 物件。

    Returns:
        str: OUTCOME_NON_TRADING 或 OUTCOME_PENDING。
    """
    if uploader.is_continuous_market:
        return OUTCOME_PENDING
    if not settled:
        logger.info(
            "%s %s 尚未定案，爬蟲 fallback 至更早交易日，不標記非交易日。",
            uploader.asset_label, date,
        )
        return OUTCOME_PENDING
    if "target_date_available" not in meta:
        # 沒有這個欄位就沒有「請求日確實無報價」的正面證據（舊版爬蟲／回應
        # 退化）。留白待重驗只是多問幾次，誤記卻是永久遮蔽，兩害相權取輕。
        logger.warning(
            "%s %s 爬蟲 fallback 但 meta 未提供 target_date_available，"
            "無從確認該日是否真的無報價，不記帳以免誤標非交易日。",
            uploader.asset_label, date,
        )
        return OUTCOME_PENDING
    if meta["target_date_available"]:
        # 爬蟲說請求日有報價、回傳列卻不含它：自相矛盾，寧可留白待重驗。
        logger.warning(
            "%s %s meta.target_date_available 為真但回傳未含請求日，"
            "狀態不一致，不記帳以免誤標非交易日。",
            uploader.asset_label, date,
        )
        return OUTCOME_PENDING
    uploader._record_uploaded_date(date)
    return OUTCOME_NON_TRADING


def upload_date_range(uploader, start_date, end_date, on_date=None):
    """依序上傳日期區間，來源端失敗逐日隔離、不中斷整批。

    `SourceError`（爬蟲活著、只是這天抓不到）與 `CrawlError`（該日資料格式／
    型別異常）僅該日失敗，收集後繼續處理後續日期；若不隔離，昇冪清單上的
    第一個「毒日期」會讓其後日期每天都沒機會被嘗試。只有 `NetworkError`
    （連不上爬蟲，後續日期必然同樣失敗）才往外拋、由呼叫端中止整批。

    攔截順序關鍵：`SourceError ⊂ NetworkError ⊂ CrawlError`，故必須
    「先 SourceError → 再 NetworkError（重拋）→ 最後 CrawlError」，
    否則父類別會把子類別整個吃掉。

    Args:
        uploader: 上傳器實例。
        start_date (str): 起始日期（YYYY-MM-DD，含）。
        end_date (str): 結束日期（YYYY-MM-DD，含）。
        on_date (callable | None): 每日開始前呼叫的回呼（傳入日期字串），
            供呼叫端更新任務進度。

    Returns:
        dict: 含 record_count（總筆數）與 failures（[{date, error}]）。

    Raises:
        NetworkError: 無法連線爬蟲（整批中止）。
    """
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    total_records = 0
    failures = []

    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        if on_date is not None:
            on_date(date_str)
        try:
            result = uploader.upload(date_str)
        except SourceError as e:
            logger.warning(
                "%s %s 來源端抓取失敗，跳過該日並排入重試：%s",
                uploader.asset_label, date_str, e,
            )
            failures.append({"date": date_str, "error": str(e)})
        except NetworkError:
            # 連不上爬蟲：後續日期必然同樣失敗，整批中止交由呼叫端重試。
            raise
        except CrawlError as e:
            logger.error(
                "%s %s 資料格式異常，跳過該日：%s",
                uploader.asset_label, date_str, e,
            )
            failures.append({"date": date_str, "error": str(e)})
        else:
            total_records += result["record_count"]
        current += timedelta(days=1)

    return {"record_count": total_records, "failures": failures}


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


def backfill_missing(uploader, days=30, today=None, deep=False,
                     reverify_days=0):
    """掃描近 N 天缺漏並補抓（冪等、可重跑）。

    對每個候選缺漏日呼叫 fetch_and_store：

        - 取得請求日自身資料 → filled，計入 filled。
        - 已定案且來源確認無報價／只回更早日期 → 非交易日（已記帳），
          計入 non_trading（`out_of_range` 因同樣「已記帳、不必再問」而
          併入此欄）。
        - 尚未定案（含 24/7 商品當日）→ 計入 still_pending（留待次日）。

    NetworkError 逐日捕捉、記入 network_errors 供呼叫端交由 retry_queue，
    不中斷整體掃描；`SourceError`（含來源殘缺資料）為其子類，同樣逐日隔離。
    `CrawlError`（格式／型別異常）另記入 crawl_errors：這類錯誤重試多半無用，
    但仍不得中斷掃描——否則清單上第一個「毒日期」會讓其後所有日期（乃至
    後續商品）全部沒機會處理。

    孤兒帳本清理（帳本有列但價格表 0 列）：

        - `deep=True`：清整個 days 窗（一次性修復／人工觸發）。舊 bug／管線
          停擺期間被誤標為已完成的「真實交易日」會被補回價格，真正的非交易日
          則重新記帳。
        - `reverify_days=N`（日常排程）：只清最近 N 天，讓排程路徑也具備自我
          修復能力——否則誤標一旦寫入，`find_missing_dates` 永遠不會再把該日
          列為候選，只能等人工 deep 重驗。窗口取小是為了避免每天重問幾十個
          已確認的非交易日。

    Args:
        uploader: 上傳器實例。
        days (int): 掃描天數。
        today (str | date | None): 掃描基準日，預設為當日。
        deep (bool): 是否先清除整個窗的孤兒帳本再重驗，預設 False。
        reverify_days (int): deep=False 時要清除孤兒帳本的天數，預設 0
            （不清）。超過 days 時取 days。

    Returns:
        dict: 補抓摘要。
    """
    orphans_cleared = 0
    if deep:
        orphans_cleared = _delete_ledger_orphans(
            uploader, days=days, today=today
        )
    elif reverify_days > 0:
        orphans_cleared = _delete_ledger_orphans(
            uploader, days=min(reverify_days, days), today=today
        )

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
        "crawl_errors": [],
    }
    for date_str in missing:
        try:
            result = fetch_and_store(uploader, date_str)
        except NetworkError as e:
            logger.warning(
                "%s 缺漏補抓 %s 抓取失敗：%s（交由重試佇列）。",
                uploader.asset_label, date_str, e,
            )
            summary["network_errors"].append(date_str)
            continue
        except CrawlError as e:
            logger.error(
                "%s 缺漏補抓 %s 格式異常：%s（略過該日，繼續掃描）。",
                uploader.asset_label, date_str, e,
            )
            summary["crawl_errors"].append(date_str)
            continue
        outcome = result.get("outcome")
        if result["filled"]:
            summary["filled"] += 1
            summary["filled_dates"].append(date_str)
            summary["records"] += result["record_count"]
        elif outcome == OUTCOME_PENDING:
            # 尚未定案（24/7 商品當日或未過完的日期），留待次日。
            summary["still_pending"] += 1
        else:
            summary["non_trading"] += 1
    logger.info(
        "%s 缺漏補抓完成：清孤兒 %d、掃描 %d、補回 %d、非交易日 %d、"
        "待次日 %d、抓取失敗 %d、格式異常 %d。",
        uploader.asset_label, summary["orphans_cleared"], summary["scanned"],
        summary["filled"], summary["non_trading"], summary["still_pending"],
        len(summary["network_errors"]), len(summary["crawl_errors"]),
    )
    return summary


def _delete_ledger_orphans(uploader, days=30, today=None):
    """刪除窗內「帳本有列但價格表無對應列」的孤兒帳本，回傳刪除筆數。

    刪除後這些日期會重新成為缺漏候選，交由爬蟲重驗：

        - 24/7 連續市場（Bitcoin）：每日皆有資料，帳本日期理應都對應到價格
          列，孤兒必為舊 bug 造成，刪除後可補回。
        - 非連續市場：孤兒可能是「舊 bug／停擺誤標的真實交易日」（應補回），
          也可能是「合法非交易日標記」（重驗後會再次記帳）。一律刪除交由爬蟲
          重驗，才能救回被誤標的真實交易日。

    只刪帳本列、不動價格列：帳本不含任何資料，重驗即可重建，故本操作可重複
    執行且不會造成資料遺失。

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

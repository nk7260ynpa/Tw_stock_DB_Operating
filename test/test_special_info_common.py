"""SPECIAL_INFO 價格上傳共用邏輯單元測試。

以記憶體 FakeConn／FakeUploader 驗證帳本語意、缺漏偵測與孤兒清理，
不連真實 DB 或爬蟲。
"""

import unittest
from datetime import date as date_cls
from datetime import timedelta

import pandas as pd
from pydantic import BaseModel, ValidationError

from data_upload import special_info_common
from data_upload.base import (
    CrawlError,
    NetworkError,
    OutOfRangeError,
    SourceError,
)


class FakeResult:
    """模擬 SQLAlchemy 查詢結果。"""

    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeConn:
    """記憶體版連線：以 SQL 字串判斷操作，維護 price/ledger 兩組日期集合。"""

    def __init__(self, price_dates, ledger_dates, price_table, uploaded_table):
        self.price_dates = set(price_dates)
        self.ledger_dates = set(ledger_dates)
        self.price_table = price_table
        self.uploaded_table = uploaded_table
        self.deleted = []
        self.commit_count = 0

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        if sql.startswith("SELECT") and self.price_table in sql:
            start, end = params["start"], params["end"]
            rows = [
                (d,) for d in sorted(self.price_dates) if start <= d <= end
            ]
            return FakeResult(rows)
        if sql.startswith("SELECT") and self.uploaded_table in sql:
            start, end = params["start"], params["end"]
            rows = [
                (d,) for d in sorted(self.ledger_dates) if start <= d <= end
            ]
            return FakeResult(rows)
        if sql.startswith("DELETE"):
            self.ledger_dates.discard(params["date"])
            self.deleted.append(params["date"])
            return FakeResult([])
        if "INSERT IGNORE INTO" in sql:
            self.ledger_dates.add(params["date"])
            return FakeResult([])
        return FakeResult([])

    def commit(self):
        self.commit_count += 1


class FakeUploader:
    """記憶體版上傳器：crawl_data 依 responses 映射回傳資料或空。"""

    def __init__(self, is_continuous, responses, price_dates=None,
                 ledger_dates=None, statuses=None, metas=None, errors=None):
        """初始化。

        Args:
            is_continuous (bool): 是否為 24/7 連續市場。
            responses (dict): {請求日: 實際日 | None}；None 表示回空 df。
            price_dates (set | None): 初始價格表日期。
            ledger_dates (set | None): 初始帳本日期。
            statuses (dict | None): {請求日: 爬蟲 status}；未列出者採健康
                爬蟲預設（回空→"empty"、有資料→"ok"）。要模擬退化契約
                （舊版無 status）請顯式傳入 None。
            metas (dict | None): {請求日: 爬蟲 meta 物件}；未列出者採健康
                爬蟲預設（含 target_date_available）。要模擬 meta 缺欄位
                請顯式傳入 {}。
            errors (dict | None): {請求日: 要拋出的例外實例}。
        """
        self.is_continuous_market = is_continuous
        self.statuses = statuses or {}
        self.metas = metas or {}
        self.errors = errors or {}
        self.price_table = "FakePrice"
        self.uploaded_table = "FakeUploaded"
        self.asset_label = "測試商品"
        self.responses = responses
        self.conn = FakeConn(
            price_dates or set(), ledger_dates or set(),
            self.price_table, self.uploaded_table,
        )
        self.network_error_dates = set()

    def crawl_data(self, date):
        """模擬爬蟲回應。

        未指定 statuses／metas 時採「健康爬蟲」的預設契約：回空即
        `status=empty`（探測確認無報價），fallback 即
        `meta.target_date_available=False`（確認請求日無報價）。要測退化
        契約（status 缺席、meta 缺欄位）請顯式傳入 statuses／metas。
        """
        if date in self.errors:
            raise self.errors[date]
        if date in self.network_error_dates:
            raise NetworkError(f"模擬網路失敗（{date}）")
        actual = self.responses.get(date, "__MISSING__")
        is_empty = actual == "__MISSING__" or actual is None
        default_status = "empty" if is_empty else "ok"
        default_meta = {} if is_empty else {
            "target_date_available": actual == date
        }
        special_info_common.record_crawl_state(
            self,
            self.statuses.get(date, default_status),
            self.metas.get(date, default_meta),
        )
        if is_empty:
            return pd.DataFrame()
        return pd.DataFrame([{
            "Date": actual, "Product": "X",
            "Open": 1, "High": 1, "Low": 1, "Close": 1, "Volume": 1,
        }])

    def check_schema(self, df):
        return df

    def _replace_into(self, df):
        if df.empty:
            return
        for d in df["Date"].tolist():
            self.conn.price_dates.add(str(d))
        self.conn.commit()

    def _record_uploaded_date(self, date):
        self.conn.ledger_dates.add(date)
        self.conn.commit()

    def upload(self, date):
        """模擬實際上傳器的 upload：先看帳本，再取得資料並記帳。"""
        if date in self.conn.ledger_dates:
            return {"date": date, "record_count": 0}
        result = special_info_common.fetch_and_store(self, date)
        return {"date": date, "record_count": result["record_count"]}


class TestFetchAndStoreLedgerSemantics(unittest.TestCase):
    """測試 fetch_and_store 的帳本記帳語意。"""

    def test_exact_date_records_actual(self):
        """實際==請求：記錄實際交易日，filled 為 True。"""
        up = FakeUploader(False, {"2026-07-01": "2026-07-01"})
        result = special_info_common.fetch_and_store(up, "2026-07-01")
        self.assertTrue(result["filled"])
        self.assertIn("2026-07-01", up.conn.ledger_dates)
        self.assertIn("2026-07-01", up.conn.price_dates)

    def test_continuous_fallback_not_record_request(self):
        """24/7 商品 fallback（實際<請求）：只記實際日、不記請求日。"""
        up = FakeUploader(True, {"2026-07-05": "2026-07-04"})
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertFalse(result["filled"])
        self.assertIn("2026-07-04", up.conn.ledger_dates)
        self.assertNotIn("2026-07-05", up.conn.ledger_dates)

    def test_non_continuous_fallback_records_request(self):
        """非 24/7 商品 fallback：實際日與請求日皆記帳（請求日=非交易日）。"""
        up = FakeUploader(False, {"2026-07-05": "2026-07-03"})
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertFalse(result["filled"])
        self.assertIn("2026-07-03", up.conn.ledger_dates)
        self.assertIn("2026-07-05", up.conn.ledger_dates)

    def test_continuous_empty_not_recorded(self):
        """24/7 商品回空：不記帳（留待次日回補）。"""
        up = FakeUploader(True, {"2026-07-05": None})
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertFalse(result["filled"])
        self.assertNotIn("2026-07-05", up.conn.ledger_dates)

    def test_non_continuous_empty_recorded(self):
        """非 24/7 商品回空：記帳請求日為非交易日。"""
        up = FakeUploader(False, {"2026-07-05": None})
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertFalse(result["filled"])
        self.assertIn("2026-07-05", up.conn.ledger_dates)


class TestFindMissingDates(unittest.TestCase):
    """測試 find_missing_dates 缺漏偵測。"""

    def test_continuous_ignores_ledger_finds_orphans(self):
        """連續市場：忽略帳本，孤兒帳本日期（帳本有、價格無）仍算缺漏。"""
        up = FakeUploader(
            True, {},
            price_dates={"2026-07-01", "2026-07-02", "2026-07-04"},
            ledger_dates={"2026-07-03"},  # 孤兒：帳本有但價格無
        )
        missing = special_info_common.find_missing_dates(
            up, days=5, today="2026-07-05"
        )
        # 窗 2026-07-01..07-05；缺 07-03（孤兒）與 07-05
        self.assertIn("2026-07-03", missing)
        self.assertIn("2026-07-05", missing)
        self.assertNotIn("2026-07-01", missing)

    def test_non_continuous_skips_ledger(self):
        """非連續市場：帳本已標記者（非交易日/已檢查）不算缺漏。"""
        up = FakeUploader(
            False, {},
            price_dates={"2026-07-01", "2026-07-02"},
            ledger_dates={"2026-07-03", "2026-07-04", "2026-07-05"},
        )
        missing = special_info_common.find_missing_dates(
            up, days=5, today="2026-07-05"
        )
        # 全部日期不是在 price 就是在 ledger → 無缺漏
        self.assertEqual(missing, [])


class TestBackfillMissing(unittest.TestCase):
    """測試 backfill_missing 掃描補抓與冪等性。"""

    def test_fill_and_non_trading_and_idempotent(self):
        """混合情境：補回交易日、標記非交易日，並驗證重跑冪等。"""
        # 窗 2026-07-01..07-05；price 已有 07-01；
        # 07-02 交易日（爬蟲回自身）、07-03 交易日、07-04/07-05 非交易日（fallback）
        up = FakeUploader(
            False,
            {
                "2026-07-02": "2026-07-02",
                "2026-07-03": "2026-07-03",
                "2026-07-04": "2026-07-03",  # fallback → 非交易日
                "2026-07-05": "2026-07-03",  # fallback → 非交易日
            },
            price_dates={"2026-07-01"},
            ledger_dates={"2026-07-01"},
        )
        summary = special_info_common.backfill_missing(
            up, days=5, today="2026-07-05"
        )
        self.assertEqual(summary["scanned"], 4)  # 07-02..07-05
        self.assertEqual(summary["filled"], 2)  # 07-02、07-03
        self.assertEqual(summary["non_trading"], 2)  # 07-04、07-05
        self.assertIn("2026-07-02", up.conn.price_dates)
        self.assertIn("2026-07-03", up.conn.price_dates)

        # 冪等：再跑一次應無缺漏可掃
        summary2 = special_info_common.backfill_missing(
            up, days=5, today="2026-07-05"
        )
        self.assertEqual(summary2["scanned"], 0)
        self.assertEqual(summary2["filled"], 0)

    def test_continuous_backfills_orphans(self):
        """連續市場：孤兒帳本日期經 backfill 後補回價格。"""
        up = FakeUploader(
            True,
            {
                "2026-07-03": "2026-07-03",
                "2026-07-05": "2026-07-05",
            },
            price_dates={"2026-07-01", "2026-07-02", "2026-07-04"},
            ledger_dates={"2026-07-03"},  # 孤兒
        )
        summary = special_info_common.backfill_missing(
            up, days=5, today="2026-07-05"
        )
        # 07-03（孤兒）與 07-05 皆補回
        self.assertEqual(summary["filled"], 2)
        self.assertIn("2026-07-03", up.conn.price_dates)
        self.assertIn("2026-07-05", up.conn.price_dates)

    def test_network_error_collected_not_raised(self):
        """掃描過程遇 NetworkError 逐日收集、不中斷。"""
        up = FakeUploader(
            False,
            {"2026-07-04": "2026-07-04", "2026-07-05": "2026-07-05"},
            price_dates={"2026-07-01", "2026-07-02", "2026-07-03"},
            ledger_dates={"2026-07-01", "2026-07-02", "2026-07-03"},
        )
        up.network_error_dates = {"2026-07-04"}
        summary = special_info_common.backfill_missing(
            up, days=5, today="2026-07-05"
        )
        self.assertIn("2026-07-04", summary["network_errors"])
        self.assertEqual(summary["filled"], 1)  # 07-05 仍補回


class TestDeleteLedgerOrphans(unittest.TestCase):
    """測試 _delete_ledger_orphans 孤兒帳本清理（供 deep 重驗使用）。"""

    def test_deletes_orphans_for_any_asset(self):
        """刪除帳本有列但價格無列的孤兒（不分連續與否）。"""
        up = FakeUploader(
            False, {},
            price_dates={"2026-07-01"},
            ledger_dates={"2026-07-01", "2026-07-02", "2026-07-03"},
        )
        count = special_info_common._delete_ledger_orphans(
            up, days=5, today="2026-07-05"
        )
        self.assertEqual(count, 2)
        self.assertNotIn("2026-07-02", up.conn.ledger_dates)
        self.assertNotIn("2026-07-03", up.conn.ledger_dates)
        self.assertIn("2026-07-01", up.conn.ledger_dates)


class TestDeepBackfill(unittest.TestCase):
    """測試 deep 深度重驗補抓。"""

    def test_deep_heals_wrongly_marked_trading_day(self):
        """非連續市場 deep：救回被誤標為已完成的真實交易日。

        07-03 被舊 bug 標在帳本卻無價格，且為真實交易日（爬蟲回自身）；
        07-04（週六）也在帳本卻無價格，為真實非交易日（爬蟲 fallback）。
        deep 清孤兒後重驗：07-03 補回價格；07-04 重新記帳為非交易日。
        """
        up = FakeUploader(
            False,
            {
                "2026-07-03": "2026-07-03",  # 真實交易日
                "2026-07-04": "2026-07-03",  # fallback → 非交易日
            },
            price_dates={"2026-07-01", "2026-07-02"},
            ledger_dates={
                "2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04",
            },
        )
        summary = special_info_common.backfill_missing(
            up, days=5, today="2026-07-05", deep=True
        )
        # 07-03（誤標交易日）被補回；07-04 重新記為非交易日
        self.assertIn("2026-07-03", up.conn.price_dates)
        self.assertIn("2026-07-03", summary["filled_dates"])
        self.assertGreaterEqual(summary["orphans_cleared"], 2)
        self.assertIn("2026-07-04", up.conn.ledger_dates)

    def test_non_deep_skips_ledger_orphans(self):
        """非 deep（日常模式）：帳本已標記的孤兒不重驗（避免反覆檢查）。"""
        up = FakeUploader(
            False,
            {"2026-07-03": "2026-07-03"},
            price_dates={"2026-07-01", "2026-07-02"},
            ledger_dates={
                "2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04",
                "2026-07-05",
            },
        )
        summary = special_info_common.backfill_missing(
            up, days=5, today="2026-07-05", deep=False
        )
        # 07-03 在帳本 → 非 deep 不重驗 → 不補回
        self.assertEqual(summary["scanned"], 0)
        self.assertNotIn("2026-07-03", up.conn.price_dates)


class TestSettledGuard(unittest.TestCase):
    """測試「未定案日期不得標記為非交易日」守衛。

    2026-08-17／08-18 四商品的孤兒帳本即出自此漏洞：盤前（或美股開盤前）
    去問「今天」，來源只給得出昨天的日 K，舊碼就把今天記成非交易日而永久
    遮蔽。
    """

    def setUp(self):
        self.today = date_cls.today().strftime("%Y-%m-%d")
        self.yesterday = (
            date_cls.today() - timedelta(days=1)
        ).strftime("%Y-%m-%d")

    def test_unsettled_empty_not_recorded(self):
        """請求日為今日且回空：不記帳，留待次日重驗。"""
        up = FakeUploader(
            False, {self.today: None}, statuses={self.today: "empty"}
        )
        result = special_info_common.fetch_and_store(up, self.today)
        self.assertNotIn(self.today, up.conn.ledger_dates)
        self.assertEqual(
            result["outcome"], special_info_common.OUTCOME_PENDING
        )

    def test_unsettled_fallback_not_recorded(self):
        """請求日為今日且爬蟲 fallback 至昨日：只記昨日，不得標記今日。"""
        up = FakeUploader(
            False, {self.today: self.yesterday},
            statuses={self.today: "ok"},
            metas={self.today: {"target_date_available": False}},
        )
        result = special_info_common.fetch_and_store(up, self.today)
        self.assertIn(self.yesterday, up.conn.ledger_dates)
        self.assertNotIn(self.today, up.conn.ledger_dates)
        self.assertEqual(
            result["outcome"], special_info_common.OUTCOME_PENDING
        )

    def test_settled_fallback_records(self):
        """對照組：已定案的過去日期 fallback 時仍標記為非交易日。"""
        up = FakeUploader(
            False, {"2026-07-05": "2026-07-03"},
            statuses={"2026-07-05": "ok"},
            metas={"2026-07-05": {"target_date_available": False}},
        )
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertIn("2026-07-05", up.conn.ledger_dates)
        self.assertEqual(
            result["outcome"], special_info_common.OUTCOME_NON_TRADING
        )


class TestStatusDrivenLedger(unittest.TestCase):
    """測試以爬蟲 status／meta 決定記帳的行為。"""

    def test_ok_with_zero_rows_raises(self):
        """status=ok 卻 0 筆屬自相矛盾：視為失敗，不得記帳。"""
        up = FakeUploader(
            False, {"2026-07-05": None}, statuses={"2026-07-05": "ok"}
        )
        with self.assertRaises(SourceError):
            special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertNotIn("2026-07-05", up.conn.ledger_dates)

    def test_empty_status_records_ledger(self):
        """status=empty（探測確認無報價）：記帳為非交易日。"""
        up = FakeUploader(
            False, {"2026-07-05": None}, statuses={"2026-07-05": "empty"}
        )
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertIn("2026-07-05", up.conn.ledger_dates)
        self.assertEqual(
            result["outcome"], special_info_common.OUTCOME_NON_TRADING
        )

    def test_out_of_range_records_ledger(self):
        """out_of_range：重試無用，記帳避免每日重複詢問。"""
        up = FakeUploader(
            False, {},
            errors={"1990-01-02": OutOfRangeError("超出範圍")},
        )
        result = special_info_common.fetch_and_store(up, "1990-01-02")
        self.assertIn("1990-01-02", up.conn.ledger_dates)
        self.assertEqual(
            result["outcome"], special_info_common.OUTCOME_OUT_OF_RANGE
        )

    def test_source_error_never_records_ledger(self):
        """來源端失敗一律往外拋且不記帳（否則失敗會被永久遮蔽）。"""
        up = FakeUploader(
            False, {},
            errors={"2026-07-05": SourceError("爬取失敗，0 筆不代表無資料")},
        )
        with self.assertRaises(SourceError):
            special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertNotIn("2026-07-05", up.conn.ledger_dates)

    def test_target_date_available_conflict_not_recorded(self):
        """meta 說請求日有報價、回傳卻不含它：矛盾，不記帳。"""
        up = FakeUploader(
            False, {"2026-07-05": "2026-07-03"},
            statuses={"2026-07-05": "ok"},
            metas={"2026-07-05": {"target_date_available": True}},
        )
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertIn("2026-07-03", up.conn.ledger_dates)
        self.assertNotIn("2026-07-05", up.conn.ledger_dates)
        self.assertEqual(
            result["outcome"], special_info_common.OUTCOME_PENDING
        )


class TestUploadDateRange(unittest.TestCase):
    """測試日期區間上傳的失敗隔離。"""

    def test_source_error_isolated_and_collected(self):
        """單日來源端失敗只跳過該日，其後日期照常處理。"""
        up = FakeUploader(
            False,
            {
                "2026-07-01": "2026-07-01",
                "2026-07-03": "2026-07-03",
            },
            errors={"2026-07-02": SourceError("這天抓不到")},
        )
        seen = []
        result = special_info_common.upload_date_range(
            up, "2026-07-01", "2026-07-03", on_date=seen.append
        )
        self.assertEqual(seen, ["2026-07-01", "2026-07-02", "2026-07-03"])
        self.assertEqual(result["record_count"], 2)
        self.assertEqual(len(result["failures"]), 1)
        self.assertEqual(result["failures"][0]["date"], "2026-07-02")
        self.assertIn("2026-07-03", up.conn.price_dates)

    def test_network_error_aborts_batch(self):
        """連不上爬蟲時整批中止，交由呼叫端排入重試。"""
        up = FakeUploader(False, {"2026-07-01": "2026-07-01"})
        up.network_error_dates.add("2026-07-02")
        with self.assertRaises(NetworkError):
            special_info_common.upload_date_range(
                up, "2026-07-01", "2026-07-03"
            )
        self.assertNotIn("2026-07-03", up.conn.price_dates)


class TestScheduledReverify(unittest.TestCase):
    """測試日常排程（deep=False）以 reverify_days 清除近期孤兒帳本。"""

    def test_reverify_clears_recent_orphans_only(self):
        """只清最近 N 天的孤兒，較舊的誤標留給人工 deep 重驗。"""
        up = FakeUploader(
            False,
            {"2026-07-28": "2026-07-28"},
            price_dates=set(),
            ledger_dates={"2026-07-10", "2026-07-28"},
        )
        summary = special_info_common.backfill_missing(
            up, days=30, today="2026-07-30", deep=False, reverify_days=7
        )
        # 07-28 在重驗窗內 → 清孤兒後重驗補回；07-10 在窗外 → 保留
        self.assertEqual(summary["orphans_cleared"], 1)
        self.assertIn("2026-07-28", up.conn.price_dates)
        self.assertIn("2026-07-10", up.conn.ledger_dates)

    def test_reverify_zero_keeps_legacy_behaviour(self):
        """reverify_days=0（預設）維持舊行為：不清孤兒。"""
        up = FakeUploader(
            False,
            {"2026-07-28": "2026-07-28"},
            ledger_dates={"2026-07-28"},
        )
        summary = special_info_common.backfill_missing(
            up, days=30, today="2026-07-30", deep=False
        )
        self.assertEqual(summary["orphans_cleared"], 0)
        self.assertNotIn("2026-07-28", up.conn.price_dates)


class _StrictRow(BaseModel):
    """供測試產生真實 pydantic ValidationError 的最小 schema。"""

    Close: int


class TestNullValueGuard(unittest.TestCase):
    """測試必要欄位含空值時一律視為抓取失敗、絕不記帳。

    真實案例：2026-08-17 yfinance 回傳道瓊／納斯達克「有 volume 但 OHLC
    全為 null」的殘缺 K 棒，爬蟲仍標記 status=ok 且 target_date_available
    為真，狀態欄位無從察覺，只能在資料層攔。
    """

    def _payload(self, date, open_value):
        return {
            "date": date,
            "status": "ok",
            "data": [{
                "date": date, "product": "X", "open": open_value,
                "high": 1.0, "low": 1.0, "close": 1.0, "volume": 100,
            }],
            "meta": {"target_date_available": True},
        }

    def test_null_ohlc_raises_source_error(self):
        """OHLC 含 null：拋 SourceError（可重試）而非放行寫入。"""
        up = FakeUploader(False, {})
        with self.assertRaises(SourceError):
            special_info_common.parse_price_response(
                up, self._payload("2026-08-17", None), "2026-08-17"
            )

    def test_zero_value_is_not_null(self):
        """數值 0（如匯率 volume=0）不得被誤判為空值。"""
        up = FakeUploader(False, {})
        payload = self._payload("2026-08-17", 0)
        payload["data"][0]["volume"] = 0
        df = special_info_common.parse_price_response(
            up, payload, "2026-08-17"
        )
        self.assertEqual(len(df), 1)

    def test_null_row_never_records_ledger(self):
        """殘缺資料日一律不得寫入帳本（否則永久遮蔽該日）。"""
        up = FakeUploader(
            False, {}, errors={"2026-08-17": SourceError("殘缺資料")},
        )
        with self.assertRaises(SourceError):
            special_info_common.fetch_and_store(up, "2026-08-17")
        self.assertNotIn("2026-08-17", up.conn.ledger_dates)


class TestBackfillFailureIsolation(unittest.TestCase):
    """測試補抓逐日隔離：單一毒日期不得中斷整批掃描。"""

    def test_crawl_error_isolated_and_scan_continues(self):
        """CrawlError 記入 crawl_errors 並繼續掃描其後日期。"""
        up = FakeUploader(
            False,
            {"2026-07-01": "2026-07-01", "2026-07-03": "2026-07-03"},
            errors={"2026-07-02": CrawlError("型別不符")},
            statuses={
                "2026-07-01": "ok", "2026-07-03": "ok",
            },
        )
        summary = special_info_common.backfill_missing(
            up, days=3, today="2026-07-03"
        )
        self.assertEqual(summary["crawl_errors"], ["2026-07-02"])
        self.assertEqual(summary["filled"], 2)
        self.assertNotIn("2026-07-02", up.conn.ledger_dates)

    def test_source_error_isolated_as_network_error(self):
        """SourceError（含殘缺資料）記入 network_errors 並繼續掃描。"""
        up = FakeUploader(
            False,
            {"2026-07-01": "2026-07-01", "2026-07-03": "2026-07-03"},
            errors={"2026-07-02": SourceError("殘缺資料")},
            statuses={"2026-07-01": "ok", "2026-07-03": "ok"},
        )
        summary = special_info_common.backfill_missing(
            up, days=3, today="2026-07-03"
        )
        self.assertEqual(summary["network_errors"], ["2026-07-02"])
        self.assertEqual(summary["filled"], 2)
        self.assertNotIn("2026-07-02", up.conn.ledger_dates)

    def test_schema_validation_error_becomes_crawl_error(self):
        """check_schema 的 pydantic ValidationError 轉為 CrawlError、不記帳。"""
        up = FakeUploader(
            False, {"2026-07-01": "2026-07-01"}, statuses={"2026-07-01": "ok"},
        )

        def _raise_validation_error(df):
            _StrictRow(Close=None)

        up.check_schema = _raise_validation_error
        with self.assertRaises(CrawlError):
            special_info_common.fetch_and_store(up, "2026-07-01")
        self.assertNotIn("2026-07-01", up.conn.ledger_dates)

    def test_validation_error_class_is_available(self):
        """確認測試用 schema 的確會拋 pydantic ValidationError。"""
        with self.assertRaises(ValidationError):
            _StrictRow(Close=None)


class TestDegradedContractNeverRecords(unittest.TestCase):
    """測試「契約退化」的回應一律不得寫帳本（不知道 != 沒有）。

    帳本是永久標記，寫下去該日就再也不會被列為補抓候選。因此只有正面證據
    （`status=empty`／`target_date_available=False`）才可記帳；狀態或 meta
    缺席時一律留白待重驗——多問幾次的成本遠低於永久遮蔽一天的行情。
    """

    def test_non_dict_response_raises_source_error(self):
        """回應不是物件（如代理層回字串）視為抓取失敗，不得當成無資料。"""
        up = FakeUploader(False, {})
        with self.assertRaises(SourceError):
            special_info_common.parse_price_response(
                up, "unexpected", "2026-07-05"
            )

    def test_empty_without_status_not_recorded(self):
        """回空但 status 缺席（舊版爬蟲）：不記帳，留待重驗。"""
        up = FakeUploader(False, {"2026-07-05": None},
                          statuses={"2026-07-05": None})
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertEqual(
            result["outcome"], special_info_common.OUTCOME_PENDING
        )
        self.assertNotIn("2026-07-05", up.conn.ledger_dates)

    def test_empty_with_error_status_not_recorded(self):
        """回空且 status 非 empty：不記帳（0 筆不代表當日沒有）。"""
        up = FakeUploader(False, {"2026-07-05": None},
                          statuses={"2026-07-05": "unknown_status"})
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertEqual(
            result["outcome"], special_info_common.OUTCOME_PENDING
        )
        self.assertNotIn("2026-07-05", up.conn.ledger_dates)

    def test_fallback_without_meta_field_not_recorded(self):
        """fallback 但 meta 沒有 target_date_available：不記帳請求日。"""
        up = FakeUploader(False, {"2026-07-05": "2026-07-03"},
                          metas={"2026-07-05": {}})
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertEqual(
            result["outcome"], special_info_common.OUTCOME_PENDING
        )
        # 實際交易日仍照記，只是不把請求日標成非交易日。
        self.assertIn("2026-07-03", up.conn.ledger_dates)
        self.assertNotIn("2026-07-05", up.conn.ledger_dates)

    def test_fallback_with_explicit_false_is_recorded(self):
        """meta 明確為 False（爬蟲確認該日無報價）才標記非交易日。"""
        up = FakeUploader(
            False, {"2026-07-05": "2026-07-03"},
            metas={"2026-07-05": {"target_date_available": False}},
        )
        result = special_info_common.fetch_and_store(up, "2026-07-05")
        self.assertEqual(
            result["outcome"], special_info_common.OUTCOME_NON_TRADING
        )
        self.assertIn("2026-07-05", up.conn.ledger_dates)


if __name__ == "__main__":
    unittest.main()

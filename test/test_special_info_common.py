"""SPECIAL_INFO 價格上傳共用邏輯單元測試。

以記憶體 FakeConn／FakeUploader 驗證帳本語意、缺漏偵測與孤兒清理，
不連真實 DB 或爬蟲。
"""

import unittest

import pandas as pd

from data_upload import special_info_common
from data_upload.base import NetworkError


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
                 ledger_dates=None):
        """初始化。

        Args:
            is_continuous (bool): 是否為 24/7 連續市場。
            responses (dict): {請求日: 實際日 | None}；None 表示回空 df。
            price_dates (set | None): 初始價格表日期。
            ledger_dates (set | None): 初始帳本日期。
        """
        self.is_continuous_market = is_continuous
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
        if date in self.network_error_dates:
            raise NetworkError(f"模擬網路失敗（{date}）")
        actual = self.responses.get(date, "__MISSING__")
        if actual == "__MISSING__" or actual is None:
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


if __name__ == "__main__":
    unittest.main()

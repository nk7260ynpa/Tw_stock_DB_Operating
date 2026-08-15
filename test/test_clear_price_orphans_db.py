"""`clear_price_orphans` 對真實資料庫的邊界行為測試。

以檔案型 SQLite 建立與 MySQL 同構的 `*UploadDate` 帳本表，直接對真實 SQL 引擎
執行清孤兒作業，驗證三條安全邊界確實成立（而非僅比對 SQL 字串）：

1. **不動 `Open=True`**：已成功上傳、已有價格的日期一律保留。
2. **週末不清**：週末為確定非交易日，保留標記不重試。
3. **視窗外不清**：早於重驗視窗、以及今日本身的日期一律保留。
"""

import datetime
import os
import tempfile
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text


class TestClearPriceOrphansAgainstDatabase(unittest.TestCase):
    """以真實 SQL 引擎驗證清孤兒作業的資料變動。"""

    # 基準日 2026-01-12（週一），REVERIFY_DAYS=7 → 視窗為 01-06 ～ 01-11。
    TODAY = datetime.date(2026, 1, 12)

    # (日期, Open, 是否應被清除, 說明)
    FIXTURES = [
        ("2026-01-02", 0, False, "視窗外（更早）平日孤兒，保留不重試"),
        ("2026-01-05", 0, False, "視窗外（早一天）平日孤兒，保留不重試"),
        ("2026-01-06", 0, True, "視窗內平日孤兒，應清除重驗"),
        ("2026-01-07", 1, False, "視窗內平日且已上傳，絕不可動"),
        ("2026-01-08", 0, True, "視窗內平日孤兒，應清除重驗"),
        ("2026-01-09", 1, False, "視窗內平日且已上傳，絕不可動"),
        ("2026-01-10", 0, False, "視窗內週六，確定非交易日，保留"),
        ("2026-01-11", 0, False, "視窗內週日，確定非交易日，保留"),
        ("2026-01-12", 0, False, "今日本身，排除於視窗外，保留"),
    ]

    def setUp(self):
        """建立暫存 SQLite 帳本表並塞入測試資料。"""
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "ledger.db")
        self.engine = create_engine(f"sqlite:///{self.db_path}")
        with self.engine.connect() as conn:
            conn.execute(
                text("CREATE TABLE UploadDate (Date DATE, `Open` BOOLEAN)")
            )
            conn.execute(
                text("INSERT INTO UploadDate VALUES (:date, :open)"),
                [
                    {"date": date_str, "open": open_val}
                    for date_str, open_val, _, _ in self.FIXTURES
                ],
            )
            conn.commit()
        self.addCleanup(self._tmpdir.cleanup)
        self.addCleanup(self.engine.dispose)

    def _run_clear(self, days=7):
        """以暫存 SQLite 連線執行 clear_price_orphans。

        Args:
            days (int): 重驗視窗天數。

        Returns:
            list[str]: 被清除的日期清單。
        """
        import DailyUpload

        with patch("DailyUpload.MySQLRouter") as mock_router_cls:
            mock_router_cls.return_value.mysql_conn = self.engine.connect()
            return DailyUpload.clear_price_orphans(
                "TWSE", days=days, today=self.TODAY
            )

    def _remaining_rows(self):
        """讀取清理後帳本剩餘內容。

        Returns:
            dict[str, int]: 日期字串對應的 Open 值。
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT Date, `Open` FROM UploadDate")
            ).fetchall()
        return {row[0]: row[1] for row in rows}

    def test_clears_only_in_window_weekday_orphans(self):
        """測試僅清除視窗內平日的 Open=False 帳本。"""
        cleared = self._run_clear()

        expected = [d for d, _, should_clear, _ in self.FIXTURES if should_clear]
        self.assertEqual(cleared, expected)

    def test_open_true_rows_never_deleted(self):
        """測試 Open=True（已有價格）的日期絕不被刪除。"""
        self._run_clear()
        remaining = self._remaining_rows()

        for date_str, open_val, _, reason in self.FIXTURES:
            if open_val:
                self.assertIn(date_str, remaining, f"{date_str} 被誤刪：{reason}")
                self.assertTrue(remaining[date_str])

    def test_weekend_and_out_of_window_rows_kept(self):
        """測試週末與視窗外（含今日）的孤兒帳本保留、不清不重試。"""
        self._run_clear()
        remaining = self._remaining_rows()

        for date_str, _, should_clear, reason in self.FIXTURES:
            if should_clear:
                self.assertNotIn(date_str, remaining, f"{date_str} 未清除")
            else:
                self.assertIn(date_str, remaining, f"{date_str} 被誤刪：{reason}")

    def test_row_count_only_decreases_by_cleared(self):
        """測試帳本總筆數僅減少被清除的筆數（無額外刪除）。"""
        before = len(self._remaining_rows())

        cleared = self._run_clear()

        after = len(self._remaining_rows())
        self.assertEqual(before - after, len(cleared))

    def test_idempotent_on_rerun(self):
        """測試重跑冪等：第二次已無孤兒可清，帳本不再變動。"""
        self._run_clear()
        after_first = self._remaining_rows()

        cleared_again = self._run_clear()

        self.assertEqual(cleared_again, [])
        self.assertEqual(self._remaining_rows(), after_first)

    def test_larger_window_reaches_older_orphans(self):
        """測試放大視窗（deep 修復）可涵蓋更早的平日孤兒。"""
        # days=30 → 視窗起點 2025-12-14，涵蓋 01-02、01-05 兩筆更早孤兒。
        cleared = self._run_clear(days=30)

        self.assertEqual(
            cleared,
            ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-08"],
        )
        remaining = self._remaining_rows()
        # 放大視窗後 Open=True 與週末仍不受影響。
        self.assertEqual(
            sorted(remaining),
            ["2026-01-07", "2026-01-09", "2026-01-10", "2026-01-11",
             "2026-01-12"],
        )


if __name__ == "__main__":
    unittest.main()

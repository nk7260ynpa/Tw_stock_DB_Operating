"""行情類孤兒帳本一次性 deep 修復入口單元測試。"""

import unittest
from unittest.mock import patch, MagicMock

from data_upload.base import NetworkError


class TestParseArgs(unittest.TestCase):
    """測試 parse_args 函式。"""

    def test_defaults(self):
        """測試預設參數。"""
        import backfill_price

        args = backfill_price.parse_args([])

        self.assertEqual(args.days, 30)
        self.assertEqual(args.host, backfill_price.HOST)
        self.assertEqual(args.crawlerhost, backfill_price.CRAWLERHOST)

    def test_custom_days(self):
        """測試自訂 --days。"""
        import backfill_price

        args = backfill_price.parse_args(["--days", "60"])

        self.assertEqual(args.days, 60)


@patch("backfill_price.time.sleep")
class TestRunBackfill(unittest.TestCase):
    """測試 run_backfill 函式。

    類層級 patch `time.sleep`，避免測試實際等待節流秒數；各測試方法的最後一個
    參數即為該 mock。
    """

    @patch("backfill_price._classify_dates")
    @patch("backfill_price.upload.day_upload")
    @patch("backfill_price.clear_price_orphans")
    def test_clears_and_requeries(
        self, mock_clear, mock_day_upload, mock_classify, mock_sleep
    ):
        """測試清除孤兒帳本後逐日重抓並分類。"""
        import backfill_price

        # 只有 TWSE 清出兩筆孤兒，其餘來源無。
        mock_clear.side_effect = [
            ["2026-01-05", "2026-01-06"],  # TWSE
            [], [], [], [],
        ]
        mock_classify.return_value = (1, 1, [])  # 補回 1、非交易日 1

        summaries = backfill_price.run_backfill(
            days=30, host="h", user="u", password="p", crawlerhost="c",
        )

        # TWSE 兩個孤兒日期都應被重抓。
        requeried = [c.args[0] for c in mock_day_upload.call_args_list]
        self.assertEqual(requeried, ["2026-01-05", "2026-01-06"])

        twse_summary = summaries[0]
        self.assertEqual(twse_summary["db_name"], "TWSE")
        self.assertEqual(twse_summary["cleared"], 2)
        self.assertEqual(twse_summary["filled"], 1)
        self.assertEqual(twse_summary["non_trading"], 1)
        self.assertEqual(twse_summary["network_errors"], [])
        # 涵蓋全部五個來源。
        self.assertEqual(len(summaries), len(backfill_price.DB_NAMES))

    @patch("backfill_price._classify_dates")
    @patch("backfill_price.upload.day_upload")
    @patch("backfill_price.clear_price_orphans")
    def test_network_error_recorded(
        self, mock_clear, mock_day_upload, mock_classify, mock_sleep
    ):
        """測試重抓遇 NetworkError 時記入 network_errors、不列入分類。"""
        import backfill_price

        mock_clear.side_effect = [["2026-01-05"], [], [], [], []]
        mock_day_upload.side_effect = NetworkError("net down")
        mock_classify.return_value = (0, 0, [])

        summaries = backfill_price.run_backfill(
            days=30, host="h", user="u", password="p", crawlerhost="c",
        )

        twse_summary = summaries[0]
        self.assertEqual(twse_summary["network_errors"], ["2026-01-05"])
        # 網路失敗的日期不應納入分類（requeried 為空）。
        mock_classify.assert_any_call("TWSE", [], "h", "u", "p")

    @patch("backfill_price._classify_dates")
    @patch("backfill_price.upload.day_upload")
    @patch("backfill_price.clear_price_orphans")
    def test_forwards_connection_args_and_window(
        self, mock_clear, mock_day_upload, mock_classify, mock_sleep
    ):
        """測試命令列指定的連線參數與視窗天數會傳給清孤兒作業。

        避免清孤兒連到預設主機、重驗卻連到自訂主機的不一致。
        """
        import backfill_price

        mock_clear.return_value = []
        mock_classify.return_value = (0, 0, [])

        backfill_price.run_backfill(
            days=45, host="h", user="u", password="p", crawlerhost="c",
        )

        for call_obj in mock_clear.call_args_list:
            self.assertEqual(call_obj.kwargs["days"], 45)
            self.assertEqual(call_obj.kwargs["host"], "h")
            self.assertEqual(call_obj.kwargs["user"], "u")
            self.assertEqual(call_obj.kwargs["password"], "p")

    @patch("backfill_price._classify_dates")
    @patch("backfill_price.upload.day_upload")
    @patch("backfill_price.clear_price_orphans")
    def test_day_upload_receives_source_specific_opt(
        self, mock_clear, mock_day_upload, mock_classify, mock_sleep
    ):
        """測試重抓時帶入該來源的 dbname 與連線參數。"""
        import backfill_price

        mock_clear.side_effect = [["2026-01-05"], [], [], [], []]
        mock_classify.return_value = (1, 0, [])

        backfill_price.run_backfill(
            days=30, host="h", user="u", password="p", crawlerhost="c",
        )

        opt = mock_day_upload.call_args_list[0].args[1]
        self.assertEqual(opt.dbname, "TWSE")
        self.assertEqual(opt.host, "h")
        self.assertEqual(opt.crawlerhost, "c")

    @patch("backfill_price._classify_dates")
    @patch("backfill_price.upload.day_upload")
    @patch("backfill_price.clear_price_orphans")
    def test_throttles_before_each_requery(
        self, mock_clear, mock_day_upload, mock_classify, mock_sleep
    ):
        """測試每次重抓前皆隨機節流，避免背靠背打爆上游爬蟲。

        deep 修復可能一次清出數十個日期，需與 daily_craw 一樣以 3~15 秒隨機
        間隔送出請求。
        """
        import backfill_price

        mock_clear.side_effect = [
            ["2026-01-05", "2026-01-06", "2026-01-07"], [], [], [], [],
        ]
        mock_classify.return_value = (3, 0, [])

        backfill_price.run_backfill(
            days=30, host="h", user="u", password="p", crawlerhost="c",
        )

        self.assertEqual(mock_sleep.call_count, mock_day_upload.call_count)
        for call_obj in mock_sleep.call_args_list:
            self.assertGreaterEqual(call_obj.args[0], 3)
            self.assertLessEqual(call_obj.args[0], 15)

    @patch("backfill_price._classify_dates")
    @patch("backfill_price.upload.day_upload")
    @patch("backfill_price.clear_price_orphans")
    def test_unrestored_recorded_in_summary(
        self, mock_clear, mock_day_upload, mock_classify, mock_sleep
    ):
        """測試帳本未回填的日期會列入摘要（不可靜默視為成功）。"""
        import backfill_price

        mock_clear.side_effect = [["2026-01-05"], [], [], [], []]
        mock_classify.return_value = (0, 0, ["2026-01-05"])

        summaries = backfill_price.run_backfill(
            days=30, host="h", user="u", password="p", crawlerhost="c",
        )

        self.assertEqual(summaries[0]["unrestored"], ["2026-01-05"])


class TestClassifyDates(unittest.TestCase):
    """測試 _classify_dates 函式。"""

    @patch("backfill_price.MySQLRouter")
    def test_classifies_filled_and_non_trading(self, mock_router_cls):
        """測試依帳本 Open 值分類為已補回／仍非交易日。"""
        import backfill_price

        mock_conn = MagicMock()
        # 第一天 Open=1（補回），第二天 Open=0（非交易日）
        mock_conn.execute.return_value.scalar.side_effect = [1, 0]
        mock_router_cls.return_value.mysql_conn = mock_conn

        filled, non_trading, unrestored = backfill_price._classify_dates(
            "TWSE", ["2026-01-05", "2026-01-06"], "h", "u", "p",
        )

        self.assertEqual(filled, 1)
        self.assertEqual(non_trading, 1)
        self.assertEqual(unrestored, [])
        mock_conn.close.assert_called_once()

    @patch("backfill_price.MySQLRouter")
    def test_missing_ledger_row_counts_as_unrestored(self, mock_router_cls):
        """測試帳本查無該日（重抓失敗未回填）歸類為未回填而非非交易日。

        清孤兒已刪除原標記，若 upload 因非網路錯誤提前返回而未寫回帳本，
        該日既非「已補回」也非「仍非交易日」，必須明確回報。
        """
        import backfill_price

        mock_conn = MagicMock()
        # 第一天 Open=1（補回），第二天查無資料列（未回填）
        mock_conn.execute.return_value.scalar.side_effect = [1, None]
        mock_router_cls.return_value.mysql_conn = mock_conn

        filled, non_trading, unrestored = backfill_price._classify_dates(
            "TWSE", ["2026-01-05", "2026-01-06"], "h", "u", "p",
        )

        self.assertEqual(filled, 1)
        self.assertEqual(non_trading, 0)
        self.assertEqual(unrestored, ["2026-01-06"])

    @patch("backfill_price.MySQLRouter")
    def test_empty_dates_no_connection(self, mock_router_cls):
        """測試無日期時直接回傳 (0, 0, []) 且不建立連線。"""
        import backfill_price

        result = backfill_price._classify_dates("TWSE", [], "h", "u", "p")

        self.assertEqual(result, (0, 0, []))
        mock_router_cls.assert_not_called()


class TestMain(unittest.TestCase):
    """測試 main 函式。"""

    @patch("backfill_price.run_backfill")
    def test_returns_0_on_success(self, mock_run):
        """測試無網路失敗時回傳 0。"""
        import backfill_price

        mock_run.return_value = [
            {"db_name": "TWSE", "cleared": 1, "filled": 1,
             "non_trading": 0, "network_errors": [], "unrestored": []},
        ]

        self.assertEqual(backfill_price.main(["--days", "10"]), 0)

    @patch("backfill_price.run_backfill")
    def test_returns_1_on_network_error(self, mock_run):
        """測試有網路失敗時回傳 1。"""
        import backfill_price

        mock_run.return_value = [
            {"db_name": "TWSE", "cleared": 1, "filled": 0,
             "non_trading": 0, "network_errors": ["2026-01-05"],
             "unrestored": []},
        ]

        self.assertEqual(backfill_price.main([]), 1)

    @patch("backfill_price.run_backfill")
    def test_returns_1_on_unrestored(self, mock_run):
        """測試有帳本未回填時回傳 1（提醒需人工確認或重跑）。"""
        import backfill_price

        mock_run.return_value = [
            {"db_name": "TWSE", "cleared": 1, "filled": 0,
             "non_trading": 0, "network_errors": [],
             "unrestored": ["2026-01-05"]},
        ]

        self.assertEqual(backfill_price.main([]), 1)


if __name__ == "__main__":
    unittest.main()

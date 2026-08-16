"""「毒日期／毒任務」隔離的單元測試模組。

`Tw_stock_crawer` v2.13.0 起，來源端失敗會回 `status="error"` 而非缺 `data`
鍵，本專案據此拋出可重試例外。若這類失敗與「連不上爬蟲」共用同一種例外，
批次補抓會在**第一個**失敗日期／任務上中斷——而 `missing_dates` 為昇冪排序，
最舊的「毒日期」會每天在同一處中斷，其後日期永遠不會被嘗試，直到滑出
30 天視窗即永久遺失。

本模組守住的不變量：**來源端失敗只跳過該筆，不得中斷整批**；而傳輸層失敗
（爬蟲不可達）維持既有的「整批排入重試並中止本輪」行為。
"""

import unittest
from unittest.mock import MagicMock, patch

from data_upload.base import CrawlError, NetworkError, SourceError


class TestSourceErrorHierarchy(unittest.TestCase):
    """測試例外繼承關係（重試判準依賴於此）。"""

    def test_source_error_is_retryable(self):
        """測試 SourceError 屬於 NetworkError，故仍會進 retry queue。"""
        from retry_queue import is_network_error

        self.assertTrue(issubclass(SourceError, NetworkError))
        self.assertTrue(is_network_error(SourceError("來源失敗")))

    def test_source_error_is_distinguishable(self):
        """測試傳輸層 NetworkError 不會被誤判成 SourceError。

        兩者的批次策略相反（中止 vs 續跑），混淆將導致爬蟲全掛時
        仍逐日空轉，或單日失敗即癱瘓整批補抓。
        """
        self.assertNotIsInstance(NetworkError("連線逾時"), SourceError)
        self.assertTrue(issubclass(SourceError, CrawlError))


class TestDailyCrawIsolatesSourceError(unittest.TestCase):
    """測試 daily_craw 對兩種失敗的批次策略。"""

    @patch("DailyUpload._add_to_retry_queue")
    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_source_error_does_not_block_later_dates(
        self, mock_get_missing, mock_sleep, mock_day_upload,
        mock_clear, mock_add_retry,
    ):
        """測試單日來源端失敗不阻斷其後日期的補抓。

        這是新契約下最危險的回歸：最舊的缺漏日期若持續抓不到，
        會使其後所有缺漏永遠不被嘗試。
        """
        import DailyUpload

        mock_clear.return_value = []
        mock_get_missing.side_effect = [
            ["2026-08-10", "2026-08-11", "2026-08-12"],  # TWSE
            [], [], [], [],  # TPEX / TAIFEX / FAOI / MGTS
        ]
        mock_day_upload.side_effect = [
            SourceError("TWSE（2026-08-10）爬取失敗，0 筆不代表無資料"),
            None,
            None,
        ]

        DailyUpload.daily_craw()

        dates = [call.args[0] for call in mock_day_upload.call_args_list]
        self.assertEqual(dates, ["2026-08-10", "2026-08-11", "2026-08-12"])

    @patch("DailyUpload._add_to_retry_queue")
    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_source_error_queues_only_that_date(
        self, mock_get_missing, mock_sleep, mock_day_upload,
        mock_clear, mock_add_retry,
    ):
        """測試來源端失敗只把該日排入重試（而非整批）。"""
        import DailyUpload

        mock_clear.return_value = []
        mock_get_missing.side_effect = [
            ["2026-08-10", "2026-08-11", "2026-08-12"],
            [], [], [], [],
        ]
        mock_day_upload.side_effect = [SourceError("來源失敗"), None, None]

        DailyUpload.daily_craw()

        mock_add_retry.assert_called_once()
        params = mock_add_retry.call_args.args[1]
        self.assertEqual(params["dates"], ["2026-08-10"])

    @patch("DailyUpload._add_to_retry_queue")
    @patch("DailyUpload.clear_price_orphans")
    @patch("DailyUpload.upload.day_upload")
    @patch("DailyUpload.time.sleep")
    @patch("DailyUpload.get_missing_dates")
    def test_network_error_still_breaks_and_queues_all(
        self, mock_get_missing, mock_sleep, mock_day_upload,
        mock_clear, mock_add_retry,
    ):
        """測試爬蟲不可達時仍整批排入重試並中止（既有行為不得退化）。"""
        import DailyUpload

        mock_clear.return_value = []
        mock_get_missing.side_effect = [
            ["2026-08-10", "2026-08-11", "2026-08-12"],
            [], [], [], [],
        ]
        mock_day_upload.side_effect = NetworkError("Connection refused")

        DailyUpload.daily_craw()

        dates = [call.args[0] for call in mock_day_upload.call_args_list]
        self.assertEqual(dates, ["2026-08-10"])
        params = mock_add_retry.call_args.args[1]
        self.assertEqual(
            params["dates"], ["2026-08-10", "2026-08-11", "2026-08-12"]
        )


class TestProcessRetryQueueIsolatesSourceError(unittest.TestCase):
    """測試 process_retry_queue 對兩種失敗的批次策略。"""

    def _make_task(self, task_id):
        """建立可執行的假重試任務。

        Args:
            task_id (str): 任務 ID。

        Returns:
            MagicMock: 具備 process_retry_queue 所需屬性的任務物件。
        """
        task = MagicMock()
        task.task_id = task_id
        task.task_type = "daily_upload"
        task.retry_count = 0
        task.max_retries = 5
        return task

    def _run(self, side_effect):
        """以指定的執行結果跑一輪重試佇列。

        Args:
            side_effect (list): `_execute_retry_task` 的逐次結果。

        Returns:
            tuple: (實際執行的 task_id 清單, update_status 的呼叫清單)。
        """
        import web_server

        tasks = [self._make_task("t1"), self._make_task("t2")]
        mock_queue = MagicMock()
        mock_queue.get_pending.return_value = tasks

        executed = []

        def fake_execute(task):
            executed.append(task.task_id)
            result = side_effect.pop(0)
            if isinstance(result, Exception):
                raise result

        original = web_server.retry_queue
        try:
            web_server.retry_queue = mock_queue
            with patch.object(
                web_server, "check_network_available", return_value=True
            ), patch.object(
                web_server, "_execute_retry_task", side_effect=fake_execute
            ):
                web_server.process_retry_queue()
        finally:
            web_server.retry_queue = original

        return executed, mock_queue.update_status.call_args_list

    def test_source_error_does_not_block_later_tasks(self):
        """測試單一任務來源端失敗不阻斷佇列其餘任務。"""
        executed, calls = self._run([SourceError("來源失敗"), None])

        self.assertEqual(executed, ["t1", "t2"])
        self.assertIn(("t1", "pending"), [c.args[:2] for c in calls])
        self.assertIn(("t2", "success"), [c.args[:2] for c in calls])

    def test_network_error_still_breaks_the_round(self):
        """測試爬蟲不可達時仍中斷本輪（既有行為不得退化）。"""
        executed, _ = self._run([NetworkError("Connection refused"), None])

        self.assertEqual(executed, ["t1"])


if __name__ == "__main__":
    unittest.main()

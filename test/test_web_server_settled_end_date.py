"""排程行情抓取「只抓到昨日」的守門測試。

v3 把爬蟲抓取窗自早上 07:3x 搬到晚上 21:0x 後，13:0x UTC／09:0x ET 時
yfinance 已存在「當日」那一根進行中的日 K；若排程照舊以「今日」為區間上界，
半根 K 會被 REPLACE INTO 寫入價格表並記帳，之後帳本與價格表雙重跳過使該日
**永遠不會被重驗**。本檔把「上界必須是昨日」釘成可執行的守門。
"""

import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch


class TestSettledEndDate(unittest.TestCase):
    """測試 settled_end_date 的取值。"""

    def test_returns_previous_day(self):
        """回傳基準時間的前一日。"""
        import web_server

        now = datetime(2026, 8, 18, 21, 6, 0)
        self.assertEqual(web_server.settled_end_date(now), "2026-08-17")

    def test_crosses_month_boundary(self):
        """跨月時日期需正確退回上個月最後一天。"""
        import web_server

        now = datetime(2026, 9, 1, 21, 6, 0)
        self.assertEqual(web_server.settled_end_date(now), "2026-08-31")

    def test_default_uses_now(self):
        """未指定基準時間時以當下計算。"""
        import web_server

        expected = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.assertEqual(web_server.settled_end_date(), expected)


class TestPriceSchedulesUseSettledEndDate(unittest.TestCase):
    """五個行情排程的區間上界都必須是昨日，不得是今日。"""

    SCHEDULES = (
        ("oil_price", "run_oil_price_scheduled", "run_oil_price_upload_job"),
        ("gold_price", "run_gold_price_scheduled",
         "run_gold_price_upload_job"),
        ("bitcoin_price", "run_bitcoin_price_scheduled",
         "run_bitcoin_price_upload_job"),
        ("currency_price", "run_currency_price_scheduled",
         "run_currency_price_upload_job"),
        ("indices_price", "run_indices_price_scheduled",
         "run_indices_price_upload_job"),
    )

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server
        web_server.upload_jobs.clear()

    def test_end_date_is_yesterday(self):
        """排入佇列的 end_date 與任務紀錄的日期皆為昨日。"""
        import web_server

        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (
            datetime.now() - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        for job_type, sched_name, job_name in self.SCHEDULES:
            with self.subTest(job_type=job_type):
                web_server.upload_jobs.clear()
                with patch.object(web_server, "job_queue") as mock_queue:
                    getattr(web_server, sched_name)()

                mock_queue.enqueue.assert_called_once()
                job_id, func, params = mock_queue.enqueue.call_args.args
                self.assertEqual(func, getattr(web_server, job_name))
                # params = (job_id, start_date, end_date)
                self.assertEqual(params[2], yesterday)
                self.assertNotEqual(params[2], today)
                job = web_server.upload_jobs[job_id]
                self.assertEqual(job["end_date"], yesterday)
                self.assertEqual(job["date"], yesterday)
                self.assertLess(job["start_date"], job["end_date"])


class TestBackfillScheduleUsesSettledEndDate(unittest.TestCase):
    """SPECIAL_INFO 缺漏自我修復排程的掃描基準日必須是昨日。"""

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server
        web_server.upload_jobs.clear()

    def test_scheduled_passes_yesterday_as_today(self):
        """排程須把昨日當成 backfill_missing 的掃描基準日傳下去。"""
        import web_server

        yesterday = (
            datetime.now() - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        with patch.object(web_server, "job_queue") as mock_queue:
            web_server.run_special_info_backfill_scheduled()

        job_id, func, params = mock_queue.enqueue.call_args.args
        self.assertEqual(func, web_server.run_special_info_backfill_job)
        # params = (job_id, days, deep, today)
        self.assertEqual(params[2], False)
        self.assertEqual(params[3], yesterday)
        self.assertEqual(
            web_server.upload_jobs[job_id]["end_date"], yesterday
        )

    def test_job_forwards_today_to_uploader(self):
        """作業層須把掃描基準日轉交給各上傳器的 backfill_missing。"""
        import web_server

        fake_summary = {
            "asset": "測試商品", "scanned": 0, "filled": 0,
            "filled_dates": [], "non_trading": 0, "still_pending": 0,
            "records": 0, "orphans_cleared": 0, "network_errors": [],
        }
        uploader = MagicMock()
        uploader.backfill_missing.return_value = fake_summary

        web_server.upload_jobs["j1"] = {
            "job_id": "j1", "status": "queued", "record_count": 0,
            "summary": [], "errors": [],
        }
        with patch.object(web_server, "db_conn"), \
                patch.object(
                    web_server, "SPECIAL_INFO_ASSETS",
                    [("oil_price", lambda conn, host: uploader)]):
            web_server.run_special_info_backfill_job(
                "j1", days=30, deep=False, today="2026-08-17"
            )

        uploader.backfill_missing.assert_called_once_with(
            days=30, today="2026-08-17", deep=False
        )


if __name__ == "__main__":
    unittest.main()

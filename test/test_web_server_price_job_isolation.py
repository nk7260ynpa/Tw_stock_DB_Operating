"""行情上傳任務的逐日失敗隔離與重試排入單元測試。

驗證單日來源端失敗不會中斷整個日期區間，且每個失敗日期都各自排入重試佇列
（舊版只會在第一個失敗日中止、且只排一筆重試，其後日期整段沒機會被嘗試）。
"""

import unittest
from unittest.mock import MagicMock, patch

import web_server
from data_upload.base import NetworkError, SourceError


class FakeUploader:
    """依日期決定成功／失敗的假上傳器。"""

    def __init__(self, failures=None, network_failures=None):
        """初始化。

        Args:
            failures (set | None): 要拋 SourceError 的日期。
            network_failures (set | None): 要拋 NetworkError 的日期。
        """
        self.asset_label = "原油價格"
        self.failures = failures or set()
        self.network_failures = network_failures or set()
        self.uploaded = []

    def upload(self, date):
        if date in self.network_failures:
            raise NetworkError(f"連不上爬蟲（{date}）")
        if date in self.failures:
            raise SourceError(f"來源端抓取失敗（{date}）")
        self.uploaded.append(date)
        return {"date": date, "record_count": 2}


class TestPriceJobIsolation(unittest.TestCase):
    """測試 run_oil_price_upload_job 的失敗隔離行為。"""

    def setUp(self):
        self.job_id = "job-iso"
        web_server.upload_jobs[self.job_id] = {
            "job_id": self.job_id,
            "type": "oil_price",
            "status": "queued",
            "date": "2026-07-01",
            "record_count": 0,
            "errors": [],
        }
        self.retry_queue = MagicMock()

    def tearDown(self):
        web_server.upload_jobs.pop(self.job_id, None)

    def _run(self, uploader):
        with patch.object(web_server, "db_conn"), \
                patch.object(web_server, "OilPriceUploader",
                             lambda conn, host: uploader), \
                patch.object(web_server, "retry_queue", self.retry_queue):
            web_server.run_oil_price_upload_job(
                self.job_id, "2026-07-01", "2026-07-03"
            )
        return web_server.upload_jobs[self.job_id]

    def test_source_error_does_not_stop_later_dates(self):
        """單日來源端失敗後仍繼續處理其後日期。"""
        uploader = FakeUploader(failures={"2026-07-02"})
        job = self._run(uploader)

        self.assertEqual(uploader.uploaded, ["2026-07-01", "2026-07-03"])
        self.assertEqual(job["status"], "completed_with_errors")
        self.assertEqual(job["record_count"], 4)
        self.assertEqual(len(job["errors"]), 1)
        self.assertIn("2026-07-02", job["errors"][0])

    def test_each_failed_date_enqueued_for_retry(self):
        """每個失敗日期各自排入重試佇列，不是只排最後一筆。"""
        uploader = FakeUploader(failures={"2026-07-01", "2026-07-03"})
        self._run(uploader)

        retried = sorted(
            call.args[1]["date"] for call in self.retry_queue.add.call_args_list
        )
        self.assertEqual(retried, ["2026-07-01", "2026-07-03"])
        for call in self.retry_queue.add.call_args_list:
            self.assertEqual(call.args[0], "oil_price")
            self.assertEqual(call.kwargs["created_by_job_id"], self.job_id)

    def test_all_success_marks_completed(self):
        """全數成功時維持 completed，且不排入任何重試。"""
        uploader = FakeUploader()
        job = self._run(uploader)

        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["record_count"], 6)
        self.assertEqual(job["errors"], [])
        self.retry_queue.add.assert_not_called()

    def test_network_error_aborts_and_marks_failed(self):
        """連不上爬蟲時整批中止並標記失敗（維持既有行為）。"""
        uploader = FakeUploader(network_failures={"2026-07-02"})
        job = self._run(uploader)

        self.assertEqual(uploader.uploaded, ["2026-07-01"])
        self.assertEqual(job["status"], "failed")
        self.retry_queue.add.assert_called_once()


class TestScheduledBackfillReverify(unittest.TestCase):
    """測試排程補抓會帶入重驗天數。"""

    def test_scheduled_enqueues_reverify_days(self):
        """排程建立的補抓任務須帶 SPECIAL_INFO_REVERIFY_DAYS。"""
        with patch.object(web_server, "job_queue") as mock_queue:
            web_server.run_special_info_backfill_scheduled()

        params = mock_queue.enqueue.call_args.args[2]
        self.assertEqual(params[4], web_server.SPECIAL_INFO_REVERIFY_DAYS)
        self.assertGreater(web_server.SPECIAL_INFO_REVERIFY_DAYS, 0)

    def test_job_forwards_reverify_days(self):
        """作業層須把重驗天數轉交給各上傳器的 backfill_missing。"""
        fake_summary = {
            "asset": "測試商品", "scanned": 0, "filled": 0,
            "filled_dates": [], "non_trading": 0, "still_pending": 0,
            "records": 0, "orphans_cleared": 0, "network_errors": [],
        }
        uploader = MagicMock()
        uploader.backfill_missing.return_value = fake_summary

        web_server.upload_jobs["j-rv"] = {
            "job_id": "j-rv", "status": "queued", "record_count": 0,
            "summary": [], "errors": [],
        }
        try:
            with patch.object(web_server, "db_conn"), \
                    patch.object(
                        web_server, "SPECIAL_INFO_ASSETS",
                        [("oil_price", lambda conn, host: uploader)]):
                web_server.run_special_info_backfill_job(
                    "j-rv", days=30, deep=False, today="2026-08-17",
                    reverify_days=7,
                )
        finally:
            web_server.upload_jobs.pop("j-rv", None)

        uploader.backfill_missing.assert_called_once_with(
            days=30, today="2026-08-17", deep=False, reverify_days=7
        )


if __name__ == "__main__":
    unittest.main()

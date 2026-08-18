"""SPECIAL_INFO 缺漏自我修復 API 端點與作業單元測試。"""

import unittest
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient


class TestSpecialInfoBackfillAPI(unittest.TestCase):
    """測試缺漏自我修復 API 端點。"""

    @classmethod
    def setUpClass(cls):
        """建立測試用 FastAPI TestClient。"""
        import web_server
        cls.client = TestClient(web_server.app)

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server
        web_server.upload_jobs.clear()

    def test_get_backfill_schedule(self):
        """測試取得缺漏自我修復每日排程設定。"""
        res = self.client.get("/api/special-info-backfill/schedule")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("time", data)
        self.assertIn("days", data)

    @patch("web_server.save_config")
    @patch("web_server.load_config")
    @patch("web_server.setup_schedule")
    def test_update_backfill_schedule_success(
        self, mock_setup, mock_load, mock_save
    ):
        """測試成功更新缺漏自我修復每日排程。"""
        mock_load.return_value = {
            "schedule_time": "20:07",
            "special_info_backfill_schedule": {"time": "21:30"},
        }

        res = self.client.put(
            "/api/special-info-backfill/schedule",
            json={"time": "09:30"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["time"], "09:30")
        self.assertIn("message", data)
        mock_setup.assert_called_once()

    def test_update_backfill_schedule_invalid_time(self):
        """測試無效時間被拒絕。"""
        res = self.client.put(
            "/api/special-info-backfill/schedule",
            json={"time": "99:99"},
        )
        self.assertEqual(res.status_code, 400)

    @patch("web_server.job_queue")
    def test_run_backfill_success(self, mock_queue):
        """測試手動觸發缺漏自我修復任務。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/special-info-backfill/run", json={"days": 30}
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("job_id", data)
        self.assertEqual(data["status"], "queued")
        mock_queue.enqueue.assert_called_once()

    @patch("web_server.job_queue")
    def test_run_backfill_default_days(self, mock_queue):
        """測試未帶 body 時使用預設天數。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post("/api/special-info-backfill/run")

        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["status"], "queued")

    def test_run_backfill_invalid_days(self):
        """測試 days<=0 被拒絕。"""
        res = self.client.post(
            "/api/special-info-backfill/run", json={"days": 0}
        )
        self.assertEqual(res.status_code, 400)


class TestSpecialInfoBackfillJob(unittest.TestCase):
    """測試 run_special_info_backfill_job 作業邏輯。"""

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server
        web_server.upload_jobs.clear()

    @patch("routers.MySQLRouter")
    def test_job_iterates_assets_and_queues_network_errors(
        self, mock_router_cls
    ):
        """測試作業逐商品掃描、彙總筆數並將網路失敗日交由 retry_queue。"""
        import web_server

        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        # 讓每個 Uploader.backfill_missing 回傳固定摘要
        fake_summary = {
            "asset": "測試商品", "scanned": 3, "filled": 2,
            "filled_dates": ["2026-07-04", "2026-07-05"],
            "non_trading": 0, "still_pending": 0, "records": 2,
            "network_errors": ["2026-07-03"],
        }

        job_id = "testjob1"
        web_server.upload_jobs[job_id] = {
            "job_id": job_id, "type": "special_info_backfill",
            "status": "queued", "record_count": 0, "summary": [],
            "errors": [],
        }

        mock_retry = MagicMock()
        with patch.object(
            web_server.OilPriceUploader, "backfill_missing",
            autospec=True, return_value=fake_summary,
        ), patch.object(
            web_server.GoldPriceUploader, "backfill_missing",
            autospec=True, return_value=fake_summary,
        ), patch.object(
            web_server.BitcoinPriceUploader, "backfill_missing",
            autospec=True, return_value=fake_summary,
        ), patch.object(
            web_server.CurrencyPriceUploader, "backfill_missing",
            autospec=True, return_value=fake_summary,
        ), patch.object(
            web_server.IndicesPriceUploader, "backfill_missing",
            autospec=True, return_value=fake_summary,
        ), patch.object(web_server, "retry_queue", mock_retry):
            web_server.run_special_info_backfill_job(job_id, days=30)

        job = web_server.upload_jobs[job_id]
        self.assertEqual(job["status"], "completed")
        # 5 商品 × 2 筆 = 10
        self.assertEqual(job["record_count"], 10)
        self.assertEqual(len(job["summary"]), 5)
        # 5 商品 × 1 網路失敗 → retry_queue.add 呼叫 5 次
        self.assertEqual(mock_retry.add.call_count, 5)


class TestLoadConfigBackfill(unittest.TestCase):
    """測試 load_config 向後相容（缺漏自我修復排程）。"""

    @patch("web_server.CONFIG_PATH")
    def test_missing_backfill_schedule_uses_default(self, mock_path):
        """測試缺少 special_info_backfill_schedule 時使用預設值。"""
        import web_server

        mock_path.exists.return_value = True
        old_config = {"schedule_time": "20:07"}

        with patch("builtins.open", unittest.mock.mock_open(
            read_data='{"schedule_time": "20:07"}'
        )):
            with patch("json.load", return_value=old_config.copy()):
                config = web_server.load_config()

        self.assertEqual(
            config["special_info_backfill_schedule"], {"time": "21:27"}
        )


if __name__ == "__main__":
    unittest.main()

"""AI 摘要 API 端點單元測試。"""

import unittest
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient


class TestYTSummaryAPI(unittest.TestCase):
    """測試 YT 精華摘要 API 端點。"""

    @classmethod
    def setUpClass(cls):
        """建立測試用 FastAPI TestClient。"""
        import web_server
        cls.client = TestClient(web_server.app)

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server
        web_server.upload_jobs.clear()

    @patch("web_server.job_queue")
    def test_generate_success(self, mock_queue):
        """測試成功建立 YT 精華摘要任務。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/yt-summary/generate",
            json={"date": "2026-04-09"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("job_id", data)
        self.assertEqual(data["status"], "queued")

    def test_generate_invalid_date(self):
        """測試無效日期格式。"""
        res = self.client.post(
            "/api/yt-summary/generate",
            json={"date": "invalid"},
        )
        self.assertEqual(res.status_code, 400)

    @patch("web_server.job_queue")
    def test_job_has_correct_type(self, mock_queue):
        """測試建立的任務類型正確。"""
        import web_server
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/yt-summary/generate",
            json={"date": "2026-04-09"},
        )
        data = res.json()
        job = web_server.upload_jobs[data["job_id"]]
        self.assertEqual(job["type"], "yt_summary")
        self.assertEqual(job["date"], "2026-04-09")

    def test_get_schedule(self):
        """測試取得 YT 精華摘要排程。"""
        res = self.client.get("/api/yt-summary/schedule")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("time", data)

    @patch("web_server.setup_schedule")
    @patch("web_server.save_config")
    @patch("web_server.load_config")
    def test_update_schedule(
        self, mock_load, mock_save, mock_setup
    ):
        """測試更新 YT 精華摘要排程。"""
        mock_load.return_value = {
            "schedule_time": "20:07",
            "tdcc_schedule": {"time": "10:00"},
            "ctee_schedule": {"time": "21:00"},
            "cnyes_schedule": {"time": "21:30"},
            "ptt_schedule": {"time": "22:00"},
            "moneyudn_schedule": {"time": "22:30"},
            "yt_transcript_schedule": {"time": "19:05"},
            "oil_price_schedule": {"time": "07:00"},
            "gold_price_schedule": {"time": "07:05"},
            "bitcoin_price_schedule": {"time": "07:10"},
            "currency_price_schedule": {"time": "07:15"},
            "indices_price_schedule": {"time": "07:20"},
            "yt_summary_schedule": {"time": "19:15"},
            "news_summary_schedule": {"time": "20:03"},
        }

        res = self.client.put(
            "/api/yt-summary/schedule",
            json={"time": "19:30"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("message", data)
        mock_save.assert_called_once()
        mock_setup.assert_called_once()

    def test_update_schedule_invalid_time(self):
        """測試無效時間格式。"""
        res = self.client.put(
            "/api/yt-summary/schedule",
            json={"time": "25:00"},
        )
        self.assertEqual(res.status_code, 400)

    @patch("web_server.Path")
    def test_list_generated(self, mock_path_cls):
        """測試列出已產生的 YT 精華摘要。"""
        # 直接測試 endpoint 回應結構
        res = self.client.get("/api/yt-summary/generated")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("generated", data)


class TestNewsSummaryAPI(unittest.TestCase):
    """測試每日新聞摘要 API 端點。"""

    @classmethod
    def setUpClass(cls):
        """建立測試用 FastAPI TestClient。"""
        import web_server
        cls.client = TestClient(web_server.app)

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server
        web_server.upload_jobs.clear()

    @patch("web_server.job_queue")
    def test_generate_success(self, mock_queue):
        """測試成功建立每日新聞摘要任務。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/news-summary/generate",
            json={"date": "2026-04-08"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("job_id", data)
        self.assertEqual(data["status"], "queued")

    def test_generate_invalid_date(self):
        """測試無效日期格式。"""
        res = self.client.post(
            "/api/news-summary/generate",
            json={"date": "bad-date"},
        )
        self.assertEqual(res.status_code, 400)

    @patch("web_server.job_queue")
    def test_job_has_correct_type(self, mock_queue):
        """測試建立的任務類型正確。"""
        import web_server
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/news-summary/generate",
            json={"date": "2026-04-08"},
        )
        data = res.json()
        job = web_server.upload_jobs[data["job_id"]]
        self.assertEqual(job["type"], "news_summary")
        self.assertEqual(job["date"], "2026-04-08")

    def test_get_schedule(self):
        """測試取得每日新聞摘要排程。"""
        res = self.client.get("/api/news-summary/schedule")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("time", data)

    @patch("web_server.setup_schedule")
    @patch("web_server.save_config")
    @patch("web_server.load_config")
    def test_update_schedule(
        self, mock_load, mock_save, mock_setup
    ):
        """測試更新每日新聞摘要排程。"""
        mock_load.return_value = {
            "schedule_time": "20:07",
            "tdcc_schedule": {"time": "10:00"},
            "ctee_schedule": {"time": "21:00"},
            "cnyes_schedule": {"time": "21:30"},
            "ptt_schedule": {"time": "22:00"},
            "moneyudn_schedule": {"time": "22:30"},
            "yt_transcript_schedule": {"time": "19:05"},
            "oil_price_schedule": {"time": "07:00"},
            "gold_price_schedule": {"time": "07:05"},
            "bitcoin_price_schedule": {"time": "07:10"},
            "currency_price_schedule": {"time": "07:15"},
            "indices_price_schedule": {"time": "07:20"},
            "yt_summary_schedule": {"time": "19:15"},
            "news_summary_schedule": {"time": "20:03"},
        }

        res = self.client.put(
            "/api/news-summary/schedule",
            json={"time": "20:30"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("message", data)

    def test_list_generated(self):
        """測試列出已產生的每日新聞摘要。"""
        res = self.client.get("/api/news-summary/generated")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("generated", data)


if __name__ == "__main__":
    unittest.main()

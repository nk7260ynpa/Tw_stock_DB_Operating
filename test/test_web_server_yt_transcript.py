"""YT 逐字稿 API 端點單元測試。"""

import unittest
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient


class TestYTTranscriptAPI(unittest.TestCase):
    """測試 YT 逐字稿 API 端點。"""

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
    def test_create_upload_success(self, mock_queue):
        """測試成功建立 YT 逐字稿上傳任務。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/yt-transcript/upload",
            json={"date": "2026-03-11"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("job_id", data)
        self.assertEqual(data["status"], "queued")

    def test_create_upload_invalid_date(self):
        """測試無效日期格式。"""
        res = self.client.post(
            "/api/yt-transcript/upload",
            json={"date": "invalid"},
        )

        self.assertEqual(res.status_code, 400)

    @patch("web_server.job_queue")
    def test_queues_when_running(self, mock_queue):
        """測試已有執行中任務時排入佇列。"""
        import web_server
        mock_queue.enqueue.return_value = 1

        web_server.upload_jobs["existing"] = {
            "job_id": "existing",
            "status": "running",
        }

        res = self.client.post(
            "/api/yt-transcript/upload",
            json={"date": "2026-03-11"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["status"], "queued")
        self.assertIn("queue_position", data)

    @patch("web_server.job_queue")
    def test_job_has_correct_type(self, mock_queue):
        """測試建立的任務類型正確。"""
        import web_server
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/yt-transcript/upload",
            json={"date": "2026-03-11"},
        )
        data = res.json()
        job_id = data["job_id"]

        job = web_server.upload_jobs[job_id]
        self.assertEqual(job["type"], "yt_transcript")
        self.assertEqual(job["date"], "2026-03-11")

    @patch("web_server.MySQLRouter")
    def test_list_uploaded_success(self, mock_router_cls):
        """測試列出已上傳的 YT 逐字稿日期。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        from datetime import date
        mock_conn.execute.return_value.fetchall.return_value = [
            (date(2026, 3, 11),),
            (date(2026, 3, 10),),
        ]

        res = self.client.get("/api/yt-transcript/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(len(data["uploaded"]), 2)

    @patch("web_server.MySQLRouter")
    def test_list_uploaded_db_error(self, mock_router_cls):
        """測試資料庫連線失敗時回傳空清單。"""
        mock_router_cls.side_effect = Exception("連線失敗")

        res = self.client.get("/api/yt-transcript/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["uploaded"], [])

    def test_get_schedule(self):
        """測試取得 YT 逐字稿排程設定。"""
        res = self.client.get("/api/yt-transcript/schedule")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("time", data)

    @patch("web_server.setup_schedule")
    @patch("web_server.save_config")
    def test_update_schedule_success(self, mock_save, mock_setup):
        """測試更新 YT 逐字稿排程設定成功。"""
        res = self.client.put(
            "/api/yt-transcript/schedule",
            json={"time": "18:30"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["time"], "18:30")
        self.assertIn("18:30", data["message"])

    def test_update_schedule_invalid_time(self):
        """測試無效時間格式。"""
        res = self.client.put(
            "/api/yt-transcript/schedule",
            json={"time": "25:00"},
        )

        self.assertEqual(res.status_code, 400)

    @patch("web_server.MySQLRouter")
    def test_get_status_exists(self, mock_router_cls):
        """測試查詢存在的 YT 逐字稿狀態。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        from datetime import date
        mock_conn.execute.return_value.fetchone.return_value = (
            date(2026, 3, 11),
            "測試標題",
            "https://youtube.com/watch?v=test",
            "1:00:00",
            "2026-03-11/2026-03-11.md",
            "success",
            None,
        )

        res = self.client.get(
            "/api/yt-transcript/status",
            params={"date": "2026-03-11"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertTrue(data["exists"])
        self.assertEqual(data["status"], "success")
        self.assertEqual(data["title"], "測試標題")

    @patch("web_server.MySQLRouter")
    def test_get_status_not_found(self, mock_router_cls):
        """測試查詢不存在的 YT 逐字稿狀態。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = None

        res = self.client.get(
            "/api/yt-transcript/status",
            params={"date": "2026-03-11"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertFalse(data["exists"])


if __name__ == "__main__":
    unittest.main()

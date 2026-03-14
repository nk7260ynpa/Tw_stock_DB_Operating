"""季度營業收入 API 端點單元測試。"""

import unittest
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient


class TestQuarterRevenueAPI(unittest.TestCase):
    """測試季度營業收入 API 端點。"""

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
    def test_create_quarter_revenue_upload_success(self, mock_queue):
        """測試成功建立季度營業收入抓取任務。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/quarter-revenue/upload",
            json={"year": 113, "quarter": 1},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("job_id", data)
        self.assertEqual(data["status"], "queued")

    def test_invalid_quarter(self):
        """測試無效季度被拒絕。"""
        res = self.client.post(
            "/api/quarter-revenue/upload",
            json={"year": 113, "quarter": 5},
        )

        self.assertEqual(res.status_code, 400)

    def test_invalid_quarter_zero(self):
        """測試季度為 0 被拒絕。"""
        res = self.client.post(
            "/api/quarter-revenue/upload",
            json={"year": 113, "quarter": 0},
        )

        self.assertEqual(res.status_code, 400)

    def test_invalid_year_too_low(self):
        """測試年份太低被拒絕。"""
        res = self.client.post(
            "/api/quarter-revenue/upload",
            json={"year": 50, "quarter": 1},
        )

        self.assertEqual(res.status_code, 400)

    def test_invalid_year_too_high(self):
        """測試年份太高被拒絕。"""
        res = self.client.post(
            "/api/quarter-revenue/upload",
            json={"year": 300, "quarter": 1},
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
            "/api/quarter-revenue/upload",
            json={"year": 113, "quarter": 1},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["status"], "queued")
        self.assertIn("queue_position", data)

    @patch("web_server.MySQLRouter")
    def test_list_uploaded_quarters(self, mock_router_cls):
        """測試列出已上傳的季度記錄。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        mock_conn.execute.return_value.fetchall.return_value = [
            (113, "1"),
        ]

        res = self.client.get("/api/quarter-revenue/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("uploaded", data)
        self.assertEqual(len(data["uploaded"]), 1)
        self.assertEqual(data["uploaded"][0]["year"], 113)
        self.assertEqual(data["uploaded"][0]["quarter"], "1")

    @patch("web_server.MySQLRouter")
    def test_list_uploaded_quarters_empty(self, mock_router_cls):
        """測試無已上傳記錄時回傳空清單。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = []

        res = self.client.get("/api/quarter-revenue/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["uploaded"], [])

    @patch("web_server.MySQLRouter")
    def test_list_uploaded_quarters_db_error(self, mock_router_cls):
        """測試資料庫連線失敗時回傳空清單。"""
        mock_router_cls.side_effect = Exception("連線失敗")

        res = self.client.get("/api/quarter-revenue/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["uploaded"], [])


if __name__ == "__main__":
    unittest.main()

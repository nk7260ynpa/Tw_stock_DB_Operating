"""公司產業對照 API 端點單元測試。"""

import unittest
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient


class TestCompanyInfoAPI(unittest.TestCase):
    """測試公司產業對照 API 端點。"""

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
        """測試成功建立公司產業對照上傳任務。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post("/api/company-info/upload")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("job_id", data)
        self.assertEqual(data["status"], "queued")

    @patch("web_server.job_queue")
    def test_queues_when_running(self, mock_queue):
        """測試已有執行中任務時排入佇列。"""
        import web_server

        mock_queue.enqueue.return_value = 1

        web_server.upload_jobs["existing"] = {
            "job_id": "existing",
            "status": "running",
        }

        res = self.client.post("/api/company-info/upload")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["status"], "queued")
        self.assertIn("queue_position", data)

    @patch("web_server.job_queue")
    def test_job_has_correct_type(self, mock_queue):
        """測試建立的任務類型正確。"""
        import web_server

        mock_queue.enqueue.return_value = 0

        res = self.client.post("/api/company-info/upload")
        data = res.json()
        job_id = data["job_id"]

        job = web_server.upload_jobs[job_id]
        self.assertEqual(job["type"], "company_info")
        self.assertEqual(job["company_info_count"], 0)
        self.assertEqual(job["industry_map_count"], 0)

    @patch("web_server.MySQLRouter")
    def test_get_status_success(self, mock_router_cls):
        """測試取得公司產業對照狀態。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        # 第一次呼叫回傳 CompanyInfo count，第二次回傳 IndustryMap count
        mock_conn.execute.return_value.scalar.side_effect = [999, 30]

        res = self.client.get("/api/company-info/status")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["company_info_count"], 999)
        self.assertEqual(data["industry_map_count"], 30)

    @patch("web_server.MySQLRouter")
    def test_get_status_db_error(self, mock_router_cls):
        """測試資料庫連線失敗時回傳 0。"""
        mock_router_cls.side_effect = Exception("連線失敗")

        res = self.client.get("/api/company-info/status")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["company_info_count"], 0)
        self.assertEqual(data["industry_map_count"], 0)


if __name__ == "__main__":
    unittest.main()

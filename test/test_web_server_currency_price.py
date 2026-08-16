"""匯率 API 端點單元測試。"""

import unittest
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient


class TestCurrencyPriceAPI(unittest.TestCase):
    """測試匯率 API 端點。"""

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
    def test_create_currency_price_upload_success(self, mock_queue):
        """測試成功建立匯率上傳任務。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/currency-price/upload",
            json={
                "start_date": "2026-03-12",
                "end_date": "2026-03-19",
            },
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("job_id", data)
        self.assertEqual(data["status"], "queued")

    def test_create_currency_price_upload_invalid_date(self):
        """測試無效日期格式被拒絕。"""
        res = self.client.post(
            "/api/currency-price/upload",
            json={
                "start_date": "2026/03/19",
                "end_date": "2026/03/19",
            },
        )

        self.assertEqual(res.status_code, 400)

    def test_create_currency_price_upload_end_before_start(self):
        """測試結束日期早於起始日期被拒絕。"""
        res = self.client.post(
            "/api/currency-price/upload",
            json={
                "start_date": "2026-03-19",
                "end_date": "2026-03-12",
            },
        )

        self.assertEqual(res.status_code, 400)

    @patch("routers.MySQLRouter")
    def test_list_uploaded_currency_price(self, mock_router_cls):
        """測試列出已上傳的匯率日期。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        mock_conn.execute.return_value.fetchall.return_value = [
            ("2026-03-19",),
            ("2026-03-18",),
        ]

        res = self.client.get("/api/currency-price/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("uploaded", data)
        self.assertEqual(len(data["uploaded"]), 2)
        self.assertEqual(data["uploaded"][0], "2026-03-19")

    @patch("routers.MySQLRouter")
    def test_list_uploaded_currency_price_empty(self, mock_router_cls):
        """測試無已上傳記錄時回傳空清單。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = []

        res = self.client.get("/api/currency-price/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["uploaded"], [])

    @patch("routers.MySQLRouter")
    def test_list_uploaded_currency_price_db_error(self, mock_router_cls):
        """測試資料庫連線失敗時回傳空清單。"""
        mock_router_cls.side_effect = Exception("連線失敗")

        res = self.client.get("/api/currency-price/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["uploaded"], [])

    def test_get_currency_price_schedule(self):
        """測試取得匯率每日排程設定。"""
        res = self.client.get("/api/currency-price/schedule")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("time", data)

    @patch("web_server.save_config")
    @patch("web_server.load_config")
    @patch("web_server.setup_schedule")
    def test_update_currency_price_schedule_success(
        self, mock_setup, mock_load, mock_save
    ):
        """測試成功更新匯率每日排程。"""
        mock_load.return_value = {
            "schedule_time": "20:07",
            "currency_price_schedule": {"time": "07:15"},
        }

        res = self.client.put(
            "/api/currency-price/schedule",
            json={"time": "08:15"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["time"], "08:15")
        self.assertIn("message", data)

    def test_update_currency_price_schedule_invalid_time(self):
        """測試無效時間被拒絕。"""
        res = self.client.put(
            "/api/currency-price/schedule",
            json={"time": "25:00"},
        )

        self.assertEqual(res.status_code, 400)

    def test_update_currency_price_schedule_empty_time(self):
        """測試空時間被拒絕。"""
        res = self.client.put(
            "/api/currency-price/schedule",
            json={"time": ""},
        )

        self.assertEqual(res.status_code, 400)


class TestLoadConfigCurrencyPrice(unittest.TestCase):
    """測試 load_config 向後相容（匯率）。"""

    @patch("web_server.CONFIG_PATH")
    def test_missing_currency_price_schedule_uses_default(self, mock_path):
        """測試缺少 currency_price_schedule 時使用預設值。"""
        import web_server

        mock_path.exists.return_value = True

        old_config = {"schedule_time": "20:07"}

        with patch("builtins.open", unittest.mock.mock_open(
            read_data='{"schedule_time": "20:07"}'
        )):
            with patch("json.load", return_value=old_config.copy()):
                config = web_server.load_config()

        self.assertEqual(
            config["currency_price_schedule"], {"time": "07:42"}
        )


if __name__ == "__main__":
    unittest.main()

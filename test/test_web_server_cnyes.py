"""CNYES 新聞 API 端點單元測試。"""

import unittest
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient


class TestCNYESNewsAPI(unittest.TestCase):
    """測試 CNYES 新聞 API 端點。"""

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
        """測試成功建立 CNYES 新聞上傳任務。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/cnyes-news/upload",
            json={"start_date": "2026-02-27", "end_date": "2026-02-27"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("job_id", data)
        self.assertEqual(data["status"], "queued")

    @patch("web_server.job_queue")
    def test_create_upload_date_range(self, mock_queue):
        """測試建立日期範圍上傳任務。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post(
            "/api/cnyes-news/upload",
            json={"start_date": "2026-02-25", "end_date": "2026-02-27"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("job_id", data)

    def test_create_upload_invalid_date(self):
        """測試無效日期被拒絕。"""
        res = self.client.post(
            "/api/cnyes-news/upload",
            json={"start_date": "invalid", "end_date": "2026-02-27"},
        )

        self.assertEqual(res.status_code, 400)

    def test_create_upload_end_before_start(self):
        """測試結束日期早於起始日期被拒絕。"""
        res = self.client.post(
            "/api/cnyes-news/upload",
            json={"start_date": "2026-02-28", "end_date": "2026-02-25"},
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
            "/api/cnyes-news/upload",
            json={"start_date": "2026-02-27", "end_date": "2026-02-27"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["status"], "queued")
        self.assertIn("queue_position", data)

    @patch("routers.MySQLRouter")
    def test_list_uploaded(self, mock_router_cls):
        """測試列出已上傳的 CNYES 新聞日期。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        mock_conn.execute.return_value.fetchall.return_value = [
            ("2026-02-27",),
            ("2026-02-26",),
        ]

        res = self.client.get("/api/cnyes-news/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("uploaded", data)
        self.assertEqual(len(data["uploaded"]), 2)
        self.assertEqual(data["uploaded"][0], "2026-02-27")

    @patch("routers.MySQLRouter")
    def test_list_uploaded_empty(self, mock_router_cls):
        """測試無已上傳記錄時回傳空清單。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = []

        res = self.client.get("/api/cnyes-news/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["uploaded"], [])

    @patch("routers.MySQLRouter")
    def test_list_uploaded_db_error(self, mock_router_cls):
        """測試資料庫連線失敗時回傳空清單。"""
        mock_router_cls.side_effect = Exception("連線失敗")

        res = self.client.get("/api/cnyes-news/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["uploaded"], [])

    def test_get_schedule(self):
        """測試取得 CNYES 新聞排程設定。"""
        res = self.client.get("/api/cnyes-news/schedule")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("time", data)

    @patch("web_server.save_config")
    @patch("web_server.load_config")
    @patch("web_server.setup_schedule")
    def test_update_schedule_success(
        self, mock_setup, mock_load, mock_save
    ):
        """測試成功更新 CNYES 新聞排程。"""
        mock_load.return_value = {
            "schedule_time": "20:07",
            "tdcc_schedule": {"time": "10:00"},
            "ctee_schedule": {"time": "21:00"},
            "cnyes_schedule": {"time": "21:30"},
        }

        res = self.client.put(
            "/api/cnyes-news/schedule",
            json={"time": "22:00"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["time"], "22:00")
        self.assertIn("message", data)

    def test_update_schedule_invalid_time(self):
        """測試無效時間被拒絕。"""
        res = self.client.put(
            "/api/cnyes-news/schedule",
            json={"time": "25:00"},
        )

        self.assertEqual(res.status_code, 400)

    def test_update_schedule_empty_time(self):
        """測試空時間被拒絕。"""
        res = self.client.put(
            "/api/cnyes-news/schedule",
            json={"time": ""},
        )

        self.assertEqual(res.status_code, 400)


class TestLoadConfigWithCNYES(unittest.TestCase):
    """測試 load_config 向後相容（CNYES）。"""

    @patch("web_server.CONFIG_PATH")
    def test_missing_cnyes_schedule_uses_default(self, mock_path):
        """測試缺少 cnyes_schedule 時使用預設值。"""
        import web_server

        mock_path.exists.return_value = True

        old_config = {
            "schedule_time": "20:07",
            "tdcc_schedule": {"time": "10:00"},
            "ctee_schedule": {"time": "21:00"},
        }

        with patch("builtins.open", unittest.mock.mock_open(
            read_data='{"schedule_time": "20:07", '
            '"tdcc_schedule": {"time": "10:00"}, '
            '"ctee_schedule": {"time": "21:00"}}'
        )):
            with patch("json.load", return_value=old_config.copy()):
                config = web_server.load_config()

        self.assertIn("cnyes_schedule", config)
        self.assertEqual(config["cnyes_schedule"]["time"], "07:48")

    @patch("web_server.CONFIG_PATH")
    def test_existing_cnyes_schedule_preserved(self, mock_path):
        """測試已有 cnyes_schedule 時保留原值。"""
        import web_server

        mock_path.exists.return_value = True

        # 帶 config_version：已完成遷移的設定，既有值原樣保留（含窗外自訂）。
        config_data = {
            "config_version": 2,
            "schedule_time": "20:07",
            "tdcc_schedule": {"time": "10:00"},
            "ctee_schedule": {"time": "21:00"},
            "cnyes_schedule": {"time": "22:30"},
        }

        with patch("builtins.open", unittest.mock.mock_open(
            read_data='{}'
        )):
            with patch("json.load", return_value=config_data.copy()):
                config = web_server.load_config()

        self.assertEqual(config["cnyes_schedule"]["time"], "22:30")


class TestCNYESNewsScheduled(unittest.TestCase):
    """測試 CNYES 新聞排程任務（回溯時數）。"""

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server
        web_server.upload_jobs.clear()

    @patch("web_server.job_queue")
    def test_scheduled_uses_48_hours(self, mock_queue):
        """排程改於早上抓取，回溯時數應為 48 小時以涵蓋昨日整天。"""
        import web_server

        self.assertEqual(web_server.NEWS_SCHEDULE_HOURS, 48)

        web_server.run_cnyes_news_scheduled()

        mock_queue.enqueue.assert_called_once()
        func, params = mock_queue.enqueue.call_args.args[1:]
        self.assertEqual(func, web_server.run_cnyes_news_hours_job)
        self.assertEqual(params[1], 48)


if __name__ == "__main__":
    unittest.main()

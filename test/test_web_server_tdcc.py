"""TDCC API 端點單元測試。"""

import unittest
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient


class TestTDCCAPI(unittest.TestCase):
    """測試 TDCC API 端點。"""

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
    def test_create_tdcc_upload_success(self, mock_queue):
        """測試成功建立 TDCC 上傳任務。"""
        mock_queue.enqueue.return_value = 0

        res = self.client.post("/api/tdcc/upload")

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

        res = self.client.post("/api/tdcc/upload")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["status"], "queued")
        self.assertIn("queue_position", data)

    @patch("routers.MySQLRouter")
    def test_list_uploaded_tdcc(self, mock_router_cls):
        """測試列出已上傳的 TDCC 日期。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        mock_conn.execute.return_value.fetchall.return_value = [
            ("2024-01-05",),
            ("2024-01-12",),
        ]

        res = self.client.get("/api/tdcc/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("uploaded", data)
        self.assertEqual(len(data["uploaded"]), 2)
        self.assertEqual(data["uploaded"][0], "2024-01-05")

    @patch("routers.MySQLRouter")
    def test_list_uploaded_tdcc_empty(self, mock_router_cls):
        """測試無已上傳記錄時回傳空清單。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = []

        res = self.client.get("/api/tdcc/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["uploaded"], [])

    @patch("routers.MySQLRouter")
    def test_list_uploaded_tdcc_db_error(self, mock_router_cls):
        """測試資料庫連線失敗時回傳空清單。"""
        mock_router_cls.side_effect = Exception("連線失敗")

        res = self.client.get("/api/tdcc/uploaded")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["uploaded"], [])

    def test_get_tdcc_schedule(self):
        """測試取得 TDCC 每日排程設定。"""
        res = self.client.get("/api/tdcc/schedule")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("time", data)
        self.assertNotIn("day", data)

    @patch("web_server.save_config")
    @patch("web_server.load_config")
    @patch("web_server.setup_schedule")
    def test_update_tdcc_schedule_success(
        self, mock_setup, mock_load, mock_save
    ):
        """測試成功更新 TDCC 每日排程。"""
        mock_load.return_value = {
            "schedule_time": "20:07",
            "tdcc_schedule": {"time": "10:00"},
        }

        res = self.client.put(
            "/api/tdcc/schedule",
            json={"time": "09:30"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["time"], "09:30")
        self.assertNotIn("day", data)
        self.assertIn("message", data)

    def test_update_tdcc_schedule_invalid_time(self):
        """測試無效時間被拒絕。"""
        res = self.client.put(
            "/api/tdcc/schedule",
            json={"time": "25:00"},
        )

        self.assertEqual(res.status_code, 400)

    def test_update_tdcc_schedule_empty_time(self):
        """測試空時間被拒絕。"""
        res = self.client.put(
            "/api/tdcc/schedule",
            json={"time": ""},
        )

        self.assertEqual(res.status_code, 400)


class TestLoadConfigMigration(unittest.TestCase):
    """測試 load_config 向後相容遷移邏輯。"""

    @patch("web_server.CONFIG_PATH")
    @patch("web_server.save_config")
    def test_migrate_old_format_with_day(self, mock_save, mock_path):
        """測試舊格式（含 day）自動遷移為新格式。"""
        import web_server

        mock_path.exists.return_value = True

        # 帶 config_version：隔離舊 day 格式遷移，不觸發窗內時間遷移。
        old_config = {
            "config_version": web_server.CONFIG_VERSION,
            "schedule_time": "20:07",
            "tdcc_schedule": {"day": "saturday", "time": "10:00"},
        }

        with patch("builtins.open", unittest.mock.mock_open(
            read_data='{"schedule_time": "20:07", '
            '"tdcc_schedule": {"day": "saturday", "time": "10:00"}}'
        )):
            with patch("json.load", return_value=old_config.copy()):
                config = web_server.load_config()

        self.assertNotIn("day", config["tdcc_schedule"])
        self.assertEqual(config["tdcc_schedule"]["time"], "10:00")
        mock_save.assert_called_once()

    @patch("web_server.CONFIG_PATH")
    def test_new_format_no_migration(self, mock_path):
        """測試新格式不觸發遷移。"""
        import web_server

        mock_path.exists.return_value = True

        # 帶 config_version：新格式且已遷移，不應觸發任何遷移或寫回。
        new_config = {
            "config_version": web_server.CONFIG_VERSION,
            "schedule_time": "20:07",
            "tdcc_schedule": {"time": "10:00"},
        }

        with patch("builtins.open", unittest.mock.mock_open(
            read_data='{"schedule_time": "20:07", '
            '"tdcc_schedule": {"time": "10:00"}}'
        )):
            with patch("json.load", return_value=new_config.copy()):
                with patch("web_server.save_config") as mock_save:
                    config = web_server.load_config()

        self.assertEqual(config["tdcc_schedule"], {"time": "10:00"})
        mock_save.assert_not_called()

    @patch("web_server.CONFIG_PATH")
    def test_missing_tdcc_schedule_uses_default(self, mock_path):
        """測試缺少 tdcc_schedule 時使用預設值。"""
        import web_server

        mock_path.exists.return_value = True

        old_config = {"schedule_time": "20:07"}

        with patch("builtins.open", unittest.mock.mock_open(
            read_data='{"schedule_time": "20:07"}'
        )):
            with patch("json.load", return_value=old_config.copy()):
                config = web_server.load_config()

        self.assertEqual(config["tdcc_schedule"], {"time": "21:03"})


if __name__ == "__main__":
    unittest.main()

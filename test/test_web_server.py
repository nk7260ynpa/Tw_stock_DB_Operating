"""Web 管理介面模組單元測試。"""

import json
import unittest
from unittest.mock import patch, MagicMock, mock_open

from fastapi.testclient import TestClient


class TestLoadConfig(unittest.TestCase):
    """測試 load_config 函式。"""

    @patch("web_server.CONFIG_PATH")
    def test_load_config_file_exists(self, mock_path):
        """測試設定檔存在時正確讀取內容。"""
        import web_server

        mock_path.exists.return_value = True
        config_data = {"schedule_time": "21:30"}

        with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
            result = web_server.load_config()

        self.assertEqual(result, {"schedule_time": "21:30"})

    @patch("web_server.CONFIG_PATH")
    def test_load_config_file_not_exists(self, mock_path):
        """測試設定檔不存在時回傳預設值。"""
        import web_server

        mock_path.exists.return_value = False

        result = web_server.load_config()

        self.assertEqual(result, {"schedule_time": "20:07"})


class TestSaveConfig(unittest.TestCase):
    """測試 save_config 函式。"""

    @patch("web_server.CONFIG_PATH")
    def test_save_config_writes_json(self, mock_path):
        """測試正確寫入 JSON 設定檔。"""
        import web_server

        m = mock_open()
        with patch("builtins.open", m):
            web_server.save_config({"schedule_time": "22:00"})

        m.assert_called_once_with(mock_path, "w", encoding="utf-8")
        written = "".join(
            call.args[0] for call in m().write.call_args_list
        )
        self.assertIn("22:00", written)


class TestSetupSchedule(unittest.TestCase):
    """測試 setup_schedule 函式。"""

    @patch("web_server.schedule_lib")
    def test_setup_schedule_clears_and_sets(self, mock_schedule):
        """測試設定排程時先清除再建立新排程。"""
        import web_server

        web_server.setup_schedule("18:00")

        mock_schedule.clear.assert_called_once()
        mock_schedule.every.return_value.day.at.assert_called_once_with(
            "18:00"
        )


class TestRunUploadJob(unittest.TestCase):
    """測試 run_upload_job 函式。"""

    @patch("web_server.day_upload")
    @patch("web_server.time.sleep")
    def test_single_date_single_db(self, mock_sleep, mock_day_upload):
        """測試單日單資料庫上傳任務。"""
        import web_server

        job_id = "test-001"
        web_server.upload_jobs[job_id] = {
            "job_id": job_id,
            "status": "pending",
            "total": 0,
            "completed": 0,
            "current_date": "",
            "current_db": "",
            "errors": [],
            "finished_at": None,
        }

        web_server.run_upload_job(job_id, "2026-01-02", "2026-01-02", ["TWSE"])

        mock_day_upload.assert_called_once()
        self.assertEqual(web_server.upload_jobs[job_id]["status"], "completed")
        self.assertEqual(web_server.upload_jobs[job_id]["completed"], 1)
        self.assertEqual(web_server.upload_jobs[job_id]["total"], 1)

        del web_server.upload_jobs[job_id]

    @patch("web_server.day_upload")
    @patch("web_server.time.sleep")
    def test_date_range_multiple_dbs(self, mock_sleep, mock_day_upload):
        """測試日期範圍與多資料庫上傳任務。"""
        import web_server

        job_id = "test-002"
        web_server.upload_jobs[job_id] = {
            "job_id": job_id,
            "status": "pending",
            "total": 0,
            "completed": 0,
            "current_date": "",
            "current_db": "",
            "errors": [],
            "finished_at": None,
        }

        web_server.run_upload_job(
            job_id, "2026-01-02", "2026-01-03", ["TWSE", "TPEX"]
        )

        # 2 日 x 2 資料庫 = 4 次呼叫
        self.assertEqual(mock_day_upload.call_count, 4)
        self.assertEqual(web_server.upload_jobs[job_id]["completed"], 4)
        self.assertEqual(web_server.upload_jobs[job_id]["status"], "completed")

        del web_server.upload_jobs[job_id]

    @patch("web_server.day_upload", side_effect=Exception("連線失敗"))
    @patch("web_server.time.sleep")
    def test_upload_error_recorded(self, mock_sleep, mock_day_upload):
        """測試上傳失敗時錯誤被記錄。"""
        import web_server

        job_id = "test-003"
        web_server.upload_jobs[job_id] = {
            "job_id": job_id,
            "status": "pending",
            "total": 0,
            "completed": 0,
            "current_date": "",
            "current_db": "",
            "errors": [],
            "finished_at": None,
        }

        web_server.run_upload_job(job_id, "2026-01-02", "2026-01-02", ["TWSE"])

        self.assertEqual(web_server.upload_jobs[job_id]["status"], "completed")
        self.assertEqual(len(web_server.upload_jobs[job_id]["errors"]), 1)
        self.assertIn("連線失敗", web_server.upload_jobs[job_id]["errors"][0])

        del web_server.upload_jobs[job_id]


class TestAPIEndpoints(unittest.TestCase):
    """測試 API 端點。"""

    @classmethod
    def setUpClass(cls):
        """建立測試用 FastAPI TestClient。"""
        import web_server
        cls.client = TestClient(web_server.app)

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server
        web_server.upload_jobs.clear()

    def test_get_databases(self):
        """測試取得資料庫清單。"""
        res = self.client.get("/api/databases")

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("databases", data)
        self.assertEqual(
            data["databases"], ["TWSE", "TPEX", "TAIFEX", "FAOI", "MGTS"]
        )

    @patch("web_server.load_config", return_value={"schedule_time": "20:07"})
    def test_get_schedule(self, mock_config):
        """測試取得排程時間。"""
        res = self.client.get("/api/schedule")

        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), {"time": "20:07"})

    @patch("web_server.setup_schedule")
    @patch("web_server.save_config")
    @patch("web_server.load_config", return_value={"schedule_time": "20:07"})
    def test_update_schedule_success(
        self, mock_load, mock_save, mock_setup
    ):
        """測試成功更新排程時間。"""
        res = self.client.put(
            "/api/schedule",
            json={"time": "21:30"},
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["time"], "21:30")
        mock_save.assert_called_once()
        mock_setup.assert_called_once_with("21:30")

    def test_update_schedule_invalid_time(self):
        """測試無效時間格式被拒絕。"""
        res = self.client.put(
            "/api/schedule",
            json={"time": "25:00"},
        )

        self.assertEqual(res.status_code, 400)

    def test_update_schedule_bad_format(self):
        """測試錯誤時間格式被拒絕。"""
        res = self.client.put(
            "/api/schedule",
            json={"time": "abc"},
        )

        self.assertEqual(res.status_code, 400)

    @patch("web_server.threading.Thread")
    def test_create_upload_success(self, mock_thread):
        """測試成功建立上傳任務。"""
        mock_thread.return_value.start = MagicMock()

        res = self.client.post(
            "/api/upload",
            json={
                "start_date": "2026-01-02",
                "end_date": "2026-01-02",
                "databases": ["TWSE"],
            },
        )

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("job_id", data)
        self.assertEqual(data["status"], "pending")

    def test_create_upload_empty_databases(self):
        """測試未選擇資料庫時被拒絕。"""
        res = self.client.post(
            "/api/upload",
            json={
                "start_date": "2026-01-02",
                "end_date": "2026-01-02",
                "databases": [],
            },
        )

        self.assertEqual(res.status_code, 400)

    def test_create_upload_invalid_database(self):
        """測試無效資料庫名稱被拒絕。"""
        res = self.client.post(
            "/api/upload",
            json={
                "start_date": "2026-01-02",
                "end_date": "2026-01-02",
                "databases": ["INVALID"],
            },
        )

        self.assertEqual(res.status_code, 400)

    def test_create_upload_invalid_date(self):
        """測試無效日期格式被拒絕。"""
        res = self.client.post(
            "/api/upload",
            json={
                "start_date": "not-a-date",
                "end_date": "2026-01-02",
                "databases": ["TWSE"],
            },
        )

        self.assertEqual(res.status_code, 400)

    def test_create_upload_end_before_start(self):
        """測試結束日期早於起始日期被拒絕。"""
        res = self.client.post(
            "/api/upload",
            json={
                "start_date": "2026-01-05",
                "end_date": "2026-01-02",
                "databases": ["TWSE"],
            },
        )

        self.assertEqual(res.status_code, 400)

    @patch("web_server.threading.Thread")
    def test_create_upload_rejects_when_running(self, mock_thread):
        """測試已有執行中任務時拒絕新任務。"""
        import web_server

        mock_thread.return_value.start = MagicMock()

        web_server.upload_jobs["existing"] = {
            "job_id": "existing",
            "status": "running",
        }

        res = self.client.post(
            "/api/upload",
            json={
                "start_date": "2026-01-02",
                "end_date": "2026-01-02",
                "databases": ["TWSE"],
            },
        )

        self.assertEqual(res.status_code, 409)

    def test_list_upload_jobs_empty(self):
        """測試無任務時回傳空清單。"""
        res = self.client.get("/api/upload/jobs")

        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), [])

    @patch("web_server.threading.Thread")
    def test_list_upload_jobs_with_data(self, mock_thread):
        """測試有任務時回傳任務清單。"""
        import web_server

        mock_thread.return_value.start = MagicMock()

        self.client.post(
            "/api/upload",
            json={
                "start_date": "2026-01-02",
                "end_date": "2026-01-02",
                "databases": ["TWSE"],
            },
        )

        res = self.client.get("/api/upload/jobs")

        self.assertEqual(res.status_code, 200)
        self.assertEqual(len(res.json()), 1)

    def test_get_upload_status_not_found(self):
        """測試查詢不存在的任務回傳 404。"""
        res = self.client.get("/api/upload/status/nonexistent")

        self.assertEqual(res.status_code, 404)

    @patch("web_server.threading.Thread")
    def test_get_upload_status_found(self, mock_thread):
        """測試查詢已存在的任務回傳正確狀態。"""
        import web_server

        mock_thread.return_value.start = MagicMock()

        create_res = self.client.post(
            "/api/upload",
            json={
                "start_date": "2026-01-02",
                "end_date": "2026-01-02",
                "databases": ["TWSE"],
            },
        )
        job_id = create_res.json()["job_id"]

        res = self.client.get(f"/api/upload/status/{job_id}")

        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["job_id"], job_id)


if __name__ == "__main__":
    unittest.main()

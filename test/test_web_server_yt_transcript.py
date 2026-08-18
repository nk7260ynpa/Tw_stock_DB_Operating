"""YT 逐字稿 API 端點單元測試。"""

import unittest
from datetime import datetime
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

    @patch("routers.MySQLRouter")
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

    @patch("routers.MySQLRouter")
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

    @patch("routers.MySQLRouter")
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

    @patch("routers.MySQLRouter")
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


class TestYTTranscriptTargetDate(unittest.TestCase):
    """測試 YT 逐字稿排程目標日期的計算（含跨午夜補跑）。"""

    def test_on_time_run_targets_yesterday(self):
        """準時於 21:24 執行時，目標日為前一日。"""
        import web_server

        now = datetime(2026, 8, 18, 21, 24, 3)
        self.assertEqual(
            web_server.yt_transcript_target_date("21:24", now), "2026-08-17"
        )

    def test_late_same_evening_run_targets_yesterday(self):
        """同一晚稍晚補跑（23:50）仍取前一日，不受影響。"""
        import web_server

        now = datetime(2026, 8, 18, 23, 50, 0)
        self.assertEqual(
            web_server.yt_transcript_target_date("21:24", now), "2026-08-17"
        )

    def test_after_midnight_catchup_keeps_scheduled_day(self):
        """跨午夜補跑（隔日 00:30）仍對應前一晚的排程日，不可少抓一天。"""
        import web_server

        now = datetime(2026, 8, 19, 0, 30, 0)
        self.assertEqual(
            web_server.yt_transcript_target_date("21:24", now), "2026-08-17"
        )

    def test_next_morning_catchup_keeps_scheduled_day(self):
        """宿主睡到隔天早上才補跑（09:00），目標日不應被推成當日。"""
        import web_server

        now = datetime(2026, 8, 19, 9, 0, 0)
        self.assertEqual(
            web_server.yt_transcript_target_date("21:24", now), "2026-08-17"
        )

    def test_invalid_schedule_time_falls_back_to_now_minus_one(self):
        """排定時刻不合法時退回「當下減一天」，不讓排程整個掛掉。"""
        import web_server

        now = datetime(2026, 8, 19, 9, 0, 0)
        self.assertEqual(
            web_server.yt_transcript_target_date("壞掉的值", now), "2026-08-18"
        )

    def test_schedule_time_read_from_config(self):
        """未指定排定時刻時，改由設定檔取得。"""
        import web_server

        now = datetime(2026, 8, 19, 9, 0, 0)
        with patch.object(
            web_server, "load_config",
            return_value={"yt_transcript_schedule": {"time": "21:24"}},
        ):
            self.assertEqual(
                web_server.yt_transcript_target_date(now=now), "2026-08-17"
            )


class TestYTTranscriptScheduled(unittest.TestCase):
    """測試 YT 逐字稿排程任務（抓排程日的前一日影片）。"""

    def setUp(self):
        """每次測試前清空任務清單。"""
        import web_server
        web_server.upload_jobs.clear()

    @patch("web_server.job_queue")
    def test_scheduled_uses_target_date(self, mock_queue):
        """排程應以 yt_transcript_target_date 決定日期並帶入任務。"""
        import web_server

        with patch.object(
            web_server, "yt_transcript_target_date",
            return_value="2026-08-17",
        ):
            web_server.run_yt_transcript_scheduled()

        mock_queue.enqueue.assert_called_once()
        args = mock_queue.enqueue.call_args.args
        # enqueue(job_id, run_yt_transcript_upload_job, (job_id, 目標日))
        job_id, func, params = args
        self.assertEqual(func, web_server.run_yt_transcript_upload_job)
        self.assertEqual(params[1], "2026-08-17")
        # 任務紀錄的日期亦應為目標日
        self.assertEqual(web_server.upload_jobs[job_id]["date"], "2026-08-17")


if __name__ == "__main__":
    unittest.main()

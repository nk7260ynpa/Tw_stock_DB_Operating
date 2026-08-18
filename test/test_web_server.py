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
        # 帶 config_version：已完成一次性遷移的設定，既有值原樣保留（不再遷移）。
        config_data = {"schedule_time": "21:30",
                       "config_version": web_server.CONFIG_VERSION}

        with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
            result = web_server.load_config()

        # 向後相容：自動補上缺少的排程欄位
        self.assertEqual(result["schedule_time"], "21:30")
        self.assertIn("tdcc_schedule", result)
        self.assertIn("ctee_schedule", result)
        self.assertIn("cnyes_schedule", result)

    @patch("web_server.CONFIG_PATH")
    def test_load_config_file_not_exists(self, mock_path):
        """測試設定檔不存在時回傳預設值。"""
        import web_server

        mock_path.exists.return_value = False

        result = web_server.load_config()

        self.assertEqual(result["schedule_time"], "21:00")
        self.assertIn("tdcc_schedule", result)
        self.assertIn("ctee_schedule", result)
        self.assertIn("cnyes_schedule", result)


class TestScheduleDefaults(unittest.TestCase):
    """守門測試：程式碼預設排程時間必須完全等於商定的 21:00 時間表。

    時間值散落在 load_config 的 default、setup_schedule 的 .get() 後備值與
    13 支 GET/PUT 端點三處，任一處漏改都會在特定路徑上退回舊時間，故以單一
    真值表逐鍵比對。
    """

    # 商定時間表（相對間隔與舊 07:30 窗完全一致，整體後移 13.5 小時）。
    EXPECTED = {
        "schedule_time": "21:00",
        "tdcc_schedule": "21:03",
        "oil_price_schedule": "21:06",
        "gold_price_schedule": "21:08",
        "bitcoin_price_schedule": "21:10",
        "currency_price_schedule": "21:12",
        "indices_price_schedule": "21:14",
        "ctee_schedule": "21:16",
        "cnyes_schedule": "21:18",
        "ptt_schedule": "21:20",
        "moneyudn_schedule": "21:22",
        "yt_transcript_schedule": "21:24",
        "special_info_backfill_schedule": "21:27",
    }

    @patch("web_server.CONFIG_PATH")
    def test_defaults_match_agreed_table(self, mock_path):
        """無設定檔時取得的預設值應逐鍵等於商定時間表。"""
        import web_server

        mock_path.exists.return_value = False
        config = web_server.load_config()

        for key, expected in self.EXPECTED.items():
            actual = (config[key]["time"] if isinstance(config[key], dict)
                      else config[key])
            self.assertEqual(actual, expected, f"{key} 預設時間不符")

    def test_all_defaults_inside_crawl_window(self):
        """所有爬蟲抓取排程預設值都必須落在集中時間窗內。"""
        import web_server

        for key, expected in self.EXPECTED.items():
            self.assertTrue(
                web_server._in_crawl_window(expected),
                f"{key} 預設 {expected} 落在窗外",
            )

    def test_crawl_window_bounds(self):
        """時間窗常數本身應為 21:00~21:30。"""
        import web_server

        self.assertEqual(web_server.CRAWL_WINDOW_START, "21:00")
        self.assertEqual(web_server.CRAWL_WINDOW_END, "21:30")

    def test_schedule_times_are_distinct(self):
        """各排程時間必須彼此錯開，避免同時併發搶爬蟲資源。"""
        times = list(self.EXPECTED.values())
        self.assertEqual(len(times), len(set(times)), f"排程時間重複：{times}")


class TestCrawlScheduleMigration(unittest.TestCase):
    """測試爬蟲排程一次性遷移（21:00~21:30 時間窗）。"""

    @patch("web_server.save_config")
    @patch("web_server.CONFIG_PATH")
    def test_v2_morning_config_is_migrated(self, mock_path, mock_save):
        """v2（07:30 窗）設定應整批收斂到 21:00 窗的新預設並寫回。

        這是實機既有部署的狀態，也是本次搬窗最重要的路徑：若 CONFIG_VERSION
        沒遞增，持久化的 07:xx 會壓過新的程式碼預設，改了等於沒改。
        """
        import web_server

        mock_path.exists.return_value = True
        old_config = {
            "config_version": 2,
            "schedule_time": "07:30",
            "tdcc_schedule": {"time": "07:33"},
            "oil_price_schedule": {"time": "07:36"},
            "gold_price_schedule": {"time": "07:38"},
            "bitcoin_price_schedule": {"time": "07:40"},
            "currency_price_schedule": {"time": "07:42"},
            "indices_price_schedule": {"time": "07:44"},
            "ctee_schedule": {"time": "07:46"},
            "cnyes_schedule": {"time": "07:48"},
            "ptt_schedule": {"time": "07:50"},
            "moneyudn_schedule": {"time": "07:52"},
            "yt_transcript_schedule": {"time": "07:54"},
            "special_info_backfill_schedule": {"time": "07:57"},
        }

        with patch("builtins.open", unittest.mock.mock_open(read_data="{}")):
            with patch("json.load", return_value=old_config.copy()):
                config = web_server.load_config()

        expected = TestScheduleDefaults.EXPECTED
        self.assertEqual(config["schedule_time"], expected["schedule_time"])
        for key in web_server.CRAWL_SCHEDULE_KEYS:
            self.assertEqual(
                config[key]["time"], expected[key], f"{key} 未收斂到新預設"
            )
        self.assertEqual(config["config_version"], web_server.CONFIG_VERSION)
        mock_save.assert_called_once()

    @patch("web_server.save_config")
    @patch("web_server.CONFIG_PATH")
    def test_v1_evening_config_is_migrated(self, mock_path, mock_save):
        """v1（無 config_version）的晚間舊預設應全部收斂到新預設。

        v1 的 CTEE 21:00 與 CNYES 21:30 恰好落在新窗的兩個端點內，單靠
        _in_crawl_window 判不出來，必須由 _SUPERSEDED_IN_WINDOW_DEFAULTS 攔下；
        否則 CTEE 會停在 21:00、與 daily_craw 撞在同一分鐘。
        """
        import web_server

        mock_path.exists.return_value = True
        old_config = {
            "schedule_time": "19:07",
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
            "special_info_backfill_schedule": {"time": "08:00"},
        }

        with patch("builtins.open", unittest.mock.mock_open(read_data="{}")):
            with patch("json.load", return_value=old_config.copy()):
                config = web_server.load_config()

        expected = TestScheduleDefaults.EXPECTED
        self.assertEqual(config["schedule_time"], expected["schedule_time"])
        for key in web_server.CRAWL_SCHEDULE_KEYS:
            self.assertEqual(
                config[key]["time"], expected[key], f"{key} 未收斂到新預設"
            )
        self.assertEqual(config["config_version"], web_server.CONFIG_VERSION)
        mock_save.assert_called_once()

    @patch("web_server.save_config")
    @patch("web_server.CONFIG_PATH")
    def test_superseded_in_window_defaults_are_all_covered(
        self, mock_path, mock_save
    ):
        """_SUPERSEDED_IN_WINDOW_DEFAULTS 的每個舊值都必須被收斂掉。"""
        import web_server

        expected = TestScheduleDefaults.EXPECTED
        superseded_map = web_server._SUPERSEDED_IN_WINDOW_DEFAULTS
        for key, superseded in superseded_map.items():
            with self.subTest(key=key):
                # 前提：這些舊值確實落在窗內，才需要本機制攔截。
                self.assertTrue(web_server._in_crawl_window(superseded))
                mock_path.exists.return_value = True
                old_config = {"schedule_time": "19:07",
                              key: {"time": superseded}}
                with patch("builtins.open",
                           unittest.mock.mock_open(read_data="{}")):
                    with patch("json.load", return_value=old_config.copy()):
                        config = web_server.load_config()
                self.assertEqual(config[key]["time"], expected[key])

    # 全部歷史預設值（v1 / v2），用來把「盤點」變成可執行的守門：任何一個
    # 歷史值若未被收斂到新預設，代表 _SUPERSEDED_IN_WINDOW_DEFAULTS 漏列。
    HISTORICAL_DEFAULTS = {
        "schedule_time": ("20:07", "07:30"),
        "tdcc_schedule": ("10:00", "07:33"),
        "ctee_schedule": ("21:00", "07:46"),
        "cnyes_schedule": ("21:30", "07:48"),
        "ptt_schedule": ("22:00", "07:50"),
        "moneyudn_schedule": ("22:30", "07:52"),
        "yt_transcript_schedule": ("19:05", "07:54"),
        "oil_price_schedule": ("07:00", "07:36"),
        "gold_price_schedule": ("07:05", "07:38"),
        "bitcoin_price_schedule": ("07:10", "07:40"),
        "currency_price_schedule": ("07:15", "07:42"),
        "indices_price_schedule": ("07:20", "07:44"),
        "special_info_backfill_schedule": ("08:00", "07:57"),
    }

    @patch("web_server.save_config")
    @patch("web_server.CONFIG_PATH")
    def test_every_historical_default_is_migrated(self, mock_path, mock_save):
        """每個 v1／v2 歷史預設值都必須被收斂到新預設，一個都不能漏。

        _SUPERSEDED_IN_WINDOW_DEFAULTS 的既有測試只能證明「已列出的有效」，
        無法偵測漏列；本測試改由歷史值本身出發，漏列會直接紅燈。
        """
        import web_server

        expected = TestScheduleDefaults.EXPECTED
        mock_path.exists.return_value = True
        for key, olds in self.HISTORICAL_DEFAULTS.items():
            for old_value in olds:
                with self.subTest(key=key, old=old_value):
                    if key == "schedule_time":
                        old_config = {"schedule_time": old_value}
                    else:
                        old_config = {"schedule_time": "19:07",
                                      key: {"time": old_value}}
                    with patch("builtins.open",
                               unittest.mock.mock_open(read_data="{}")):
                        with patch("json.load",
                                   return_value=old_config.copy()):
                            config = web_server.load_config()
                    actual = (config[key] if key == "schedule_time"
                              else config[key]["time"])
                    self.assertEqual(
                        actual, expected[key],
                        f"{key} 的歷史值 {old_value} 未收斂到新預設"
                    )

    @patch("web_server.save_config")
    @patch("web_server.CONFIG_PATH")
    def test_migrated_config_preserves_user_custom(self, mock_path, mock_save):
        """已遷移（帶 config_version）的設定即使窗外也保留使用者自訂、不再遷移。"""
        import web_server

        mock_path.exists.return_value = True
        user_config = {
            "config_version": web_server.CONFIG_VERSION,
            "schedule_time": "21:05",
            "ctee_schedule": {"time": "23:00"},
        }

        with patch("builtins.open", unittest.mock.mock_open(read_data="{}")):
            with patch("json.load", return_value=user_config.copy()):
                config = web_server.load_config()

        # 版本相符 → 不觸發遷移，使用者自訂（含窗外）原樣保留、不寫回
        self.assertEqual(config["schedule_time"], "21:05")
        self.assertEqual(config["ctee_schedule"]["time"], "23:00")
        mock_save.assert_not_called()

    @patch("web_server.save_config")
    @patch("web_server.CONFIG_PATH")
    def test_migration_keeps_in_window_values(self, mock_path, mock_save):
        """遷移時已落在新窗內的自訂值應被保留，不覆寫為預設。"""
        import web_server

        mock_path.exists.return_value = True
        old_config = {
            "schedule_time": "19:07",
            "ctee_schedule": {"time": "21:17"},
        }

        with patch("builtins.open", unittest.mock.mock_open(read_data="{}")):
            with patch("json.load", return_value=old_config.copy()):
                config = web_server.load_config()

        self.assertEqual(config["schedule_time"], "21:00")
        # 窗內自訂 21:17 應保留（非重置為預設 21:16）
        self.assertEqual(config["ctee_schedule"]["time"], "21:17")


class TestSaveConfig(unittest.TestCase):
    """測試 save_config 函式。"""

    def test_save_config_writes_json(self):
        """測試正確寫入 JSON 設定檔。

        save_config 改為「暫存檔 + os.replace」原子寫入後，open 的對象是暫存檔而非
        CONFIG_PATH，故改為直接斷言最終落地的檔案內容（conftest 已把設定路徑導向
        測試專用暫存目錄）。
        """
        import web_server

        web_server.save_config({"schedule_time": "22:00"})

        written = web_server.CONFIG_PATH.read_text(encoding="utf-8")
        self.assertIn("22:00", written)


class TestSetupSchedule(unittest.TestCase):
    """測試 setup_schedule 函式。"""

    @patch("web_server.schedule_lib")
    def test_setup_schedule_clears_and_sets(self, mock_schedule):
        """測試設定排程時先清除再建立新排程。"""
        import web_server

        web_server.setup_schedule("18:00")

        mock_schedule.clear.assert_called_once()
        # 每日主排程 18:00 與 exhausted 隔日重排 06:30 皆應登記
        mock_schedule.every.return_value.day.at.assert_any_call("18:00")
        mock_schedule.every.return_value.day.at.assert_any_call(
            web_server.REQUEUE_EXHAUSTED_TIME
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
        mock_setup.assert_called_once()
        args = mock_setup.call_args[0]
        self.assertEqual(args[0], "21:30")

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

    @patch("web_server.job_queue")
    def test_create_upload_success(self, mock_queue):
        """測試成功建立上傳任務。"""
        mock_queue.enqueue.return_value = 0

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
        self.assertEqual(data["status"], "queued")

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

    @patch("web_server.job_queue")
    def test_create_upload_queues_when_running(self, mock_queue):
        """測試已有執行中任務時排入佇列。"""
        import web_server

        mock_queue.enqueue.return_value = 1

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

        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["status"], "queued")
        self.assertIn("queue_position", data)

    def test_list_upload_jobs_empty(self):
        """測試無任務時回傳空清單。"""
        res = self.client.get("/api/upload/jobs")

        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), [])

    @patch("web_server.job_queue")
    def test_list_upload_jobs_with_data(self, mock_queue):
        """測試有任務時回傳任務清單。"""
        mock_queue.enqueue.return_value = 0

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

    @patch("web_server.job_queue")
    def test_get_upload_status_found(self, mock_queue):
        """測試查詢已存在的任務回傳正確狀態。"""
        mock_queue.enqueue.return_value = 0

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

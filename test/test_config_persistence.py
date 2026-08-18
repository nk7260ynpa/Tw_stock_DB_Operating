"""持久化設定位置與相容遷移的單元測試。

守住三件事（皆為曾實際踩過或高風險的坑）：

1. 設定檔**不得**放在 log 目錄下：部署把 logs/ 掛成具名 volume、手動 run.sh 掛 host
   目錄，設定寄生其中就會隨掛載方式靜默消失。
2. 舊位置（logs/config.json）的既有設定必須被一次性搬遷，且**原樣保留**
   config_version，讓既有的排程時間窗遷移語意不被打亂。
3. 新舊並存時的優先順序明確（新位置優先），且兩邊都沒有時退回程式碼預設值。
"""

import builtins
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import web_server

# 於 import 當下（早於 conftest 的路徑隔離夾具）記下「未經 patch 的預設值」，
# 供結構性檢查使用。
DEFAULT_CONFIG_DIR = web_server.CONFIG_DIR
DEFAULT_CONFIG_PATH = web_server.CONFIG_PATH
DEFAULT_LOG_DIR = web_server.LOG_DIR

# repo 根目錄（本檔位於 <repo>/test/）。
REPO_ROOT = Path(__file__).resolve().parent.parent


class ConfigPathTestCase(unittest.TestCase):
    """提供「以暫存目錄取代真實設定路徑」的共用 setUp。"""

    def setUp(self):
        """建立暫存的設定目錄與舊 log 目錄，並改寫模組層路徑常數。"""
        self._tmp = tempfile.TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self.config_dir = tmp_path / "config"
        self.log_dir = tmp_path / "logs"
        self.config_dir.mkdir()
        self.log_dir.mkdir()
        self.config_path = self.config_dir / "config.json"
        self.legacy_path = self.log_dir / "config.json"
        self.legacy_backup_path = self.log_dir / "config.json.migrated"

        patchers = [
            patch.object(web_server, "CONFIG_DIR", self.config_dir),
            patch.object(web_server, "CONFIG_PATH", self.config_path),
            patch.object(web_server, "LEGACY_CONFIG_PATH", self.legacy_path),
            patch.object(
                web_server, "LEGACY_CONFIG_BACKUP_PATH", self.legacy_backup_path
            ),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.addCleanup(self._tmp.cleanup)

    def write_legacy(self, config):
        """把設定寫入舊位置（logs/config.json）。

        Args:
            config (dict): 設定內容。
        """
        self.legacy_path.write_text(
            json.dumps(config, ensure_ascii=False), encoding="utf-8"
        )

    def write_current(self, config):
        """把設定寫入新位置（config/config.json）。

        Args:
            config (dict): 設定內容。
        """
        self.config_path.write_text(
            json.dumps(config, ensure_ascii=False), encoding="utf-8"
        )


class TestConfigLocation(unittest.TestCase):
    """測試設定檔位置本身（結構性防呆，避免有人搬回 log 目錄）。"""

    def test_config_path_is_not_under_log_dir(self):
        """設定檔不得位於 log 目錄下。"""
        self.assertNotEqual(DEFAULT_CONFIG_PATH.parent, DEFAULT_LOG_DIR)
        self.assertNotIn(
            DEFAULT_LOG_DIR.resolve(),
            list(DEFAULT_CONFIG_PATH.resolve().parents),
            "設定檔又被放回 log 目錄，具名 volume 部署會使設定靜默丟失。",
        )

    def test_config_dir_defaults_to_workspace_config(self):
        """預設設定目錄為專案根目錄下的 config/。"""
        self.assertEqual(DEFAULT_CONFIG_DIR, web_server.BASE_DIR / "config")

    def test_config_dir_can_be_overridden_by_env(self):
        """CONFIG_DIR 環境變數可覆寫設定目錄（於全新直譯器中驗證）。"""
        with tempfile.TemporaryDirectory() as tmp:
            override = Path(tmp) / "custom_config"
            env = dict(os.environ, CONFIG_DIR=str(override))
            result = subprocess.run(
                [sys.executable, "-c",
                 "import web_server; print(web_server.CONFIG_PATH)"],
                cwd=str(web_server.BASE_DIR), env=env,
                capture_output=True, text=True, check=True,
            )
            self.assertEqual(
                result.stdout.strip(), str(override / "config.json")
            )
            self.assertTrue(override.is_dir(), "設定目錄應於啟動時自動建立。")


class TestConfigReadWrite(ConfigPathTestCase):
    """測試新位置的讀寫。"""

    def test_save_then_load_round_trip(self):
        """save_config 寫入新位置後，load_config 應讀回相同內容。"""
        config = dict(web_server.load_config())
        config["schedule_time"] = "21:15"

        web_server.save_config(config)

        self.assertTrue(self.config_path.exists())
        self.assertFalse(self.legacy_path.exists(),
                         "不應再寫入舊的 log 目錄位置。")
        self.assertEqual(web_server.load_config()["schedule_time"], "21:15")

    def test_save_config_creates_missing_directory(self):
        """設定目錄不存在時，save_config 應自動建立而非拋錯。"""
        for path in self.config_dir.iterdir():
            path.unlink()
        self.config_dir.rmdir()

        web_server.save_config({"config_version": web_server.CONFIG_VERSION,
                                "schedule_time": "07:30"})

        self.assertTrue(self.config_path.exists())

    def test_load_config_returns_defaults_when_nothing_exists(self):
        """新舊位置都沒有設定時回傳程式碼預設值，且不建立檔案。"""
        config = web_server.load_config()

        self.assertEqual(config["config_version"], web_server.CONFIG_VERSION)
        self.assertEqual(config["schedule_time"], "07:30")
        self.assertFalse(self.config_path.exists(),
                         "沒有既有設定時不應憑空寫出設定檔。")


class TestConfigWriteDurability(ConfigPathTestCase):
    """測試設定寫入的原子性與毀損設定檔的處理。"""

    def test_save_config_is_atomic_on_failure(self):
        """寫入途中失敗時，既有設定檔必須原封不動且不留殘骸。"""
        self.write_current({"config_version": web_server.CONFIG_VERSION,
                            "schedule_time": "07:30"})
        before = self.config_path.read_text(encoding="utf-8")

        with patch("json.dump", side_effect=OSError("磁碟空間不足")):
            with self.assertRaises(OSError):
                web_server.save_config({"schedule_time": "19:30"})

        self.assertEqual(self.config_path.read_text(encoding="utf-8"), before,
                         "寫入失敗不應動到既有設定檔。")
        leftovers = [item.name for item in self.config_dir.iterdir()
                     if item.name.endswith(".tmp")]
        self.assertEqual(leftovers, [], f"不應殘留暫存檔：{leftovers}")

    def test_corrupt_config_is_quarantined_and_defaults_used(self):
        """毀損的設定檔應被改名隔離，服務改用預設值而非啟動失敗。"""
        self.config_path.write_text('{"schedule_time": "07:3', encoding="utf-8")

        with self.assertLogs("web_server", level="ERROR"):
            config = web_server.load_config()

        self.assertEqual(config["schedule_time"], "07:30")
        self.assertFalse(self.config_path.exists())
        corrupt = self.config_path.with_name(self.config_path.name + ".corrupt")
        self.assertTrue(corrupt.exists(), "毀損檔應改名保留供人工檢視。")

    def test_invalid_utf8_config_is_quarantined(self):
        """非 UTF-8 位元組（UnicodeDecodeError）也算毀損，須被隔離。"""
        self.config_path.write_bytes(b'{"schedule_time": "\xff\xfe"}')

        with self.assertLogs("web_server", level="ERROR"):
            config = web_server.load_config()

        self.assertEqual(config["schedule_time"], "07:30")
        self.assertTrue(
            self.config_path.with_name("config.json.corrupt").exists()
        )

    def test_non_object_config_is_quarantined(self):
        """頂層不是 JSON 物件（如 list／int）也算毀損，不可讓它往下炸成 TypeError。"""
        for payload in ("[]", "123", '"07:30"'):
            with self.subTest(payload=payload):
                corrupt = self.config_path.with_name("config.json.corrupt")
                if corrupt.exists():
                    corrupt.unlink()
                self.config_path.write_text(payload, encoding="utf-8")

                with self.assertLogs("web_server", level="ERROR"):
                    config = web_server.load_config()

                self.assertEqual(config["schedule_time"], "07:30")
                self.assertTrue(corrupt.exists(), "非物件內容應被隔離。")

    def test_unreadable_config_is_not_quarantined(self):
        """讀不到（OSError）不等於內容壞掉：絕不改名隔離，原檔須原封不動。"""
        self.write_current({"config_version": web_server.CONFIG_VERSION,
                            "schedule_time": "20:15"})
        before = self.config_path.read_text(encoding="utf-8")

        real_open = builtins.open

        def fake_open(file, *args, **kwargs):
            if str(file) == str(self.config_path):
                raise PermissionError("權限不足")
            return real_open(file, *args, **kwargs)

        with patch("builtins.open", side_effect=fake_open):
            with self.assertLogs("web_server", level="ERROR") as captured:
                config = web_server.load_config()

        self.assertEqual(config["schedule_time"], "07:30",
                         "讀不到時本輪退回預設值。")
        self.assertTrue(self.config_path.exists(), "原設定檔不可被改名或刪除。")
        self.assertEqual(self.config_path.read_text(encoding="utf-8"), before)
        self.assertFalse(
            self.config_path.with_name("config.json.corrupt").exists(),
            "讀取失敗不得把完好的設定改名為 .corrupt。",
        )
        self.assertIn("保留原檔不動", "\n".join(captured.output))

    def test_unserializable_config_leaves_no_temp_file(self):
        """json.dump 遇不可序列化物件（TypeError）時也要清掉暫存檔。"""
        self.write_current({"config_version": web_server.CONFIG_VERSION,
                            "schedule_time": "07:30"})

        with self.assertRaises(TypeError):
            web_server.save_config({"schedule_time": object()})

        leftovers = [item.name for item in self.config_dir.iterdir()
                     if item.name.endswith(".tmp")]
        self.assertEqual(leftovers, [], f"不應殘留暫存檔：{leftovers}")

    def test_temp_file_name_is_unique_per_write(self):
        """每次寫入須用不同的暫存檔名，避免併發寫入互相覆蓋。

        排程端點是同步 def（跑在 FastAPI threadpool），前端首屏會併發呼叫多個
        load_config／save_config；共用固定暫存檔名時，兩個執行緒會寫壞對方的暫存檔，
        原子替換也就失去意義。
        """
        real_dump = json.dump
        names = []

        def record_dump(obj, fp, **kwargs):
            names.append(fp.name)
            return real_dump(obj, fp, **kwargs)

        with patch("json.dump", side_effect=record_dump):
            web_server.save_config({"schedule_time": "07:30"})
            web_server.save_config({"schedule_time": "07:31"})

        self.assertEqual(len(names), 2)
        for name in names:
            self.assertTrue(name.endswith(".tmp"), name)
        self.assertNotEqual(names[0], names[1],
                            f"每次寫入的暫存檔名不可相同：{names}")

    def test_bad_field_types_are_repaired_to_defaults(self):
        """第二層型別／格式錯須就地換成預設值，而非讓服務卡在重啟迴圈。

        這些值一路傳到 setup_schedule 才爆炸的話，--restart always 下就是重啟
        迴圈；修復而非隔離則能保住同一份設定中其餘正常的使用者自訂。
        """
        cases = (
            ({"tdcc_schedule": None}, "tdcc_schedule"),
            ({"tdcc_schedule": 5}, "tdcc_schedule"),
            ({"tdcc_schedule": "sunday 07:33"}, "tdcc_schedule"),
            ({"tdcc_schedule": ["day"]}, "tdcc_schedule"),
            ({"tdcc_schedule": {"time": 5}}, "tdcc_schedule"),
            ({"ctee_schedule": {"time": "26:99"}}, "ctee_schedule"),
            ({"schedule_time": None}, "schedule_time"),
            ({"schedule_time": "sunday"}, "schedule_time"),
            ({"config_version": "3"}, "config_version"),
        )
        corrupt = self.config_path.with_name("config.json.corrupt")
        for payload, broken_key in cases:
            with self.subTest(payload=payload):
                # 同一組壞欄位的 warning 會去重，逐個 payload 重設旗標。
                web_server._repaired_fields_warned = ()
                self.write_current(payload)

                with self.assertLogs("web_server", level="WARNING") as captured:
                    config = web_server.load_config()

                self.assertEqual(config["schedule_time"], "07:30")
                self.assertEqual(config["tdcc_schedule"], {"time": "07:33"})
                self.assertEqual(config["config_version"],
                                 web_server.CONFIG_VERSION)
                self.assertIn(broken_key, "\n".join(captured.output))
                self.assertFalse(corrupt.exists(),
                                 "格式錯應就地修復，不該隔離整份設定。")

    def test_repair_keeps_other_user_settings(self):
        """修復壞欄位時，同一份設定中其餘合法的自訂必須保留。"""
        self.write_current({
            "config_version": web_server.CONFIG_VERSION,
            "schedule_time": "07:31",
            "tdcc_schedule": None,
            "ctee_schedule": {"time": "07:47"},
        })

        with self.assertLogs("web_server", level="WARNING"):
            config = web_server.load_config()

        self.assertEqual(config["schedule_time"], "07:31")
        self.assertEqual(config["ctee_schedule"], {"time": "07:47"})
        self.assertEqual(config["tdcc_schedule"], {"time": "07:33"})

    def test_version_2_config_with_missing_keys_starts(self):
        """已是新版本號但欄位殘缺時仍須補齊（形狀正規化不受 version gate 拘束）。"""
        self.write_current({"config_version": web_server.CONFIG_VERSION})

        config = web_server.load_config()

        self.assertEqual(config["schedule_time"], "07:30")
        for key in web_server.CRAWL_SCHEDULE_KEYS:
            self.assertIsInstance(config[key], dict)
            self.assertRegex(config[key]["time"], r"^\d{2}:\d{2}$")

    def test_repair_warning_logged_once_per_field_set(self):
        """同一組壞欄位不該每次 load_config 都刷 warning，換一組時仍要再警告。"""
        self.write_current({"config_version": web_server.CONFIG_VERSION,
                            "tdcc_schedule": None})

        with self.assertLogs("web_server", level="WARNING"):
            web_server.load_config()

        with self.assertLogs("web_server", level="DEBUG") as second:
            web_server.load_config()
        repeated = [line for line in second.output
                    if line.startswith("WARNING") and "格式不符" in line]
        self.assertEqual(repeated, [], f"warning 不應重複記錄：{repeated}")

        # 換一組壞欄位（使用者又改壞別的）→ 仍須警告
        self.write_current({"config_version": web_server.CONFIG_VERSION,
                            "ctee_schedule": 5})
        with self.assertLogs("web_server", level="WARNING") as third:
            web_server.load_config()
        self.assertIn("ctee_schedule", "\n".join(third.output))

    def test_unexpected_normalize_failure_is_quarantined(self):
        """保險絲：正規化仍拋出預期外例外時，隔離毀損檔並退回預設值。"""
        self.write_current({"schedule_time": "07:31"})

        with patch.object(web_server, "_normalize_config",
                          side_effect=AttributeError("預期外結構")):
            with self.assertLogs("web_server", level="ERROR"):
                config = web_server.load_config()

        self.assertEqual(config["schedule_time"], "07:30")
        self.assertTrue(
            self.config_path.with_name("config.json.corrupt").exists(),
            "預期外例外應隔離設定檔，避免卡在重啟迴圈。",
        )

    def test_write_back_failure_does_not_break_startup(self):
        """一次性遷移寫回失敗時，服務仍須以記憶體內的設定繼續啟動。"""
        self.write_current({"config_version": 1, "schedule_time": "23:00"})

        with patch.object(web_server, "save_config",
                          side_effect=OSError("唯讀檔案系統")):
            with self.assertLogs("web_server", level="ERROR") as captured:
                config = web_server.load_config()

        # 遷移結果仍在記憶體內生效（窗外的 23:00 收斂回 07:30）
        self.assertEqual(config["schedule_time"], "07:30")
        self.assertEqual(config["config_version"], web_server.CONFIG_VERSION)
        self.assertIn("以記憶體內的設定繼續執行", "\n".join(captured.output))

    def test_corrupt_config_lets_legacy_migration_run(self):
        """毀損的新設定被隔離後，舊位置的設定應能補上（不被永久遮蔽）。"""
        self.config_path.write_text("{ 壞掉了", encoding="utf-8")
        self.write_legacy({"config_version": web_server.CONFIG_VERSION,
                           "schedule_time": "07:31"})

        with self.assertLogs("web_server", level="ERROR"):
            config = web_server.load_config()

        self.assertEqual(config["schedule_time"], "07:31")
        self.assertTrue(self.config_path.exists())


class TestLegacyCoexistWarningOnce(ConfigPathTestCase):
    """測試「新舊並存」warning 只記一次，避免刷爆 log。"""

    def test_warning_logged_only_once(self):
        """load_config 被反覆呼叫時不應每次都記 warning。"""
        self.write_current({"config_version": web_server.CONFIG_VERSION,
                            "schedule_time": "07:30"})
        self.write_legacy({"schedule_time": "19:07"})

        with self.assertLogs("web_server", level="WARNING") as first:
            web_server.load_config()
        self.assertTrue(any("新舊位置同時存在" in line for line in first.output))

        with self.assertLogs("web_server", level="DEBUG") as second:
            web_server.load_config()
        repeated = [line for line in second.output
                    if line.startswith("WARNING") and "新舊位置同時存在" in line]
        self.assertEqual(repeated, [], f"warning 不應重複記錄：{repeated}")


class TestLegacyConfigMigration(ConfigPathTestCase):
    """測試舊位置（logs/config.json）的一次性遷移。"""

    def test_legacy_config_is_migrated_to_new_location(self):
        """舊位置有、新位置沒有時，應原樣搬遷並保留使用者自訂。"""
        legacy = {"config_version": web_server.CONFIG_VERSION,
                  "schedule_time": "07:31",
                  "ctee_schedule": {"time": "07:47"}}
        self.write_legacy(legacy)

        migrated = web_server.migrate_legacy_config()

        self.assertTrue(migrated)
        saved = json.loads(self.config_path.read_text(encoding="utf-8"))
        self.assertEqual(saved["schedule_time"], "07:31")
        self.assertEqual(saved["ctee_schedule"], {"time": "07:47"})
        self.assertEqual(saved["config_version"], web_server.CONFIG_VERSION)

    def test_migration_keeps_legacy_file_as_backup(self):
        """搬遷後舊檔改名保留備份，原檔名不再存在（避免重複判讀）。"""
        self.write_legacy({"config_version": web_server.CONFIG_VERSION,
                           "schedule_time": "07:31"})

        web_server.migrate_legacy_config()

        self.assertFalse(self.legacy_path.exists())
        self.assertTrue(self.legacy_backup_path.exists())

    def test_migration_is_idempotent(self):
        """再次啟動不應重複搬遷（舊檔已改名，且新位置內容不被覆蓋）。"""
        self.write_legacy({"config_version": web_server.CONFIG_VERSION,
                           "schedule_time": "07:31"})
        web_server.migrate_legacy_config()

        web_server.save_config({"config_version": web_server.CONFIG_VERSION,
                                "schedule_time": "22:00"})
        self.assertFalse(web_server.migrate_legacy_config())
        self.assertEqual(web_server.load_config()["schedule_time"], "22:00")

    def test_load_config_triggers_migration(self):
        """load_config 應自行觸發搬遷，讓服務啟動即沿用舊設定。"""
        self.write_legacy({"config_version": web_server.CONFIG_VERSION,
                           "schedule_time": "07:31"})

        config = web_server.load_config()

        self.assertEqual(config["schedule_time"], "07:31")
        self.assertTrue(self.config_path.exists())

    def test_migration_preserves_config_version_semantics(self):
        """搬遷不得竄改 config_version：舊版設定搬過來後仍須觸發窗遷移。"""
        self.write_legacy({
            "schedule_time": "19:07",                 # v1 舊時段，無 config_version
            "ctee_schedule": {"time": "21:00"},
        })

        config = web_server.load_config()

        # 版本遞補到最新，且落在窗外的舊時段被收斂進 07:30~08:00。
        self.assertEqual(config["config_version"], web_server.CONFIG_VERSION)
        self.assertTrue(web_server._in_crawl_window(config["schedule_time"]))
        self.assertTrue(
            web_server._in_crawl_window(config["ctee_schedule"]["time"])
        )
        saved = json.loads(self.config_path.read_text(encoding="utf-8"))
        self.assertEqual(saved["config_version"], web_server.CONFIG_VERSION)

    def test_new_location_wins_when_both_exist(self):
        """新舊位置同時存在時以新位置為準，且不動舊檔、記錄 warning。"""
        self.write_current({"config_version": web_server.CONFIG_VERSION,
                            "schedule_time": "07:35"})
        self.write_legacy({"config_version": web_server.CONFIG_VERSION,
                           "schedule_time": "19:07"})

        with self.assertLogs("web_server", level="WARNING") as captured:
            migrated = web_server.migrate_legacy_config()

        self.assertFalse(migrated)
        self.assertIn("新舊位置同時存在", "\n".join(captured.output))
        self.assertEqual(web_server.load_config()["schedule_time"], "07:35")
        # 舊檔保持原樣（不刪、不改名），保留人工比對的機會。
        legacy = json.loads(self.legacy_path.read_text(encoding="utf-8"))
        self.assertEqual(legacy["schedule_time"], "19:07")

    def test_failed_write_keeps_legacy_file(self):
        """新位置寫入失敗時必須保留舊檔（下次啟動可再試，設定不被丟掉）。"""
        self.write_legacy({"config_version": web_server.CONFIG_VERSION,
                           "schedule_time": "07:31"})

        with patch.object(web_server, "save_config",
                          side_effect=OSError("唯讀檔案系統")):
            with self.assertLogs("web_server", level="ERROR"):
                migrated = web_server.migrate_legacy_config()

        self.assertFalse(migrated)
        self.assertTrue(self.legacy_path.exists(), "搬遷失敗不應動舊檔。")
        self.assertFalse(self.legacy_backup_path.exists())
        self.assertFalse(self.config_path.exists())

    def test_broken_legacy_config_falls_back_to_defaults(self):
        """舊設定檔毀損時不應讓服務啟動失敗，退回預設值並告警。"""
        self.legacy_path.write_text("{ not json", encoding="utf-8")

        with self.assertLogs("web_server", level="WARNING"):
            config = web_server.load_config()

        self.assertEqual(config["schedule_time"], "07:30")
        self.assertFalse(self.config_path.exists())


class TestLegacyReadFailureWarnOnce(ConfigPathTestCase):
    """測試舊設定檔毀損時的 warning 也只記一次。"""

    def test_broken_legacy_warning_logged_only_once(self):
        """load_config 被反覆呼叫時，毀損舊檔不應每次都刷 warning。"""
        self.legacy_path.write_text("{ 壞掉了", encoding="utf-8")

        with self.assertLogs("web_server", level="WARNING") as first:
            web_server.load_config()
        self.assertTrue(any("讀取舊設定檔" in line for line in first.output))

        with self.assertLogs("web_server", level="DEBUG") as second:
            web_server.load_config()
        repeated = [line for line in second.output
                    if line.startswith("WARNING") and "讀取舊設定檔" in line]
        self.assertEqual(repeated, [], f"warning 不應重複記錄：{repeated}")


class TestScheduleEndpointTimeValidation(unittest.TestCase):
    """端點驗證須與 load_config 的形狀正規化使用同一判準。

    端點放行、重啟卻被判為格式不符而換回預設值的話，使用者的設定等於被靜默
    丟棄——正是本 repo 要杜絕的失敗模式。
    """

    ENDPOINTS = (
        "/api/schedule",
        "/api/ctee-news/schedule",
        "/api/tdcc/schedule",
    )

    def setUp(self):
        """建立測試用 HTTP client。"""
        from fastapi.testclient import TestClient

        self.client = TestClient(web_server.app)

    def test_loosely_formatted_times_are_rejected(self):
        """`_is_valid_time` 判為不合法者，端點必須回 400 而非存進設定檔。"""
        for endpoint in self.ENDPOINTS:
            for value in ("07:30:00", "7:30", "07:5", "24:00", " 07:30"):
                with self.subTest(endpoint=endpoint, time=value):
                    self.assertFalse(web_server._is_valid_time(value))
                    res = self.client.put(endpoint, json={"time": value})
                    self.assertEqual(res.status_code, 400)

    def test_no_endpoint_uses_loose_time_parsing(self):
        """守門：任何排程端點都不得改回寬鬆的 split(":") 自行解析。"""
        source = REPO_ROOT.joinpath("web_server.py").read_text(encoding="utf-8")
        self.assertNotIn(
            'time_parts = req.time.split(":")', source,
            "排程時間驗證請統一改用 _is_valid_time。",
        )


class TestDeploymentMountsAgreement(unittest.TestCase):
    """測試 run.sh 與 CI deploy 掛載同一個設定位置（防止兩條路徑再度分岔）。

    斷言的是「掛載效果」（哪個 host 路徑對到容器內哪個路徑），而非某個變數叫什麼
    名字：日後改寫變數命名但行為正確時不應紅燈，行為真的分岔時才要紅燈。
    """

    CONTAINER_CONFIG_PATH = "/workspace/config"
    HOST_CONFIG_SUFFIX = "Tw_stock_DB_Operating/config"

    _VAR_RE = re.compile(r"\$\{?(\w+)\}?")
    _MOUNT_RE = re.compile(r"(?:-v|--volume)\s+\"?([^\"\s]+)\"?")

    def setUp(self):
        """讀入 run.sh 與 .gitlab-ci.yml 內容。"""
        base = web_server.BASE_DIR
        self.run_sh = (base / "run.sh").read_text(encoding="utf-8")
        self.ci_yml = (base / ".gitlab-ci.yml").read_text(encoding="utf-8")

    @classmethod
    def _assignments(cls, text):
        """蒐集檔案內可靜態解析的變數指派（shell 的 A="b" 與 YAML 的 A: "b"）。

        Args:
            text (str): 檔案內容。

        Returns:
            dict[str, str]: 變數名對應值；無法靜態解析者（如命令替換）自然不會入列。
        """
        assigns = {}
        assigns.update(re.findall(r'^\s*(\w+)="([^"\n]*)"\s*$', text, re.M))
        assigns.update(re.findall(r'^\s+(\w+):\s+"([^"\n]+)"', text, re.M))
        return assigns

    @classmethod
    def _expand(cls, value, assigns):
        """反覆展開 $VAR / ${VAR}，無法解析者原樣保留。

        Args:
            value (str): 待展開字串。
            assigns (dict[str, str]): 變數表。

        Returns:
            str: 展開後字串。
        """
        for _ in range(5):
            expanded = cls._VAR_RE.sub(
                lambda m: assigns.get(m.group(1), m.group(0)), value
            )
            if expanded == value:
                break
            value = expanded
        return value

    @classmethod
    def _mounts(cls, text):
        """取出所有 (host, container) 掛載對並展開變數。

        Args:
            text (str): run.sh 或 .gitlab-ci.yml 內容。

        Returns:
            list[tuple[str, str]]: 掛載對清單。
        """
        assigns = cls._assignments(text)
        mounts = []
        for spec in cls._MOUNT_RE.findall(text):
            expanded = cls._expand(spec, assigns)
            if expanded.count(":") >= 1:
                host, _, container = expanded.rpartition(":")
                mounts.append((host, container))
        return mounts

    def _config_mount_of(self, text):
        """取出唯一一個對到容器設定路徑的掛載，並斷言確實只有一個。

        Args:
            text (str): 檔案內容。

        Returns:
            str: 該掛載的 host 側路徑。
        """
        hosts = [host for host, container in self._mounts(text)
                 if container == self.CONTAINER_CONFIG_PATH]
        self.assertEqual(
            len(hosts), 1,
            f"應恰有一個掛載對到 {self.CONTAINER_CONFIG_PATH}，實際：{hosts}",
        )
        return hosts[0]

    def test_run_sh_mounts_repo_config_dir(self):
        """run.sh 需把「腳本所在 repo 的 config/」掛到容器設定路徑。"""
        host = self._config_mount_of(self.run_sh)
        self.assertTrue(host.endswith("/config"), f"host 側非 config 目錄：{host}")
        self.assertIn("SCRIPT_DIR", host,
                      f"run.sh 的設定掛載應相對於腳本所在目錄：{host}")

    def test_ci_deploy_mounts_same_host_config_dir(self):
        """CI deploy 需掛載 host 上同一個 repo 的 config/（絕對路徑）。"""
        host = self._config_mount_of(self.ci_yml)
        self.assertTrue(host.startswith("/"), f"CI 掛載須為絕對路徑：{host}")
        self.assertTrue(
            host.endswith(self.HOST_CONFIG_SUFFIX),
            f"CI 設定掛載的 host 路徑應為 {self.HOST_CONFIG_SUFFIX}，實際：{host}",
        )

    def test_two_launch_paths_agree_on_container_config_path(self):
        """兩條啟動路徑必須指向容器內同一個設定目錄（分岔就是本 issue 的病灶）。"""
        run_sh_containers = {c for _, c in self._mounts(self.run_sh)}
        ci_containers = {c for _, c in self._mounts(self.ci_yml)}
        self.assertIn(self.CONTAINER_CONFIG_PATH, run_sh_containers)
        self.assertIn(self.CONTAINER_CONFIG_PATH, ci_containers)

    def test_deployment_does_not_redirect_config_dir_env(self):
        """兩處都不得用 CONFIG_DIR 把設定又導回 log 目錄（掛載對了也會破功）。

        比對前先展開變數，避免 `-e CONFIG_DIR=$LOG_DIR` 這種間接寫法漏網。
        """
        log_dir_name = DEFAULT_LOG_DIR.name
        for name, text in (("run.sh", self.run_sh), (".gitlab-ci.yml", self.ci_yml)):
            with self.subTest(file=name):
                assigns = self._assignments(text)
                overrides = re.findall(r'CONFIG_DIR[=:]\s*"?([^"\s]+)', text)
                for value in overrides:
                    expanded = self._expand(value, assigns)
                    self.assertNotIn(
                        log_dir_name, expanded.split("/"),
                        f"{name} 把 CONFIG_DIR 導向 log 目錄："
                        f"{value} → {expanded}",
                    )
                    self.assertNotIn(
                        "LOG_DIR", expanded,
                        f"{name} 把 CONFIG_DIR 導向 log 目錄變數：{value}",
                    )

    def test_config_is_not_served_by_logs_volume(self):
        """設定不得再由 logs 具名 volume 提供（原本被靜默丟棄的根因）。"""
        log_hosts = {host for host, container in self._mounts(self.ci_yml)
                     if container.endswith("/logs")}
        config_host = self._config_mount_of(self.ci_yml)
        self.assertNotIn(config_host, log_hosts)
        self.assertNotIn("$LOGS_VOLUME:/workspace/config", self.ci_yml)


if __name__ == "__main__":
    unittest.main()

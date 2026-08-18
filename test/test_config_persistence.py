"""持久化設定位置與相容遷移的單元測試。

守住三件事（皆為曾實際踩過或高風險的坑）：

1. 設定檔**不得**放在 log 目錄下：部署把 logs/ 掛成具名 volume、手動 run.sh 掛 host
   目錄，設定寄生其中就會隨掛載方式靜默消失。
2. 舊位置（logs/config.json）的既有設定必須被一次性搬遷，且**原樣保留**
   config_version，讓既有的排程時間窗遷移語意不被打亂。
3. 新舊並存時的優先順序明確（新位置優先），且兩邊都沒有時退回程式碼預設值。
"""

import json
import os
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


class TestDeploymentMountsAgreement(unittest.TestCase):
    """測試 run.sh 與 CI deploy 掛載同一個設定位置（防止兩條路徑再度分岔）。"""

    CONTAINER_CONFIG_PATH = "/workspace/config"
    HOST_CONFIG_DIR = "Tw_stock_DB_Operating/config"

    def setUp(self):
        """讀入 run.sh 與 .gitlab-ci.yml 內容。"""
        base = web_server.BASE_DIR
        self.run_sh = (base / "run.sh").read_text(encoding="utf-8")
        self.ci_yml = (base / ".gitlab-ci.yml").read_text(encoding="utf-8")

    def test_run_sh_mounts_config_dir(self):
        """run.sh 需把 host 的 config/ 掛進容器設定路徑。"""
        self.assertIn(f'-v "${{CONFIG_DIR}}:{self.CONTAINER_CONFIG_PATH}"',
                      self.run_sh)
        self.assertIn('CONFIG_DIR="${SCRIPT_DIR}/config"', self.run_sh)

    def test_ci_deploy_mounts_same_host_config_dir(self):
        """CI deploy 需掛載與 run.sh 相同的 host 絕對路徑。"""
        self.assertIn(f'CONFIG_PATH: "{self.CONTAINER_CONFIG_PATH}"',
                      self.ci_yml)
        self.assertIn(
            f'--volume "${{HOST_ROOT}}/{self.HOST_CONFIG_DIR}:$CONFIG_PATH"',
            self.ci_yml,
        )

    def test_ci_does_not_put_config_in_logs_volume(self):
        """設定不得再被塞進 logs 具名 volume。"""
        self.assertNotIn("$LOGS_VOLUME:/workspace/config", self.ci_yml)


if __name__ == "__main__":
    unittest.main()

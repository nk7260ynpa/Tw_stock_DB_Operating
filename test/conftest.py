"""pytest 全域夾具。

**設定路徑隔離（重要）**：`web_server` 的設定路徑是模組層常數，測試若只 patch
`CONFIG_PATH`（既有 `test_web_server_*.py` 的慣用寫法），`load_config` 內的舊設定
搬遷仍會讀到 repo 內**真實**的 `logs/config.json`，把開發機上的設定改名搬走——
這正是本專案要杜絕的「設定被靜默搬動」。故在此以 autouse 夾具，把所有設定相關
路徑一律導向測試專用的暫存目錄，任何測試都不會碰到真實檔案。
"""

import pytest

import web_server


@pytest.fixture(autouse=True)
def isolate_config_paths(monkeypatch, tmp_path):
    """把設定檔路徑導向暫存目錄，避免測試動到 repo 內的真實設定。

    Args:
        monkeypatch: pytest 的 monkeypatch 夾具。
        tmp_path: pytest 提供的測試專用暫存目錄。

    Yields:
        pathlib.Path: 該測試使用的暫存根目錄（需要時可取用）。
    """
    config_dir = tmp_path / "config"
    log_dir = tmp_path / "logs"
    config_dir.mkdir()
    log_dir.mkdir()

    monkeypatch.setattr(web_server, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(web_server, "CONFIG_PATH", config_dir / "config.json")
    monkeypatch.setattr(web_server, "LEGACY_CONFIG_PATH", log_dir / "config.json")
    monkeypatch.setattr(
        web_server, "LEGACY_CONFIG_BACKUP_PATH", log_dir / "config.json.migrated"
    )
    yield tmp_path

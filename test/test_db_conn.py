"""連線生命週期管理單元測試模組。

驗證 MySQL 連線一律經 `routers.db_conn` 取得，且任何離開路徑都會關閉連線。
過去的寫法只在成功路徑呼叫 `conn.close()`，例外時連線會一路洩漏到垃圾回收；
`process_retry_queue` 單輪可執行數十個任務，累積下來足以耗盡 MySQL 連線數。
"""

import ast
import pathlib
import unittest
from unittest.mock import MagicMock, patch

from routers import db_conn

# 專案根目錄（本檔位於 test/ 之下）。
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# 允許直接取得連線的檔案：`routers.py` 是唯一的連線取得點，
# `clients.py` 是它底層的 Engine 建立函式。
_CONN_OWNER_FILES = {"routers.py", "clients.py"}

# 掃描範圍：正式程式碼（排除測試、前端與虛擬環境）。
_SKIP_DIRS = {"test", "frontend", "node_modules", ".git", "static", "logs"}


def _iter_production_files():
    """列出需檢查的正式程式碼檔案。

    Yields:
        pathlib.Path: 正式程式碼的 .py 檔路徑。
    """
    for path in sorted(REPO_ROOT.rglob("*.py")):
        rel = path.relative_to(REPO_ROOT)
        if rel.parts[0] in _SKIP_DIRS or rel.name in _CONN_OWNER_FILES:
            continue
        yield path


def _find_raw_acquisitions(path):
    """找出檔案中未經 `db_conn` 的連線取得點。

    Args:
        path (pathlib.Path): 待檢查的 .py 檔路徑。

    Returns:
        list[str]: 違規位置說明（檔名與行號）。
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr == "mysql_conn":
            hits.append(f"{path.name}:{node.lineno} 直接取用 .mysql_conn")
        elif isinstance(node, ast.Call):
            func = node.func
            name = (
                func.attr if isinstance(func, ast.Attribute)
                else getattr(func, "id", None)
            )
            if name in ("mysql_conn", "mysql_conn_db"):
                hits.append(f"{path.name}:{node.lineno} 直接呼叫 {name}()")
    return hits


class TestDbConnContextManager(unittest.TestCase):
    """測試 `db_conn` 的連線釋放行為。"""

    @patch("routers.MySQLRouter")
    def test_closes_connection_on_success(self, mock_router_cls):
        """測試正常離開時關閉連線。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        with db_conn("host", "user", "pw", "TWSE") as conn:
            self.assertIs(conn, mock_conn)
            mock_conn.close.assert_not_called()

        mock_conn.close.assert_called_once()

    @patch("routers.MySQLRouter")
    def test_closes_connection_on_exception(self, mock_router_cls):
        """測試區塊內拋出例外時仍關閉連線，且例外照常往外傳。

        這是本次修正的核心：舊寫法只有成功路徑會 close，爬蟲失敗、SQL 錯誤
        等任何例外都會讓連線洩漏。
        """
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn

        with self.assertRaises(RuntimeError):
            with db_conn("host", "user", "pw", "NEWS"):
                raise RuntimeError("上傳失敗")

        mock_conn.close.assert_called_once()

    @patch("routers.MySQLRouter")
    def test_passes_connection_parameters(self, mock_router_cls):
        """測試連線參數原樣傳給 MySQLRouter。"""
        mock_router_cls.return_value.mysql_conn = MagicMock()

        with db_conn("h:3306", "root", "stock", "SPECIAL_INFO"):
            pass

        mock_router_cls.assert_called_once_with(
            "h:3306", "root", "stock", "SPECIAL_INFO"
        )

    @patch("routers.MySQLRouter")
    def test_db_name_defaults_to_none(self, mock_router_cls):
        """測試未指定資料庫時傳入 None。"""
        mock_router_cls.return_value.mysql_conn = MagicMock()

        with db_conn("h:3306", "root", "stock"):
            pass

        mock_router_cls.assert_called_once_with("h:3306", "root", "stock", None)


class TestNoRawConnectionAcquisition(unittest.TestCase):
    """結構性防護：正式程式碼不得繞過 `db_conn` 直接取得連線。

    個別函式的測試只能證明「當下這幾處」有關好連線，無法阻止日後新增程式碼
    時又寫回舊樣式。本測試以 AST 掃描全部正式程式碼，讓復發會直接紅燈。
    """

    def test_no_raw_mysql_conn_outside_routers(self):
        """測試除 routers.py／clients.py 外沒有任何裸連線取得。"""
        violations = []
        for path in _iter_production_files():
            violations.extend(_find_raw_acquisitions(path))

        self.assertEqual(
            violations, [],
            "以下位置未經 db_conn 取得連線，例外時會洩漏：\n"
            + "\n".join(violations),
        )

    def test_scan_actually_covers_web_server(self):
        """測試掃描範圍確實涵蓋 web_server.py（避免掃描器空轉而假通過）。"""
        names = {p.name for p in _iter_production_files()}
        for expected in (
            "web_server.py", "DailyUpload.py", "upload.py",
            "backfill_price.py", "backfill_special_info.py",
        ):
            self.assertIn(expected, names)

    def test_detector_catches_raw_pattern(self):
        """測試偵測器本身有效：對已知的裸連線樣式必須報違規。"""
        sample = REPO_ROOT / "test" / "_conn_detector_sample.py"
        sample.write_text(
            "from routers import MySQLRouter\n"
            "def bad():\n"
            "    conn = MySQLRouter('h', 'u', 'p', 'TWSE').mysql_conn\n"
            "    conn.close()\n",
            encoding="utf-8",
        )
        try:
            self.assertTrue(_find_raw_acquisitions(sample))
        finally:
            sample.unlink()


if __name__ == "__main__":
    unittest.main()

"""每日新聞摘要模組單元測試。"""

import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

from ai_summary.news_summary import (
    NewsSummaryGenerator,
    SOURCES,
    MAX_ARTICLES_PER_SOURCE,
)


class TestNewsSummaryGenerator(unittest.TestCase):
    """測試 NewsSummaryGenerator。"""

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    def setUp(self):
        """建立測試用的 generator。"""
        self.generator = NewsSummaryGenerator()

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    def test_init(self):
        """測試初始化。"""
        gen = NewsSummaryGenerator()
        self.assertEqual(gen.model, "claude-sonnet-4-20250514")

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    @patch.object(NewsSummaryGenerator, "_query_headlines")
    def test_run_no_news(self, mock_query):
        """測試無新聞資料時回傳 skipped。"""
        mock_query.return_value = {
            "CTEE": [], "CNYES": [], "PTT": [], "MoneyUDN": []
        }

        result = self.generator.run("2026-04-01")
        self.assertEqual(result["status"], "skipped")
        self.assertIn("無新聞資料", result["error"])

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    @patch.object(NewsSummaryGenerator, "_run_agent")
    @patch.object(NewsSummaryGenerator, "_query_headlines")
    @patch("ai_summary.news_summary.OUTPUT_DIR")
    @patch("ai_summary.news_summary.NEWS_CONTENT_DIR")
    def test_run_success(
        self, mock_news_dir, mock_output_dir,
        mock_query, mock_run_agent,
    ):
        """測試成功產生每日新聞摘要。"""
        mock_query.return_value = {
            "CTEE": [
                {"time": "10:00", "head": "台股大漲", "content_file": "2026-04-01/abc.txt"},
            ],
            "CNYES": [
                {"time": "11:00", "head": "美股收高", "content_file": "2026-04-01/def.md"},
            ],
            "PTT": [],
            "MoneyUDN": [],
        }

        # 模擬全文讀取
        mock_file = MagicMock()
        mock_file.read_text.return_value = "新聞全文內容"
        mock_news_dir.__truediv__ = MagicMock(
            return_value=MagicMock(
                __truediv__=MagicMock(return_value=mock_file)
            )
        )

        mock_output_path = MagicMock()
        mock_output_path.exists.return_value = True
        mock_output_path.__str__ = MagicMock(
            return_value="/workspace/DailyNews/2026-04-01.md"
        )
        mock_output_dir.__truediv__ = MagicMock(
            return_value=mock_output_path
        )
        mock_output_dir.mkdir = MagicMock()

        mock_run_agent.return_value = {
            "result": "摘要完成",
            "cost": 0.05,
            "is_error": False,
        }

        result = self.generator.run("2026-04-01")
        self.assertEqual(result["status"], "success")
        self.assertIn("stats", result)
        self.assertEqual(result["stats"]["CTEE"], 1)
        self.assertEqual(result["stats"]["CNYES"], 1)
        self.assertEqual(result["stats"]["PTT"], 0)
        mock_run_agent.assert_called_once()

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    def test_read_articles_empty(self):
        """測試所有來源無資料時的全文讀取。"""
        headlines = {
            "CTEE": [], "CNYES": [], "PTT": [], "MoneyUDN": []
        }
        content, stats = self.generator._read_articles(headlines)
        self.assertIn("當日無資料", content)
        self.assertEqual(stats["CTEE"], 0)

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    @patch("ai_summary.news_summary.NEWS_CONTENT_DIR")
    def test_read_articles_with_data(self, mock_news_dir):
        """測試有資料時的全文讀取。"""
        headlines = {
            "CTEE": [
                {"time": "10:00", "head": f"新聞{i}", "content_file": f"2026-04-01/{i}.txt"}
                for i in range(15)
            ],
            "CNYES": [],
            "PTT": [],
            "MoneyUDN": [],
        }

        mock_file = MagicMock()
        mock_file.read_text.return_value = "全文內容"
        mock_news_dir.__truediv__ = MagicMock(
            return_value=MagicMock(
                __truediv__=MagicMock(return_value=mock_file)
            )
        )

        content, stats = self.generator._read_articles(headlines)
        self.assertEqual(stats["CTEE"], 15)
        # 最多 10 篇全文
        self.assertEqual(content.count("[全文"), MAX_ARTICLES_PER_SOURCE)
        # 剩餘 5 篇僅標題
        self.assertEqual(content.count("[僅標題]"), 5)

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    @patch("ai_summary.news_summary.NEWS_CONTENT_DIR")
    def test_read_articles_file_not_found(self, mock_news_dir):
        """測試全文檔案不存在時降級為僅標題。"""
        headlines = {
            "CTEE": [
                {"time": "10:00", "head": "新聞1", "content_file": "2026-04-01/missing.txt"},
            ],
            "CNYES": [],
            "PTT": [],
            "MoneyUDN": [],
        }

        mock_file = MagicMock()
        mock_file.read_text.side_effect = FileNotFoundError
        mock_news_dir.__truediv__ = MagicMock(
            return_value=MagicMock(
                __truediv__=MagicMock(return_value=mock_file)
            )
        )

        content, stats = self.generator._read_articles(headlines)
        self.assertIn("[僅標題]", content)
        self.assertEqual(stats["CTEE"], 1)


class TestNewsSummaryQueryHeadlines(unittest.TestCase):
    """測試 _query_headlines MySQL 查詢。"""

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    @patch("ai_summary.news_summary.MySQLRouter")
    def test_query_headlines(self, mock_router_cls):
        """測試 MySQL 查詢回傳正確分類。"""
        mock_conn = MagicMock()
        mock_router_cls.return_value.mysql_conn = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            ("CTEE", "10:00:00", "台股大漲", "2026-04-01/abc.txt"),
            ("CNYES", "11:00:00", "美股收高", "2026-04-01/def.md"),
            ("PTT", "12:00:00", "多頭來了", "2026-04-01/ghi.md"),
        ]

        gen = NewsSummaryGenerator()
        result = gen._query_headlines("2026-04-01")

        self.assertEqual(len(result["CTEE"]), 1)
        self.assertEqual(result["CTEE"][0]["head"], "台股大漲")
        self.assertEqual(len(result["CNYES"]), 1)
        self.assertEqual(len(result["PTT"]), 1)
        self.assertEqual(len(result["MoneyUDN"]), 0)
        mock_conn.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()

"""YT 精華摘要模組單元測試。"""

import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

from ai_summary.yt_summary import YTSummaryGenerator, TRANSCRIPT_DIR, OUTPUT_DIR


class TestYTSummaryGenerator(unittest.TestCase):
    """測試 YTSummaryGenerator。"""

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    def setUp(self):
        """建立測試用的 generator。"""
        self.generator = YTSummaryGenerator()

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    def test_init(self):
        """測試初始化。"""
        gen = YTSummaryGenerator()
        self.assertEqual(gen.model, "claude-sonnet-4-20250514")
        self.assertEqual(gen.max_turns, 10)

    @patch.dict("os.environ", {}, clear=True)
    def test_init_missing_api_key(self):
        """測試缺少 ANTHROPIC_API_KEY 時拋出錯誤。"""
        # 移除可能存在的 key
        import os
        os.environ.pop("ANTHROPIC_API_KEY", None)
        with self.assertRaises(RuntimeError) as ctx:
            YTSummaryGenerator()
        self.assertIn("ANTHROPIC_API_KEY", str(ctx.exception))

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    @patch.object(Path, "exists", return_value=False)
    def test_run_transcript_not_exists(self, mock_exists):
        """測試逐字稿不存在時回傳 skipped。"""
        result = self.generator.run("2026-04-01")
        self.assertEqual(result["status"], "skipped")
        self.assertIn("無 YT 逐字稿資料", result["error"])

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    @patch.object(YTSummaryGenerator, "_run_agent")
    @patch("ai_summary.yt_summary.OUTPUT_DIR")
    @patch("ai_summary.yt_summary.TRANSCRIPT_DIR")
    def test_run_success(
        self, mock_transcript_dir, mock_output_dir,
        mock_run_agent,
    ):
        """測試成功產生 YT 精華摘要。"""
        # 模擬逐字稿存在
        mock_transcript_path = MagicMock()
        mock_transcript_path.exists.return_value = True
        mock_transcript_dir.__truediv__ = MagicMock(
            return_value=MagicMock(
                __truediv__=MagicMock(return_value=mock_transcript_path)
            )
        )

        # 模擬輸出路徑
        mock_output_path = MagicMock()
        mock_output_path.exists.return_value = True
        mock_output_path.__str__ = MagicMock(
            return_value="/workspace/YTNews/2026-04-01.md"
        )
        mock_output_dir.__truediv__ = MagicMock(
            return_value=mock_output_path
        )
        mock_output_dir.mkdir = MagicMock()

        mock_run_agent.return_value = {
            "result": "摘要完成",
            "cost": 0.01,
            "is_error": False,
        }

        result = self.generator.run("2026-04-01")
        self.assertEqual(result["status"], "success")
        mock_run_agent.assert_called_once()

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"})
    @patch.object(YTSummaryGenerator, "_run_agent")
    @patch("ai_summary.yt_summary.OUTPUT_DIR")
    @patch("ai_summary.yt_summary.TRANSCRIPT_DIR")
    def test_run_output_not_generated(
        self, mock_transcript_dir, mock_output_dir,
        mock_run_agent,
    ):
        """測試 agent 執行後輸出檔案未產生。"""
        mock_transcript_path = MagicMock()
        mock_transcript_path.exists.return_value = True
        mock_transcript_dir.__truediv__ = MagicMock(
            return_value=MagicMock(
                __truediv__=MagicMock(return_value=mock_transcript_path)
            )
        )

        mock_output_path = MagicMock()
        mock_output_path.exists.return_value = False
        mock_output_dir.__truediv__ = MagicMock(
            return_value=mock_output_path
        )
        mock_output_dir.mkdir = MagicMock()

        mock_run_agent.return_value = {
            "result": "done",
            "cost": 0.01,
            "is_error": False,
        }

        result = self.generator.run("2026-04-01")
        self.assertEqual(result["status"], "failed")
        self.assertIn("未產生", result["error"])


if __name__ == "__main__":
    unittest.main()

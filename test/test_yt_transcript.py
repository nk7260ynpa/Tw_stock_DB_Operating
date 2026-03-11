"""YouTube 逐字稿模組單元測試。"""

import json
import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from pathlib import Path

from data_upload.yt_transcript import YTTranscriptUploader


class TestYTTranscriptUploader(unittest.TestCase):
    """測試 YTTranscriptUploader。"""

    def setUp(self):
        """建立測試用的 mock 物件。"""
        self.mock_conn = MagicMock()
        self.uploader = YTTranscriptUploader(
            self.mock_conn, gemini_api_key="test-api-key"
        )

    def test_init_with_api_key(self):
        """測試透過參數傳入 API key。"""
        uploader = YTTranscriptUploader(
            self.mock_conn, gemini_api_key="my-key"
        )
        self.assertEqual(uploader.api_key, "my-key")

    @patch("data_upload.yt_transcript.GEMINI_API_KEY_PATH")
    def test_init_load_api_key_from_file(self, mock_path):
        """測試從檔案讀取 API key。"""
        mock_path.exists.return_value = True
        mock_path.read_text.return_value = "file-api-key\n"
        uploader = YTTranscriptUploader(self.mock_conn)
        self.assertEqual(uploader.api_key, "file-api-key")

    @patch("data_upload.yt_transcript.GEMINI_API_KEY_PATH")
    def test_init_file_not_found(self, mock_path):
        """測試 API key 檔案不存在時拋出 FileNotFoundError。"""
        mock_path.exists.return_value = False
        with self.assertRaises(FileNotFoundError):
            YTTranscriptUploader(self.mock_conn)

    @patch("subprocess.run")
    def test_get_latest_stream_url_success(self, mock_run):
        """測試成功取得目標日期的影片 URL。"""
        video_data = {
            "upload_date": "20260311",
            "url": "https://www.youtube.com/watch?v=test123",
            "title": "測試影片標題",
            "duration": 3661,
        }
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps(video_data),
        )

        url, title, duration = self.uploader.get_latest_stream_url(
            "2026-03-11"
        )
        self.assertEqual(url, "https://www.youtube.com/watch?v=test123")
        self.assertEqual(title, "測試影片標題")
        self.assertEqual(duration, "1:01:01")

    @patch("subprocess.run")
    def test_get_latest_stream_url_with_id_only(self, mock_run):
        """測試只有 id 沒有 url 時自動組合 URL。"""
        video_data = {
            "upload_date": "20260311",
            "id": "abc123",
            "title": "測試",
            "duration": 125,
        }
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps(video_data),
        )

        url, title, duration = self.uploader.get_latest_stream_url(
            "2026-03-11"
        )
        self.assertEqual(url, "https://www.youtube.com/watch?v=abc123")
        self.assertEqual(duration, "2:05")

    @patch("subprocess.run")
    def test_get_latest_stream_url_no_match(self, mock_run):
        """測試找不到目標日期影片時回傳 None。"""
        video_data = {
            "upload_date": "20260310",
            "id": "abc123",
            "title": "昨天的影片",
            "duration": 100,
        }
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps(video_data),
        )

        url, title, duration = self.uploader.get_latest_stream_url(
            "2026-03-11"
        )
        self.assertIsNone(url)
        self.assertIsNone(title)
        self.assertIsNone(duration)

    @patch("subprocess.run")
    def test_get_latest_stream_url_ytdlp_error(self, mock_run):
        """測試 yt-dlp 執行失敗。"""
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr="Error occurred",
        )

        url, title, duration = self.uploader.get_latest_stream_url(
            "2026-03-11"
        )
        self.assertIsNone(url)

    @patch("subprocess.run")
    def test_get_latest_stream_url_timeout(self, mock_run):
        """測試 yt-dlp 執行逾時。"""
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd="yt-dlp", timeout=120
        )

        url, title, duration = self.uploader.get_latest_stream_url(
            "2026-03-11"
        )
        self.assertIsNone(url)

    def test_check_existing_true(self):
        """測試已有成功記錄時回傳 True。"""
        self.mock_conn.execute.return_value.scalar.return_value = 1
        self.assertTrue(self.uploader.check_existing("2026-03-11"))

    def test_check_existing_false(self):
        """測試無成功記錄時回傳 False。"""
        self.mock_conn.execute.return_value.scalar.return_value = 0
        self.assertFalse(self.uploader.check_existing("2026-03-11"))

    @patch.object(YTTranscriptUploader, "check_existing", return_value=True)
    def test_upload_skipped(self, mock_check):
        """測試已存在記錄時跳過上傳。"""
        result = self.uploader.upload("2026-03-11")
        self.assertEqual(result["status"], "skipped")

    @patch.object(YTTranscriptUploader, "check_existing", return_value=False)
    @patch.object(
        YTTranscriptUploader, "get_latest_stream_url",
        return_value=(None, None, None),
    )
    @patch.object(YTTranscriptUploader, "update_db")
    def test_upload_no_video(self, mock_update, mock_get, mock_check):
        """測試找不到影片時回傳 failed。"""
        result = self.uploader.upload("2026-03-11")
        self.assertEqual(result["status"], "failed")
        self.assertIn("未找到", result["error"])

    @patch.object(YTTranscriptUploader, "check_existing", return_value=False)
    @patch.object(
        YTTranscriptUploader, "get_latest_stream_url",
        return_value=(
            "https://www.youtube.com/watch?v=test",
            "測試標題",
            "1:00:00",
        ),
    )
    @patch.object(
        YTTranscriptUploader, "extract_transcript", return_value=None
    )
    @patch.object(YTTranscriptUploader, "update_db")
    def test_upload_transcript_failed(
        self, mock_update, mock_extract, mock_get, mock_check
    ):
        """測試逐字稿提取失敗。"""
        result = self.uploader.upload("2026-03-11")
        self.assertEqual(result["status"], "failed")
        self.assertIn("Gemini", result["error"])

    @patch.object(YTTranscriptUploader, "check_existing", return_value=False)
    @patch.object(
        YTTranscriptUploader, "get_latest_stream_url",
        return_value=(
            "https://www.youtube.com/watch?v=test",
            "測試標題",
            "1:00:00",
        ),
    )
    @patch.object(
        YTTranscriptUploader, "extract_transcript",
        return_value="# 逐字稿內容\n\n測試內容。",
    )
    @patch.object(
        YTTranscriptUploader, "save_transcript",
        return_value="2026-03-11/2026-03-11.md",
    )
    @patch.object(YTTranscriptUploader, "update_db")
    def test_upload_success(
        self, mock_update, mock_save, mock_extract, mock_get, mock_check
    ):
        """測試完整上傳流程成功。"""
        result = self.uploader.upload("2026-03-11")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["title"], "測試標題")
        self.assertIsNone(result["error"])

    @patch("data_upload.yt_transcript.NEWS_CONTENT_BASE", Path("/tmp/test_yt"))
    def test_save_transcript(self):
        """測試逐字稿儲存至檔案系統。"""
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "data_upload.yt_transcript.NEWS_CONTENT_BASE",
                Path(tmpdir),
            ):
                content_file = self.uploader.save_transcript(
                    "# 測試內容", "2026-03-11"
                )
                self.assertEqual(
                    content_file, "2026-03-11/2026-03-11.md"
                )
                saved_path = Path(tmpdir) / "2026-03-11" / "2026-03-11.md"
                self.assertTrue(saved_path.exists())
                self.assertEqual(
                    saved_path.read_text(encoding="utf-8"), "# 測試內容"
                )

    def test_duration_format_no_hours(self):
        """測試時長格式化（無小時）。"""
        video_data = {
            "upload_date": "20260311",
            "id": "test",
            "title": "Test",
            "duration": 125,  # 2:05
        }
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps(video_data),
            )
            _, _, duration = self.uploader.get_latest_stream_url("2026-03-11")
            self.assertEqual(duration, "2:05")

    def test_duration_format_with_hours(self):
        """測試時長格式化（有小時）。"""
        video_data = {
            "upload_date": "20260311",
            "id": "test",
            "title": "Test",
            "duration": 7265,  # 2:01:05
        }
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps(video_data),
            )
            _, _, duration = self.uploader.get_latest_stream_url("2026-03-11")
            self.assertEqual(duration, "2:01:05")


if __name__ == "__main__":
    unittest.main()

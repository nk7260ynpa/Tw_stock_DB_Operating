"""YouTube 逐字稿模組單元測試。"""

import json
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path

from data_upload.yt_transcript import YTTranscriptUploader


class TestYTTranscriptUploader(unittest.TestCase):
    """測試 YTTranscriptUploader。"""

    def setUp(self):
        """建立測試用的 mock 物件。"""
        self.mock_conn = MagicMock()
        self.uploader = YTTranscriptUploader(self.mock_conn)

    def test_init(self):
        """測試初始化。"""
        uploader = YTTranscriptUploader(self.mock_conn)
        self.assertEqual(uploader.conn, self.mock_conn)

    # --- _extract_video_id ---

    def test_extract_video_id_standard_url(self):
        """測試從標準 YouTube URL 提取影片 ID。"""
        video_id = YTTranscriptUploader._extract_video_id(
            "https://www.youtube.com/watch?v=Zg_YRHt1JTc"
        )
        self.assertEqual(video_id, "Zg_YRHt1JTc")

    def test_extract_video_id_short_url(self):
        """測試從短網址提取影片 ID。"""
        video_id = YTTranscriptUploader._extract_video_id(
            "https://youtu.be/Zg_YRHt1JTc"
        )
        self.assertEqual(video_id, "Zg_YRHt1JTc")

    def test_extract_video_id_with_params(self):
        """測試從含多個參數的 URL 提取影片 ID。"""
        video_id = YTTranscriptUploader._extract_video_id(
            "https://www.youtube.com/watch?v=abc12345678&t=120"
        )
        self.assertEqual(video_id, "abc12345678")

    def test_extract_video_id_invalid_url(self):
        """測試無效 URL 回傳 None。"""
        video_id = YTTranscriptUploader._extract_video_id(
            "https://example.com/not-youtube"
        )
        self.assertIsNone(video_id)

    # --- _format_transcript ---

    def test_format_transcript_basic(self):
        """測試基本的逐字稿格式化。"""
        snippets = ["大家好", "歡迎收看", "今天的節目"]
        result = YTTranscriptUploader._format_transcript(snippets)
        self.assertTrue(result.startswith("# 逐字稿"))
        self.assertIn("大家好", result)
        self.assertIn("歡迎收看", result)

    def test_format_transcript_paragraphs(self):
        """測試段落分隔（超過 _LINES_PER_PARAGRAPH 行）。"""
        snippets = [f"第{i}句" for i in range(15)]
        result = YTTranscriptUploader._format_transcript(snippets)
        # 標題 + 至少 2 個段落
        paragraphs = result.split("\n\n")
        self.assertGreaterEqual(len(paragraphs), 3)

    # --- extract_transcript ---

    @patch("data_upload.yt_transcript.YouTubeTranscriptApi")
    def test_extract_transcript_success(self, mock_api_class):
        """測試成功提取字幕。"""
        mock_api = MagicMock()
        mock_api_class.return_value = mock_api

        mock_snippet1 = MagicMock()
        mock_snippet1.text = "大家好歡迎收看"
        mock_snippet2 = MagicMock()
        mock_snippet2.text = "今天的節目"
        mock_api.fetch.return_value = [mock_snippet1, mock_snippet2]

        result = self.uploader.extract_transcript(
            "https://www.youtube.com/watch?v=test1234567"
        )
        self.assertIsNotNone(result)
        self.assertIn("大家好", result)
        self.assertIn("今天的節目", result)

    @patch("data_upload.yt_transcript.YouTubeTranscriptApi")
    def test_extract_transcript_no_subtitles(self, mock_api_class):
        """測試無字幕時回傳 None。"""
        mock_api = MagicMock()
        mock_api_class.return_value = mock_api
        mock_api.fetch.side_effect = Exception("No transcripts found")

        result = self.uploader.extract_transcript(
            "https://www.youtube.com/watch?v=test1234567"
        )
        self.assertIsNone(result)

    def test_extract_transcript_invalid_url(self):
        """測試無效 URL 回傳 None。"""
        result = self.uploader.extract_transcript("https://example.com")
        self.assertIsNone(result)

    @patch("data_upload.yt_transcript.YouTubeTranscriptApi")
    def test_extract_transcript_empty_content(self, mock_api_class):
        """測試字幕內容為空時回傳 None。"""
        mock_api = MagicMock()
        mock_api_class.return_value = mock_api

        mock_snippet = MagicMock()
        mock_snippet.text = "   "
        mock_api.fetch.return_value = [mock_snippet]

        result = self.uploader.extract_transcript(
            "https://www.youtube.com/watch?v=test1234567"
        )
        self.assertIsNone(result)

    # --- _match_video_date ---

    def test_match_video_date_upload_date(self):
        """測試 upload_date 欄位匹配。"""
        video = {"upload_date": "20260311", "title": "Test"}
        self.assertTrue(
            YTTranscriptUploader._match_video_date(video, "2026-03-11")
        )

    def test_match_video_date_title_format(self):
        """測試標題中 YYYY/M/D 格式匹配。"""
        video = {"title": "2026/3/11(三) 早晨財經"}
        self.assertTrue(
            YTTranscriptUploader._match_video_date(video, "2026-03-11")
        )

    def test_match_video_date_english_month(self):
        """測試英文月份格式匹配。"""
        video = {"title": "March 11, 2026 - Morning"}
        self.assertTrue(
            YTTranscriptUploader._match_video_date(video, "2026-03-11")
        )

    def test_match_video_date_no_match(self):
        """測試日期不匹配。"""
        video = {"upload_date": "20260310", "title": "March 10"}
        self.assertFalse(
            YTTranscriptUploader._match_video_date(video, "2026-03-11")
        )

    # --- get_latest_stream_url ---

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

    # --- DB / 儲存 ---

    def test_check_existing_true(self):
        """測試已有成功記錄時回傳 True。"""
        self.mock_conn.execute.return_value.scalar.return_value = 1
        self.assertTrue(self.uploader.check_existing("2026-03-11"))

    def test_check_existing_false(self):
        """測試無成功記錄時回傳 False。"""
        self.mock_conn.execute.return_value.scalar.return_value = 0
        self.assertFalse(self.uploader.check_existing("2026-03-11"))

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

    # --- _chinese_title ---

    def test_chinese_title_weekday(self):
        """測試中文標題產生（含星期）。"""
        title = YTTranscriptUploader._chinese_title("2026-03-11")
        self.assertEqual(title, "2026/3/11(三) 游庭皓的財經皓角")

    def test_chinese_title_monday(self):
        """測試中文標題（星期一）。"""
        title = YTTranscriptUploader._chinese_title("2026-03-09")
        self.assertEqual(title, "2026/3/9(一) 游庭皓的財經皓角")

    def test_chinese_title_invalid_date(self):
        """測試無效日期格式仍回傳標題。"""
        title = YTTranscriptUploader._chinese_title("invalid")
        self.assertEqual(title, "invalid 游庭皓的財經皓角")

    # --- upload 流程 ---

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
        self.assertIn("字幕", result["error"])

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
        return_value="# 逐字稿\n\n測試內容。\n",
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
        self.assertEqual(result["title"], "2026/3/11(三) 游庭皓的財經皓角")
        self.assertIsNone(result["error"])

    def test_duration_format_no_hours(self):
        """測試時長格式化（無小時）。"""
        video_data = {
            "upload_date": "20260311",
            "id": "test",
            "title": "Test",
            "duration": 125,
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
            "duration": 7265,
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

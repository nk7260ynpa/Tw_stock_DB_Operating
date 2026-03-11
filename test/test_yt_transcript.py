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
        self.uploader = YTTranscriptUploader(self.mock_conn)

    def test_init(self):
        """測試初始化不需要 API key。"""
        uploader = YTTranscriptUploader(self.mock_conn)
        self.assertEqual(uploader.conn, self.mock_conn)

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

    def test_parse_vtt_basic(self):
        """測試基本 VTT 解析。"""
        vtt_content = (
            "WEBVTT\n"
            "Kind: captions\n"
            "Language: zh-Hant\n"
            "\n"
            "00:00:00.080 --> 00:00:02.560\n"
            "大家好 歡迎來到今天的直播\n"
            "\n"
            "00:00:02.560 --> 00:00:05.120\n"
            "今天我們要來聊聊台股的走勢\n"
            "\n"
        )
        result = YTTranscriptUploader._parse_vtt(vtt_content)
        self.assertIsNotNone(result)
        self.assertTrue(result.startswith("# 逐字稿"))
        self.assertIn("大家好", result)
        self.assertIn("台股的走勢", result)
        # 不應包含時間戳
        self.assertNotIn("00:00:00", result)
        self.assertNotIn("-->", result)

    def test_parse_vtt_with_html_tags(self):
        """測試 VTT 解析能移除 HTML 標籤。"""
        vtt_content = (
            "WEBVTT\n"
            "\n"
            "00:00:00.000 --> 00:00:02.000\n"
            "<c>大家好</c> <00:00:01.000>歡迎\n"
            "\n"
        )
        result = YTTranscriptUploader._parse_vtt(vtt_content)
        self.assertIsNotNone(result)
        self.assertIn("大家好", result)
        self.assertIn("歡迎", result)
        self.assertNotIn("<c>", result)
        self.assertNotIn("</c>", result)

    def test_parse_vtt_with_style_marks(self):
        """測試 VTT 解析能移除 WebVTT 樣式標記。"""
        vtt_content = (
            "WEBVTT\n"
            "\n"
            "00:00:00.000 --> 00:00:02.000 align:start position:0%\n"
            "align:start position:0% 大家好\n"
            "\n"
        )
        result = YTTranscriptUploader._parse_vtt(vtt_content)
        self.assertIsNotNone(result)
        self.assertIn("大家好", result)
        self.assertNotIn("align:", result)
        self.assertNotIn("position:", result)

    def test_parse_vtt_dedup_overlapping(self):
        """測試 VTT 解析能去除 YouTube 自動字幕重複行。"""
        vtt_content = (
            "WEBVTT\n"
            "\n"
            "00:00:00.000 --> 00:00:02.000\n"
            "大家好\n"
            "\n"
            "00:00:01.000 --> 00:00:03.000\n"
            "大家好\n"
            "\n"
            "00:00:02.000 --> 00:00:04.000\n"
            "歡迎收看\n"
            "\n"
        )
        result = YTTranscriptUploader._parse_vtt(vtt_content)
        self.assertIsNotNone(result)
        # "大家好" 只應出現一次
        count = result.count("大家好")
        self.assertEqual(count, 1)

    def test_parse_vtt_removes_sequence_numbers(self):
        """測試 VTT 解析能移除 cue 序號。"""
        vtt_content = (
            "WEBVTT\n"
            "\n"
            "1\n"
            "00:00:00.000 --> 00:00:02.000\n"
            "第一句\n"
            "\n"
            "2\n"
            "00:00:02.000 --> 00:00:04.000\n"
            "第二句\n"
            "\n"
        )
        result = YTTranscriptUploader._parse_vtt(vtt_content)
        self.assertIsNotNone(result)
        self.assertIn("第一句", result)
        self.assertIn("第二句", result)

    def test_parse_vtt_empty_content(self):
        """測試空字幕內容回傳 None。"""
        vtt_content = "WEBVTT\n\n"
        result = YTTranscriptUploader._parse_vtt(vtt_content)
        self.assertIsNone(result)

    def test_parse_vtt_paragraphs(self):
        """測試 VTT 解析產生段落分隔。"""
        # 產生超過 _LINES_PER_PARAGRAPH 行的內容
        lines = []
        lines.append("WEBVTT\n\n")
        for i in range(15):
            lines.append(f"00:00:{i:02d}.000 --> 00:00:{i+1:02d}.000\n")
            lines.append(f"第{i+1}句話\n\n")
        vtt_content = "".join(lines)
        result = YTTranscriptUploader._parse_vtt(vtt_content)
        self.assertIsNotNone(result)
        # 應該有段落分隔（空行）
        paragraphs = result.split("\n\n")
        # 標題 + 至少 2 個段落
        self.assertGreaterEqual(len(paragraphs), 3)

    @patch.object(YTTranscriptUploader, "_download_subtitle")
    def test_extract_transcript_success(self, mock_download):
        """測試成功提取字幕並解析。"""
        mock_download.return_value = (
            "WEBVTT\n\n"
            "00:00:00.000 --> 00:00:02.000\n"
            "大家好歡迎收看\n\n"
        )
        result = self.uploader.extract_transcript(
            "https://www.youtube.com/watch?v=test"
        )
        self.assertIsNotNone(result)
        self.assertIn("大家好", result)

    @patch.object(YTTranscriptUploader, "_download_subtitle")
    def test_extract_transcript_no_subtitles(self, mock_download):
        """測試無字幕時回傳 None。"""
        mock_download.return_value = None
        result = self.uploader.extract_transcript(
            "https://www.youtube.com/watch?v=test"
        )
        self.assertIsNone(result)

    @patch.object(YTTranscriptUploader, "_download_subtitle")
    def test_extract_transcript_fallback_english(self, mock_download):
        """測試繁中字幕不可用時退回英文字幕。"""
        def side_effect(url, tmpdir, lang):
            if "zh" in lang:
                return None
            return (
                "WEBVTT\n\n"
                "00:00:00.000 --> 00:00:02.000\n"
                "Hello everyone\n\n"
            )
        mock_download.side_effect = side_effect
        result = self.uploader.extract_transcript(
            "https://www.youtube.com/watch?v=test"
        )
        self.assertIsNotNone(result)
        self.assertIn("Hello everyone", result)

    @patch("subprocess.run")
    @patch("data_upload.yt_transcript.glob_mod.glob")
    def test_download_subtitle_success(self, mock_glob, mock_run):
        """測試成功下載字幕檔案。"""
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            # 建立模擬 VTT 檔案
            vtt_path = Path(tmpdir) / "sub.zh-Hant.vtt"
            vtt_path.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\n測試\n")

            mock_run.return_value = MagicMock(returncode=0, stderr="")
            mock_glob.return_value = [str(vtt_path)]

            result = YTTranscriptUploader._download_subtitle(
                "https://youtube.com/watch?v=test",
                tmpdir,
                "zh-Hant,zh-TW,zh",
            )
            self.assertIsNotNone(result)
            self.assertIn("WEBVTT", result)

    @patch("subprocess.run")
    def test_download_subtitle_failure(self, mock_run):
        """測試字幕下載失敗。"""
        mock_run.return_value = MagicMock(
            returncode=1, stderr="No subtitles"
        )
        result = YTTranscriptUploader._download_subtitle(
            "https://youtube.com/watch?v=test",
            "/tmp/test",
            "zh-Hant",
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

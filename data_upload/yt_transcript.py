"""YouTube 逐字稿上傳模組。

從 YouTube 取得「游庭皓的財經皓角」直播影片，
使用 Gemini API 提取逐字稿，並儲存至檔案系統及 MySQL。
"""

import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

logger = logging.getLogger(__name__)

# 全文存放根目錄
NEWS_CONTENT_BASE = Path("/workspace/NewsContents/YT")

# YouTube 頻道直播播放清單
CHANNEL_STREAMS_URL = "https://www.youtube.com/@yutinghaofinance/streams"

# Gemini API key 檔案路徑
GEMINI_API_KEY_PATH = Path("/workspace/GeminiAPI")


class YTTranscriptUploader:
    """YouTube 逐字稿上傳器。

    1. 用 yt-dlp 取得最新直播影片列表
    2. 篩選目標日期的影片
    3. 用 Gemini API 提取逐字稿
    4. 儲存至檔案系統
    5. 寫入 metadata 至 MySQL
    """

    def __init__(self, conn, gemini_api_key=None):
        """初始化。

        Args:
            conn: SQLAlchemy 連線物件（NEWS 資料庫）。
            gemini_api_key: Gemini API key（若為 None 則從檔案讀取）。
        """
        self.conn = conn
        if gemini_api_key:
            self.api_key = gemini_api_key
        else:
            self.api_key = self._load_api_key()

    @staticmethod
    def _load_api_key():
        """從檔案讀取 Gemini API key。"""
        if GEMINI_API_KEY_PATH.exists():
            return GEMINI_API_KEY_PATH.read_text(encoding="utf-8").strip()
        raise FileNotFoundError(
            f"Gemini API key 檔案不存在: {GEMINI_API_KEY_PATH}"
        )

    def get_latest_stream_url(self, target_date):
        """用 yt-dlp 取得最新直播影片，篩選目標日期。

        Args:
            target_date: 目標日期字串（YYYY-MM-DD）。

        Returns:
            tuple: (video_url, title, duration) 或 (None, None, None)。
        """
        try:
            result = subprocess.run(
                [
                    "yt-dlp",
                    "--flat-playlist",
                    "--dump-json",
                    "--playlist-end", "10",
                    CHANNEL_STREAMS_URL,
                ],
                capture_output=True,
                text=True,
                timeout=120,
            )

            if result.returncode != 0:
                logger.error("yt-dlp 執行失敗: %s", result.stderr)
                return None, None, None

            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue
                try:
                    video = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # 檢查上傳日期是否匹配目標日期
                upload_date = video.get("upload_date", "")
                if upload_date:
                    # yt-dlp 的 upload_date 格式為 YYYYMMDD
                    formatted = (
                        f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
                    )
                    if formatted == target_date:
                        video_url = video.get("url") or video.get("id", "")
                        if video_url and not video_url.startswith("http"):
                            video_url = f"https://www.youtube.com/watch?v={video_url}"
                        title = video.get("title", "")
                        duration = video.get("duration")
                        duration_str = ""
                        if duration:
                            hours = int(duration) // 3600
                            minutes = (int(duration) % 3600) // 60
                            seconds = int(duration) % 60
                            if hours > 0:
                                duration_str = f"{hours}:{minutes:02d}:{seconds:02d}"
                            else:
                                duration_str = f"{minutes}:{seconds:02d}"
                        logger.info(
                            "找到目標日期 %s 的影片: %s", target_date, title
                        )
                        return video_url, title, duration_str

            logger.info("未找到目標日期 %s 的直播影片。", target_date)
            return None, None, None

        except subprocess.TimeoutExpired:
            logger.error("yt-dlp 執行逾時")
            return None, None, None
        except Exception as e:
            logger.error("取得直播列表失敗: %s", e)
            return None, None, None

    def extract_transcript(self, video_url):
        """用 Gemini API 提取逐字稿。

        Args:
            video_url: YouTube 影片 URL。

        Returns:
            str: 逐字稿 Markdown 內容，失敗時回傳 None。
        """
        try:
            import google.generativeai as genai

            genai.configure(api_key=self.api_key)

            model = genai.GenerativeModel("gemini-2.0-flash")

            prompt = (
                "請將以下 YouTube 影片的內容轉為詳細的逐字稿。"
                "請用繁體中文輸出，保留講者的原始用語和語氣。"
                "使用 Markdown 格式，以主題段落分段，"
                "每個段落加上適當的標題。"
                "不需要加入時間戳記。"
                f"\n\n影片網址：{video_url}"
            )

            response = model.generate_content(prompt)
            transcript = response.text

            if transcript:
                logger.info(
                    "逐字稿提取成功，長度: %d 字元", len(transcript)
                )
                return transcript

            logger.warning("Gemini API 回傳空內容")
            return None

        except Exception as e:
            logger.error("Gemini API 提取逐字稿失敗: %s", e)
            return None

    def save_transcript(self, content, date):
        """儲存逐字稿至檔案系統。

        Args:
            content: 逐字稿 Markdown 內容。
            date: 日期字串（YYYY-MM-DD）。

        Returns:
            str: 儲存的相對路徑（ContentFile），失敗回傳 None。
        """
        content_dir = NEWS_CONTENT_BASE / date
        content_dir.mkdir(parents=True, exist_ok=True)

        file_path = content_dir / f"{date}.md"
        try:
            file_path.write_text(content, encoding="utf-8")
            content_file = f"{date}/{date}.md"
            logger.info("逐字稿已儲存至 %s", file_path)
            return content_file
        except Exception as e:
            logger.error("逐字稿儲存失敗: %s", e)
            return None

    def check_existing(self, date):
        """檢查指定日期是否已有成功的逐字稿記錄。

        Args:
            date: 日期字串（YYYY-MM-DD）。

        Returns:
            bool: 已存在則回傳 True。
        """
        result = self.conn.execute(
            text(
                "SELECT COUNT(*) FROM YTTranscript "
                "WHERE Date = :date AND Status = 'success'"
            ),
            {"date": date},
        ).scalar()
        return result > 0

    def update_db(self, date, title, url, duration, content_file, status,
                  error_message=None):
        """新增或更新 YTTranscript 資料表記錄。

        Args:
            date: 日期字串（YYYY-MM-DD）。
            title: 影片標題。
            url: 影片 URL。
            duration: 影片時長。
            content_file: 逐字稿檔案路徑。
            status: 狀態（success/failed/pending）。
            error_message: 錯誤訊息（選填）。
        """
        try:
            self.conn.execute(
                text(
                    "INSERT INTO YTTranscript "
                    "(Date, Title, url, Duration, ContentFile, Status, ErrorMessage) "
                    "VALUES (:date, :title, :url, :duration, :content_file, "
                    ":status, :error_message) "
                    "ON DUPLICATE KEY UPDATE "
                    "Title = :title, url = :url, Duration = :duration, "
                    "ContentFile = :content_file, Status = :status, "
                    "ErrorMessage = :error_message"
                ),
                {
                    "date": date,
                    "title": title,
                    "url": url,
                    "duration": duration,
                    "content_file": content_file,
                    "status": status,
                    "error_message": error_message,
                },
            )
            self.conn.commit()
            logger.info("YTTranscript DB 更新完成: %s (%s)", date, status)
        except Exception as e:
            logger.error("YTTranscript DB 更新失敗: %s", e)

    def upload(self, date):
        """執行指定日期的 YT 逐字稿上傳流程。

        1. 檢查重複
        2. 取得影片 URL
        3. 提取逐字稿
        4. 儲存檔案
        5. 寫入 DB

        Args:
            date: 日期字串（YYYY-MM-DD）。

        Returns:
            dict: 包含 date、status、title、error 的結果字典。
        """
        # 檢查是否已有成功記錄
        if self.check_existing(date):
            logger.info("YT 逐字稿 %s 已存在，跳過。", date)
            return {
                "date": date,
                "status": "skipped",
                "title": None,
                "error": "該日期已有成功記錄",
            }

        # 寫入 pending 狀態
        self.update_db(date, None, "", None, None, "pending")

        # 取得影片 URL
        video_url, title, duration = self.get_latest_stream_url(date)
        if not video_url:
            error_msg = f"未找到 {date} 的直播影片"
            self.update_db(date, None, "", None, None, "failed", error_msg)
            logger.warning(error_msg)
            return {
                "date": date,
                "status": "failed",
                "title": None,
                "error": error_msg,
            }

        # 更新 DB 為 pending（帶影片資訊）
        self.update_db(date, title, video_url, duration, None, "pending")

        # 提取逐字稿
        transcript = self.extract_transcript(video_url)
        if not transcript:
            error_msg = "Gemini API 提取逐字稿失敗"
            self.update_db(
                date, title, video_url, duration, None, "failed", error_msg
            )
            return {
                "date": date,
                "status": "failed",
                "title": title,
                "error": error_msg,
            }

        # 儲存檔案
        content_file = self.save_transcript(transcript, date)
        if not content_file:
            error_msg = "逐字稿檔案儲存失敗"
            self.update_db(
                date, title, video_url, duration, None, "failed", error_msg
            )
            return {
                "date": date,
                "status": "failed",
                "title": title,
                "error": error_msg,
            }

        # 更新 DB 為 success
        self.update_db(
            date, title, video_url, duration, content_file, "success"
        )

        logger.info(
            "YT 逐字稿上傳完成: %s - %s", date, title
        )
        return {
            "date": date,
            "status": "success",
            "title": title,
            "error": None,
        }

"""YouTube 逐字稿上傳模組。

從 YouTube 取得「游庭皓的財經皓角」直播影片，
使用 youtube-transcript-api 抓取字幕並整理為 Markdown 格式逐字稿，
儲存至檔案系統及 MySQL。
"""

import json
import logging
import re
import subprocess
from datetime import datetime
from pathlib import Path

from sqlalchemy import text
from youtube_transcript_api import YouTubeTranscriptApi

logger = logging.getLogger(__name__)

# 全文存放根目錄
NEWS_CONTENT_BASE = Path("/workspace/NewsContents/YT")

# YouTube 頻道直播播放清單
CHANNEL_STREAMS_URL = "https://www.youtube.com/@yutinghaofinance/streams"

# 逐字稿段落合併行數
_LINES_PER_PARAGRAPH = 10


class YTTranscriptUploader:
    """YouTube 逐字稿上傳器。

    1. 用 yt-dlp 取得最新直播影片列表
    2. 篩選目標日期的影片
    3. 用 youtube-transcript-api 抓取字幕並整理為 Markdown
    4. 儲存至檔案系統
    5. 寫入 metadata 至 MySQL
    """

    def __init__(self, conn):
        """初始化。

        Args:
            conn: SQLAlchemy 連線物件（NEWS 資料庫）。
        """
        self.conn = conn

    @staticmethod
    def _match_video_date(video, target_date):
        """比對影片是否屬於目標日期。

        依序嘗試：
        1. upload_date 欄位（YYYYMMDD）
        2. 標題中的日期（支援 YYYY/M/D 和英文月份格式）

        Args:
            video (dict): yt-dlp 回傳的影片 metadata。
            target_date (str): 目標日期（YYYY-MM-DD）。

        Returns:
            bool: 匹配則回傳 True。
        """
        # 解析目標日期
        try:
            target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            return False

        # 方法 1：upload_date 欄位
        upload_date = video.get("upload_date", "")
        if upload_date and len(upload_date) == 8:
            formatted = (
                f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
            )
            if formatted == target_date:
                return True

        title = video.get("title", "")

        # 方法 2：標題中的 YYYY/M/D 格式（如 "2026/3/11(二)"）
        match = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", title)
        if match:
            try:
                title_dt = datetime(
                    int(match.group(1)),
                    int(match.group(2)),
                    int(match.group(3)),
                )
                if title_dt.date() == target_dt.date():
                    return True
            except ValueError:
                pass

        # 方法 3：英文月份格式（如 "March 11, 2026"）
        month_names = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12,
        }
        match = re.search(
            r"(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)"
            r"\s+(\d{1,2}),?\s+(\d{4})",
            title,
            re.IGNORECASE,
        )
        if match:
            try:
                month = month_names[match.group(1).lower()]
                day = int(match.group(2))
                year = int(match.group(3))
                title_dt = datetime(year, month, day)
                if title_dt.date() == target_dt.date():
                    return True
            except (ValueError, KeyError):
                pass

        return False

    def get_latest_stream_url(self, target_date):
        """用 yt-dlp 取得最新直播影片，篩選目標日期。

        先以 flat-playlist 快速取得影片列表並篩選日期，
        再以單一影片查詢取得中文標題（hl=zh-TW）。

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

                # 檢查日期是否匹配目標日期
                if not self._match_video_date(video, target_date):
                    continue

                video_id = video.get("id") or video.get("url", "")
                video_url = (
                    f"https://www.youtube.com/watch?v={video_id}"
                    if video_id and not video_id.startswith("http")
                    else video_id
                )

                # 取得中文標題與精確時長
                title, duration_str = self._fetch_video_detail(
                    video_url, video
                )

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

    @staticmethod
    def _fetch_video_detail(video_url, fallback):
        """取得單一影片的中文標題與時長。

        使用 yt-dlp 搭配 hl=zh-TW 取得中文標題，
        失敗時回退使用 flat-playlist 的資料。

        Args:
            video_url: YouTube 影片 URL。
            fallback: flat-playlist 回傳的原始 metadata dict。

        Returns:
            tuple: (title, duration_str)。
        """
        title = fallback.get("title", "")
        duration = fallback.get("duration")

        try:
            result = subprocess.run(
                [
                    "yt-dlp",
                    "--dump-json",
                    "--no-download",
                    "--extractor-args", "youtube:hl=zh-TW",
                    video_url,
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode == 0 and result.stdout.strip():
                detail = json.loads(result.stdout.strip())
                title = detail.get("title", title)
                duration = detail.get("duration", duration)
        except Exception as e:
            logger.warning("取得影片詳細資訊失敗，使用 fallback: %s", e)

        duration_str = ""
        if duration:
            hours = int(duration) // 3600
            minutes = (int(duration) % 3600) // 60
            seconds = int(duration) % 60
            if hours > 0:
                duration_str = f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                duration_str = f"{minutes}:{seconds:02d}"

        return title, duration_str

    @staticmethod
    def _extract_video_id(video_url):
        """從 YouTube URL 提取影片 ID。

        Args:
            video_url: YouTube 影片 URL。

        Returns:
            str: 影片 ID，無法提取時回傳 None。
        """
        match = re.search(r"[?&]v=([a-zA-Z0-9_-]{11})", video_url)
        if match:
            return match.group(1)
        match = re.search(r"youtu\.be/([a-zA-Z0-9_-]{11})", video_url)
        if match:
            return match.group(1)
        return None

    def extract_transcript(self, video_url):
        """用 youtube-transcript-api 抓取字幕並整理為 Markdown 逐字稿。

        優先嘗試繁體中文字幕（zh-TW, zh-Hant, zh），
        若無則嘗試英文字幕（en）。

        Args:
            video_url: YouTube 影片 URL。

        Returns:
            str: 逐字稿 Markdown 內容，失敗時回傳 None。
        """
        video_id = self._extract_video_id(video_url)
        if not video_id:
            logger.error("無法從 URL 提取影片 ID: %s", video_url)
            return None

        try:
            api = YouTubeTranscriptApi()
            transcript = api.fetch(
                video_id,
                languages=["zh-TW", "zh-Hant", "zh", "en"],
            )

            snippets = [
                snippet.text.replace("\n", " ")
                for snippet in transcript
                if snippet.text.strip()
            ]

            if not snippets:
                logger.warning("影片字幕內容為空: %s", video_url)
                return None

            result = self._format_transcript(snippets)
            logger.info(
                "逐字稿提取成功，共 %d 段，長度: %d 字元",
                len(snippets), len(result),
            )
            return result

        except Exception as e:
            logger.error("字幕提取失敗: %s", e)
            return None

    @staticmethod
    def _format_transcript(snippets):
        """將字幕片段整理為 Markdown 格式。

        每 N 個片段合併為一段，用空行分隔。

        Args:
            snippets: 字幕文字片段列表。

        Returns:
            str: Markdown 格式逐字稿。
        """
        paragraphs = []
        for i in range(0, len(snippets), _LINES_PER_PARAGRAPH):
            chunk = snippets[i:i + _LINES_PER_PARAGRAPH]
            paragraphs.append("".join(chunk))

        return "# 逐字稿\n\n" + "\n\n".join(paragraphs) + "\n"

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
            error_msg = "字幕提取失敗（影片無可用字幕）"
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

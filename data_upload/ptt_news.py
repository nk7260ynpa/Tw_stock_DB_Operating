"""PTT 股版新聞上傳模組。

從爬蟲服務取得 PTT 股版新聞資料，
將 metadata 寫入 MySQL NEWS.PTT 表，
全文內容存為 md 檔至 /workspace/NewsContents/PTT/。
"""

import hashlib
import logging
from pathlib import Path

import pandas as pd
import requests
from pydantic import BaseModel
from sqlalchemy import text

from data_upload.base import (
    NetworkError,
    STATUS_PARTIAL,
    SourceError,
    check_crawl_status,
    partial_retry_reason,
)

logger = logging.getLogger(__name__)

# 來源標籤，供 log 與錯誤訊息使用。
SOURCE_LABEL = "PTT 新聞"

# 全文存放根目錄
NEWS_CONTENT_BASE = Path("/workspace/NewsContents/PTT")


class PTTNewsType(BaseModel):
    """PTT 股版新聞 metadata schema。

    欄位名稱對應 NEWS.PTT 表結構。
    注意：PTT 沒有 HashTag 和 SubHead 欄位（與 CNYES、CTEE 不同）。
    """

    Date: str
    Time: str | None = None
    Author: str | None = None
    Head: str
    url: str
    ContentFile: str | None = None


class PTTNewsUploader:
    """PTT 股版新聞上傳器。

    1. 呼叫爬蟲 API 取得新聞資料
    2. metadata 寫入 MySQL NEWS.PTT
    3. 全文存為 md 至 NewsContents/PTT/YYYY-MM-DD/
    """

    def __init__(self, conn, crawler_host):
        """初始化 PTT 新聞上傳器。

        Args:
            conn: SQLAlchemy 連線物件（NEWS 資料庫）。
            crawler_host (str): 爬蟲服務主機位址（含 port）。
        """
        self.conn = conn
        self.crawler_host = crawler_host
        # 最近一次爬取的狀態，供 upload 於資料落地後決定是否排入重試。
        self._last_status = None
        self._last_partial_reason = None

    @staticmethod
    def url_hash(url):
        """計算 URL 的 MD5 雜湊前 12 位。

        Args:
            url (str): 新聞 URL。

        Returns:
            str: MD5 雜湊前 12 位字元。
        """
        return hashlib.md5(url.encode()).hexdigest()[:12]

    def crawl_data(self, date):
        """從爬蟲服務取得指定日期的 PTT 股版新聞。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            pd.DataFrame: 新聞資料 DataFrame，爬取失敗時回傳空 DataFrame。
        """
        # 重置狀態，避免範圍模式共用實例時殘留上一次的結果。
        self._last_status = None
        self._last_partial_reason = None
        url = f"http://{self.crawler_host}/ptt_news"
        try:
            resp = requests.get(url, params={"date": date}, timeout=600)
            resp.raise_for_status()
        except (requests.ConnectionError, requests.Timeout) as e:
            raise NetworkError(
                f"PTT 新聞爬蟲網路連線失敗（{date}）：{e}"
            ) from e
        except Exception as e:
            logger.error("PTT 新聞爬蟲呼叫失敗（%s）：%s", date, e)
            return pd.DataFrame()

        result = resp.json()
        # 先判讀 status 再取用 data：新契約下爬取失敗也會回 data: []，
        # 不先判讀會把「抓取失敗」誤當成「當日無新聞」而靜默漏抓且不重試。
        self._last_status = check_crawl_status(
            result, SOURCE_LABEL, f"（{date}）", allow_partial=True,
        )
        self._last_partial_reason = partial_retry_reason(result)
        records = result.get("data", [])

        if not records:
            logger.info("PTT 新聞 %s 無資料。", date)
            return pd.DataFrame()

        df = pd.DataFrame(records)
        # 將 NaN 轉為 None（避免 Pydantic 驗證失敗）
        df = df.where(df.notna(), None)
        return df

    def crawl_data_by_hours(self, hours):
        """從爬蟲服務取得過去指定小時數的 PTT 股版新聞。

        使用 hours 參數呼叫爬蟲 API，取得跨日的新聞資料。

        Args:
            hours (int): 要回溯的小時數（1-72）。

        Returns:
            pd.DataFrame: 新聞資料 DataFrame，爬取失敗時回傳空 DataFrame。
        """
        # 重置狀態，避免範圍模式共用實例時殘留上一次的結果。
        self._last_status = None
        self._last_partial_reason = None
        url = f"http://{self.crawler_host}/ptt_news"
        try:
            resp = requests.get(
                url, params={"hours": hours}, timeout=600
            )
            resp.raise_for_status()
        except (requests.ConnectionError, requests.Timeout) as e:
            raise NetworkError(
                f"PTT 新聞爬蟲網路連線失敗（hours={hours}）：{e}"
            ) from e
        except Exception as e:
            logger.error(
                "PTT 新聞爬蟲呼叫失敗（hours=%d）：%s", hours, e
            )
            return pd.DataFrame()

        result = resp.json()
        # 先判讀 status 再取用 data：新契約下爬取失敗也會回 data: []，
        # 不先判讀會把「抓取失敗」誤當成「當日無新聞」而靜默漏抓且不重試。
        self._last_status = check_crawl_status(
            result, SOURCE_LABEL, f"（hours={hours}）", allow_partial=True,
        )
        self._last_partial_reason = partial_retry_reason(result)
        records = result.get("data", [])

        if not records:
            logger.info("PTT 新聞過去 %d 小時無資料。", hours)
            return pd.DataFrame()

        df = pd.DataFrame(records)
        # 將 NaN 轉為 None（避免 Pydantic 驗證失敗）
        df = df.where(df.notna(), None)
        return df

    def _check_incomplete(self, context):
        """依最近一次爬取狀態決定是否排入重試（須於資料落地後呼叫）。

        `partial` 代表抓到的資料不完整。此時已抓到的部分一律先寫入（新聞以
        URL 去重，重抓為冪等，不會重複），再依 meta 判斷重抓是否有意義：
        部分全文抓取失敗或因逾時提前收工 → 重抓可補齊，拋 `SourceError`
        排入 retry queue；來源硬上限（`source_truncated`）→ 重抓也拿不到，
        僅記錄警告，避免無謂的重試循環。

        Args:
            context (str): 情境說明，如「（2026-08-16）」。

        Raises:
            SourceError: 不完整且重抓有機會補齊時拋出（爬蟲仍可達，
                呼叫端應僅跳過該筆、繼續處理後續日期）。
        """
        if self._last_status != STATUS_PARTIAL:
            return
        reason = self._last_partial_reason
        if reason is None:
            logger.warning(
                "%s%s 抓取不完整，但受限於來源可提供範圍，重抓亦無法補齊。",
                SOURCE_LABEL, context,
            )
            return
        raise SourceError(
            f"{SOURCE_LABEL}{context} 抓取不完整（{reason}），"
            "已存入取得的部分，排入重試以補齊剩餘資料"
        )

    def get_existing_urls(self, date):
        """查詢指定日期已存在的新聞 URL。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            set[str]: 已存在的 URL 集合。
        """
        rows = self.conn.execute(
            text("SELECT url FROM PTT WHERE Date = :date"),
            {"date": date},
        ).fetchall()
        return {row[0] for row in rows}

    def filter_new_records(self, df, date):
        """過濾已存在的新聞記錄。

        Args:
            df (pd.DataFrame): 新聞資料 DataFrame。
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            pd.DataFrame: 過濾後僅包含新記錄的 DataFrame。
        """
        if df.empty:
            return df

        existing_urls = self.get_existing_urls(date)
        if not existing_urls:
            return df

        new_df = df[~df["url"].isin(existing_urls)].copy()
        skipped = len(df) - len(new_df)
        if skipped > 0:
            logger.info(
                "PTT 新聞 %s 跳過 %d 筆已存在記錄。", date, skipped
            )
        return new_df

    @staticmethod
    def _clean_value(value, default=""):
        """清理欄位值，將 NaN 轉為 None。

        Args:
            value: 欄位值。
            default: 預設值。

        Returns:
            清理後的值。
        """
        if value is None:
            return default if default != "" else None
        if isinstance(value, float) and pd.isna(value):
            return default if default != "" else None
        return value

    def check_schema(self, df):
        """使用 Pydantic 驗證 DataFrame schema。

        Args:
            df (pd.DataFrame): 待驗證的 DataFrame。

        Returns:
            pd.DataFrame: 驗證後的 DataFrame（僅含 metadata 欄位）。
        """
        records = df.to_dict(orient="records")
        validated = []
        for record in records:
            # 只取 metadata 欄位進行驗證（NaN 轉為 None）
            url_val = self._clean_value(record.get("url"), "")
            content_file = (
                f"{self.url_hash(url_val)}.md" if url_val else None
            )
            meta = {
                "Date": self._clean_value(record.get("Date"), ""),
                "Time": self._clean_value(record.get("Time")),
                "Author": self._clean_value(record.get("Author")),
                "Head": self._clean_value(record.get("Head"), ""),
                "url": url_val,
                "ContentFile": content_file,
            }
            validated.append(PTTNewsType(**meta).model_dump())
        return pd.DataFrame(validated)

    def save_contents(self, df, date):
        """將新聞全文存為 md 檔案。

        Args:
            df (pd.DataFrame): 包含 Content 與 url 欄位的 DataFrame。
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            int: 成功儲存的檔案數量。
        """
        content_dir = NEWS_CONTENT_BASE / date
        content_dir.mkdir(parents=True, exist_ok=True)

        saved = 0
        for _, row in df.iterrows():
            content = row.get("Content", "")
            url = row.get("url", "")
            if not url:
                continue

            file_name = f"{self.url_hash(url)}.md"
            file_path = content_dir / file_name

            try:
                file_path.write_text(
                    content if content else "", encoding="utf-8"
                )
                saved += 1
            except Exception as e:
                logger.error(
                    "PTT 新聞全文儲存失敗（%s）：%s", file_path, e
                )

        return saved

    def upload_metadata(self, df):
        """上傳 metadata 至 NEWS.PTT 資料表。

        Args:
            df (pd.DataFrame): 經 schema 驗證後的 metadata DataFrame。

        Returns:
            int: 寫入的記錄數量。
        """
        if df.empty:
            return 0

        df.to_sql(
            "PTT", self.conn,
            if_exists="append", index=False, chunksize=500,
        )
        self.conn.commit()
        return len(df)

    def upload(self, date):
        """執行指定日期的 PTT 新聞上傳流程。

        1. 從爬蟲取得新聞資料
        2. 過濾已存在的記錄
        3. 驗證 schema
        4. 寫入 metadata 至資料庫
        5. 儲存全文至檔案系統

        Args:
            date (str): 日期字串（YYYY-MM-DD）。

        Returns:
            dict: 包含 date、record_count、file_count 的結果字典。

        Raises:
            SourceError: 爬取失敗或結果不完整且重抓有機會補齊時拋出
                （可重試，資料已取得的部分先落地）。
            OutOfRangeError: 日期超出來源可回溯範圍時拋出（不可重試）。
        """
        raw_df = self.crawl_data(date)

        if raw_df.empty:
            logger.info("PTT 新聞 %s 無資料可上傳。", date)
            self._check_incomplete(f"（{date}）")
            return {"date": date, "record_count": 0, "file_count": 0}

        new_df = self.filter_new_records(raw_df, date)

        if new_df.empty:
            logger.info("PTT 新聞 %s 所有記錄皆已存在，跳過上傳。", date)
            self._check_incomplete(f"（{date}）")
            return {"date": date, "record_count": 0, "file_count": 0}

        # 驗證 schema 並上傳 metadata
        meta_df = self.check_schema(new_df)
        record_count = self.upload_metadata(meta_df)

        # 儲存全文
        file_count = self.save_contents(new_df, date)

        # 記錄已上傳日期
        self.record_uploaded_date(date)

        logger.info(
            "PTT 新聞 %s 已上傳 %d 筆 metadata，儲存 %d 個全文檔案。",
            date, record_count, file_count,
        )
        self._check_incomplete(f"（{date}）")
        return {
            "date": date,
            "record_count": record_count,
            "file_count": file_count,
        }

    def upload_by_hours(self, hours):
        """以時數模式執行 PTT 新聞上傳流程。

        使用 hours 參數呼叫爬蟲 API 取得過去指定小時數的新聞，
        自動依 Date 分組處理跨日資料，並對每個日期分別執行
        去重、schema 驗證、metadata 上傳、全文儲存。

        Args:
            hours (int): 要回溯的小時數（1-72）。

        Returns:
            dict: 包含 hours、record_count、file_count、dates 的結果字典。

        Raises:
            SourceError: 爬取失敗或結果不完整且重抓有機會補齊時拋出
                （可重試，資料已取得的部分先落地）。
            OutOfRangeError: 區間超出來源可回溯範圍時拋出（不可重試）。
        """
        raw_df = self.crawl_data_by_hours(hours)

        if raw_df.empty:
            logger.info("PTT 新聞過去 %d 小時無資料可上傳。", hours)
            self._check_incomplete(f"（hours={hours}）")
            return {
                "hours": hours,
                "record_count": 0,
                "file_count": 0,
                "dates": [],
            }

        total_records = 0
        total_files = 0
        processed_dates = []

        # 依 Date 分組處理跨日資料
        for date_str, group_df in raw_df.groupby("Date"):
            new_df = self.filter_new_records(group_df, date_str)

            if new_df.empty:
                logger.info(
                    "PTT 新聞 %s 所有記錄皆已存在，跳過上傳。",
                    date_str,
                )
                continue

            # 驗證 schema 並上傳 metadata
            meta_df = self.check_schema(new_df)
            record_count = self.upload_metadata(meta_df)

            # 儲存全文
            file_count = self.save_contents(new_df, date_str)

            # 記錄已上傳日期
            self.record_uploaded_date(date_str)

            total_records += record_count
            total_files += file_count
            processed_dates.append(date_str)

        logger.info(
            "PTT 新聞（hours=%d）已上傳 %d 筆 metadata，"
            "儲存 %d 個全文檔案，涵蓋日期：%s。",
            hours, total_records, total_files, processed_dates,
        )
        self._check_incomplete(f"（hours={hours}）")
        return {
            "hours": hours,
            "record_count": total_records,
            "file_count": total_files,
            "dates": processed_dates,
        }

    def record_uploaded_date(self, date):
        """將日期記錄至 PTTUploaded 資料表。

        Args:
            date (str): 日期字串（YYYY-MM-DD）。
        """
        try:
            self.conn.execute(
                text(
                    "INSERT IGNORE INTO PTTUploaded (Date) VALUES (:date)"
                ),
                {"date": date},
            )
            self.conn.commit()
        except Exception as e:
            logger.error("記錄 PTT 已上傳日期失敗（%s）：%s", date, e)

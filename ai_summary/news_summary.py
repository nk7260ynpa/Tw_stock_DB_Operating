"""每日新聞摘要產生器。"""

import logging
from pathlib import Path

from sqlalchemy import text

from ai_summary.base import AISummaryBase
from DailyUpload import HOST, USER, PASSWORD
from routers import MySQLRouter

logger = logging.getLogger(__name__)

NEWS_CONTENT_DIR = Path("/workspace/NewsContents")
OUTPUT_DIR = Path("/workspace/DailyNews")
MAX_ARTICLES_PER_SOURCE = 10

SOURCES = {
    "CTEE": "工商時報",
    "CNYES": "鉅亨網",
    "PTT": "批踢踢股版",
    "MoneyUDN": "經濟日報",
}

SYSTEM_PROMPT = """\
你是一位台股新聞分析助手。你的任務是根據提供的多來源新聞標題與全文，\
產生每日新聞摘要。

摘要格式要求：

# {YYYY-MM-DD} 台股每日新聞摘要

## 市場總覽

（綜合所有來源，用 3~5 句話描述當日台股市場重點）

## 重點新聞

### 工商時報（CTEE）

- **{標題}**：{一句話摘要}
- ...

### 鉅亨網（CNYES）

- **{標題}**：{一句話摘要}
- ...

### 批踢踢股版（PTT）

- **{標題}**：{一句話摘要}
- ...

### 經濟日報（MoneyUDN）

- **{標題}**：{一句話摘要}
- ...

## 統計

| 來源 | 新聞數量 |
|------|----------|
| CTEE | N |
| CNYES | N |
| PTT | N |
| MoneyUDN | N |
| **合計** | **N** |

注意事項：
- 摘要以繁體中文撰寫
- PTT 內容可能含推文雜訊，摘要時聚焦文章本文
- MoneyUDN 的 .md 可能含圖片語法，摘要時忽略圖片
- 若某來源該日無新聞，該區段標注「（當日無資料）」
"""


class NewsSummaryGenerator(AISummaryBase):
    """每日新聞摘要產生器。

    查詢 MySQL 取得各來源新聞標題，讀取全文後透過 Claude Agent SDK
    產生摘要，並寫入 DailyNews 目錄。
    """

    def _query_headlines(self, date: str) -> dict[str, list[dict]]:
        """查詢指定日期所有新聞來源的標題與 ContentFile。

        Args:
            date: 目標日期（YYYY-MM-DD）。

        Returns:
            dict: 以來源名稱為 key，每個 value 是含 time、head、
                content_file 的 dict list。
        """
        conn = MySQLRouter(HOST, USER, PASSWORD, "NEWS").mysql_conn
        try:
            sql = text(
                "(SELECT 'CTEE' AS Source, Time, Head, ContentFile "
                "FROM CTEE WHERE Date = :date) "
                "UNION ALL "
                "(SELECT 'CNYES', Time, Head, ContentFile "
                "FROM CNYES WHERE Date = :date) "
                "UNION ALL "
                "(SELECT 'PTT', Time, Head, ContentFile "
                "FROM PTT WHERE Date = :date) "
                "UNION ALL "
                "(SELECT 'MoneyUDN', Time, Head, ContentFile "
                "FROM MoneyUDN WHERE Date = :date) "
                "ORDER BY Source, Time DESC"
            )
            rows = conn.execute(sql, {"date": date}).fetchall()
        finally:
            conn.close()

        headlines: dict[str, list[dict]] = {
            source: [] for source in SOURCES
        }
        for row in rows:
            source = row[0]
            if source in headlines:
                headlines[source].append({
                    "time": str(row[1]) if row[1] else "",
                    "head": row[2] or "",
                    "content_file": row[3] or "",
                })

        return headlines

    def _read_articles(
        self, headlines: dict[str, list[dict]]
    ) -> tuple[str, dict[str, int]]:
        """讀取各來源的新聞全文。

        每來源最多讀取 MAX_ARTICLES_PER_SOURCE 篇全文，
        其餘僅保留標題。

        Args:
            headlines: 各來源新聞標題字典。

        Returns:
            tuple: (組裝好的文字內容, 各來源新聞數量統計)。
        """
        sections = []
        stats: dict[str, int] = {}

        for source, source_name in SOURCES.items():
            articles = headlines.get(source, [])
            stats[source] = len(articles)

            if not articles:
                sections.append(
                    f"=== {source_name}（{source}）===\n（當日無資料）\n"
                )
                continue

            lines = [
                f"=== {source_name}（{source}）"
                f"共 {len(articles)} 篇 ==="
            ]

            for i, article in enumerate(articles):
                head = article["head"]
                content_file = article["content_file"]

                if i < MAX_ARTICLES_PER_SOURCE and content_file:
                    file_path = NEWS_CONTENT_DIR / source / content_file
                    try:
                        content = file_path.read_text(encoding="utf-8")
                        # 截斷過長的文章（避免 prompt 過大）
                        if len(content) > 3000:
                            content = content[:3000] + "\n...(截斷)"
                        lines.append(
                            f"\n[全文 {i + 1}] {head}\n{content}"
                        )
                    except (FileNotFoundError, OSError):
                        lines.append(f"[僅標題] {head}")
                else:
                    lines.append(f"[僅標題] {head}")

            sections.append("\n".join(lines))

        return "\n\n".join(sections), stats

    def run(self, date: str) -> dict:
        """產生指定日期的每日新聞摘要。

        Args:
            date: 目標日期（YYYY-MM-DD）。

        Returns:
            dict: 包含 status、output_path、stats 等欄位的結果。
        """
        output_path = OUTPUT_DIR / f"{date}.md"

        logger.info("查詢 %s 新聞資料...", date)
        headlines = self._query_headlines(date)

        total = sum(len(v) for v in headlines.values())
        if total == 0:
            logger.warning("該日期無新聞資料：%s", date)
            return {
                "status": "skipped",
                "error": f"該日期無新聞資料（{date}）",
            }

        logger.info("讀取新聞全文（共 %d 篇）...", total)
        news_content, stats = self._read_articles(headlines)

        # 確保輸出目錄存在
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        prompt = (
            f"以下是 {date} 的台股相關新聞資料，"
            f"請根據這些資料產生每日新聞摘要，"
            f"並使用 Write 工具寫入 {output_path}\n\n"
            f"摘要標題格式：# {date} 台股每日新聞摘要\n"
            f"檔名必須是 {date}.md\n\n"
            f"--- 新聞資料 ---\n\n"
            f"{news_content}"
        )

        logger.info("開始產生每日新聞摘要：%s", date)

        self._run_agent(
            prompt=prompt,
            allowed_tools=["Write"],
            cwd="/workspace",
            system_prompt=SYSTEM_PROMPT,
        )

        if output_path.exists():
            logger.info("每日新聞摘要已寫入：%s", output_path)
            return {
                "status": "success",
                "output_path": str(output_path),
                "stats": stats,
            }

        logger.error("每日新聞摘要輸出檔案未產生：%s", output_path)
        return {
            "status": "failed",
            "error": f"摘要檔案未產生（{output_path}）",
            "stats": stats,
        }

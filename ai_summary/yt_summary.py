"""YT 逐字稿精華摘要產生器。"""

import logging
from pathlib import Path

from ai_summary.base import AISummaryBase

logger = logging.getLogger(__name__)

TRANSCRIPT_DIR = Path("/workspace/NewsContents/YT")
OUTPUT_DIR = Path("/workspace/YTNews")

SYSTEM_PROMPT = """\
你是一位台股財經分析助手。你的任務是根據游庭皓的財經皓角直播逐字稿，\
整理出精華摘要。

摘要格式要求：

# {YYYY-MM-DD} 游庭皓的財經皓角 — 精華摘要

## 今日重點

（用 3~5 句話概述今日直播的核心觀點）

## 市場觀點

- **{主題}**：{觀點摘要}
- ...

## 個股分析

- **{股票名稱/代號}**：{分析重點}
- ...

## 操作建議

- {建議內容}
- ...

## 其他重點

- {其他值得關注的內容}
- ...

注意事項：
- 摘要以繁體中文撰寫
- 逐字稿可能包含口語化內容，摘要時需整理為書面語
- 聚焦於有投資參考價值的內容（市場分析、個股點評、操作策略）
- 過濾掉閒聊、開場寒暄等無關內容
- 若逐字稿內容過短或無實質內容，標注「（當日內容較少，無法產生完整摘要）」
"""


class YTSummaryGenerator(AISummaryBase):
    """YT 逐字稿精華摘要產生器。

    讀取指定日期的 YT 逐字稿，透過 Claude Agent SDK 產生精華摘要，
    並寫入 YTNews 目錄。
    """

    def run(self, date: str) -> dict:
        """產生指定日期的 YT 精華摘要。

        Args:
            date: 目標日期（YYYY-MM-DD）。

        Returns:
            dict: 包含 status、output_path 等欄位的結果。
        """
        transcript_path = TRANSCRIPT_DIR / date / f"{date}.md"
        output_path = OUTPUT_DIR / f"{date}.md"

        if not transcript_path.exists():
            logger.warning("YT 逐字稿不存在：%s", transcript_path)
            return {
                "status": "skipped",
                "error": f"該日期無 YT 逐字稿資料（{date}）",
            }

        # 確保輸出目錄存在
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        prompt = (
            f"請執行以下步驟：\n\n"
            f"1. 使用 Read 工具讀取逐字稿檔案：{transcript_path}\n"
            f"2. 根據逐字稿內容，產生精華摘要\n"
            f"3. 使用 Write 工具將摘要寫入：{output_path}\n\n"
            f"摘要標題格式：# {date} 游庭皓的財經皓角 — 精華摘要\n"
            f"檔名必須是 {date}.md"
        )

        logger.info("開始產生 YT 精華摘要：%s", date)

        self._run_agent(
            prompt=prompt,
            allowed_tools=["Read", "Write"],
            cwd="/workspace",
            system_prompt=SYSTEM_PROMPT,
        )

        if output_path.exists():
            logger.info("YT 精華摘要已寫入：%s", output_path)
            return {
                "status": "success",
                "output_path": str(output_path),
            }

        logger.error("YT 精華摘要輸出檔案未產生：%s", output_path)
        return {
            "status": "failed",
            "error": f"摘要檔案未產生（{output_path}）",
        }

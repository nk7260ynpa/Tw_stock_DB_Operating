"""AI 摘要基底類別，封裝 Claude Agent SDK 呼叫邏輯。"""

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ResultMessage,
    query,
)

logger = logging.getLogger(__name__)

# 預設模型
DEFAULT_MODEL = os.environ.get(
    "AI_SUMMARY_MODEL", "claude-sonnet-4-20250514"
)
# 最大 agentic 輪數
DEFAULT_MAX_TURNS = 10


class AISummaryBase(ABC):
    """AI 摘要任務的基底類別。

    子類須實作 ``run(date)`` 方法，定義完整的摘要產生流程。
    """

    def __init__(self):
        self.model = DEFAULT_MODEL
        self.max_turns = DEFAULT_MAX_TURNS

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY 環境變數未設定，"
                "無法使用 AI 摘要功能。"
            )

    def _run_agent(
        self,
        prompt: str,
        allowed_tools: list[str],
        cwd: str = "/workspace",
        system_prompt: str | None = None,
    ) -> dict:
        """透過 Claude Agent SDK 執行 agent 任務。

        在新的 event loop 中執行 async query（配合 sync scheduler thread）。

        Args:
            prompt: 要傳給 agent 的 prompt。
            allowed_tools: 允許使用的工具清單。
            cwd: agent 的工作目錄。
            system_prompt: 系統 prompt。

        Returns:
            dict: 包含 result、cost、is_error 欄位。

        Raises:
            RuntimeError: Agent 執行失敗時。
        """
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self._async_run_agent(
                    prompt, allowed_tools, cwd, system_prompt
                )
            )
        finally:
            loop.close()

    async def _async_run_agent(
        self,
        prompt: str,
        allowed_tools: list[str],
        cwd: str,
        system_prompt: str | None,
    ) -> dict:
        """非同步執行 agent query。

        Args:
            prompt: 要傳給 agent 的 prompt。
            allowed_tools: 允許使用的工具清單。
            cwd: agent 的工作目錄。
            system_prompt: 系統 prompt。

        Returns:
            dict: 包含 result、cost、is_error 欄位。
        """
        options = ClaudeAgentOptions(
            allowed_tools=allowed_tools,
            permission_mode="acceptEdits",
            cwd=cwd,
            model=self.model,
            max_turns=self.max_turns,
        )
        if system_prompt:
            options.system_prompt = system_prompt

        result_data = {
            "result": None,
            "cost": None,
            "is_error": False,
        }

        async for message in query(prompt=prompt, options=options):
            if isinstance(message, ResultMessage):
                result_data["result"] = message.result
                result_data["cost"] = message.total_cost_usd
                result_data["is_error"] = bool(message.is_error)

        if result_data["is_error"]:
            raise RuntimeError(
                f"Agent 執行失敗：{result_data['result']}"
            )

        logger.info(
            "Agent 任務完成，花費 $%.4f",
            result_data["cost"] or 0,
        )
        return result_data

    @abstractmethod
    def run(self, date: str) -> dict:
        """執行摘要產生流程。

        Args:
            date: 目標日期（YYYY-MM-DD）。

        Returns:
            dict: 包含 status、output_path 等欄位的結果。
        """

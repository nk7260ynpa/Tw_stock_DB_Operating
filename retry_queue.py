"""網路失敗重試佇列模組。

當排程任務因網路中斷失敗時，將任務加入重試佇列，
每小時自動檢查網路狀態並重試，最多重試 5 次。
超過上限的任務標為 exhausted，保留在佇列中供手動一鍵重新執行。

針對「資料尚未發布」這類暫時性失敗（當下抓不到、隔日才會有），
另提供「隔日重排」機制：每日將未達重排上限的 exhausted 任務
重設為 pending 再試一輪，避免永久放棄可回補的資料。
"""

import json
import logging
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


@dataclass
class RetryTask:
    """重試任務資料類別。

    Attributes:
        task_id: 任務唯一識別碼。
        task_type: 任務類型（daily_upload/ctee_news/cnyes_news/
            ptt_news/moneyudn_news/tdcc）。
        params: 任務參數（如 db_name、date、hours 等）。
        error_message: 失敗時的錯誤訊息。
        failed_at: 首次失敗時間。
        retry_count: 已重試次數。
        max_retries: 最大重試次數。
        status: 任務狀態（pending/retrying/success/exhausted）。
        last_retry_at: 最後一次重試時間。
        created_by_job_id: 建立此重試任務的原始排程 job ID。
        requeue_count: 自 exhausted 被「隔日重排」重設為 pending 的次數。
        max_requeues: 最大隔日重排次數，超過後維持 exhausted 視為永久失敗。
    """

    task_id: str
    task_type: str
    params: dict
    error_message: str
    failed_at: str
    retry_count: int = 0
    max_retries: int = 5
    status: str = "pending"
    last_retry_at: str | None = None
    created_by_job_id: str | None = None
    requeue_count: int = 0
    max_requeues: int = 3


class RetryQueue:
    """網路失敗重試佇列。

    內部使用 dict 儲存任務，透過 threading.Lock 確保執行緒安全。
    每次變更自動持久化至 JSON 檔案。
    """

    def __init__(self, persist_path):
        """初始化重試佇列。

        Args:
            persist_path (str | Path): 持久化檔案路徑。
        """
        self._tasks: dict[str, RetryTask] = {}
        self._lock = threading.Lock()
        self._persist_path = Path(persist_path)
        self._load()

    def _load(self):
        """從 JSON 檔案載入佇列。"""
        if not self._persist_path.exists():
            return
        try:
            with open(self._persist_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for task_data in data:
                task = RetryTask(**task_data)
                self._tasks[task.task_id] = task
            logger.info(
                "已從 %s 載入 %d 筆重試任務。",
                self._persist_path, len(self._tasks),
            )
        except Exception as e:
            logger.error("載入重試佇列失敗：%s", e)

    def _save(self):
        """將佇列持久化至 JSON 檔案。

        注意：呼叫前須已持有 _lock。
        """
        try:
            data = [asdict(task) for task in self._tasks.values()]
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._persist_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error("持久化重試佇列失敗：%s", e)

    def add(self, task_type, params, error_message,
            created_by_job_id=None):
        """新增重試任務。

        Args:
            task_type (str): 任務類型。
            params (dict): 任務參數。
            error_message (str): 錯誤訊息。
            created_by_job_id (str | None): 原始排程 job ID。

        Returns:
            str: 新增任務的 task_id。
        """
        task_id = str(uuid.uuid4())[:8]
        task = RetryTask(
            task_id=task_id,
            task_type=task_type,
            params=params,
            error_message=error_message,
            failed_at=datetime.now().isoformat(),
            created_by_job_id=created_by_job_id,
        )
        with self._lock:
            self._tasks[task_id] = task
            self._save()
        logger.info(
            "已加入重試佇列：%s（%s，%s）",
            task_id, task_type, params,
        )
        return task_id

    def remove(self, task_id):
        """移除指定任務。

        Args:
            task_id (str): 任務 ID。

        Returns:
            bool: 成功移除回傳 True，任務不存在回傳 False。
        """
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                self._save()
                return True
            return False

    def get_pending(self):
        """取得所有 pending 狀態的任務。

        Returns:
            list[RetryTask]: pending 任務清單。
        """
        with self._lock:
            return [
                t for t in self._tasks.values()
                if t.status == "pending"
            ]

    def get_all(self):
        """取得所有任務。

        Returns:
            list[RetryTask]: 所有任務清單。
        """
        with self._lock:
            return list(self._tasks.values())

    def update_status(self, task_id, status, error_message=None):
        """更新任務狀態。

        Args:
            task_id (str): 任務 ID。
            status (str): 新狀態（pending/retrying/success/exhausted）。
            error_message (str | None): 更新錯誤訊息。
        """
        with self._lock:
            if task_id not in self._tasks:
                return
            task = self._tasks[task_id]
            task.status = status
            if error_message is not None:
                task.error_message = error_message
            if status == "retrying":
                task.last_retry_at = datetime.now().isoformat()
                task.retry_count += 1
            self._save()

    def clear_completed(self):
        """清除所有已完成（success）的任務。

        Returns:
            int: 清除的任務數量。
        """
        with self._lock:
            to_remove = [
                tid for tid, t in self._tasks.items()
                if t.status == "success"
            ]
            for tid in to_remove:
                del self._tasks[tid]
            if to_remove:
                self._save()
            return len(to_remove)

    def reset_exhausted(self):
        """將所有 exhausted 任務重設為 pending，retry_count 歸零。

        供使用者於 Web 介面「一鍵重新執行」手動觸發，無視重排上限。

        Returns:
            int: 重設的任務數量。
        """
        with self._lock:
            count = 0
            for task in self._tasks.values():
                if task.status == "exhausted":
                    task.status = "pending"
                    task.retry_count = 0
                    count += 1
            if count > 0:
                self._save()
            return count

    def requeue_exhausted(self):
        """隔日重排：將未達重排上限的 exhausted 任務重設為 pending。

        用於「資料尚未發布」這類暫時性失敗：當日抓不到資料被標為
        exhausted 後，隔日資料通常已可取得，故每日將其重設為 pending
        再試一輪。每重排一次 requeue_count +1、retry_count 歸零；
        達 max_requeues 後維持 exhausted，視為永久失敗（如美股假日
        永無資料），交由人工處理。

        Returns:
            tuple[int, int]: (重排筆數, 已達上限維持 exhausted 筆數)。
        """
        with self._lock:
            requeued = 0
            kept = 0
            for task in self._tasks.values():
                if task.status != "exhausted":
                    continue
                if task.requeue_count >= task.max_requeues:
                    kept += 1
                    continue
                task.status = "pending"
                task.retry_count = 0
                task.requeue_count += 1
                requeued += 1
            if requeued > 0:
                self._save()
            return requeued, kept

    def clear_exhausted(self):
        """清除所有已放棄（exhausted）的任務。

        Returns:
            int: 清除的任務數量。
        """
        with self._lock:
            to_remove = [
                tid for tid, t in self._tasks.items()
                if t.status == "exhausted"
            ]
            for tid in to_remove:
                del self._tasks[tid]
            if to_remove:
                self._save()
            return len(to_remove)


def is_network_error(exception):
    """判斷異常是否為網路相關錯誤。

    Args:
        exception: 要判斷的異常。

    Returns:
        bool: 若為網路錯誤回傳 True。
    """
    from data_upload.base import NetworkError
    if isinstance(exception, NetworkError):
        return True
    if isinstance(exception, requests.ConnectionError):
        return True
    if isinstance(exception, requests.Timeout):
        return True
    if isinstance(exception, ConnectionError):
        return True
    if isinstance(exception, OSError):
        # ECONNREFUSED, ENETUNREACH, EHOSTUNREACH 等
        return True
    return False


def check_network_available(crawler_host):
    """檢查爬蟲服務是否可達。

    透過 HTTP GET 爬蟲根路徑判斷網路連通性。

    Args:
        crawler_host (str): 爬蟲主機位址（含 port）。

    Returns:
        bool: 可達回傳 True，不可達回傳 False。
    """
    try:
        resp = requests.get(
            f"http://{crawler_host}/",
            timeout=10,
        )
        return True
    except Exception:
        return False

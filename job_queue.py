"""任務佇列模組。

提供 FIFO 佇列機制，讓多個上傳任務依序執行，
同一時間只有一個任務在執行。
"""

import logging
import threading
from collections import deque

logger = logging.getLogger(__name__)


class JobQueue:
    """任務佇列，管理上傳任務的排隊與依序執行。

    使用 collections.deque 作為 FIFO 佇列，
    threading.Event 喚醒消費者執行緒，避免忙等待。

    Attributes:
        upload_jobs: 共用的任務狀態字典。
        jobs_lock: 保護 upload_jobs 的執行緒鎖。
    """

    def __init__(self, upload_jobs, jobs_lock):
        """初始化佇列。

        Args:
            upload_jobs: 共用的任務狀態字典。
            jobs_lock: 保護 upload_jobs 的執行緒鎖。
        """
        self.upload_jobs = upload_jobs
        self.jobs_lock = jobs_lock
        self._queue = deque()
        self._queue_lock = threading.Lock()
        self._event = threading.Event()
        self._active_job_id = None

    def enqueue(self, job_id, target, args=()):
        """將任務加入佇列。

        若目前無執行中的任務，則直接啟動；否則排入佇列等待。

        Args:
            job_id: 任務 ID。
            target: 要執行的目標函式。
            args: 傳給目標函式的參數。

        Returns:
            int: 排隊位置（0 表示正在執行或即將執行）。
        """
        with self._queue_lock:
            if self._active_job_id is None:
                self._active_job_id = job_id
                t = threading.Thread(
                    target=self._run_and_notify,
                    args=(job_id, target, args),
                    daemon=True,
                )
                t.start()
                logger.info("任務 %s 直接啟動執行", job_id)
                return 0

            self._queue.append((job_id, target, args))
            position = len(self._queue)

            with self.jobs_lock:
                self.upload_jobs[job_id]["status"] = "queued"
                self.upload_jobs[job_id]["queue_position"] = position

            logger.info("任務 %s 排入佇列，位置 %d", job_id, position)
            return position

    def consumer_loop(self):
        """消費者執行緒主迴圈。

        持續等待事件通知，當有任務完成時檢查並啟動下一個任務。
        """
        logger.info("佇列消費者執行緒已啟動")
        while True:
            self._event.wait()
            self._event.clear()
            self._process_next()

    def _process_next(self):
        """檢查並啟動佇列中的下一個任務。"""
        with self._queue_lock:
            if not self._queue:
                self._active_job_id = None
                return

            job_id, target, args = self._queue.popleft()
            self._active_job_id = job_id
            self._update_positions()

        with self.jobs_lock:
            if job_id in self.upload_jobs:
                self.upload_jobs[job_id].pop("queue_position", None)

        t = threading.Thread(
            target=self._run_and_notify,
            args=(job_id, target, args),
            daemon=True,
        )
        t.start()
        logger.info("佇列取出任務 %s 開始執行", job_id)

    def _run_and_notify(self, job_id, target, args):
        """包裝任務執行，結束後喚醒消費者。

        Args:
            job_id: 任務 ID。
            target: 要執行的目標函式。
            args: 傳給目標函式的參數。
        """
        try:
            target(*args)
        except Exception as e:
            logger.error("任務 %s 執行異常: %s", job_id, e)
        finally:
            self._event.set()

    def _update_positions(self):
        """更新佇列中剩餘任務的排隊位置。

        必須在持有 _queue_lock 的狀態下呼叫。
        """
        for i, (qjob_id, _, _) in enumerate(self._queue):
            with self.jobs_lock:
                if qjob_id in self.upload_jobs:
                    self.upload_jobs[qjob_id]["queue_position"] = i + 1

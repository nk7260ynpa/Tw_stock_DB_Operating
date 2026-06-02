"""RetryQueue 單元測試模組。"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

import requests

from retry_queue import RetryQueue, RetryTask, is_network_error, check_network_available
from data_upload.base import NetworkError


class TestRetryQueueAdd(unittest.TestCase):
    """測試 RetryQueue.add 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()
        self.queue = RetryQueue(self.tmp.name)

    def tearDown(self):
        """清理測試環境。"""
        os.unlink(self.tmp.name)

    def test_add_task(self):
        """測試新增任務。"""
        task_id = self.queue.add(
            "daily_upload",
            {"db_name": "TWSE", "dates": ["2026-01-01"]},
            "Connection refused",
        )

        self.assertIsNotNone(task_id)
        tasks = self.queue.get_all()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].task_type, "daily_upload")
        self.assertEqual(tasks[0].status, "pending")
        self.assertEqual(tasks[0].retry_count, 0)

    def test_add_multiple_tasks(self):
        """測試新增多筆任務。"""
        self.queue.add("ctee_news", {"hours": 24}, "timeout")
        self.queue.add("cnyes_news", {"hours": 24}, "timeout")

        tasks = self.queue.get_all()
        self.assertEqual(len(tasks), 2)

    def test_add_with_job_id(self):
        """測試新增任務時帶入 created_by_job_id。"""
        task_id = self.queue.add(
            "tdcc", {}, "error", created_by_job_id="abc123"
        )

        tasks = self.queue.get_all()
        self.assertEqual(tasks[0].created_by_job_id, "abc123")


class TestRetryQueueRemove(unittest.TestCase):
    """測試 RetryQueue.remove 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()
        self.queue = RetryQueue(self.tmp.name)

    def tearDown(self):
        """清理測試環境。"""
        os.unlink(self.tmp.name)

    def test_remove_existing_task(self):
        """測試移除存在的任務。"""
        task_id = self.queue.add("tdcc", {}, "error")

        result = self.queue.remove(task_id)

        self.assertTrue(result)
        self.assertEqual(len(self.queue.get_all()), 0)

    def test_remove_nonexistent_task(self):
        """測試移除不存在的任務。"""
        result = self.queue.remove("nonexistent")

        self.assertFalse(result)


class TestRetryQueueGetPending(unittest.TestCase):
    """測試 RetryQueue.get_pending 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()
        self.queue = RetryQueue(self.tmp.name)

    def tearDown(self):
        """清理測試環境。"""
        os.unlink(self.tmp.name)

    def test_get_pending_filters_correctly(self):
        """測試 get_pending 只回傳 pending 狀態的任務。"""
        tid1 = self.queue.add("ctee_news", {"hours": 24}, "err1")
        tid2 = self.queue.add("cnyes_news", {"hours": 24}, "err2")
        self.queue.update_status(tid2, "success")

        pending = self.queue.get_pending()

        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].task_id, tid1)

    def test_get_pending_empty_queue(self):
        """測試空佇列時回傳空清單。"""
        pending = self.queue.get_pending()

        self.assertEqual(len(pending), 0)


class TestRetryQueueUpdateStatus(unittest.TestCase):
    """測試 RetryQueue.update_status 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()
        self.queue = RetryQueue(self.tmp.name)

    def tearDown(self):
        """清理測試環境。"""
        os.unlink(self.tmp.name)

    def test_update_status_to_retrying(self):
        """測試更新狀態為 retrying 時 retry_count 遞增。"""
        task_id = self.queue.add("tdcc", {}, "error")

        self.queue.update_status(task_id, "retrying")

        tasks = self.queue.get_all()
        self.assertEqual(tasks[0].status, "retrying")
        self.assertEqual(tasks[0].retry_count, 1)
        self.assertIsNotNone(tasks[0].last_retry_at)

    def test_update_status_to_success(self):
        """測試更新狀態為 success。"""
        task_id = self.queue.add("tdcc", {}, "error")

        self.queue.update_status(task_id, "success")

        tasks = self.queue.get_all()
        self.assertEqual(tasks[0].status, "success")

    def test_update_status_with_error_message(self):
        """測試更新狀態時附帶錯誤訊息。"""
        task_id = self.queue.add("tdcc", {}, "original error")

        self.queue.update_status(task_id, "exhausted", "new error")

        tasks = self.queue.get_all()
        self.assertEqual(tasks[0].error_message, "new error")

    def test_update_nonexistent_task(self):
        """測試更新不存在的任務不拋出異常。"""
        self.queue.update_status("nonexistent", "success")


class TestRetryQueueClearCompleted(unittest.TestCase):
    """測試 RetryQueue.clear_completed 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()
        self.queue = RetryQueue(self.tmp.name)

    def tearDown(self):
        """清理測試環境。"""
        os.unlink(self.tmp.name)

    def test_clear_completed(self):
        """測試清除已完成的任務。"""
        tid1 = self.queue.add("ctee_news", {"hours": 24}, "err1")
        tid2 = self.queue.add("cnyes_news", {"hours": 24}, "err2")
        self.queue.update_status(tid1, "success")

        count = self.queue.clear_completed()

        self.assertEqual(count, 1)
        tasks = self.queue.get_all()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].task_id, tid2)

    def test_clear_completed_no_success(self):
        """測試沒有已完成任務時回傳 0。"""
        self.queue.add("tdcc", {}, "error")

        count = self.queue.clear_completed()

        self.assertEqual(count, 0)


class TestRetryQueueResetExhausted(unittest.TestCase):
    """測試 RetryQueue.reset_exhausted 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()
        self.queue = RetryQueue(self.tmp.name)

    def tearDown(self):
        """清理測試環境。"""
        os.unlink(self.tmp.name)

    def test_reset_exhausted(self):
        """測試重設 exhausted 任務為 pending。"""
        tid = self.queue.add("tdcc", {}, "error")
        # 模擬多次重試後 exhausted
        for _ in range(5):
            self.queue.update_status(tid, "retrying")
        self.queue.update_status(tid, "exhausted")

        count = self.queue.reset_exhausted()

        self.assertEqual(count, 1)
        tasks = self.queue.get_all()
        self.assertEqual(tasks[0].status, "pending")
        self.assertEqual(tasks[0].retry_count, 0)

    def test_reset_exhausted_no_exhausted(self):
        """測試沒有 exhausted 任務時回傳 0。"""
        self.queue.add("tdcc", {}, "error")

        count = self.queue.reset_exhausted()

        self.assertEqual(count, 0)


class TestRetryQueueRequeueExhausted(unittest.TestCase):
    """測試 RetryQueue.requeue_exhausted 方法（隔日重排）。"""

    def setUp(self):
        """初始化測試環境。"""
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()
        self.queue = RetryQueue(self.tmp.name)

    def tearDown(self):
        """清理測試環境。"""
        os.unlink(self.tmp.name)

    def _make_exhausted(self, task_type="bitcoin_price",
                        params=None):
        """建立一筆 exhausted 任務並回傳 task_id。"""
        tid = self.queue.add(
            task_type, params or {"date": "2026-06-01"}, "no data"
        )
        self.queue.update_status(tid, "exhausted")
        return tid

    def test_requeue_resets_exhausted_to_pending(self):
        """測試重排將 exhausted 重設為 pending，retry_count 歸零。"""
        tid = self._make_exhausted()
        for _ in range(5):
            self.queue.update_status(tid, "retrying")
        self.queue.update_status(tid, "exhausted")

        requeued, kept = self.queue.requeue_exhausted()

        self.assertEqual(requeued, 1)
        self.assertEqual(kept, 0)
        task = self.queue.get_all()[0]
        self.assertEqual(task.status, "pending")
        self.assertEqual(task.retry_count, 0)
        self.assertEqual(task.requeue_count, 1)

    def test_requeue_stops_at_max_requeues(self):
        """測試達到 max_requeues 後維持 exhausted 不再重排。"""
        tid = self._make_exhausted()

        # 連續重排直到達上限（預設 max_requeues=3）
        for expected in range(1, 4):
            self.queue.update_status(tid, "exhausted")
            requeued, kept = self.queue.requeue_exhausted()
            self.assertEqual(requeued, 1)
            self.assertEqual(
                self.queue.get_all()[0].requeue_count, expected
            )

        # 第 4 次：已達上限，維持 exhausted
        self.queue.update_status(tid, "exhausted")
        requeued, kept = self.queue.requeue_exhausted()

        self.assertEqual(requeued, 0)
        self.assertEqual(kept, 1)
        self.assertEqual(self.queue.get_all()[0].status, "exhausted")

    def test_requeue_ignores_non_exhausted(self):
        """測試重排不影響非 exhausted 任務。"""
        self.queue.add("tdcc", {}, "error")  # pending

        requeued, kept = self.queue.requeue_exhausted()

        self.assertEqual(requeued, 0)
        self.assertEqual(kept, 0)
        self.assertEqual(self.queue.get_all()[0].status, "pending")


class TestRetryQueueClearExhausted(unittest.TestCase):
    """測試 RetryQueue.clear_exhausted 方法。"""

    def setUp(self):
        """初始化測試環境。"""
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()
        self.queue = RetryQueue(self.tmp.name)

    def tearDown(self):
        """清理測試環境。"""
        os.unlink(self.tmp.name)

    def test_clear_exhausted(self):
        """測試清除 exhausted 任務，保留其他狀態。"""
        tid1 = self.queue.add("bitcoin_price", {"date": "2026-06-01"},
                              "no data")
        tid2 = self.queue.add("tdcc", {}, "error")
        self.queue.update_status(tid1, "exhausted")

        count = self.queue.clear_exhausted()

        self.assertEqual(count, 1)
        tasks = self.queue.get_all()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].task_id, tid2)

    def test_clear_exhausted_none(self):
        """測試沒有 exhausted 任務時回傳 0。"""
        self.queue.add("tdcc", {}, "error")

        count = self.queue.clear_exhausted()

        self.assertEqual(count, 0)


class TestRetryQueueBackwardCompat(unittest.TestCase):
    """測試載入缺少新欄位的舊版 JSON 仍可正常運作。"""

    def setUp(self):
        """初始化測試環境。"""
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()

    def tearDown(self):
        """清理測試環境。"""
        os.unlink(self.tmp.name)

    def test_load_legacy_task_without_requeue_fields(self):
        """測試載入無 requeue_count/max_requeues 欄位的舊任務。"""
        legacy = [{
            "task_id": "abc12345",
            "task_type": "bitcoin_price",
            "params": {"date": "2026-06-01"},
            "error_message": "no data",
            "failed_at": "2026-06-01T07:18:04",
            "retry_count": 0,
            "max_retries": 5,
            "status": "exhausted",
            "last_retry_at": None,
            "created_by_job_id": None,
        }]
        with open(self.tmp.name, "w", encoding="utf-8") as f:
            json.dump(legacy, f)

        queue = RetryQueue(self.tmp.name)
        task = queue.get_all()[0]

        # 新欄位套用預設值
        self.assertEqual(task.requeue_count, 0)
        self.assertEqual(task.max_requeues, 3)
        # 重排機制對舊任務照常運作
        requeued, kept = queue.requeue_exhausted()
        self.assertEqual(requeued, 1)


class TestRetryQueuePersistence(unittest.TestCase):
    """測試 RetryQueue 持久化功能。"""

    def setUp(self):
        """初始化測試環境。"""
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()

    def tearDown(self):
        """清理測試環境。"""
        os.unlink(self.tmp.name)

    def test_persist_and_reload(self):
        """測試持久化後重新載入。"""
        queue1 = RetryQueue(self.tmp.name)
        queue1.add("ctee_news", {"hours": 24}, "Connection refused")
        queue1.add("tdcc", {}, "Timeout")

        # 重新載入
        queue2 = RetryQueue(self.tmp.name)
        tasks = queue2.get_all()

        self.assertEqual(len(tasks), 2)

    def test_persist_status_changes(self):
        """測試狀態變更後持久化。"""
        queue1 = RetryQueue(self.tmp.name)
        tid = queue1.add("tdcc", {}, "error")
        queue1.update_status(tid, "success")

        queue2 = RetryQueue(self.tmp.name)
        tasks = queue2.get_all()

        self.assertEqual(tasks[0].status, "success")

    def test_empty_file_on_init(self):
        """測試初始化時檔案不存在不會錯誤。"""
        os.unlink(self.tmp.name)
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        )
        self.tmp.close()
        os.unlink(self.tmp.name)

        queue = RetryQueue(self.tmp.name)
        tasks = queue.get_all()

        self.assertEqual(len(tasks), 0)

        # 重新建立檔案以供 tearDown 清理
        queue.add("tdcc", {}, "test")


class TestIsNetworkError(unittest.TestCase):
    """測試 is_network_error 函式。"""

    def test_network_error(self):
        """測試 NetworkError 回傳 True。"""
        self.assertTrue(is_network_error(NetworkError("timeout")))

    def test_connection_error(self):
        """測試 requests.ConnectionError 回傳 True。"""
        self.assertTrue(
            is_network_error(requests.ConnectionError("refused"))
        )

    def test_timeout_error(self):
        """測試 requests.Timeout 回傳 True。"""
        self.assertTrue(is_network_error(requests.Timeout("timeout")))

    def test_builtin_connection_error(self):
        """測試 Python 內建 ConnectionError 回傳 True。"""
        self.assertTrue(is_network_error(ConnectionError("refused")))

    def test_os_error(self):
        """測試 OSError 回傳 True。"""
        self.assertTrue(is_network_error(OSError("network unreachable")))

    def test_value_error(self):
        """測試 ValueError 回傳 False。"""
        self.assertFalse(is_network_error(ValueError("bad value")))

    def test_key_error(self):
        """測試 KeyError 回傳 False。"""
        self.assertFalse(is_network_error(KeyError("missing")))

    def test_generic_exception(self):
        """測試一般 Exception 回傳 False。"""
        self.assertFalse(is_network_error(Exception("something")))


class TestCheckNetworkAvailable(unittest.TestCase):
    """測試 check_network_available 函式。"""

    @patch("retry_queue.requests.get")
    def test_network_available(self, mock_get):
        """測試網路可達時回傳 True。"""
        mock_get.return_value = MagicMock(status_code=200)

        result = check_network_available("localhost:6738")

        self.assertTrue(result)
        mock_get.assert_called_once_with(
            "http://localhost:6738/", timeout=10
        )

    @patch("retry_queue.requests.get")
    def test_network_unavailable(self, mock_get):
        """測試網路不可達時回傳 False。"""
        mock_get.side_effect = requests.ConnectionError("refused")

        result = check_network_available("localhost:6738")

        self.assertFalse(result)


class TestNetworkErrorBubbling(unittest.TestCase):
    """測試 NetworkError 向上冒泡的行為。"""

    def test_network_error_is_crawl_error(self):
        """測試 NetworkError 是 CrawlError 的子類別。"""
        from data_upload.base import CrawlError
        err = NetworkError("Connection refused")
        self.assertIsInstance(err, CrawlError)

    def test_network_error_propagates(self):
        """測試 NetworkError 可以正常拋出與捕捉。"""
        with self.assertRaises(NetworkError):
            raise NetworkError("timeout")

    def test_network_error_caught_by_exception(self):
        """測試 NetworkError 可被 Exception 捕捉。"""
        try:
            raise NetworkError("timeout")
        except Exception as e:
            self.assertIsInstance(e, NetworkError)


if __name__ == "__main__":
    unittest.main()

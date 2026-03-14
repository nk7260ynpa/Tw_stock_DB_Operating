"""任務佇列模組單元測試。"""

import threading
import time
import unittest

from job_queue import JobQueue


class TestJobQueueEnqueue(unittest.TestCase):
    """測試 JobQueue.enqueue 方法。"""

    def setUp(self):
        """每次測試前建立新的佇列。"""
        self.upload_jobs = {}
        self.jobs_lock = threading.Lock()
        self.queue = JobQueue(self.upload_jobs, self.jobs_lock)

    def test_first_job_starts_immediately(self):
        """測試第一個任務直接啟動，回傳位置 0。"""
        called = threading.Event()

        def dummy(job_id):
            called.set()

        self.upload_jobs["job1"] = {"status": "queued"}
        position = self.queue.enqueue("job1", dummy, ("job1",))

        self.assertEqual(position, 0)
        called.wait(timeout=2)
        self.assertTrue(called.is_set())

    def test_second_job_queued(self):
        """測試第二個任務排入佇列，回傳位置 1。"""
        blocker = threading.Event()

        def blocking_task(job_id):
            blocker.wait(timeout=5)

        self.upload_jobs["job1"] = {"status": "queued"}
        self.upload_jobs["job2"] = {"status": "queued"}

        self.queue.enqueue("job1", blocking_task, ("job1",))
        position = self.queue.enqueue("job2", blocking_task, ("job2",))

        self.assertEqual(position, 1)
        self.assertEqual(self.upload_jobs["job2"]["status"], "queued")
        self.assertEqual(self.upload_jobs["job2"]["queue_position"], 1)

        blocker.set()

    def test_multiple_jobs_queued_positions(self):
        """測試多個任務排隊時位置正確。"""
        blocker = threading.Event()

        def blocking_task(job_id):
            blocker.wait(timeout=5)

        for i in range(4):
            self.upload_jobs[f"job{i}"] = {"status": "queued"}

        self.queue.enqueue("job0", blocking_task, ("job0",))
        self.queue.enqueue("job1", blocking_task, ("job1",))
        self.queue.enqueue("job2", blocking_task, ("job2",))
        pos3 = self.queue.enqueue("job3", blocking_task, ("job3",))

        self.assertEqual(pos3, 3)
        self.assertEqual(self.upload_jobs["job1"]["queue_position"], 1)
        self.assertEqual(self.upload_jobs["job2"]["queue_position"], 2)
        self.assertEqual(self.upload_jobs["job3"]["queue_position"], 3)

        blocker.set()


class TestJobQueueExecution(unittest.TestCase):
    """測試 JobQueue 任務依序執行。"""

    def setUp(self):
        """每次測試前建立新的佇列並啟動消費者。"""
        self.upload_jobs = {}
        self.jobs_lock = threading.Lock()
        self.queue = JobQueue(self.upload_jobs, self.jobs_lock)
        self.consumer = threading.Thread(
            target=self.queue.consumer_loop, daemon=True
        )
        self.consumer.start()

    def test_jobs_execute_sequentially(self):
        """測試任務依序執行，不會同時執行。"""
        execution_order = []
        lock = threading.Lock()

        def task(job_id):
            with lock:
                execution_order.append(f"{job_id}_start")
            time.sleep(0.1)
            with lock:
                execution_order.append(f"{job_id}_end")

        self.upload_jobs["a"] = {"status": "queued"}
        self.upload_jobs["b"] = {"status": "queued"}

        self.queue.enqueue("a", task, ("a",))
        self.queue.enqueue("b", task, ("b",))

        time.sleep(0.5)

        self.assertEqual(execution_order[0], "a_start")
        self.assertEqual(execution_order[1], "a_end")
        self.assertEqual(execution_order[2], "b_start")
        self.assertEqual(execution_order[3], "b_end")

    def test_next_job_starts_after_completion(self):
        """測試前一個任務完成後自動啟動下一個。"""
        completed = []

        def task(job_id):
            time.sleep(0.05)
            completed.append(job_id)

        self.upload_jobs["x"] = {"status": "queued"}
        self.upload_jobs["y"] = {"status": "queued"}

        self.queue.enqueue("x", task, ("x",))
        self.queue.enqueue("y", task, ("y",))

        time.sleep(0.5)

        self.assertEqual(completed, ["x", "y"])

    def test_failed_job_does_not_block_queue(self):
        """測試任務失敗不會阻塞佇列。"""
        completed = []

        def failing_task(job_id):
            raise RuntimeError("模擬失敗")

        def normal_task(job_id):
            completed.append(job_id)

        self.upload_jobs["fail"] = {"status": "queued"}
        self.upload_jobs["ok"] = {"status": "queued"}

        self.queue.enqueue("fail", failing_task, ("fail",))
        self.queue.enqueue("ok", normal_task, ("ok",))

        time.sleep(0.5)

        self.assertEqual(completed, ["ok"])

    def test_queue_position_updated_after_dequeue(self):
        """測試取出任務後佇列位置更新。"""
        blocker = threading.Event()

        def blocking_task(job_id):
            blocker.wait(timeout=5)

        def quick_task(job_id):
            pass

        for i in range(3):
            self.upload_jobs[f"j{i}"] = {"status": "queued"}

        self.queue.enqueue("j0", blocking_task, ("j0",))
        self.queue.enqueue("j1", quick_task, ("j1",))
        self.queue.enqueue("j2", quick_task, ("j2",))

        self.assertEqual(self.upload_jobs["j1"]["queue_position"], 1)
        self.assertEqual(self.upload_jobs["j2"]["queue_position"], 2)

        blocker.set()
        time.sleep(0.5)

        self.assertNotIn("queue_position", self.upload_jobs["j1"])


if __name__ == "__main__":
    unittest.main()

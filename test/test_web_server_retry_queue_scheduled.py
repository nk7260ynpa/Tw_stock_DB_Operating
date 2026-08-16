"""重試佇列排程包裝函式單元測試模組。

`process_retry_queue` 逐一**同步**執行佇列內每一筆任務，新聞類任務是整個
48 小時窗重抓（CTEE 實測 210~230 秒），一輪可達數分鐘以上。它原本被直接註冊為
`schedule_lib.every(1).hours.do(...)` 的 callback，而 `scheduler_thread` 執行
`run_pending()` 時持有 `schedule_lock`，等於一輪重試就把當日後續排程全部往後推
——與 2026-08 daily_craw 阻塞排程的事故完全同型。爬蟲 v2.14.0 起 PTT／MoneyUDN
的零星抓漏由 `ok` 改回報 `partial`，排入重試的頻率大增，此風險已從理論變常態。

本模組驗證：重試佇列必須於背景執行緒執行、不得阻塞排程，且上一輪未結束時
不重複啟動（同一任務被兩輪同時執行會重複寫入）。
"""

import threading
import time
import unittest
from unittest.mock import patch


class TestRunRetryQueueScheduled(unittest.TestCase):
    """測試 run_retry_queue_scheduled 的非阻塞與重入保護行為。

    `retry_queue_running` 是模組層全域旗標，背景執行緒於 `finally` 內才把它設回
    False。若測試結束時只重設旗標而不等待執行緒收尾，殘留執行緒可能在下一個測試
    呼叫之後才清旗標，使「應被擋下」的第二次觸發真的跑起來（典型的 flaky 來源）。
    因此 setUp／tearDown 一律先 join 殘留執行緒，再重設旗標。
    """

    RETRY_THREAD_NAME = "retry-queue"

    def _retry_threads(self):
        """取得目前仍存活的 retry-queue 背景執行緒集合。

        Returns:
            set[threading.Thread]: 存活中的 retry-queue 執行緒。
        """
        return {
            t for t in threading.enumerate()
            if t.name == self.RETRY_THREAD_NAME and t.is_alive()
        }

    def _join_retry_threads(self, timeout=10):
        """等待所有 retry-queue 背景執行緒收尾。

        Args:
            timeout (float): 總等待秒數上限。
        """
        deadline = time.monotonic() + timeout
        for thread in self._retry_threads():
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        self.assertEqual(
            self._retry_threads(), set(),
            "仍有 retry-queue 背景執行緒未收尾，將污染後續測試",
        )

    def _reset_state(self):
        """先等背景執行緒收尾，再重設重入旗標。"""
        self._join_retry_threads()
        with self.web_server.retry_queue_lock:
            self.web_server.retry_queue_running = False

    def setUp(self):
        """每個測試前清乾淨背景執行緒與重入旗標，避免測試間互相污染。"""
        import web_server

        self.web_server = web_server
        self._reset_state()

    def tearDown(self):
        """測試結束後等待背景執行緒收尾並重設重入旗標。"""
        self._reset_state()

    def test_returns_immediately_without_blocking(self):
        """測試呼叫端不會被長時間的重試佇列阻塞。

        排程執行緒在 callback 執行期間持有 `schedule_lock`，一旦阻塞，
        其後所有排程（新聞、商品價格、自我修復）都會延後觸發。
        """
        started = threading.Event()
        release = threading.Event()

        def slow_process():
            started.set()
            release.wait(timeout=10)

        with patch.object(
            self.web_server, "process_retry_queue", slow_process
        ):
            try:
                begin = time.monotonic()
                self.assertTrue(self.web_server.run_retry_queue_scheduled())
                elapsed = time.monotonic() - begin

                # 呼叫端應立即返回，而非等待整輪重試跑完。
                self.assertLess(elapsed, 1.0)
                self.assertTrue(started.wait(timeout=5))
            finally:
                # 即使斷言失敗也要放行，否則背景執行緒會卡滿 10 秒。
                release.set()

    def test_skips_when_previous_run_still_running(self):
        """測試上一輪尚未結束時略過本次觸發，不重複啟動。

        重試任務多為 append 寫入（`DailyPrice` 無去重），同一任務被兩輪
        並行執行會直接產生重複列。
        """
        started = threading.Event()
        release = threading.Event()
        call_count = []

        def slow_process():
            call_count.append(1)
            started.set()
            release.wait(timeout=10)

        with patch.object(
            self.web_server, "process_retry_queue", slow_process
        ):
            try:
                self.assertTrue(self.web_server.run_retry_queue_scheduled())
                # 等第一輪確實進入執行狀態（以事件同步，不用 sleep 猜時間）。
                self.assertTrue(started.wait(timeout=5))

                # 包裝函式要嘛直接返回、要嘛在返回前就已 `Thread.start()`，
                # 故呼叫一結束即可比對執行緒集合，無須 sleep 等待。
                before = self._retry_threads()
                self.assertFalse(self.web_server.run_retry_queue_scheduled())
                self.assertEqual(self._retry_threads(), before)
                self.assertEqual(len(call_count), 1)
            finally:
                release.set()

    def test_flag_reset_after_completion(self):
        """測試正常結束後重入旗標被釋放，下個小時可再次執行。"""
        with patch.object(
            self.web_server, "process_retry_queue", lambda: None
        ):
            self.assertTrue(self.web_server.run_retry_queue_scheduled())

            self._join_retry_threads()
            with self.web_server.retry_queue_lock:
                self.assertFalse(self.web_server.retry_queue_running)

    def test_flag_reset_after_exception(self):
        """測試處理過程拋例外時仍釋放旗標，不會永久卡死重試佇列。"""
        def boom():
            raise RuntimeError("重試炸了")

        with patch.object(self.web_server, "process_retry_queue", boom):
            self.assertTrue(self.web_server.run_retry_queue_scheduled())

            self._join_retry_threads()
            with self.web_server.retry_queue_lock:
                self.assertFalse(self.web_server.retry_queue_running)

    def test_flag_reset_when_thread_start_fails(self):
        """測試執行緒建立失敗時釋放旗標，避免此後每輪重試都被略過。"""
        with patch.object(
            self.web_server, "process_retry_queue", lambda: None
        ), patch.object(
            self.web_server.threading, "Thread"
        ) as mock_thread:
            mock_thread.return_value.start.side_effect = RuntimeError(
                "can't start new thread"
            )

            self.assertFalse(self.web_server.run_retry_queue_scheduled())

            with self.web_server.retry_queue_lock:
                self.assertFalse(self.web_server.retry_queue_running)


class TestRetryQueueScheduleRegistration(unittest.TestCase):
    """測試排程註冊的是非阻塞包裝而非 process_retry_queue 本身。"""

    def test_setup_schedule_registers_wrapper(self):
        """測試 setup_schedule 註冊 run_retry_queue_scheduled。"""
        import schedule as schedule_lib

        import web_server

        try:
            web_server.setup_schedule("07:30")
            registered = [j.job_func.func for j in schedule_lib.jobs]
            self.assertEqual(
                registered.count(web_server.run_retry_queue_scheduled), 1
            )
            self.assertNotIn(web_server.process_retry_queue, registered)
        finally:
            schedule_lib.clear()


class TestManualRetryEndpointSharesGuard(unittest.TestCase):
    """測試手動觸發端點與排程共用同一支包裝與重入旗標。

    手動端點若自行開執行緒跑 `process_retry_queue`，就會與排程輪並行執行
    同一批任務。
    """

    def test_endpoint_delegates_to_wrapper(self):
        """測試端點呼叫包裝函式，且啟動成功時回報已觸發。"""
        import web_server

        with patch.object(
            web_server, "run_retry_queue_scheduled", return_value=True
        ) as mock_run:
            response = web_server.retry_all_pending()

        mock_run.assert_called_once_with()
        self.assertIn("已觸發", response["message"])
        self.assertTrue(response["started"])

    def test_endpoint_reports_skip_when_already_running(self):
        """測試上一輪仍在執行時據實回報略過，而非假稱已觸發。"""
        import web_server

        with patch.object(
            web_server, "run_retry_queue_scheduled", return_value=False
        ):
            response = web_server.retry_all_pending()

        self.assertIn("略過", response["message"])
        # 前端據此改用警示樣式；略過不該顯示成綠色成功。
        self.assertFalse(response["started"])


if __name__ == "__main__":
    unittest.main()

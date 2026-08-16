"""daily_craw 排程包裝函式單元測試模組。

驗證 2026-08 事故的修正：daily_craw 必須於背景執行緒執行，不得阻塞排程執行緒，
且上一輪未結束時不重複啟動。
"""

import threading
import time
import unittest
from unittest.mock import patch


class TestRunDailyCrawScheduled(unittest.TestCase):
    """測試 run_daily_craw_scheduled 的非阻塞與重入保護行為。

    `daily_craw_running` 是模組層全域旗標，而 `run_daily_craw_scheduled` 會啟動
    背景執行緒、於結束時在 `finally` 內把旗標設回 False。若測試結束時只重設旗標
    而不等待該執行緒收尾，上一個測試殘留的執行緒可能在下一個測試呼叫之後才把旗標
    清掉，使「應被擋下」的第二次觸發真的跑起來（曾造成連跑 10 次紅 1 次）。
    因此 setUp／tearDown 一律先 join 殘留執行緒，再重設旗標。
    """

    DAILY_CRAW_THREAD_NAME = "daily-craw"

    def _daily_craw_threads(self):
        """取得目前仍存活的 daily-craw 背景執行緒集合。

        Returns:
            set[threading.Thread]: 存活中的 daily-craw 執行緒。
        """
        return {
            t for t in threading.enumerate()
            if t.name == self.DAILY_CRAW_THREAD_NAME and t.is_alive()
        }

    def _join_daily_craw_threads(self, timeout=10):
        """等待所有 daily-craw 背景執行緒收尾。

        Args:
            timeout (float): 總等待秒數上限。
        """
        deadline = time.monotonic() + timeout
        for thread in self._daily_craw_threads():
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        self.assertEqual(
            self._daily_craw_threads(), set(),
            "仍有 daily-craw 背景執行緒未收尾，將污染後續測試",
        )

    def _reset_state(self):
        """先等背景執行緒收尾，再重設重入旗標。"""
        self._join_daily_craw_threads()
        with self.web_server.daily_craw_lock:
            self.web_server.daily_craw_running = False

    def setUp(self):
        """每個測試前清乾淨背景執行緒與重入旗標，避免測試間互相污染。"""
        import web_server

        self.web_server = web_server
        self._reset_state()

    def tearDown(self):
        """測試結束後等待背景執行緒收尾並重設重入旗標。"""
        self._reset_state()

    def test_returns_immediately_without_blocking(self):
        """測試呼叫端不會被長時間的 daily_craw 阻塞。

        這是事故的核心：daily_craw 曾在排程執行緒內同步跑逾 20 小時，
        使當日後續排程全數延後。
        """
        started = threading.Event()
        release = threading.Event()

        def slow_daily_craw():
            started.set()
            release.wait(timeout=10)

        with patch.object(self.web_server, "daily_craw", slow_daily_craw):
            try:
                begin = time.monotonic()
                self.web_server.run_daily_craw_scheduled()
                elapsed = time.monotonic() - begin

                # 呼叫端應立即返回，而非等待 daily_craw 結束。
                self.assertLess(elapsed, 1.0)
                self.assertTrue(started.wait(timeout=5))
            finally:
                # 即使斷言失敗也要放行，否則背景執行緒會卡滿 10 秒。
                release.set()

    def test_skips_when_previous_run_still_running(self):
        """測試上一輪尚未結束時略過本次觸發，不重複啟動。"""
        started = threading.Event()
        release = threading.Event()
        call_count = []

        def slow_daily_craw():
            call_count.append(1)
            started.set()
            release.wait(timeout=10)

        with patch.object(self.web_server, "daily_craw", slow_daily_craw):
            try:
                self.web_server.run_daily_craw_scheduled()
                # 等第一輪確實進入執行狀態（以事件同步，不用 sleep 猜時間）。
                self.assertTrue(started.wait(timeout=5))

                # `run_daily_craw_scheduled` 要嘛直接返回、要嘛在返回前就已
                # `Thread.start()`，故呼叫一結束即可比對執行緒集合，
                # 無須 sleep 等待——sleep 只是猜時間，正是原本 flaky 的來源。
                before = self._daily_craw_threads()
                self.web_server.run_daily_craw_scheduled()
                self.assertEqual(self._daily_craw_threads(), before)
                self.assertEqual(len(call_count), 1)
            finally:
                release.set()

    def test_flag_reset_after_completion(self):
        """測試 daily_craw 正常結束後重入旗標被釋放，隔日可再次執行。"""
        with patch.object(self.web_server, "daily_craw", lambda: None):
            self.web_server.run_daily_craw_scheduled()

            # 直接 join 背景執行緒，不用輪詢 sleep 猜它何時結束。
            self._join_daily_craw_threads()
            with self.web_server.daily_craw_lock:
                self.assertFalse(self.web_server.daily_craw_running)

    def test_flag_reset_after_exception(self):
        """測試 daily_craw 拋出例外時仍會釋放旗標，不會永久卡住後續排程。"""
        def boom():
            raise RuntimeError("爬蟲炸了")

        with patch.object(self.web_server, "daily_craw", boom):
            self.web_server.run_daily_craw_scheduled()

            # 直接 join 背景執行緒，不用輪詢 sleep 猜它何時結束。
            self._join_daily_craw_threads()
            with self.web_server.daily_craw_lock:
                self.assertFalse(self.web_server.daily_craw_running)

    def test_flag_reset_when_thread_start_fails(self):
        """測試執行緒建立失敗時釋放旗標，避免此後每日排程都被略過。"""
        with patch.object(self.web_server, "daily_craw", lambda: None), \
                patch.object(self.web_server.threading, "Thread") as mock_thread:
            mock_thread.return_value.start.side_effect = RuntimeError(
                "can't start new thread"
            )

            self.web_server.run_daily_craw_scheduled()

            with self.web_server.daily_craw_lock:
                self.assertFalse(self.web_server.daily_craw_running)


class TestScheduleRegistration(unittest.TestCase):
    """測試排程註冊的是非阻塞包裝而非 daily_craw 本身。"""

    def test_setup_schedule_registers_wrapper(self):
        """測試 setup_schedule 註冊 run_daily_craw_scheduled。"""
        import schedule as schedule_lib

        import web_server

        try:
            web_server.setup_schedule("07:30")
            jobs = [j for j in schedule_lib.jobs
                    if j.job_func.func is web_server.run_daily_craw_scheduled]
            self.assertEqual(len(jobs), 1)
            self.assertNotIn(
                web_server.daily_craw,
                [j.job_func.func for j in schedule_lib.jobs],
            )
        finally:
            schedule_lib.clear()


if __name__ == "__main__":
    unittest.main()

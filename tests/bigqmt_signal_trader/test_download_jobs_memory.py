#encoding:utf-8
"""内存模式下载任务（redis_client=None 回退）。

背景：shm 等无 redis 部署里，提交（RPC）与执行同进程——任务走进程内注册表 +
后台守护线程逐只下载。关键约束：下载挂起只允许挂 worker 线程（adjust/drain
线程上的内联同步下载会死锁，2026-09-16 14:46 实测）。
"""
import os
import sys
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.download_jobs import (
    submit_download_job, read_download_status, wait_download_job, DONE, FAILED,
    RUNNING, PENDING,
)


class MemoryModeTest(unittest.TestCase):

    def test_submit_without_redis_runs_in_background_and_completes(self):
        downloaded = []

        def download_func(code, period, start_time, end_time):
            downloaded.append((code, period, start_time, end_time))

        job = submit_download_job(
            None, "acct", ["513300.SH", "600036.SH", "000001.SZ"], "1d",
            start_time="20260901", end_time="20260915",
            download_func=download_func)
        self.assertEqual(job["state"], PENDING)
        final = wait_download_job(None, "acct", job["job_id"], wait_seconds=10.0)
        self.assertEqual(final.get("state"), DONE)
        self.assertEqual(len(downloaded), 3)
        self.assertEqual([c for c, _, _, _ in downloaded],
                         ["513300.SH", "600036.SH", "000001.SZ"])
        self.assertEqual(final.get("done"), 3)

    def test_status_is_readable_while_running_and_after(self):
        import threading

        release = threading.Event()

        def slow_download(code, period, start_time, end_time):
            if code == "000001.SZ":
                release.wait(5.0)

        job = submit_download_job(
            None, "acct", ["513300.SH", "000001.SZ"], "1d",
            download_func=slow_download)
        # 第一只完成后、第二只阻塞期间：state=RUNNING done=1
        deadline = time.time() + 5.0
        saw_running = False
        while time.time() < deadline:
            status = read_download_status(None, "acct", job["job_id"])
            if status and status.get("state") == RUNNING and status.get("done") == 1:
                saw_running = True
                break
            time.sleep(0.05)
        self.assertTrue(saw_running, "应观察到 RUNNING done=1 的进度快照")
        release.set()
        final = wait_download_job(None, "acct", job["job_id"], wait_seconds=10.0)
        self.assertEqual(final.get("state"), DONE)

    def test_download_failure_marks_the_job_failed_with_progress(self):
        def boom(code, period, start_time, end_time):
            if code == "600036.SH":
                raise RuntimeError("native path hung")
            return True

        job = submit_download_job(
            None, "acct", ["513300.SH", "600036.SH", "000001.SZ"], "1d",
            download_func=boom)
        final = wait_download_job(None, "acct", job["job_id"], wait_seconds=10.0)
        self.assertEqual(final.get("state"), FAILED)
        self.assertIn("native path hung", final.get("error") or "")
        self.assertEqual(final.get("done"), 1, "失败前完成的进度要保留")

    def test_empty_stock_list_is_rejected(self):
        with self.assertRaises(ValueError):
            submit_download_job(None, "acct", [], "1d", download_func=lambda *a: None)

    def test_memory_mode_needs_a_download_func(self):
        with self.assertRaises(ValueError):
            submit_download_job(None, "acct", ["513300.SH"], "1d")


if __name__ == "__main__":
    unittest.main()

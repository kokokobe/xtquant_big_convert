# coding: utf-8
"""跨进程 shm RPC：子进程当服务端，本进程当客户端 —— 真实部署的形状。

QMT 里的桥在 XtItClient 进程内 start_receiving，外部 python 客户端在另一个
进程 send_request。section/event 都是命名内核对象，跨进程共享靠的是内核，
本测试用两个真实进程验证这一跳（mem_share_probe v4 已在 QMT 沙箱面板验证过
同样的内核原语，本测试验证的是传输层协议架在上面之后依然成立）。
"""
import os
import subprocess
import sys
import time
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

WINDOWS_ONLY = unittest.skipUnless(os.name == "nt", "共享内存传输是 Windows 专有")

_SERVER_SRC = r'''
#encoding:utf-8
import sys, time
sys.path.insert(0, sys.argv[1])
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

def echo(request):
    return {
        "schema_version": 1,
        "request_id": request.get("request_id"),
        "method": request.get("method"),
        "ok": True,
        "data": {"echo": request.get("params"),
                 "pid": __import__("os").getpid()},
    }

account = sys.argv[2]
server = SharedMemoryTransport(account_id=account, print_prefix="[shm-xproc-srv]")
server.start_receiving(echo, background_threads=True)
print("[shm-xproc-srv] ready", flush=True)
time.sleep(float(sys.argv[3]))
server.stop()
'''


@WINDOWS_ONLY
class CrossProcessRpcTest(unittest.TestCase):

    def test_round_trip_across_two_processes(self):
        account = "x%d" % (int(time.time() * 1000) % 100000000)
        proc = subprocess.Popen(
            [sys.executable, "-c", _SERVER_SRC,
             os.path.join(ROOT, "src"), account, "20"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.addCleanup(proc.kill)
        # 等子进程打出 ready（内核对象都是就绪后创建的）
        deadline = time.time() + 30
        ready = False
        import threading
        lines = []
        reader = threading.Thread(
            target=lambda: [lines.append(l) for l in iter(proc.stdout.readline, b"")],
            daemon=True)
        reader.start()
        while time.time() < deadline:
            if any(b"ready" in l for l in lines):
                ready = True
                break
            time.sleep(0.1)
        self.assertTrue(ready, "子进程服务端 30s 未就绪: %r" % lines[-10:])

        from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport
        client = SharedMemoryTransport(account_id=account,
                                       print_prefix="[shm-xproc-cli]")
        self.addCleanup(client.stop)

        out = client.send_request(
            {"request_id": "x1", "method": "ping", "params": {"a": 1}}, 10)
        self.assertTrue(out["ok"])
        self.assertEqual(out["data"]["echo"], {"a": 1})
        self.assertNotEqual(out["data"]["pid"], os.getpid())

        # 连发 20 次量一下端到端延迟（纯传输层，不含桥内处理）
        t0 = time.time()
        for i in range(20):
            out = client.send_request(
                {"request_id": "x%d" % i, "method": "ping", "params": {}}, 10)
            self.assertTrue(out["ok"])
        elapsed_ms = (time.time() - t0) * 1000.0 / 20.0
        print("[shm-xproc] avg round trip %.2f ms" % elapsed_ms)
        self.assertLess(elapsed_ms, 500.0, "往返延迟异常")

        # utf8 + 大载荷跨进程
        codes = ["%06d.SH" % i for i in range(2000)]
        out = client.send_request(
            {"request_id": "big", "method": "big", "params": {"codes": codes}}, 30)
        self.assertEqual(out["data"]["echo"]["codes"][-1], codes[-1])


if __name__ == "__main__":
    unittest.main()

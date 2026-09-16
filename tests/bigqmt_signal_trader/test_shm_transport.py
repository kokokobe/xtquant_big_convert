# coding: utf-8
"""共享内存传输（mmap 命名 section + ctypes 内核事件/互斥体，零 TCP）。

存在的理由是合规，不是速度：QMT 终端的审计扫描每 ~10s 走一遍进程 TCP 表，
白名单外对端（含 127.0.0.1 回环）一经记 illegal IP 就 stopAllStrategy 全杀
——zmq 的 ipc 在 Windows 上退化成回环 TCP signaler（2026-09-15
15:12:52,492 illegal IP: 127.0.0.1:59298 实锤），redis 6379 同样被抓。
共享内存 + 命名事件不建任何 socket，扫描在 TCP 表里看不到它。

原型已在 QMT 沙箱面板实测放行（src/mem_share_probe.py v4）：跨进程 section
共享 OK、事件推送 0.0ms OK。本文件验证传输层语义与 pipe 对齐：

    裸 IPC 往返（108B 载荷）   命名管道 0.012ms   shm 预计同量级（无协议栈）
    端到端 RPC（活桥）          3~12ms，瓶颈在桥内处理，不在线缆
"""
import os
import sys
import threading
import time
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.transports.base import TransportError, TransportTimeout
from bigqmt_signal_trader.transports.factory import KNOWN_TRANSPORTS, build_transport
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport


WINDOWS_ONLY = unittest.skipUnless(os.name == "nt", "共享内存传输是 Windows 专有")


def _echo(request):
    return {
        "schema_version": 1,
        "request_id": request.get("request_id"),
        "method": request.get("method"),
        "ok": True,
        "data": {"echo": request.get("params")},
    }


def _uniq(tag):
    """同进程内各测试用独立内核对象名；进程退出后 pagefile section 随之销毁。"""
    return "t%s%d" % (tag, int(time.time() * 1000) % 100000000)


class _Pair(object):
    """一对已启动的服务端/客户端，测完保证拆干净。"""

    def __init__(self, account, on_request=_echo, background_threads=True):
        self.server = SharedMemoryTransport(account_id=account)
        self.server.start_receiving(on_request,
                                    background_threads=background_threads)
        self.client = SharedMemoryTransport(account_id=account)

    def close(self):
        self.client.stop()
        self.server.stop()


class NamingTest(unittest.TestCase):

    def test_the_account_id_is_part_of_every_kernel_name(self):
        """两个桥 —— 一个实盘一个模拟 —— 绝不能共用一条线（#144 同类）。"""
        live = SharedMemoryTransport(account_id="8886800503")
        sim = SharedMemoryTransport(account_id="5500000001")
        self.assertNotEqual(live._req_section_name, sim._req_section_name)
        self.assertIn("8886800503", live._req_section_name)


class FactoryTest(unittest.TestCase):

    def test_shm_is_a_known_transport(self):
        self.assertIn("shm", KNOWN_TRANSPORTS)

    def test_the_factory_builds_it(self):
        transport = build_transport("shm", {}, account_id="acct")
        self.assertEqual(transport.name, "shm")
        self.assertIn("acct", transport._req_section_name)

    def test_config_can_override_the_name_prefix(self):
        transport = build_transport(
            "shm", {"shm": {"name_prefix": "custom_wire"}}, account_id="acct")
        self.assertTrue(transport._req_section_name.startswith("custom_wire_"))


class ServiceContractTest(unittest.TestCase):
    """传输要能被服务端那套 drain/inline 机制驱动，不只是自己能收发。"""

    def test_it_has_drain_request_queue(self):
        """少了这个方法，adjust 每个 tick 都会撞 Redis 兜底分支。"""
        transport = build_transport("shm", {}, account_id="acct")
        self.assertTrue(callable(getattr(transport, "drain_request_queue", None)))
        self.assertEqual(transport.drain_request_queue(max_items=20), 0)

    def test_every_transport_the_factory_builds_can_be_drained(self):
        """同一个坑不要在下一个传输上重犯。"""
        for name in ("redis", "zmq", "pipe", "shm"):
            try:
                transport = build_transport(name, {}, account_id="acct")
            except Exception:
                continue          # 缺依赖的传输跳过，不是本条要测的
            self.assertTrue(
                callable(getattr(transport, "drain_request_queue", None)),
                "%s 传输没有 drain_request_queue，adjust 会掉进 redis 兜底" % name)


@WINDOWS_ONLY
class RoundTripTest(unittest.TestCase):

    def setUp(self):
        self.pair = _Pair(_uniq("rt"))
        self.addCleanup(self.pair.close)

    def test_a_request_comes_back_with_its_own_request_id(self):
        out = self.pair.client.send_request(
            {"request_id": "r1", "method": "ping", "params": {"a": 1}}, 5)
        self.assertEqual(out["request_id"], "r1")
        self.assertTrue(out["ok"])
        self.assertEqual(out["data"]["echo"], {"a": 1})

    def test_utf8_survives_both_directions(self):
        out = self.pair.client.send_request(
            {"request_id": "r2", "method": "x",
             "params": {"名称": "维持担保比例", "值": 3.35}}, 5)
        self.assertEqual(out["data"]["echo"]["名称"], "维持担保比例")
        self.assertEqual(out["data"]["echo"]["值"], 3.35)

    def test_a_large_payload_survives_zlib_framing(self):
        """全市场查询的量级（压缩后进一个槽）不能被截断。"""
        codes = ["%06d.SH" % i for i in range(4000)]
        out = self.pair.client.send_request(
            {"request_id": "r3", "method": "big", "params": {"codes": codes}}, 30)
        self.assertEqual(len(out["data"]["echo"]["codes"]), 4000)
        self.assertEqual(out["data"]["echo"]["codes"][-1], codes[-1])

    def test_floats_keep_their_precision(self):
        out = self.pair.client.send_request(
            {"request_id": "r4", "method": "x",
             "params": {"price": 13.981600016666668}}, 5)
        self.assertEqual(out["data"]["echo"]["price"], 13.981600016666668)

    def test_a_request_id_longer_than_31_bytes_still_matches(self):
        """槽里 rid 只存 31 字节，客户端比对必须用同长前缀。

        覆盖率实测踩过：uuid4().hex（32 位）和 "verify-<长方法名>-<ms>" 都超
        31 字节，截断后完整串等值比较永远失配 —— 应答就在环里客户端却超时。
        """
        long_rid = "verify-describe_trade_detail_fields-1789484222312"
        self.assertGreater(len(long_rid), 31)
        out = self.pair.client.send_request(
            {"request_id": long_rid, "method": "x", "params": {"k": 1}}, 5)
        self.assertEqual(out["request_id"], long_rid)
        self.assertTrue(out["ok"])


@WINDOWS_ONLY
class HandlerFailureTest(unittest.TestCase):
    """handler 抛异常不能把连接搞死 —— 后面的请求还得能走。"""

    def setUp(self):
        def boom(request):
            if request.get("method") == "boom":
                raise RuntimeError("handler exploded")
            return _echo(request)

        self.pair = _Pair(_uniq("boom"), on_request=boom)
        self.addCleanup(self.pair.close)

    def test_the_error_comes_back_as_a_response_not_a_dropped_connection(self):
        out = self.pair.client.send_request(
            {"request_id": "e1", "method": "boom", "params": {}}, 5)
        self.assertFalse(out["ok"])
        self.assertIn("handler exploded", out["error"])

    def test_the_connection_still_works_afterwards(self):
        self.pair.client.send_request(
            {"request_id": "e1", "method": "boom", "params": {}}, 5)
        out = self.pair.client.send_request(
            {"request_id": "e2", "method": "fine", "params": {"ok": 1}}, 5)
        self.assertTrue(out["ok"])


@WINDOWS_ONLY
class DeferredReplyTest(unittest.TestCase):
    """应答从另一个线程补发（生产里 = adjust 线程答 drain 收到的请求）。"""

    def test_an_answer_written_on_another_thread_arrives(self):
        parked = threading.Event()
        released = threading.Event()
        holder = {}

        def deferred_handler(req):
            holder["req"] = req
            parked.set()
            released.wait(5.0)
            return None        # 服务端形态：自己 send_response，不靠返回值

        pair = _Pair(_uniq("def"), on_request=deferred_handler)
        self.addCleanup(pair.close)
        answers = []
        t = threading.Thread(target=lambda: answers.append(
            pair.client.send_request({"request_id": "d1", "method": "p"}, 10)))
        t.daemon = True
        t.start()
        self.assertTrue(parked.wait(5.0))
        threading.Timer(0.2, released.set).start()
        time.sleep(0.3)
        pair.server.send_response(holder["req"], {
            "request_id": "d1", "ok": True, "data": {"v": 1}})
        t.join(timeout=5)
        self.assertEqual(answers[0]["data"]["v"], 1)


@WINDOWS_ONLY
class ConcurrencyTest(unittest.TestCase):
    """每个线程一条自己的应答线（#186 的 shm 版）。

    共用一个应答环的话，20 个在途请求会在 4 个槽里互相覆盖 —— 串包比慢更
    严重，所以应答线按线程拆，请求环用互斥体顺序化发布。
    """

    def setUp(self):
        self.pair = _Pair(_uniq("cc"))
        self.addCleanup(self.pair.close)

    def test_twenty_threads_never_cross_responses(self):
        errors = []

        def worker(wid):
            for i in range(25):
                rid = "w%d-%d" % (wid, i)
                try:
                    out = self.pair.client.send_request(
                        {"request_id": rid, "method": "p",
                         "params": {"w": wid, "i": i}}, 15)
                except Exception as exc:
                    errors.append((rid, repr(exc)))
                    continue
                if out["request_id"] != rid:
                    errors.append(("串包", rid, out["request_id"]))
                elif out["data"]["echo"]["w"] != wid:
                    errors.append(("载荷串了", rid))

        threads = [threading.Thread(target=worker, args=(w,)) for w in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)
        self.assertEqual(errors[:5], [], "500 次并发里出了 %d 个错" % len(errors))


@WINDOWS_ONLY
class DrainModeTest(unittest.TestCase):
    """drain 模式：adjust 线程自己非阻塞轮询，不起工作线程。"""

    def _pair(self, account):
        srv = SharedMemoryTransport(account_id=account)
        srv.start_receiving(_echo, background_threads=False)
        cli = SharedMemoryTransport(account_id=account)
        self.addCleanup(srv.stop)
        self.addCleanup(cli.stop)
        return srv, cli

    def test_nothing_is_answered_until_the_adjust_thread_drains(self):
        """没人调 drain 就没人回答 —— 这正是 drain 模式的定义。"""
        srv, cli = self._pair(_uniq("dw"))
        answers = []
        t = threading.Thread(target=lambda: answers.append(
            cli.send_request({"request_id": "d1", "method": "p", "params": {}}, 10)))
        t.daemon = True
        t.start()
        time.sleep(0.6)
        self.assertEqual(answers, [], "没 drain 就被回答了，说明还在走工作线程")
        self.assertGreater(srv.drain_request_queue(max_items=20), 0)
        t.join(timeout=5)
        self.assertEqual(answers[0]["request_id"], "d1")

    def test_the_service_can_answer_through_send_response(self):
        """服务端不走 deliver 的返回值，它自己调 send_response 发响应。"""
        answers = []
        holder = {}

        def capture(req):
            holder["req"] = req
            return None

        account = _uniq("ds")
        srv = SharedMemoryTransport(account_id=account)
        srv.start_receiving(capture, background_threads=False)
        self.addCleanup(srv.stop)
        cli = SharedMemoryTransport(account_id=account)
        self.addCleanup(cli.stop)

        t = threading.Thread(target=lambda: answers.append(
            cli.send_request({"request_id": "s1", "method": "p", "params": {}}, 10)))
        t.daemon = True
        t.start()
        time.sleep(0.5)
        srv.drain_request_queue(max_items=5)
        srv.send_response(holder["req"], {"request_id": "s1", "ok": True, "data": {"v": 1}})
        t.join(timeout=5)
        self.assertEqual(answers[0]["data"]["v"], 1)

    def test_drain_is_non_blocking_when_idle(self):
        """空闲时 drain 必须立刻返回 —— 它跑在 adjust 主线程上。"""
        srv, _cli = self._pair(_uniq("di"))
        started = time.time()
        for _ in range(5):
            self.assertEqual(srv.drain_request_queue(max_items=20), 0)
        self.assertLess(time.time() - started, 0.5, "drain 在空闲时阻塞了")

    def test_drain_respects_max_items(self):
        srv, cli = self._pair(_uniq("dc"))
        for i in range(6):
            t = threading.Thread(target=lambda i=i: cli.send_request(
                {"request_id": "c%d" % i, "method": "p", "params": {}}, 10))
            t.daemon = True
            t.start()
        time.sleep(0.6)
        self.assertLessEqual(srv.drain_request_queue(max_items=2), 2)

    def test_background_mode_leaves_drain_a_no_op(self):
        """开着工作线程时 drain 不能插手，否则两边抢同一段游标。"""
        srv = SharedMemoryTransport(account_id=_uniq("db"))
        srv.start_receiving(_echo, background_threads=True)
        self.addCleanup(srv.stop)
        self.assertEqual(srv.drain_request_queue(max_items=20), 0)


@WINDOWS_ONLY
class FrameLimitTest(unittest.TestCase):
    """超大帧要有明确报错，不能静默截断。"""

    def test_an_oversized_request_fails_fast_on_the_client(self):
        import base64
        account = _uniq("ovr")
        srv = SharedMemoryTransport(account_id=account)
        srv.start_receiving(_echo, background_threads=False)
        self.addCleanup(srv.stop)
        cli = SharedMemoryTransport(account_id=account)
        self.addCleanup(cli.stop)
        blob = base64.b64encode(os.urandom(300 * 1024)).decode("ascii")
        with self.assertRaises(TransportError) as caught:
            cli.send_request({"request_id": "big", "method": "x",
                              "params": {"blob": blob}}, 5)
        self.assertIn("too large", str(caught.exception))

    def test_an_oversized_response_becomes_an_error_envelope(self):
        import base64
        account = _uniq("ovs")
        srv = SharedMemoryTransport(account_id=account)
        srv.start_receiving(_echo, background_threads=False)
        self.addCleanup(srv.stop)
        cli = SharedMemoryTransport(account_id=account)
        self.addCleanup(cli.stop)

        def huge(request):
            out = _echo(request)
            # base64 of random bytes: zlib cannot shrink it, so the frame
            # genuinely cannot fit the reply slot -- this must surface as an
            # error envelope, not a silent drop or a truncated payload.
            out["data"] = {"blob": base64.b64encode(
                os.urandom(5 * 1024 * 1024)).decode("ascii")}
            return out

        srv._on_request = huge
        answers = []
        t = threading.Thread(target=lambda: answers.append(
            cli.send_request({"request_id": "big2", "method": "x", "params": {}}, 15)))
        t.daemon = True
        t.start()
        time.sleep(0.5)
        srv.drain_request_queue(max_items=5)
        t.join(timeout=20)
        self.assertEqual(len(answers), 1)
        self.assertFalse(answers[0]["ok"])
        self.assertIn("too large", answers[0]["error"])


@WINDOWS_ONLY
class TimeoutTest(unittest.TestCase):
    """没有服务端时客户端按超时语义失败，而不是挂死或报错信封。"""

    def test_a_request_without_a_server_times_out(self):
        client = SharedMemoryTransport(account_id=_uniq("dead"))
        self.addCleanup(client.stop)
        started = time.time()
        with self.assertRaises(TransportTimeout):
            client.send_request({"request_id": "x", "method": "ping"}, 0.6)
        self.assertLess(time.time() - started, 3.0)


@WINDOWS_ONLY
class ShutdownTest(unittest.TestCase):
    """stop() 不能卡：监听线程等的是事件超时，不是阻塞 recv。"""

    def test_stop_returns_even_with_a_client_parked_in_send(self):
        account = _uniq("sd")
        server = SharedMemoryTransport(account_id=account)
        server.start_receiving(_echo)
        client = SharedMemoryTransport(account_id=account)
        threading.Thread(
            target=lambda: client.send_request(
                {"request_id": "s0", "method": "p", "params": {}}, 10),
            daemon=True).start()
        time.sleep(1.0)

        done = threading.Event()

        def stopper():
            client.stop()
            server.stop()
            done.set()

        threading.Thread(target=stopper, daemon=True).start()
        self.assertTrue(done.wait(timeout=20), "stop() 卡住了")

    def test_stop_is_safe_to_call_twice(self):
        pair = _Pair(_uniq("d2"))
        pair.close()
        pair.close()


if __name__ == "__main__":
    unittest.main()

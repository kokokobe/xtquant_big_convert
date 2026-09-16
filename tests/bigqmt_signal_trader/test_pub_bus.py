#encoding:utf-8
"""进程间发布总线（pub_bus）单测：往返/多订阅者/topic 过滤/丢包通告/注册表。
全部同进程多线程跑（跨进程语义由内核对象保证，test_shm_transport_crossproc
已验证同款原语）；每个用例独立账号 = 独立内核对象名，互不污染。
"""
import os
import sys
import threading
import time
import unittest
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import pub_bus
from bigqmt_signal_trader.pub_bus import (
    BusPublisher, BusSubscriber, get_publisher, BusError,
)

WINDOWS_ONLY = unittest.skipUnless(os.name == "nt", "发布总线是 Windows 专有")


def _acct(tag):
    return "bustest-%s-%s" % (tag, uuid.uuid4().hex[:6])


def _collect(sub, want, timeout=5.0):
    """从 listen 里收集 want 条消息（后台线程 + 事件，主线程收割）。"""
    got = []
    done = threading.Event()

    def run():
        for topic, data, seq in sub.listen():
            got.append((topic, data, seq))
            if len(got) >= want:
                done.set()
                break

    t = threading.Thread(target=run, daemon=True)
    t.start()
    done.wait(timeout)
    return got


@WINDOWS_ONLY
class RoundTripTest(unittest.TestCase):

    def test_publish_subscribe_round_trip(self):
        account = _acct("rt")
        pub = get_publisher(account)
        sub = BusSubscriber(account)
        sub.subscribe()
        time.sleep(0.2)                       # 等订阅登记对外可见
        got = []
        collector = threading.Thread(
            target=lambda: [got.append(m) for m in sub.listen(topics={"sig"})],
            daemon=True)
        collector.start()
        time.sleep(0.2)
        seq1 = pub.publish("sig", {"code": "603993.SH", "action": "BUY"})
        seq2 = pub.publish("noise", {"ignore": True})
        seq3 = pub.publish("sig", {"code": "513300.SH", "action": "SELL"})
        collector.join(timeout=5)
        sub.close()
        self.assertEqual(seq2, seq1 + 1)
        self.assertEqual(seq3, seq1 + 2)
        self.assertEqual(len(got), 2, "topic 过滤失败: %r" % (got,))
        self.assertEqual(got[0][0], "sig")
        self.assertEqual(got[0][1]["code"], "603993.SH")
        self.assertEqual(got[1][1]["code"], "513300.SH")

    def test_two_subscribers_independent_cursors(self):
        account = _acct("two")
        pub = get_publisher(account)
        sub1 = BusSubscriber(account)
        sub1.subscribe()
        time.sleep(0.2)
        pub.publish("early", {"n": 1})        # sub2 尚未订阅, 不应收到
        time.sleep(0.1)
        sub2 = BusSubscriber(account)
        sub2.subscribe()
        time.sleep(0.2)
        pub.publish("late", {"n": 2})

        got1 = _collect(sub1, 2, timeout=5.0)
        got2 = _collect(sub2, 1, timeout=5.0)
        sub1.close()
        sub2.close()
        self.assertEqual([m[1]["n"] for m in got1], [1, 2],
                         "sub1 游标从订阅前开始, early+late 都该收到")
        self.assertEqual([m[1]["n"] for m in got2], [2],
                         "sub2 游标从订阅时开始, 只收 late")


@WINDOWS_ONLY
class LostNoticeTest(unittest.TestCase):
    """环覆盖检测：订阅后不消费，灌 70 条（> 64 槽），恢复消费时先收 __bus_lost__。"""

    def test_overrun_is_detectable_not_silent(self):
        account = _acct("lost")
        pub = get_publisher(account)
        sub = BusSubscriber(account)
        sub.subscribe()
        time.sleep(0.2)
        for i in range(70):
            pub.publish("flood", {"i": i})
        time.sleep(0.3)
        msgs = []
        for topic, data, seq in sub.listen():
            msgs.append((topic, data, seq))
            if len(msgs) >= 2:
                break
        sub.close()
        self.assertEqual(msgs[0][0], "__bus_lost__")
        self.assertGreater(msgs[0][1]["lost"], 0)
        self.assertEqual(msgs[1][0], "flood")


@WINDOWS_ONLY
class RegistryTest(unittest.TestCase):

    def setUp(self):
        self._old_capacity = pub_bus.CAPACITY
        pub_bus.CAPACITY = 2     # 缩小注册表验证占满路径

    def tearDown(self):
        pub_bus.CAPACITY = self._old_capacity

    def test_registry_full_raises_clearly(self):
        account = _acct("full")
        subs = [BusSubscriber(account) for _ in range(2)]
        for s in subs:
            s.subscribe()
        try:
            with self.assertRaises(BusError):
                BusSubscriber(account).subscribe()
        finally:
            for s in subs:
                s.close()

    def test_unsubscribe_frees_the_slot(self):
        account = _acct("free")
        s1 = BusSubscriber(account)
        s1.subscribe()
        s1.unsubscribe()
        s2 = BusSubscriber(account)      # 槽位已回收, 能再登记
        s2.subscribe()
        s2.close()


class PublisherSingletonTest(unittest.TestCase):

    def test_same_account_same_instance(self):
        p1 = get_publisher("singleton-a")
        p2 = get_publisher("singleton-a")
        self.assertIs(p1, p2)
        p3 = get_publisher("singleton-b")
        self.assertIsNot(p1, p3)


@WINDOWS_ONLY
class RpcHandlerTest(unittest.TestCase):

    def test_bus_publish_handler_reaches_a_subscriber(self):
        from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
        account = _acct("rpc")
        handlers = BigQmtRpcHandlers(account_id=account, market_data=None,
                                     position_provider=None)
        sub = BusSubscriber(account)
        sub.subscribe()
        time.sleep(0.2)
        got = []
        collector = threading.Thread(
            target=lambda: [got.append(m) for m in sub.listen()], daemon=True)
        collector.start()
        time.sleep(0.2)
        result = handlers._handle_bus_publish({"topic": "trade", "data": {"k": 1}})
        self.assertTrue(result.get("published"))
        collector.join(timeout=5)
        sub.close()
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0][0], "trade")
        self.assertEqual(got[0][1], {"k": 1})


if __name__ == "__main__":
    unittest.main()

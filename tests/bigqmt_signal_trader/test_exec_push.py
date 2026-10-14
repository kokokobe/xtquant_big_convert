# coding: gbk
"""exec 推送环（D2.2-R1）测试。

三块：
1. 写端语义 —— ExecPushPublisher 入环后从同环读回（读端按协议 header/slot
   布局 mmap 开环，与 rust 侧 ShmRing::open(256, 16384) + poll_frames 同构），
   断言 payload JSON、kind 与 Task 11 契约行字段；
2. 接线开关 —— _ensure_exec_push_publisher 只在 transport=shm 且账号就绪时
   创建，创建失败降级 None 不抛；
3. 回调集成 —— order_callback / deal_callback（仿 test_exec_event_strategy_name
   的注入口）触发后环里出现契约行；publisher 坏掉不炸回调链。
"""

import base64
import ctypes
import json
import os
import sys
import unittest
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_strategy as strategy
from bigqmt_signal_trader import exec_events, exec_push
from bigqmt_signal_trader.transports import shm_transport

WINDOWS_ONLY = unittest.skipUnless(os.name == "nt", "exec 推送环是 Windows 专有")

WAIT_OBJECT_0 = 0
WAIT_TIMEOUT = 0x00000102


def _unique_prefix():
    # _sanitize 放行 alnum 与 -_.；每测试唯一 prefix 保证段名互不串扰。
    return "bqmt_exect_" + uuid.uuid4().hex[:12]


class _RingReader(object):
    """协议读端的最小 python 复刻。

    rust 侧是 ShmRing::open(section, event, mutex, 256, 16384) + poll_frames()
    + wait_event(timeout_ms)；这里用 shm_transport 同一套 header（<4sIIIQI）与
    48B slot 头布局开同一个命名 section，按 seq 顺读解码 payload JSON。
    """

    def __init__(self, section_name):
        self.ring = shm_transport._Ring(
            section_name, exec_push.EXEC_SECTION_TOTAL,
            exec_push.EXEC_SLOT_COUNT, exec_push.EXEC_SLOT_BYTES, create=True)
        self.cursor = 0

    def poll_frames(self):
        target = self.ring.write_seq()
        frames = []
        while self.cursor < target:
            self.cursor += 1
            frame = self.ring.read_slot(self.cursor)
            if frame is None:
                continue
            frames.append(json.loads(frame[1].decode("utf-8")))
        return frames

    def poll_raw_payloads(self):
        target = self.ring.write_seq()
        payloads = []
        while self.cursor < target:
            self.cursor += 1
            frame = self.ring.read_slot(self.cursor)
            if frame is None:
                continue
            payloads.append(frame[1])
        return payloads

    def event_is_signaled(self, event_name):
        """用 0 超时探测同名事件：被 SetEvent 过=可立即等到（自动复位消费掉）。"""
        k32 = shm_transport._kernel32()
        handle = k32.CreateEventW(None, 0, 0, event_name)
        if not handle:
            raise AssertionError("CreateEventW(%s) failed" % event_name)
        try:
            state = int(k32.WaitForSingleObject(ctypes.c_void_p(handle), 0))
        finally:
            k32.CloseHandle(ctypes.c_void_p(handle))
        return state == WAIT_OBJECT_0


class _FakeContext(object):
    """order_callback/deal_callback 的 ContextInfo 占位（推送路径不读它）。"""


def _order_obj(**overrides):
    attrs = dict(
        m_strInstrumentID="601388", m_strExchangeID="SH", m_nOffsetFlag=49,
        m_nVolumeTotalOriginal=158200, m_nVolumeTotal=158200, m_nVolumeTraded=0,
        m_strOrderSysID="2209", m_strRemark="sig-1", m_nOrderStatus=50,
        m_dLimitPrice=35.62, m_dTradedPrice=0.0, m_strAccountID="acct",
        m_strInsertDate="20261014", m_strInsertTime="093000",
    )
    attrs.update(overrides)
    return type("FakeOrder", (), attrs)


def _deal_obj(**overrides):
    attrs = dict(
        m_strInstrumentID="601388", m_strExchangeID="SH", m_nOffsetFlag=49,
        m_nVolume=700, m_dPrice=35.36, m_dTradeAmount=24752.0,
        m_strOrderSysID="2209", m_strTradeID="t-1", m_strRemark="sig-1",
        m_strTradeTime="093013", m_strAccountID="acct", m_nDirection=49,
    )
    attrs.update(overrides)
    return type("FakeDeal", (), attrs)


# ------------------------------------------------- 写端语义（入环 -> 读回）


@WINDOWS_ONLY
class PublisherRoundTripTest(unittest.TestCase):
    def setUp(self):
        self.prefix = _unique_prefix()
        self.publisher = exec_push.ExecPushPublisher(self.prefix, "acct1")
        self.addCleanup(self.publisher.close)
        self.reader = _RingReader(self.publisher.section_name)

    def test_order_frame_roundtrip_carries_contract_row(self):
        row = {
            "order_sys_id": "2209", "user_order_id": "sig-1",
            "stock_code": "601388.SH", "action": "SELL",
            "order_volume": 158200, "traded_volume": 0,
            "price": 35.62, "status": 50, "order_time": 1791031800,
        }
        self.publisher.publish_order(row)

        frames = self.reader.poll_frames()
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0]["kind"], "order")
        self.assertEqual(frames[0]["row"], row)

    def test_deal_frame_roundtrip_carries_contract_row(self):
        row = {
            "trade_id": "t-1", "order_sys_id": "2209", "user_order_id": "sig-1",
            "stock_code": "601388.SH", "action": "SELL",
            "volume": 700, "price": 35.36, "traded_at": "093013",
        }
        self.publisher.publish_deal(row)

        frames = self.reader.poll_frames()
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0]["kind"], "deal")
        self.assertEqual(frames[0]["row"], row)

    def test_payload_is_single_line_utf8_json(self):
        self.publisher.publish_deal({"trade_id": "t-1"})

        payloads = self.reader.poll_raw_payloads()
        self.assertEqual(len(payloads), 1)
        self.assertNotIn(b"\n", payloads[0])
        self.assertNotIn(b"\r", payloads[0])
        decoded = json.loads(payloads[0].decode("utf-8"))
        self.assertEqual(decoded["kind"], "deal")

    def test_setevent_signals_once_per_frame(self):
        self.assertFalse(self.reader.event_is_signaled(self.publisher.event_name))

        self.publisher.publish_order({"order_sys_id": "1"})
        self.assertTrue(self.reader.event_is_signaled(self.publisher.event_name))
        # 自动复位事件：消费一次后回到无信号，等下一次 publish 再亮。
        self.assertFalse(self.reader.event_is_signaled(self.publisher.event_name))

        self.publisher.publish_order({"order_sys_id": "2"})
        self.assertTrue(self.reader.event_is_signaled(self.publisher.event_name))

    def test_seq_is_monotonic_across_publisher_recreate(self):
        """reload/重跑 init 复用同名段：write_seq 从 header 续读，rust 读端
        游标永不回退（回退会让 poll_frames 卡死在旧 target）。"""
        self.publisher.publish_order({"order_sys_id": "1"})
        self.publisher.publish_order({"order_sys_id": "2"})
        self.publisher.close()

        second = exec_push.ExecPushPublisher(self.prefix, "acct1")
        self.addCleanup(second.close)
        second.publish_order({"order_sys_id": "3"})

        frames = self.reader.poll_frames()
        self.assertEqual([f["row"]["order_sys_id"] for f in frames],
                         ["1", "2", "3"])

    def test_oversized_row_counts_as_drop_not_crash(self):
        # 48000 字节随机 base64：zlib 压不进 16KB slot 容量 -> TransportError。
        blob = base64.b64encode(os.urandom(48000)).decode("ascii")
        self.publisher.publish_order({"order_sys_id": blob})

        self.assertEqual(self.publisher.drops, 1)
        self.assertEqual(self.publisher.published, 0)
        self.assertEqual(self.reader.poll_frames(), [])

    def test_publisher_does_not_raise_on_publish_errors(self):
        # 循环引用让 json.dumps 抛 ValueError；publisher 必须吞掉计 drops。
        row = {}
        row["self"] = row

        self.publisher.publish_order(row)
        self.assertEqual(self.publisher.drops, 1)

    def test_section_and_object_names_follow_protocol(self):
        publisher = exec_push.ExecPushPublisher("bigqmt_shm", "52 625295")
        self.addCleanup(publisher.close)
        self.assertEqual(publisher.section_name, "bigqmt_shm_exec_52625295")
        self.assertEqual(publisher.event_name, "bigqmt_shm_exec_evt_52625295")
        self.assertEqual(publisher.mutex_name, "bigqmt_shm_exec_mtx_52625295")


# ------------------------------------------------- 接线开关（shm 才建环）


@WINDOWS_ONLY
class EnsurePublisherGateTest(unittest.TestCase):
    def setUp(self):
        self._saved = strategy._exec_push_publisher
        strategy._exec_push_publisher = None

    def tearDown(self):
        publisher = strategy._exec_push_publisher
        if publisher is not None:
            try:
                publisher.close()
            except Exception:
                pass
        strategy._exec_push_publisher = self._saved

    def test_non_shm_transport_never_creates(self):
        for transport in ("redis", "zmq", "mysql", "", None):
            config = {"account_id": "acct1", "rpc": {"transport": transport}}
            self.assertIsNone(strategy._ensure_exec_push_publisher(config))
            self.assertIsNone(strategy._exec_push_publisher)

    def test_shm_transport_creates_protocol_named_publisher(self):
        config = {"account_id": "acct1", "rpc": {"transport": "shm"}}
        publisher = strategy._ensure_exec_push_publisher(config)

        self.assertIsNotNone(publisher)
        self.assertEqual(publisher.section_name, "bigqmt_shm_exec_acct1")
        self.assertIs(strategy._exec_push_publisher, publisher)

    def test_same_config_reuses_the_publisher(self):
        config = {"account_id": "acct1", "rpc": {"transport": "shm"}}
        first = strategy._ensure_exec_push_publisher(config)
        second = strategy._ensure_exec_push_publisher(config)

        self.assertIs(first, second)

    def test_account_change_recreates(self):
        first = strategy._ensure_exec_push_publisher(
            {"account_id": "acct1", "rpc": {"transport": "shm"}})
        second = strategy._ensure_exec_push_publisher(
            {"account_id": "acct2", "rpc": {"transport": "shm"}})

        self.assertIsNot(first, second)
        self.assertEqual(second.section_name, "bigqmt_shm_exec_acct2")

    def test_creation_failure_degrades_to_none_without_raising(self):
        class _FakeExecPush(object):
            class ExecPushPublisher(object):
                def __init__(self, *args, **kwargs):
                    raise exec_push.ExecPushError("no kernel objects today")

        saved_module = strategy._exec_push
        strategy._exec_push = _FakeExecPush()
        try:
            publisher = strategy._ensure_exec_push_publisher(
                {"account_id": "acct1", "rpc": {"transport": "shm"}})
        finally:
            strategy._exec_push = saved_module

        self.assertIsNone(publisher)
        self.assertIsNone(strategy._exec_push_publisher)

    def test_missing_account_does_not_create(self):
        self.assertIsNone(strategy._ensure_exec_push_publisher(
            {"rpc": {"transport": "shm"}}))


# ------------------------------------------------- 回调集成


@WINDOWS_ONLY
class CallbackIntegrationTest(unittest.TestCase):
    def setUp(self):
        self._saved_publisher = strategy._exec_push_publisher
        self.prefix = _unique_prefix()
        self.publisher = exec_push.ExecPushPublisher(self.prefix, "acct")
        strategy._exec_push_publisher = self.publisher
        self.reader = _RingReader(self.publisher.section_name)

    def tearDown(self):
        strategy._exec_push_publisher = self._saved_publisher
        try:
            self.publisher.close()
        except Exception:
            pass

    def test_order_callback_writes_contract_row_to_the_ring(self):
        strategy.order_callback(_FakeContext(), _order_obj())

        frames = self.reader.poll_frames()
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0]["kind"], "order")
        row = frames[0]["row"]
        self.assertEqual(row["order_sys_id"], "2209")
        self.assertEqual(row["user_order_id"], "sig-1")
        self.assertEqual(row["stock_code"], "601388.SH")
        self.assertEqual(row["action"], "SELL")
        self.assertEqual(row["order_volume"], 158200)
        self.assertEqual(row["traded_volume"], 0)
        self.assertEqual(row["price"], 35.62)
        self.assertEqual(row["status"], 50)
        self.assertEqual(row["order_time"], exec_events.date_time_seconds(
            "20261014", "093000"))

    def test_deal_callback_writes_contract_row_to_the_ring(self):
        strategy.deal_callback(_FakeContext(), _deal_obj())

        frames = self.reader.poll_frames()
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0]["kind"], "deal")
        row = frames[0]["row"]
        self.assertEqual(row["trade_id"], "t-1")
        self.assertEqual(row["order_sys_id"], "2209")
        self.assertEqual(row["user_order_id"], "sig-1")
        self.assertEqual(row["stock_code"], "601388.SH")
        self.assertEqual(row["action"], "SELL")
        self.assertEqual(row["volume"], 700)
        self.assertEqual(row["price"], 35.36)
        self.assertEqual(row["traded_at"], "093013")

    def test_presysid_order_frame_is_skipped(self):
        """issue #161：无 sysid 的 pre-sysid 帧不入环，孪生带 sysid 再来。"""
        strategy.order_callback(
            _FakeContext(), _order_obj(m_strOrderSysID=""))

        self.assertEqual(self.reader.poll_frames(), [])

    def test_publisher_none_leaves_callbacks_intact(self):
        strategy._exec_push_publisher = None

        strategy.order_callback(_FakeContext(), _order_obj())
        strategy.deal_callback(_FakeContext(), _deal_obj())

        self.assertEqual(self.reader.poll_frames(), [])

    def test_broken_publisher_does_not_break_the_callback_chain(self):
        class _BrokenPublisher(object):
            def __init__(self):
                self.calls = []

            def publish_order(self, row):
                self.calls.append("order")
                raise RuntimeError("ring exploded")

            def publish_deal(self, row):
                self.calls.append("deal")
                raise RuntimeError("ring exploded")

        broken = _BrokenPublisher()
        strategy._exec_push_publisher = broken

        # 回调链零抛错（回调线程抛错 = QMT 侧无堆栈 SystemError，issue #76）。
        strategy.order_callback(_FakeContext(), _order_obj())
        strategy.deal_callback(_FakeContext(), _deal_obj())

        self.assertEqual(broken.calls, ["order", "deal"])

    def test_normalize_failure_does_not_break_the_callback_chain(self):
        class _BrokenExecEvents(object):
            def normalize_order_event(self, *args, **kwargs):
                raise RuntimeError("normalize exploded")

            def normalize_trade_event(self, *args, **kwargs):
                raise RuntimeError("normalize exploded")

        saved = strategy._exec_events
        strategy._exec_events = _BrokenExecEvents()
        try:
            strategy.order_callback(_FakeContext(), _order_obj())
            strategy.deal_callback(_FakeContext(), _deal_obj())
        finally:
            strategy._exec_events = saved

        self.assertEqual(self.reader.poll_frames(), [])


if __name__ == "__main__":
    unittest.main()

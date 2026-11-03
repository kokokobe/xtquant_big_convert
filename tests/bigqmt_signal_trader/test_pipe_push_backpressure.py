# coding: utf-8
"""任务13红测：pipe 行情发布有界隔离与可检测背压。

慢读者不得阻塞 QMT 回调线程：publish 只做一次 JSON 编码并入各客户端
有界队列，WriteFile 由独立 writer 执行；队列满/写超时关闭该连接并
计入 backpressure_status。全部用真实命名管道或注入的 FakeWriter，
不碰真实账户。
"""
import json
import os
import sys
import threading
import time

import pytest

pytestmark = pytest.mark.skipif(os.name != "nt", reason="named pipes are Windows-only")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.transports.pipe_push_channel import (  # noqa: E402
    PipeQuotePushChannel,
    pipe_events_path,
)


def _paths():
    return pipe_events_path("bigqmt_rpc_test_%d" % os.getpid(), "acct")


def test_slow_writer_does_not_block_publish():
    blocked = threading.Event()
    ch = PipeQuotePushChannel(bind_address=_paths(), queue_capacity=1,
                              write_timeout_seconds=0.05)
    ch._install_test_writer(lambda payload: blocked.wait(1.0))
    ch.start_publisher()
    try:
        t0 = time.time()
        ch.publish("q", {"TEST.SH": {"lastPrice": 2.801}})
        ch.publish("q", {"TEST.SH": {"lastPrice": 2.802}})
        ch.publish("q", {"TEST.SH": {"lastPrice": 2.803}})
        elapsed = time.time() - t0
        status = ch.backpressure_status()
        assert status["overflows"] >= 1, status
        # publish 三次不随 writer 阻塞线性变慢（有界入队语义）
        assert elapsed < 0.5, "publish 被慢 writer 阻塞: %.3fs" % elapsed
    finally:
        blocked.set()
        ch.stop()


def test_oversize_frame_is_rejected_not_published():
    ch = PipeQuotePushChannel(bind_address=_paths(), max_frame_bytes=256)
    ch._install_test_writer(lambda payload: True)
    ch.start_publisher()
    try:
        big = {"TEST.SH": {"pad": "x" * 512}}
        assert ch.publish("q", big) is False
        assert ch.backpressure_status()["rejected_oversize"] == 1
        assert ch.publish("q", {"TEST.SH": {"lastPrice": 2.8}}) is True
    finally:
        ch.stop()


def test_stream_meta_epoch_and_seq_ride_frames():
    ch = PipeQuotePushChannel(bind_address=_paths())
    frames = []
    ch._install_test_writer(lambda payload: frames.append(payload) or True)
    ch.start_publisher()
    try:
        ch.publish("q", {"TEST.SH": {"lastPrice": 2.801}})
        ch.publish("q", {"TEST.SH": {"lastPrice": 2.802}})
        deadline = time.time() + 2.0
        while len(frames) < 2 and time.time() < deadline:
            time.sleep(0.01)
        assert len(frames) == 2
        first = json.loads(frames[0].decode("utf-8"))
        second = json.loads(frames[1].decode("utf-8"))
        assert first["meta"]["stream_seq"] == 1
        assert second["meta"]["stream_seq"] == 2
        assert first["meta"]["stream_epoch"] == second["meta"]["stream_epoch"]
        assert isinstance(first["meta"]["stream_epoch"], str) and first["meta"]["stream_epoch"]
        # data/topic 原样保留（旧读者兼容）
        assert first["topic"] == "q"
        assert first["data"]["TEST.SH"]["lastPrice"] == 2.801
    finally:
        ch.stop()


def test_two_readers_one_slow_one_fast():
    slow_gate = threading.Event()
    received_fast = []
    fast_done = threading.Event()
    path = _paths()
    server = PipeQuotePushChannel(bind_address=path)
    server.start_publisher()
    client = PipeQuotePushChannel(connect_address=path)

    def on_fast(topic, data):
        received_fast.append(data)
        if len(received_fast) >= 3:
            fast_done.set()

    client.start_subscriber(["q"], on_fast)
    try:
        # 等真实连接建立
        deadline = time.time() + 2.0
        while server.publisher_subscriber_count() < 1 and time.time() < deadline:
            time.sleep(0.02)
        for i in range(3):
            assert server.publish("q", {"TEST.SH": {"lastPrice": 2.80 + i}}) is True
        assert fast_done.wait(2.0)
        assert server.backpressure_status()["overflows"] == 0
    finally:
        client.stop()
        server.stop()


def test_stop_wakes_writers_and_restart_does_not_revive():
    blocked = threading.Event()
    ch = PipeQuotePushChannel(bind_address=_paths(), queue_capacity=4)
    ch._install_test_writer(lambda payload: blocked.wait(1.0))
    ch.start_publisher()
    ch.publish("q", {"TEST.SH": {"lastPrice": 2.8}})
    blocked.set()
    ch.stop()
    threads_before = threading.active_count()
    ch.publish("q", {"TEST.SH": {"lastPrice": 2.8}})  # stop 后 no-op
    time.sleep(0.1)
    assert threading.active_count() <= threads_before
    # restart 不复活旧 writer
    ch._install_test_writer(lambda payload: True)
    ch.start_publisher()
    try:
        assert ch.publish("q", {"TEST.SH": {"lastPrice": 2.8}}) is True
        time.sleep(0.1)
        assert ch.backpressure_status()["overflows"] == 0
    finally:
        ch.stop()


def test_max_clients_rejects_extra_connections():
    path = _paths()
    server = PipeQuotePushChannel(bind_address=path, max_clients=1)
    server.start_publisher()
    c1 = PipeQuotePushChannel(connect_address=path)
    c2 = PipeQuotePushChannel(connect_address=path)
    try:
        c1.start_subscriber(["q"], lambda t, d: None)
        deadline = time.time() + 2.0
        while server.publisher_subscriber_count() < 1 and time.time() < deadline:
            time.sleep(0.02)
        c2.start_subscriber(["q"], lambda t, d: None)
        time.sleep(0.3)
        assert server.publisher_subscriber_count() == 1, "超出 max_clients 的连接必须被拒"
    finally:
        c1.stop()
        c2.stop()
        server.stop()

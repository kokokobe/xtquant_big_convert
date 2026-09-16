#encoding:utf-8
"""总线双向实时验证:
  方向1 QMT->外部: RPC bus_publish(桥进程内发布) -> 外部订阅者收
  方向2 外部->QMT: 外部 BusPublisher 发布 -> RPC bus_inbox_drain 取
  全程测延迟与桥健康（ping）。
"""
import json
import sys
import threading
import time
import uuid

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport
from bigqmt_signal_trader.pub_bus import BusSubscriber, BusPublisher

ACCOUNT = "52625295"
rpc = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[bus-e2e]")


def rpc_call(method, params, timeout=30.0):
    request = {
        "schema_version": 1,
        "request_id": "e2e-%s-%s" % (method, uuid.uuid4().hex[:10]),
        "account_id": ACCOUNT,
        "method": method,
        "params": dict(params),
    }
    return rpc.send_request(request, timeout)


try:
    print("== 方向1: QMT -> 外部 ==")
    sub = BusSubscriber(ACCOUNT)
    sub.subscribe()
    time.sleep(0.3)                       # 注册表登记对外可见
    got = []
    arrived_at = [None]

    def collector():
        for topic, data, seq in sub.listen(topics={"signal"}):
            arrived_at[0] = time.time()
            got.append((topic, data, seq))
            if len(got) >= 2:
                break

    t = threading.Thread(target=collector, daemon=True)
    t.start()
    time.sleep(0.3)
    payloads = [{"n": 1, "px": 40.92}, {"n": 2, "px": 40.95}]
    latencies = []
    for p in payloads:
        t0 = time.time()
        r = rpc_call("bus_publish", {"topic": "signal", "data": p})
        assert r.get("ok"), r
        latencies.append(time.time() - t0)
    t.join(timeout=10)
    print("  收到 %d/2 条" % len(got))
    for topic, data, seq in got:
        print("   seq=%s topic=%s data=%s" % (seq, topic, json.dumps(data, ensure_ascii=False)))
    if got:
        print("  RPC发布->订阅者收到 往返: %.2fms" % (
            min((arrived_at[0] - latencies[0]) * 1000,) if arrived_at[0] else 0) if False else "")
    print("  RPC 调用耗时: %s" % ["%.2fms" % (l * 1000) for l in latencies])

    print("== 方向2: 外部 -> QMT ==")
    s = rpc_call("bus_inbox_start", {}, 30.0)
    print("  inbox_start: %s" % json.dumps(s.get("data"), default=str))
    time.sleep(0.3)
    ext_pub = BusPublisher(ACCOUNT)       # 外部进程的发布方
    t0 = time.time()
    ext_pub.publish("cmd", {"action": "query_positions"})
    ext_pub.publish("cmd", {"action": "rebalance", "codes": ["513300.SH"]})
    print("  外部发布 2 条 (%.2fms)" % ((time.time() - t0) * 1000))
    time.sleep(0.5)
    d = rpc_call("bus_inbox_drain", {}, 30.0)
    msgs = (d.get("data") or {}).get("messages") or []
    print("  桥收件箱取出 %d 条:" % len(msgs))
    for m in msgs:
        print("   %s" % json.dumps(m, ensure_ascii=False)[:120].encode("gbk", "replace").decode("gbk"))

    print("== 桥健康 ==")
    t0 = time.time()
    ping = rpc_call("ping", {}, 10.0)
    print("  ping ok=%s %.2fs version=%s" % (
        ping.get("ok"), time.time() - t0, (ping.get("data") or {}).get("version")))
    verdict = "PASS" if (len(got) == 2 and len(msgs) == 2 and ping.get("ok")) else "FAIL"
    print("== 双向结论: %s ==" % verdict)
finally:
    rpc.stop()
    sub.close()

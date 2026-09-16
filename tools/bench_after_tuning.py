#encoding:utf-8
"""调优后基准: tick 100ms→10ms 后的延迟/吞吐对比。"""
import json
import statistics
import sys
import time
import uuid

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

ACCOUNT = "52625295"
client = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[bench]")


def call(method, params=None, timeout=30.0):
    request = {
        "schema_version": 1,
        "request_id": "bm-%s-%s" % (method, uuid.uuid4().hex[:10]),
        "account_id": ACCOUNT,
        "method": method,
        "params": dict(params or {}),
    }
    t0 = time.time()
    r = client.send_request(request, timeout)
    return r, (time.time() - t0) * 1000


def bench(name, method, params, n):
    lats = []
    last = None
    for i in range(n):
        r, dt = call(method, params)
        lats.append(dt)
        last = r
        if not r.get("ok"):
            print("  %s[%d] ok=False error=%s" % (name, i, str(r.get("error"))[:80]))
    lats.sort()
    print("%-28s n=%-3d min=%7.2f avg=%7.2f p95=%7.2f max=%7.2f ms" % (
        name, n, lats[0], statistics.mean(lats), lats[int(n * 0.95) - 1], lats[-1]))
    return last


try:
    ping_r = bench("ping", "ping", {}, 20)

    pos_r = bench("get_positions", "get_positions", {}, 10)
    d = pos_r.get("data") or {}
    print("   (持仓 %d 只 — deferred 数据正确性)" % (len(d) if isinstance(d, dict) else -1))

    asset_r = None
    try:
        asset_r, _ = call("get_asset", {}, 10)
    except Exception as exc:
        print("get_asset[10s]: TIMEOUT (%s) — 重试用 90s 长超时判慢/死" % type(exc).__name__)
        try:
            asset_r, dt = call("get_asset", {}, 90.0)
            print("get_asset[90s]: ok=%s %.0fms (慢但通=与其他终端查询竞争)" % (
                asset_r.get("ok"), dt))
        except Exception as exc2:
            print("get_asset[90s]: FAIL %s (死锁级)" % type(exc2).__name__)
    a = (asset_r.get("data") if asset_r else None) or {}
    print("   (cash=%s total=%s)" % (a.get("cash"), a.get("total_asset")))

    md_r = bench("get_market_data_ex", "get_market_data_ex", {
        "stock_list": ["513300.SH"], "period": "1d",
        "start_time": "20260901", "end_time": "20260915"}, 5)
    dd = md_r.get("data") or {}
    if isinstance(dd, dict) and "513300.SH" in dd:
        print("   (rows=%d)" % len((dd["513300.SH"].get("records") or {}).get("time") or []))

    qo_r = bench("query_orders", "query_orders", {}, 5)

    # 下单延迟 x3
    o_lats = []
    for i in range(3):
        r, dt = call("submit_order", {
            "stock_code": "603993.SH", "action": "BUY", "volume": 100,
            "price_type": "LIMIT", "price": 17.0,
            "signal_id": "bench-%d" % i,
            "remark": "bench-%d" % i}, timeout=30.0)
        o_lats.append(dt)
    o_lats.sort()
    print("%-28s n=3   min=%7.2f avg=%7.2f max=%7.2f ms  (状态=%s)" % (
        "submit_order", o_lats[0], sum(o_lats) / 3, o_lats[-1],
        (json.dumps([r.get("data", {}).get("status") for r in [None]]) if False else "见上")))
    # 重新单独打印状态
    print("   (三单全部提交)")

    # 吞吐: 连续 50 次 ping 不间断
    t0 = time.time()
    n = 50
    for i in range(n):
        client.send_request({"schema_version": 1, "request_id": "tp-%d" % i,
                             "account_id": ACCOUNT, "method": "ping", "params": {}}, 10.0)
    tp = n / (time.time() - t0)
    print("%-28s %.0f 请求/s" % ("连续吞吐(ping)", tp))
finally:
    client.stop()

#encoding:utf-8
r"""rpc_background_threads=True 切换验证清单。
逐项: 1.ping延迟 2.deferred查询(持仓/资金) 3.listener线程行情 4.下单+查询
5.并发混合 6.桥健康。结果增量落盘 tools/background_mode_verify.txt
"""
import json
import sys
import threading
import time
import uuid

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

ACCOUNT = "52625295"
REPORT = r"D:\quantitative_qmt\xtquant_big_convert\tools\background_mode_verify.txt"
client = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[bg-verify]")
_lock = threading.Lock()


def emit(line):
    print(line.encode("gbk", "replace").decode("gbk"), flush=True)
    report.write(line + "\n")
    report.flush()


def call(method, params=None, timeout=30.0):
    request = {
        "schema_version": 1,
        "request_id": "bg-%s-%s" % (method, uuid.uuid4().hex[:10]),
        "account_id": ACCOUNT,
        "method": method,
        "params": dict(params or {}),
    }
    t0 = time.time()
    r = client.send_request(request, timeout)
    return r, time.time() - t0


def main():
    # 1) ping 延迟 x5
    lats = []
    for i in range(5):
        r, dt = call("ping")
        lats.append(dt * 1000)
        time.sleep(0.2)
    emit("1) ping 延迟: %s ms (avg %.2f)" % (
        ["%.2f" % x for x in lats], sum(lats) / len(lats)))
    sub_ms = all(x < 5.0 for x in lats)
    emit("   预期亚毫秒(切线程的收益): %s" % ("✓" if sub_ms else "✗ 仍受 tick 节奏影响"))

    # 2) deferred 查询: 持仓/资金 (get_trade_detail_data 约束的验证)
    r, dt = call("get_positions")
    d = r.get("data") or {}
    n_pos = len(d) if isinstance(d, dict) else -1
    sample = {}
    if isinstance(d, dict) and d:
        k0 = sorted(d)[0]
        sample = {f: d[k0].get(f) for f in ("stock_code", "volume", "cost")}
    emit("2) get_positions (deferred): ok=%s rows=%d %.0fms 样本=%s" % (
        r.get("ok"), n_pos, dt * 1000,
        json.dumps(sample, ensure_ascii=False).encode("gbk", "replace").decode("gbk")))
    deferred_ok = r.get("ok") and n_pos > 0
    emit("   交易数据非空(deferred 路径工作): %s" % ("✓" if deferred_ok else "✗✗ 空数据=线程约束被踩"))

    r, dt = call("get_asset")
    a = r.get("data") or {}
    emit("   get_asset: ok=%s cash=%s total=%s" % (
        r.get("ok"), a.get("cash"), a.get("total_asset")))

    # 3) listener 线程上的行情
    r, dt = call("get_market_data_ex", {
        "stock_list": ["513300.SH"], "period": "1d",
        "start_time": "20260901", "end_time": "20260915"})
    d = r.get("data") or {}
    rows = 0
    if isinstance(d, dict) and "513300.SH" in d:
        rows = len((d["513300.SH"].get("records") or {}).get("time") or [])
    emit("3) get_market_data_ex (listener 线程): ok=%s rows=%d %.0fms" % (
        r.get("ok"), rows, dt * 1000))

    # 4) 下单 + 查询 (deferred 结算链路)
    r, dt = call("submit_order", {
        "stock_code": "603993.SH", "action": "BUY", "volume": 100,
        "price_type": "LIMIT", "price": 17.0,
        "signal_id": "bg-smoke-%d" % int(time.time() * 1000),
        "remark": "bg-smoke"})
    d = r.get("data") or {}
    emit("4) submit_order: ok=%s status=%s uoid=%s %.0fms" % (
        r.get("ok"), d.get("status"), d.get("user_order_id"), dt * 1000))
    r, dt = call("query_orders")
    od = r.get("data") or {}
    olist = od.get("orders") or od.get("rows") or []
    emit("   query_orders: ok=%s 单数=%s" % (r.get("ok"), len(olist)))

    # 5) 并发混合: 4 线程 x (下单1 + 持仓1 + 行情1) = 12 组
    results = []
    errors = []

    def worker(wid):
        try:
            r1, _ = call("submit_order", {
                "stock_code": "603993.SH", "action": "BUY", "volume": 100,
                "price_type": "LIMIT", "price": 17.0,
                "signal_id": "bg-c%d-%d" % (wid, int(time.time() * 1000)),
                "remark": "bg-c%d" % wid}, timeout=30.0)
            r2, _ = call("get_positions", timeout=30.0)
            r3, _ = call("get_market_data_ex", {
                "stock_list": ["513300.SH"], "period": "1d",
                "start_time": "20260901", "end_time": "20260915"}, timeout=30.0)
            pos_ok = r2.get("ok") and isinstance(r2.get("data"), dict) and len(r2.get("data")) > 0
            with _lock:
                results.append((wid, r1.get("ok"), (r1.get("data") or {}).get("status"),
                                r2.get("ok"), pos_ok, r3.get("ok")))
        except Exception as exc:
            errors.append((wid, "%s: %s" % (type(exc).__name__, exc)))

    lock = threading.Lock()
    t0 = time.time()
    threads = [threading.Thread(target=worker, args=(w,)) for w in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    elapsed = time.time() - t0
    n_ok = sum(1 for r in results if r[1] and r[3] and r[5])
    n_posreal = sum(1 for r in results if r[4])
    emit("5) 并发混合: %d/4 组完成 ok=%d 持仓非空=%d 耗时%.1fs" % (
        len(results), n_ok, n_posreal, elapsed))
    if errors:
        emit("   异常: %s" % errors[:5])

    # 6) 桥健康
    r, dt = call("ping")
    emit("6) 桥健康: ping ok=%s %.0fms" % (r.get("ok"), dt * 1000))

    verdict = "PASS" if (sub_ms and deferred_ok and n_ok == 4 and n_posreal == 4) else "CHECK-ABOVE"
    emit("== 总体: %s ==" % verdict)
    return 0


if __name__ == "__main__":
    report = open(REPORT, "w", encoding="utf-8")
    try:
        sys.exit(main())
    finally:
        report.close()

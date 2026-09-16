#encoding:utf-8
r"""并发下单压测（DRY_RUN 网关，全程模拟，不碰真钱）——需要活桥。

pytest 运行（默认跳过）:
    set BIGQMT_LIVE_TEST=1 && python -m pytest tests/live/test_order_concurrency_live.py -v
脚本直跑:
    & "D:\python-lib-gf\python.exe" tests\live\test_order_concurrency_live.py

前置: local_config 已 rpc_allow_order_methods=True 且桥已重启（init 读配置）。
流程:
  1. ping 确认 allow_order_methods=True
  2. get_full_tick 拉 4 只股票现价（LIMIT 单用）
  3. 单发一单冒烟（603993.SH BUY 100）
  4. 12 线程 x 5 单 = 60 并发 submit_order，4 股票混合买卖
  5. get_positions 前后对比（DRY_RUN 不应改变真实持仓）
"""
import json
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "src"))

import pytest

from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

ACCOUNT = "52625295"
STOCKS = ["603993.SH", "000001.SZ", "513300.SH", "600036.SH"]
THREADS = 12
PER_THREAD = 5
REPORT = os.path.join(ROOT, "tools", "order_concurrency_report.txt")

_lock = threading.Lock()


def call(client, method, params=None, timeout=30.0):
    request = {
        "schema_version": 1,
        "request_id": "ot-%s-%d" % (method, int(time.time() * 1000)),
        "account_id": ACCOUNT,
        "method": method,
        "params": dict(params or {}),
    }
    return client.send_request(request, timeout)


def main(report=None):
    client = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[order-test]")

    def emit(line):
        print(line.encode("gbk", "replace").decode("gbk"))
        if report is not None:
            report.write(line + "\n")
            report.flush()

    try:
        # 1) 门确认
        ping = call(client, "ping")
        allowed = (ping.get("data") or {}).get("allow_order_methods")
        emit("ping: ok=%s allow_order_methods=%s" % (ping.get("ok"), allowed))
        if not allowed:
            emit("!! allow_order_methods 仍为 False —— 桥没重启加载新配置, 终止")
            return 1

        # 2) 基线持仓 + 现价
        pos_before = call(client, "get_positions")
        data_before = pos_before.get("data") or {}
        rows_before = len(data_before.get("positions") or data_before.get("rows") or [])
        emit("get_positions(基线): ok=%s rows=%s" % (pos_before.get("ok"), rows_before))
        ticks = call(client, "get_full_tick", {"stock_list": STOCKS}, timeout=20.0)
        prices = {}
        data = ticks.get("data") or {}
        if isinstance(data, dict):
            for code, row in (data.get("ticks") or data.get("data") or data).items():
                if isinstance(row, dict):
                    prices[code] = (row.get("lastPrice") or row.get("last_price")
                                    or row.get("price"))
        emit("现价: %s" % json.dumps(prices, ensure_ascii=False))

        # 3) 单发冒烟
        smoke = call(client, "submit_order", {
            "stock_code": "603993.SH", "action": "BUY", "volume": 100,
            "price_type": "LIMIT",
            "price": float(prices.get("603993.SH") or 6.0),
            "signal_id": "smoke-603993-%d" % int(time.time() * 1000),
            "remark": "order-test-smoke",
        })
        emit("冒烟单: ok=%s status=%s user_order_id=%s error=%s" % (
            smoke.get("ok"),
            (smoke.get("data") or {}).get("status"),
            (smoke.get("data") or {}).get("user_order_id"),
            str(smoke.get("error"))[:80]))
        if not smoke.get("ok"):
            emit("!! 冒烟单失败, 终止并发阶段")
            return 1

        # 4) 并发压测
        results = []
        errors = []
        t0 = time.time()

        def worker(wid):
            for i in range(PER_THREAD):
                code = STOCKS[(wid + i) % len(STOCKS)]
                action = "BUY" if (wid + i) % 2 == 0 else "SELL"
                sig = "conc-w%d-i%d-%d" % (wid, i, int(time.time() * 1000))
                try:
                    r = call(client, "submit_order", {
                        "stock_code": code, "action": action, "volume": 100,
                        "price_type": "LIMIT",
                        "price": float(prices.get(code) or 10.0),
                        "signal_id": sig,
                        "remark": sig,
                    }, timeout=30.0)
                    d = r.get("data") or {}
                    with _lock:
                        results.append((wid, i, code, action, r.get("ok"),
                                        d.get("status"), d.get("user_order_id"),
                                        str(r.get("error"))[:60]))
                except Exception as exc:
                    with _lock:
                        errors.append((wid, i, code,
                                       "%s: %s" % (type(exc).__name__, exc)))

        threads = [threading.Thread(target=worker, args=(w,))
                   for w in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=180)
        elapsed = time.time() - t0

        ok_cnt = sum(1 for r in results if r[4])
        dry_cnt = sum(1 for r in results if r[5] == "DRY_RUN")
        uoids = [r[6] for r in results if r[6]]
        emit("---- 并发汇总: %d 线程 x %d 单 = %d 发出, ok=%d, DRY_RUN=%d, "
             "耗时 %.1fs (%.0f 单/s)"
             % (THREADS, PER_THREAD, THREADS * PER_THREAD, ok_cnt, dry_cnt,
                elapsed, THREADS * PER_THREAD / elapsed if elapsed else 0))
        emit("   唯一 user_order_id=%d / %d (重复=串包)"
             % (len(set(uoids)), len(uoids)))
        if errors:
            emit("   异常 %d 个:" % len(errors))
            for e in errors[:10]:
                emit("     %s" % (e,))
        bad = [r for r in results if not r[4] or r[5] != "DRY_RUN"]
        if bad:
            emit("   非 DRY_RUN/失败 明细(前10):")
            for r in bad[:10]:
                emit("     %s" % (str(r).encode("gbk", "replace").decode("gbk"),))

        # 5) 真实持仓不应变化
        pos_after = call(client, "get_positions")
        data_after = pos_after.get("data") or {}
        rows_after = len(data_after.get("positions") or data_after.get("rows") or [])
        emit("get_positions(结束): rows=%s (基线 %s, DRY_RUN 不应变化)"
             % (rows_after, rows_before))
        verdict = "PASS" if (ok_cnt == THREADS * PER_THREAD
                             and dry_cnt == THREADS * PER_THREAD
                             and not errors) else "FAIL"
        emit("结论: %s" % verdict)
        return 0 if verdict == "PASS" else 1
    finally:
        client.stop()


def test_live_order_concurrency():
    if os.environ.get("BIGQMT_LIVE_TEST") != "1":
        pytest.skip("需要活桥: 设 BIGQMT_LIVE_TEST=1 且确认 QMT 里桥在跑")
    report = open(REPORT, "w", encoding="utf-8")
    try:
        assert main(report=report) == 0
    finally:
        report.close()


if __name__ == "__main__":
    report = open(REPORT, "w", encoding="utf-8")
    try:
        sys.exit(main(report=report))
    finally:
        report.close()

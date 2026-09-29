# coding: utf-8
"""双实例现场只读压测（无任何下单调用）。

Phase 0: 连通性 sanity（逐方法试探，失败的从压测名单剔除）
Phase 1: 单线程顺序基线（ping 密集 + 账户查询）
Phase 2: 多线程并发混跑（2/4/8/16 线程 round-robin）
Phase 3: get_market_data_ex（客户端 FormulaServer 直连快速路径）
用法: python _stress_two_instances.py
"""
import os
import statistics
import sys
import threading
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader.xtquant_compat as compat

compat.configure(account_id="52625295",
                 redis_config={"transport": "shm",
                               "formula_server": {"enabled": False}},
                 timeout_seconds=15.0)
client = compat.get_default_client()
print("account:", client.account_id, "| transport:", client.transport_name)

MIX = [
    ("ping", {}),
    ("get_positions", {}),
    ("get_asset", {}),
    ("query_orders", {}),
    ("get_full_tick", {"codes": ["513300.SH"]}),
]

errors_seen = {}


def one_call(name, params):
    t0 = time.perf_counter()
    try:
        client.call(name, params)
        ok = True
    except Exception as exc:
        ok = False
        key = "%s:%s" % (name, type(exc).__name__)
        errors_seen[key] = errors_seen.get(key, 0) + 1
    dt = (time.perf_counter() - t0) * 1000
    return ok, dt


def pct(sorted_lat, p):
    if not sorted_lat:
        return 0.0
    idx = min(int(len(sorted_lat) * p), len(sorted_lat) - 1)
    return sorted_lat[idx]


def report(tag, rows, wall=None):
    print("-- %s --" % tag)
    by = {}
    for name, ok, dt in rows:
        by.setdefault(name, []).append((ok, dt))
    for name, vals in sorted(by.items()):
        lats = sorted(d for ok, d in vals if ok)
        n_ok = len(lats)
        n_err = len(vals) - n_ok
        extra = ""
        if wall and name == "ALL":
            total = sum(len(v) for k, v in by.items() if k != "ALL")
            extra = " throughput=%.0f/s" % (total / wall)
        if lats:
            print("  %-18s n=%-4d ok=%-4d err=%-3d p50=%7.2f p95=%7.2f "
                  "p99=%7.2f max=%8.2f ms%s"
                  % (name, len(vals), n_ok, n_err, pct(lats, 0.50),
                     pct(lats, 0.95), pct(lats, 0.99), lats[-1], extra))
        else:
            print("  %-18s n=%-4d 全部失败" % (name, len(vals)))


print("== Phase 0 sanity ==")
good = []
for name, params in MIX:
    ok, dt = one_call(name, params)
    print("  %-16s %s %.2f ms" % (name, "OK" if ok else "FAIL", dt))
    if ok:
        good.append((name, params))
if len(good) < len(MIX):
    dropped = [n for n, _ in MIX if (n, _) not in good]
    print("  dropped from mix:", dropped)

print("== Phase 1 sequential baseline ==")
rows = []
t0 = time.perf_counter()
for _ in range(100):
    ok, dt = one_call("ping", {})
    rows.append(("ping", ok, dt))
wall_ping = time.perf_counter() - t0
print("  ping 100x wall=%.2fs throughput=%.0f/s" % (wall_ping, 100 / wall_ping))
for _ in range(30):
    for name, params in good[1:]:
        ok, dt = one_call(name, params)
        rows.append((name, ok, dt))
report("sequential", rows)

print("== Phase 2 concurrent mixed ==")
for nthreads, per_thread in [(2, 40), (4, 40), (8, 30), (16, 20)]:
    rows = []
    lock = threading.Lock()
    barrier = threading.Barrier(nthreads)

    def worker(idx):
        barrier.wait()
        local = []
        for i in range(per_thread):
            name, params = good[(idx + i) % len(good)]
            ok, dt = one_call(name, params)
            local.append((name, ok, dt))
        with lock:
            rows.extend(local)

    ths = [threading.Thread(target=worker, args=(i,)) for i in range(nthreads)]
    t0 = time.perf_counter()
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    wall = time.perf_counter() - t0
    report("threads=%d per=%d wall=%.2fs" % (nthreads, per_thread, wall),
           rows + [("ALL", True, 0)], wall=wall)

print("== Phase 3 get_market_data_ex (fastpath off -> RPC) ==")
mdx = ("get_market_data_ex",
       {"stock_list": ["513300.SH"], "period": "1d",
        "start_time": "20260901", "end_time": "20260923",
        "field_list": ["open", "high", "low", "close", "volume", "amount"]})
rows = []
t0 = time.perf_counter()
for _ in range(50):
    ok, dt = one_call(mdx[0], mdx[1])
    rows.append(("mdx_fastpath_1t", ok, dt))
wall = time.perf_counter() - t0
report("fastpath 1 thread", rows + [("ALL", True, 0)], wall=wall)

rows = []
lock = threading.Lock()
barrier = threading.Barrier(8)

def mdx_worker():
    barrier.wait()
    local = []
    for _ in range(20):
        ok, dt = one_call(mdx[0], mdx[1])
        local.append(("mdx_fastpath_8t", ok, dt))
    with lock:
        rows.extend(local)

ths = [threading.Thread(target=mdx_worker) for _ in range(8)]
t0 = time.perf_counter()
for t in ths:
    t.start()
for t in ths:
    t.join()
wall = time.perf_counter() - t0
report("fastpath 8 threads", rows + [("ALL", True, 0)], wall=wall)

print("== errors summary ==")
if errors_seen:
    for k, v in sorted(errors_seen.items()):
        print("  %-50s x%d" % (k, v))
else:
    print("  none")
print("DONE")

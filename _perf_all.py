# coding: utf-8
"""逐函数多轮采样延迟基准（复用 live_api_bench 的 GROUPS 参数）。
用法: python _perf_all.py [轮数]
"""
import ast
import os
import statistics
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))

# 从 live_api_bench.py 里抽出 GROUPS（带参方法清单），不执行该脚本
with open(os.path.join(ROOT, "live_api_bench.py"), encoding="utf-8") as f:
    tree = ast.parse(f.read())
GROUPS = None
for node in ast.walk(tree):
    if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "GROUPS":
        GROUPS = ast.literal_eval(node.value)
assert GROUPS, "GROUPS not found"

import bigqmt_signal_trader.xtquant_compat as compat
compat.configure(account_id="52625295",
                 redis_config={"transport": "shm",
                               # fastpath=回环TCP, 面板运行期会被审计杀, 见 _client_config_shm.py
                               "formula_server": {"enabled": False}},
                 timeout_seconds=30.0)
client = compat.get_default_client()
print("account:", client.account_id, "| transport:", client.transport_name)

N = int(sys.argv[1]) if len(sys.argv) > 1 else 5
rows = []
for cat, items in GROUPS:
    for name, params in items:
        lat = []
        status = "OK"
        err = ""
        for _ in range(N):
            t0 = time.perf_counter()
            try:
                client.call(name, params)
                lat.append((time.perf_counter() - t0) * 1000)
            except NotImplementedError as exc:
                status = "N/A(设计内)"
                err = str(exc)[:40]
                break
            except Exception as exc:
                status = "FAIL"
                err = "%s: %s" % (type(exc).__name__, str(exc)[:40])
                break
        if lat:
            lat.sort()
            rows.append((cat, name, status, lat[0], statistics.mean(lat),
                         lat[int(len(lat) * 0.95) - 1 if len(lat) > 1 else 0], lat[-1]))
        else:
            rows.append((cat, name, status, 0, 0, 0, 0))
            if err:
                print("  [skip] %-30s %s" % (name, err))

print("=" * 78)
print("%-14s %-30s %-6s %8s %8s %8s %8s"
      % ("类别", "函数", "状态", "min", "avg", "p95", "max (ms)"))
for cat, name, status, mn, avg, p95, mx in rows:
    print("%-14s %-30s %-6s %8.1f %8.1f %8.1f %8.1f"
          % (cat.encode("gbk", "replace").decode("gbk"), name, status, mn, avg, p95, mx))

ok = [r for r in rows if r[2] == "OK"]
all_lat = sorted(r[5] for r in ok)
print("-" * 78)
print("方法数=%d OK=%d  全体p50=%.1fms  全体p95=%.1fms"
      % (len(rows), len(ok), all_lat[len(all_lat) // 2], all_lat[int(len(all_lat) * 0.95)]))

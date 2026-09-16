#encoding:utf-8
"""passorder 复测 + probe_capabilities 能力矩阵"""
import json
import sys
import time

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

ACCOUNT = "52625295"
client = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[chk]")


def call(method, params=None, timeout=30.0):
    request = {
        "schema_version": 1,
        "request_id": "chk-%s-%d" % (method, int(time.time() * 1000)),
        "account_id": ACCOUNT,
        "method": method,
        "params": dict(params or {}),
    }
    return client.send_request(request, timeout)


try:
    # 1) passorder 复测（当时是排队陪葬，现在队列空了应回 not allowed）
    r = call("passorder", {})
    print("passorder:", json.dumps(r, ensure_ascii=False)[:200].encode("gbk", "replace").decode("gbk"))

    # 2) cancel_order 复测
    r = call("cancel_order", {})
    print("cancel_order:", json.dumps(r, ensure_ascii=False)[:200].encode("gbk", "replace").decode("gbk"))

    # 3) 能力矩阵
    r = call("probe_capabilities", {})
    print()
    print("== probe_capabilities ok=%s ==" % r.get("ok"))
    data = r.get("data") or {}
    if isinstance(data, dict):
        for k in sorted(data):
            v = json.dumps(data[k], ensure_ascii=False, default=str)
            print("  %s: %s" % (k, v[:220].encode("gbk", "replace").decode("gbk")))
    else:
        print(json.dumps(data, ensure_ascii=False, default=str)[:1500].encode("gbk", "replace").decode("gbk"))
    if r.get("error"):
        print("error:", r["error"].encode("gbk", "replace").decode("gbk"))
finally:
    client.stop()

#encoding:utf-8
"""紧急: 查真实委托并全撤"""
import json
import sys
import time

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

ACCOUNT = "52625295"
client = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[emergency]")


def call(method, params=None, timeout=30.0):
    request = {
        "schema_version": 1,
        "request_id": "em-%s-%d" % (method, int(time.time() * 1000)),
        "account_id": ACCOUNT,
        "method": method,
        "params": dict(params or {}),
    }
    return client.send_request(request, timeout)


def show(tag, r, limit=2000):
    print("== %s ok=%s error=%s" % (tag, r.get("ok"), str(r.get("error"))[:80]))
    s = json.dumps(r.get("data"), ensure_ascii=False, default=str)
    print(s[:limit].encode("gbk", "replace").decode("gbk"))


try:
    # 1) 当前委托（真实挂单）
    orders = call("query_orders")
    show("query_orders", orders, 3000)
    rows = (orders.get("data") or {})
    if isinstance(rows, dict):
        rows = rows.get("orders") or rows.get("rows") or []
    n_cancelled = 0
    if isinstance(rows, list):
        for o in rows:
            if not isinstance(o, dict):
                continue
            oid = (o.get("order_id") or o.get("order_sys_id")
                   or o.get("orderID") or o.get("id"))
            status = str(o.get("status") or o.get("order_status") or "")
            print("   委托: id=%s stock=%s vol=%s price=%s status=%s" % (
                oid, o.get("stock_code"), o.get("volume"), o.get("price"),
                status)[:120].encode("gbk", "replace").decode("gbk"))
            # 未终态的都撤
            if oid and any(k in status for k in ("PART", "SUBMIT", "PEND", "未", "部")):
                c = call("cancel_order", {"order_id": oid, "stock_code": o.get("stock_code")})
                print("   -> cancel: ok=%s %s" % (
                    c.get("ok"), str(c.get("error"))[:80].encode("gbk", "replace").decode("gbk")))
                n_cancelled += 1
    print("发起撤单数:", n_cancelled)

    # 2) 资金（对照今晨基线: bal 403692.27 avai 63464.87）
    asset = call("get_asset")
    show("get_asset", asset, 600)

    # 3) 持仓
    pos = call("get_positions")
    show("get_positions", pos, 800)
finally:
    client.stop()

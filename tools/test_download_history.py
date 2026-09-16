#encoding:utf-8
"""download_history_data 实弹: 下载 513300.SH 近两周日线 -> get_market_data_ex 验证落地"""
import json
import sys
import time
import uuid

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

ACCOUNT = "52625295"
client = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[dl-test]")


def call(method, params, timeout):
    request = {
        "schema_version": 1,
        "request_id": "dl-%s-%s" % (method, uuid.uuid4().hex[:10]),
        "account_id": ACCOUNT,
        "method": method,
        "params": dict(params),
    }
    return client.send_request(request, timeout)


try:
    t0 = time.time()
    r = call("download_history_data", {
        "stock_code": "513300.SH", "period": "1d",
        "start_time": "20260901", "end_time": "20260915",
    }, 120.0)
    print("download_history_data: ok=%s 耗时%.1fs" % (r.get("ok"), time.time() - t0))
    print("  data=%s" % json.dumps(r.get("data"), ensure_ascii=False, default=str)[:300].encode("gbk", "replace").decode("gbk"))
    if r.get("error"):
        print("  error=%s" % str(r.get("error"))[:200].encode("gbk", "replace").decode("gbk"))

    # 验证数据落地: 拉 513300.SH 日线区间
    v = call("get_market_data_ex", {
        "stock_list": ["513300.SH"], "period": "1d",
        "start_time": "20260901", "end_time": "20260915",
    }, 60.0)
    d = v.get("data") or {}
    print("get_market_data_ex: ok=%s" % v.get("ok"))
    s = json.dumps(d, ensure_ascii=False, default=str)
    print("  data=%s" % s[:600].encode("gbk", "replace").decode("gbk"))
finally:
    client.stop()

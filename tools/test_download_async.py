#encoding:utf-8
r"""异步下载任务实测：submit_download_history_data → get_download_status 轮询
→ get_market_data_ex 验证数据落地。

⚠️ 背景（2026-09-16 14:46 死锁实测）：内联同步下载在 drain/adjust 线程上会
永久死锁（QMT 下载完成回调落在被阻塞的同一线程），所以下载必须走异步任务
队列（download_jobs_enabled=True）。内联 download RPC 在 drain 模式下禁用。

前置：download_jobs_enabled=True（local_config）+ 桥已重启。
"""
import json
import sys
import time
import uuid

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

ACCOUNT = "52625295"
STOCK = "600036.SH"          # 本地无缓存的品种，验证真实下载
PERIOD = "1d"
START, END = "20260901", "20260915"

client = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[dl-async]")


def call(method, params, timeout):
    request = {
        "schema_version": 1,
        "request_id": "dla-%s-%s" % (method, uuid.uuid4().hex[:10]),
        "account_id": ACCOUNT,
        "method": method,
        "params": dict(params),
    }
    return client.send_request(request, timeout)


try:
    # 1) 提交异步下载任务（应秒回 job_id）
    t0 = time.time()
    sub = call("submit_download_history_data2", {
        "stock_list": [STOCK], "period": PERIOD,
        "start_time": START, "end_time": END,
    }, 30.0)
    print("submit: ok=%s 耗时%.1fs" % (sub.get("ok"), time.time() - t0))
    print("  %s" % json.dumps(sub.get("data"), ensure_ascii=False, default=str)[:300].encode("gbk", "replace").decode("gbk"))
    if not sub.get("ok"):
        print("error=%s" % str(sub.get("error"))[:200].encode("gbk", "replace").decode("gbk"))
        sys.exit(1)
    d = sub.get("data") or {}
    job_id = d.get("job_id") or d.get("jobId") or d.get("id")

    # 2) 轮询任务状态（任务字典的字段是 state: pending/running/done/failed）
    status = None
    for i in range(60):
        time.sleep(3.0)
        st = call("get_download_status", {"job_id": job_id}, 20.0)
        sd = st.get("data") or {}
        status = sd.get("state")
        print("  poll %02d: state=%s done=%s/%s" % (
            i + 1, status, sd.get("done"), sd.get("total")))
        if str(status).lower() in ("done", "completed", "finished", "ok", "true", "success"):
            break
        if str(status).lower() in ("failed", "error"):
            print("!! 任务失败")
            sys.exit(1)

    # 3) 验证数据落地
    v = call("get_market_data_ex", {
        "stock_list": [STOCK], "period": PERIOD,
        "start_time": START, "end_time": END,
    }, 60.0)
    vd = v.get("data") or {}
    rows = 0
    if isinstance(vd, dict) and STOCK in vd:
        rec = vd[STOCK].get("records") or {}
        rows = len(rec.get("time") or [])
    print("验证读回 %s: ok=%s 行数=%d" % (STOCK, v.get("ok"), rows))
    if rows:
        rec = vd[STOCK]["records"]
        print("  首行: %s open=%s close=%s" % (
            rec.get("stime", ["?"])[0], rec.get("open", [0])[0], rec.get("close", [0])[0]))
    print("结论: %s" % ("PASS" if rows > 0 else "FAIL(数据未落地)"))
finally:
    client.stop()

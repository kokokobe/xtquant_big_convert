#encoding:utf-8
r"""MARKET_DATA_METHODS 逐方法快扫: ping 探空 -> 20s 实弹 -> 归因。
每个方法测之前先 ping 到桥空闲（上一轮慢 handler 释放），保证超时归因到方法本身。
结果增量写 tools/market_data_sweep_report.txt + 最终 JSON 汇总。
"""
import json
import sys
import time
import uuid

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.redis_rpc import READ_METHODS, MARKET_DATA_METHODS
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

ACCOUNT = "52625295"
REPORT = r"D:\quantitative_qmt\xtquant_big_convert\tools\market_data_sweep_report.txt"
PER_CALL = 20.0
GATE_MAX_WAIT = 240.0

fh = open(REPORT, "w", encoding="utf-8")


def emit(line):
    print(line.encode("gbk", "replace").decode("gbk"), flush=True)
    fh.write(line + "\n")
    fh.flush()


def call(client, method, timeout):
    request = {
        "schema_version": 1,
        "request_id": "sw-%s-%s" % (method, uuid.uuid4().hex[:10]),
        "account_id": ACCOUNT,
        "method": method,
        "params": {},
    }
    return client.send_request(request, timeout)


def ping_gate(client):
    """等队列排空: ping 5s 超时轮询, 直到秒回。"""
    t0 = time.time()
    while time.time() - t0 < GATE_MAX_WAIT:
        try:
            r = call(client, "ping", 5.0)
            if r.get("ok"):
                return True
        except Exception:
            pass
        time.sleep(1.0)
    return False


def main():
    methods = sorted(set(READ_METHODS) & set(MARKET_DATA_METHODS))
    emit("== market_data 族快扫: %d 个方法, 单方法 %ds 超时, 前置 ping 探空 =="
         % (len(methods), int(PER_CALL)))
    client = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[sweep]")
    summary = {}
    try:
        if not ping_gate(client):
            emit("!! 桥不响应, 终止")
            return 1
        for name in methods:
            if not ping_gate(client):
                summary[name] = "GATE_FAIL(队列未排空)"
                emit("  {:<36} GATE-FAIL".format(name))
                continue
            try:
                r = call(client, name, PER_CALL)
            except Exception as exc:
                summary[name] = "TIMEOUT(%s)" % type(exc).__name__
                emit("  {:<36} TIMEOUT      自身慢/原生依赖".format(name))
                continue
            if r.get("ok"):
                d = json.dumps(r.get("data"), default=str)
                summary[name] = "PASS(%dB)" % len(d)
                emit("  {:<36} PASS         {}B".format(name, len(d)))
            else:
                err = str(r.get("error") or "")[:70]
                summary[name] = "REACHABLE(%s)" % err
                emit("  {:<36} REACHABLE    {}".format(name, err.encode("gbk", "replace").decode("gbk")))
    finally:
        client.stop()
    emit("---- 汇总 ----")
    n_pass = sum(1 for v in summary.values() if v.startswith("PASS"))
    n_reach = sum(1 for v in summary.values() if v.startswith("REACHABLE"))
    n_slow = sum(1 for v in summary.values() if v.startswith("TIMEOUT"))
    n_gate = sum(1 for v in summary.values() if v.startswith("GATE"))
    emit("PASS=%d REACHABLE=%d TIMEOUT(原生慢族)=%d GATE_FAIL=%d / %d"
         % (n_pass, n_reach, n_slow, n_gate, len(methods)))
    with open(REPORT + ".json", "w", encoding="utf-8") as jf:
        json.dump(summary, jf, ensure_ascii=False, indent=1)
    emit("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())

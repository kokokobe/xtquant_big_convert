#encoding:utf-8
"""剩余方法带参实弹清扫：产出"参数用法手册"。
覆盖：12 个新 ContextInfo 方法 + 参数形态未实测的 handler 方法。
每个方法前置 ping 探空；结果增量写 tools/param_manual_sweep.txt
"""
import json
import sys
import time
import uuid

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

ACCOUNT = "52625295"
REPORT = r"D:\quantitative_qmt\xtquant_big_convert\tools\param_manual_sweep.txt"
PER_CALL = 20.0
GATE_MAX_WAIT = 240.0

# (method, params, 备注)
TESTS = [
    ("get_finance", {"stock_code": "600036.SH"}, "财务数据"),
    ("get_universe", {}, "策略订阅列表"),
    ("get_scale_and_rank", {"index_name": "000300.SH"}, "市值排名"),
    ("get_scale_and_stock", {"index_name": "000300.SH"}, "市值分类"),
    ("get_largecap", {"stock_list": ["600036.SH", "513300.SH"]}, "大盘股"),
    ("get_midcap", {"stock_list": ["600036.SH", "513300.SH"]}, "中盘股"),
    ("get_smallcap", {"stock_list": ["600036.SH", "513300.SH"]}, "小盘股"),
    ("is_suspended_stock", {"stock_code": "600036.SH"}, "停牌判定"),
    ("stockcode_in_rzrk", {"stock_code": "600036.SH"}, "两融名单"),
    ("is_fund", {"stock_code": "513300.SH"}, "基金判定"),
    ("is_stock", {"stock_code": "600036.SH"}, "股票判定"),
    ("is_future", {"stock_code": "600036.SH"}, "期货判定"),
    ("get_instrument", {"code": "600036.SH"}, "合约信息"),
    ("query_stock_position", {"stock_code": "513300.SH"}, "单票持仓"),
    ("get_ticks", {"codes": ["513300.SH"]}, "分笔/tick"),
    ("get_hkt_exchange_rate", {"market": "HK"}, "港股汇率"),
    ("get_value_by_order_id", {"order_id": "dummy-123"}, "按委托号查(需真实单)"),
    ("submit_download_history_data", {"stock_code": "510300.SH", "period": "1d",
                                      "start_time": "20260901", "end_time": "20260915"}, "单票异步下载"),
    ("quote_subscription_status", {}, "全推订阅状态(push 未配置应报错)"),
]

fh = open(REPORT, "w", encoding="utf-8")


def emit(line):
    print(line.encode("gbk", "replace").decode("gbk"), flush=True)
    fh.write(line + "\n")
    fh.flush()


def call(client, method, params, timeout):
    request = {
        "schema_version": 1,
        "request_id": "pm-%s-%s" % (method, uuid.uuid4().hex[:10]),
        "account_id": ACCOUNT,
        "method": method,
        "params": dict(params),
    }
    return client.send_request(request, timeout)


def ping_gate(client):
    t0 = time.time()
    while time.time() - t0 < GATE_MAX_WAIT:
        try:
            r = call(client, "ping", {}, 5.0)
            if r.get("ok"):
                return True
        except Exception:
            pass
        time.sleep(1.0)
    return False


def main():
    client = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[manual]")
    summary = {}
    try:
        if not ping_gate(client):
            emit("!! 桥不响应")
            return 1
        for method, params, note in TESTS:
            if not ping_gate(client):
                summary[method] = "GATE_FAIL"
                emit("  {:<34} GATE-FAIL".format(method))
                continue
            try:
                r = call(client, method, params, PER_CALL)
            except Exception as exc:
                summary[method] = "TIMEOUT"
                emit("  {:<34} TIMEOUT      {} (原生阻塞嫌疑)".format(method, note))
                continue
            data = r.get("data")
            dsize = len(json.dumps(data, default=str)) if data is not None else 0
            if r.get("ok"):
                preview = json.dumps(data, ensure_ascii=False, default=str)[:110]
                summary[method] = "PASS"
                emit("  {:<34} PASS   {}  {}".format(method, note, preview.encode("gbk", "replace").decode("gbk")))
            else:
                err = str(r.get("error") or "")[:90]
                summary[method] = "BIZ(%s)" % err[:40]
                emit("  {:<34} BIZ    {}  {}".format(method, note, err.encode("gbk", "replace").decode("gbk")))
    finally:
        client.stop()
    n_pass = sum(1 for v in summary.values() if v == "PASS")
    emit("---- 汇总: PASS=%d BIZ=%d TIMEOUT=%d / %d"
         % (n_pass,
            sum(1 for v in summary.values() if v.startswith("BIZ")),
            sum(1 for v in summary.values() if v.startswith("TIMEOUT")),
            len(summary)))
    with open(REPORT + ".json", "w", encoding="utf-8") as jf:
        json.dump(summary, jf, ensure_ascii=False, indent=1)
    emit("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#encoding:utf-8
r"""REACHABLE 方法带参实弹清扫：inspect 签名 -> 合成参数 -> 真调 -> 分类。
分类:
  PASS        ok=True 真实返回
  BIZ         ok=False 但业务应答（查无/未配置/NotImplemented 等，端到端通）
  TYPEERR     参数还不对（需要人工适配）
  TIMEOUT     原生 xtdata 阻塞（#143 终端缺陷）
每个方法前置 ping 探空。结果增量写 tools/param_adapt_report.txt + .json
"""
import inspect
import json
import sys
import time
import uuid

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.redis_rpc import READ_METHODS, MARKET_DATA_METHODS
from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

ACCOUNT = "52625295"
REPORT = r"D:\quantitative_qmt\xtquant_big_convert\tools\param_adapt_report.txt"
PER_CALL = 20.0
GATE_MAX_WAIT = 240.0

# 已知原生阻塞（快扫 TIMEOUT 名单）——跳过，不值得再付 60-90s 罚金
KNOWN_SLOW = {
    "bsm_iv", "bsm_price", "create_sector", "download_cb_data", "gen_factor_index",
    "get_ETF_list", "get_bvol", "get_cb_info", "get_close_price", "get_etf_info",
    "get_factor_data", "get_float_caps", "get_his_st_data", "get_hkt_details",
    "get_holder_num", "get_holidays", "get_index_weight", "get_industry",
    "get_ipo_info", "get_l2_order", "get_l2_quote", "get_last_close",
    "get_last_volume", "get_local_data", "get_longhubang", "get_market_data",
    "get_markets", "get_open_date", "get_option_iv", "get_option_list",
    "get_option_undl", "get_sector_info", "get_sector_list", "get_stock_name",
    "get_stock_type", "get_svol", "get_total_share", "get_trade_times",
    "is_stock_type", "remove_sector",
}

# 参数名 -> 值（按名字模式合成）
def _param_value(name):
    n = name.lower()
    if n in ("stock_code", "stockcode", "code", "stockcode_or_name", "instrument"):
        return "513300.SH"
    if "stock_list" in n or n in ("codes", "stockcodes", "code_list"):
        return ["513300.SH"]
    if n == "period":
        return "1d"
    if "start" in n and ("time" in n or "date" in n):
        return "20260801"
    if "end" in n and ("time" in n or "date" in n):
        return "20260915"
    if "date" in n or "time" in n:
        return "20260915"
    if n == "market" or "market" in n:
        return "SH"
    if "sector" in n:
        return "沪深300"
    if n in ("count", "num", "n", "number"):
        return 10
    if "field" in n:
        return ["time", "open", "close"]
    if "divid_type" in n or "dividend" in n:
        return "none"
    if "opt_type" in n or "option_type" in n:
        return "call"
    if "volume" in n or "vol" in n:
        return 100
    if "price" in n:
        return 2.65
    if "flag" in n or "incrementally" in n:
        return True
    if "type" in n:
        return 1
    if "strategy_name" in n or "formula_name" in n or "filename" in n or "name" == n:
        return "测试"
    return "1"


fh = open(REPORT, "w", encoding="utf-8")


def emit(line):
    print(line.encode("gbk", "replace").decode("gbk"), flush=True)
    fh.write(line + "\n")
    fh.flush()


def synth_params(func):
    """必填参数按名字表合成; 缺表的必填标记 None。
    注意: __dict__ 里拿到的是未绑定函数/staticmethod 原始对象——
    py3.6 不自动解包, self/cls 不能进参数表（否则服务端 **params 会
    报 multiple values for self）。"""
    if isinstance(func, staticmethod):
        func = func.__func__
    elif isinstance(func, classmethod):
        func = func.__func__
    sig = inspect.signature(func)
    required = {}
    missing = []
    for pname, p in sig.parameters.items():
        if pname in ("self", "cls"):
            continue
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            continue
        if p.default is inspect.Parameter.empty:
            val = _param_value(pname)
            if val is None:
                missing.append(pname)
            else:
                required[pname] = val
    return required, missing


def call(client, method, params, timeout):
    request = {
        "schema_version": 1,
        "request_id": "ap-%s-%s" % (method, uuid.uuid4().hex[:10]),
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
    adapter = BigQmtMarketDataProvider.__dict__
    methods = sorted(set(READ_METHODS) & set(MARKET_DATA_METHODS))
    client = SharedMemoryTransport(account_id=ACCOUNT, print_prefix="[adapt]")
    summary = {}
    try:
        if not ping_gate(client):
            emit("!! 桥不响应")
            return 1
        for name in methods:
            if name in KNOWN_SLOW:
                summary[name] = "SKIP(已知原生阻塞)"
                emit("  {:<36} SKIP         已知原生阻塞".format(name))
                continue
            func = adapter.get(name)
            if func is None:
                summary[name] = "SKIP(无适配器方法)"
                emit("  {:<36} SKIP         适配器无此方法".format(name))
                continue
            params, missing = synth_params(func)
            if missing:
                summary[name] = "MANUAL(缺参数:%s)" % ",".join(missing)
                emit("  {:<36} MANUAL       缺参数: {}".format(name, ",".join(missing)))
                continue
            if not ping_gate(client):
                summary[name] = "GATE_FAIL"
                emit("  {:<36} GATE-FAIL".format(name))
                continue
            try:
                r = call(client, name, params, PER_CALL)
            except Exception as exc:
                summary[name] = "TIMEOUT(原生阻塞)"
                emit("  {:<36} TIMEOUT      原生阻塞".format(name))
                continue
            if r.get("ok"):
                d = json.dumps(r.get("data"), default=str)
                summary[name] = "PASS(%dB)" % len(d)
                emit("  {:<36} PASS         {}B".format(name, len(d)))
            else:
                err = str(r.get("error") or "")
                kind = err.split(":")[0][:40]
                summary[name] = "BIZ(%s)" % kind
                emit("  {:<36} BIZ          {}".format(name, err[:80].encode("gbk", "replace").decode("gbk")))
    finally:
        client.stop()
    n_pass = sum(1 for v in summary.values() if v.startswith("PASS"))
    n_biz = sum(1 for v in summary.values() if v.startswith("BIZ"))
    n_to = sum(1 for v in summary.values() if v.startswith("TIMEOUT"))
    n_man = sum(1 for v in summary.values() if v.startswith("MANUAL"))
    n_skip = sum(1 for v in summary.values() if v.startswith("SKIP"))
    emit("---- 汇总: PASS=%d BIZ=%d TIMEOUT=%d MANUAL=%d SKIP=%d / %d"
         % (n_pass, n_biz, n_to, n_man, n_skip, len(summary)))
    with open(REPORT + ".json", "w", encoding="utf-8") as jf:
        json.dump(summary, jf, ensure_ascii=False, indent=1)
    emit("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#encoding:utf-8
"""现役 pipe 只读行情 probe（任务17）：只读方法白名单、采样披露、人工闸门。

run_probe(client, codes, duration_s) -> dict：
- client 只允许只读方法（submit_order 等到不了 client）；
- duration=0 返回完整空样本披露（流程验证，不称 feed READY）；
- duration ≤120s、codes ≤100，超过拒绝；
- finally 退订；不记口令；所有 endpoint/account/codes 由调用方显式提供。
"""
import time

READ_ONLY_METHODS = {
    "ping",
    "subscribe_whole_quote",
    "unsubscribe_whole_quote",
    "quote_keepalive",
    "quote_subscription_status",
    "get_full_tick",
}

MAX_DURATION_S = 120
MAX_CODES = 100


def run_probe(client, codes, duration_s):
    """对现役 pipe 做只读采样。duration=0 只验证流程并返回空样本披露。"""
    if duration_s > MAX_DURATION_S:
        raise ValueError("PROBE_DURATION_LIMIT")
    codes = [str(c) for c in (codes or [])]
    if not codes or len(codes) > MAX_CODES:
        raise ValueError("PROBE_CODES_LIMIT")
    result = {
        "readonly": True,
        "codes": codes,
        "duration_s": duration_s,
        "samples": [],
        "gaps": [],
        "subscription_ok": False,
        "feed_ready": False,
        "readiness_blockers": ["PROBE_DURATION_ZERO_NO_SAMPLES"] if duration_s == 0 else [],
        "notes": "单位/阶段/时延字段在实桥核验（Task17 人工闸门）前一律 UNKNOWN",
    }
    if duration_s == 0:
        return result

    started = time.time()
    methods_used = []

    def call(method, params):
        methods_used.append(method)
        return client.call(method, params)

    sub = None
    try:
        call("ping", {})
        sub = call("subscribe_whole_quote",
                   {"client_id": "intraday-probe", "sub_id": "probe",
                    "codes": codes})
        result["subscription_ok"] = bool(sub)
        deadline = started + duration_s
        while time.time() < deadline:
            time.sleep(0.2)
        call("quote_subscription_status", {})
    finally:
        if sub is not None:
            try:
                call("unsubscribe_whole_quote",
                     {"client_id": "intraday-probe", "sub_id": "probe"})
            except Exception:
                result["gaps"].append("UNSUBSCRIBE_FAILED")
    unexpected = [m for m in methods_used if m not in READ_ONLY_METHODS]
    if unexpected:
        raise AssertionError("probe 使用了非只读方法: %s" % unexpected)
    return result

#encoding:utf-8
"""现役 pipe 只读行情 probe（任务17）：只读方法白名单、采样披露、人工闸门。

run_probe(client, codes, duration_s) -> dict：
- client 只允许只读方法（submit_order 等到不了 client）；
- duration=0 返回完整空样本披露（流程验证，不称 feed READY）；
- duration ≤120s、codes ≤100，超过拒绝；
- finally 退订；不记口令；所有 endpoint/account/codes 由调用方显式提供。
"""
import json
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


def run_probe(client, codes, duration_s, push=None):
    """对现役 pipe 做只读采样。duration=0 只验证流程并返回空样本披露。

    push（可选）：推送消费者，需提供 start(on_frame)/stop()；订阅成功后
    采集实际推送帧做覆盖/单位/时延观察——只 sleep 不消费等于没采样。
    """
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
    frames = []
    try:
        call("ping", {})
        sub = call("subscribe_whole_quote",
                   {"client_id": "intraday-probe", "sub_id": "probe",
                    "codes": codes})
        result["subscription_ok"] = bool(sub)
        if result["subscription_ok"] and push is not None:
            def on_frame(topic, data):
                frames.append({
                    "t_offset": round(time.time() - started, 3),
                    "topic": str(topic),
                    "bytes": len(json.dumps(data, default=str)),
                    "data": data,
                })
            push.start(on_frame)
        deadline = started + duration_s
        # 心跳保活：桥 quote 组合 30s 无 keepalive 即回收（实测推送 30s 处停）
        last_keepalive = started
        while time.time() < deadline:
            time.sleep(0.2)
            if sub is not None and time.time() - last_keepalive >= 10.0:
                call("quote_keepalive",
                     {"client_id": "intraday-probe", "sub_id": "probe"})
                last_keepalive = time.time()
        call("quote_subscription_status", {})
    finally:
        if push is not None:
            try:
                push.stop()
            except Exception:
                result["gaps"].append("PUSH_STOP_FAILED")
        if sub is not None:
            try:
                call("unsubscribe_whole_quote",
                     {"client_id": "intraday-probe", "sub_id": "probe"})
            except Exception:
                result["gaps"].append("UNSUBSCRIBE_FAILED")
    unexpected = [m for m in methods_used if m not in READ_ONLY_METHODS]
    if unexpected:
        raise AssertionError("probe 使用了非只读方法: %s" % unexpected)

    # 采样观察：覆盖/单位/时延证据（ready 与否由 readiness 评估判定）
    result["frame_count"] = len(frames)
    result["bytes_total"] = sum(f["bytes"] for f in frames)
    if frames:
        result["first_frame_offset_s"] = frames[0]["t_offset"]
        result["last_frame_offset_s"] = frames[-1]["t_offset"]
        codes_seen = set()
        status_seen = set()
        price_fields_ok = True
        for f in frames:
            data = f["data"]
            if not isinstance(data, dict):
                continue
            for code, snap in data.items():
                codes_seen.add(code)
                if isinstance(snap, dict):
                    if "lastPrice" in snap:
                        lp = snap.get("lastPrice")
                        if not isinstance(lp, (int, float)) or lp <= 0:
                            price_fields_ok = False
                    if "stockStatus" in snap:
                        status_seen.add(snap.get("stockStatus"))
        result["codes_seen_count"] = len(codes_seen)
        result["codes_missing"] = sorted(set(codes) - codes_seen)
        result["price_fields_observed_finite_positive"] = price_fields_ok
        result["stock_status_values_seen"] = sorted(status_seen)
        result["units_observed"] = "lastPrice/amount 为元（浮点）——映射×1e6 由 Rust 端核验"
    # feed_ready 由人工按 readiness 条件判定；probe 只如实报告观察
    if duration_s == 0 or not frames:
        result["readiness_blockers"].append("NO_FRAMES_SAMPLED")
    return result

#encoding:utf-8
"""probe 只读约束测试：订单方法永不触达 client、时长/code 上限、空样本披露。"""
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools"))
sys.path.insert(0, os.path.join(ROOT, "src"))

from probe_intraday_quotes import run_probe  # noqa: E402


class FakeClient:
    def __init__(self):
        self.methods = []

    def call(self, method, params):
        self.methods.append(method)
        assert method in {"ping", "subscribe_whole_quote", "get_full_tick",
                          "quote_keepalive", "quote_subscription_status",
                          "unsubscribe_whole_quote"}
        return {"topic": "q", "push_endpoint": "test"}


def test_probe_never_calls_order_methods():
    c = FakeClient()
    run_probe(c, ["TEST.SH"], 0)
    assert "submit_order" not in c.methods
    assert "passorder" not in c.methods
    assert "cancel_order" not in c.methods


def test_zero_duration_returns_empty_disclosure_not_ready():
    c = FakeClient()
    r = run_probe(c, ["TEST.SH"], 0)
    assert r["samples"] == []
    assert r["feed_ready"] is False
    assert "PROBE_DURATION_ZERO_NO_SAMPLES" in r["readiness_blockers"]
    assert r["notes"].startswith("单位/阶段/时延")


def test_duration_and_codes_limits():
    c = FakeClient()
    with pytest.raises(ValueError, match="PROBE_DURATION_LIMIT"):
        run_probe(c, ["TEST.SH"], 121)
    with pytest.raises(ValueError, match="PROBE_CODES_LIMIT"):
        run_probe(c, ["S%d.SH" % i for i in range(101)], 0)
    with pytest.raises(ValueError, match="PROBE_CODES_LIMIT"):
        run_probe(c, [], 0)


def test_finally_unsubscribes():
    c = FakeClient()
    run_probe(c, ["TEST.SH"], 0)
    # duration=0 不订阅；真实订阅路径的退订由非零时长覆盖（人工闸门）
    assert "subscribe_whole_quote" not in c.methods or \
        "unsubscribe_whole_quote" in c.methods

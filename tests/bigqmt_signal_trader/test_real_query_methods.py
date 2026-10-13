#coding:gbk
"""real_query_orders / real_query_deals 只读方法归一化契约测试（Task 11）。

消费端是 qplatform rust_engine 的 BigQmtRealGateway：应答 data 必须是裸 list，
行字段名与类型和 rust_engine/src/oms/bigqmt_real_gateway.rs 顶部形状表逐字
对齐（Rust 按 as_u64 / as_f64 解析，缺字段或类型错会被静默跳过）。

不依赖真 QMT：注入假 order_gateway（真实路径里它包 get_trade_detail_data）
返回固定快照行，走 process_request 全链路断言应答形状。
"""
import json
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.models import OrderSnapshot, TradeSnapshot  # noqa: E402
from bigqmt_signal_trader.redis_rpc import (  # noqa: E402
    LISTENER_DEFERRED_METHODS,
    ORDER_METHODS,
    READ_METHODS,
    BigQmtRpcHandlers,
    RedisPubSubRpcService,
    _real_order_row,
)
from test_redis_rpc import FakeMarketData, FakePositionProvider, FakeRedis  # noqa: E402


class FakeRealQueryGateway(object):
    """只实现 real_query_* 需要的两个查询入口，返回固定快照行。

    真实实现里这两个入口落在 BigQmtOrderGateway.query_orders(_strict) /
    query_trades(_strict) 上，内部包 get_trade_detail_data —— 线程策略由
    LISTENER_DEFERRED_METHODS 保证，测试里不涉及。
    """

    def __init__(self, orders=None, trades=None):
        self.orders = list(orders or [])
        self.trades = list(trades or [])
        self.order_calls = []
        self.trade_calls = []

    def query_orders(self, account_id, strategy_name):
        self.order_calls.append((account_id, strategy_name))
        return list(self.orders)

    def query_trades(self, account_id, strategy_name):
        self.trade_calls.append((account_id, strategy_name))
        return list(self.trades)


ORDER_ROWS = [
    OrderSnapshot(
        order_sys_id="sys123",
        user_order_id="bq:abc:def001",
        stock_code="600000.SH",
        action="BUY",
        volume=1000,
        traded_volume=400,
        status="56",
        price=12.34,
        order_time=1728800000,
    ),
    # 缺值行：price/order_time 缺省、成交 0 —— 字段必须仍在且类型恒定。
    OrderSnapshot(
        order_sys_id="sys456",
        user_order_id="hand-order",
        stock_code="000001.SZ",
        action="SELL",
        volume=500,
        traded_volume=0,
        status="50",
    ),
]

DEAL_ROWS = [
    TradeSnapshot(
        trade_id="deal-1",
        order_sys_id="sys123",
        stock_code="600000.SH",
        action="BUY",
        volume=400,
        price=12.34,
        traded_at="20261013093001",
        user_order_id="bq:abc:def001",
    ),
]


def _service(order_gateway):
    redis_client = FakeRedis()
    handlers = BigQmtRpcHandlers(
        account_id="acct",
        market_data=FakeMarketData(),
        position_provider=FakePositionProvider(),
        order_gateway=order_gateway,
        # 假网关没有结算语义，别让查询走等结算路径。
        order_settle_timeout_seconds=0.0,
    )
    return redis_client, RedisPubSubRpcService(redis_client, handlers, account_id="acct")


def _rpc(redis_client, service, method, params):
    service.process_request({
        "request_id": "r-real-1",
        "account_id": "acct",
        "method": method,
        "params": params,
    })
    raw = redis_client.kv.get("bigqmt:rpc:resp:acct:r-real-1")
    if not raw:
        raise AssertionError("no reply written for %s" % method)
    return json.loads(raw)


ORDER_ROW_KEYS = {
    "order_sys_id", "user_order_id", "stock_code", "action",
    "order_volume", "traded_volume", "price", "status", "order_time"}

DEAL_ROW_KEYS = {
    "trade_id", "order_sys_id", "user_order_id", "stock_code",
    "action", "volume", "price", "traded_at"}


class RealQueryOrdersTest(unittest.TestCase):
    def test_normalizes_order_rows_to_contract_shape(self):
        gateway = FakeRealQueryGateway(orders=ORDER_ROWS)
        redis_client, service = _service(gateway)

        reply = _rpc(redis_client, service, "real_query_orders", {})

        self.assertTrue(reply["ok"], reply["error"])
        self.assertIsInstance(reply["data"], list)      # 裸 list，不是包装对象
        self.assertEqual(len(reply["data"]), 2)
        row = reply["data"][0]
        self.assertEqual(set(row.keys()), ORDER_ROW_KEYS)
        self.assertEqual(row["order_sys_id"], "sys123")
        self.assertEqual(row["user_order_id"], "bq:abc:def001")   # remark 原文
        self.assertEqual(row["stock_code"], "600000.SH")
        self.assertEqual(row["action"], "BUY")
        self.assertEqual(row["order_volume"], 1000)
        self.assertEqual(row["traded_volume"], 400)
        self.assertEqual(row["price"], 12.34)
        # QMT 状态 int 原值透传（快照 "56" -> 56）；int 映射归属 Rust 侧
        # bigqmt_real_gateway.rs 的 state_for_qmt_status。
        self.assertEqual(row["status"], 56)
        self.assertEqual(row["order_time"], 1728800000)

    def test_row_field_types_are_constant_on_sparse_rows(self):
        gateway = FakeRealQueryGateway(orders=[ORDER_ROWS[1]])
        redis_client, service = _service(gateway)

        reply = _rpc(redis_client, service, "real_query_orders", {})

        row = reply["data"][0]
        for key in ("order_volume", "traded_volume", "status", "order_time"):
            self.assertIsInstance(row[key], int, key)
        self.assertIsInstance(row["price"], float)
        self.assertEqual(row["price"], 0.0)
        self.assertEqual(row["status"], 50)
        self.assertEqual(row["order_time"], 0)

    def test_user_order_id_filter_is_exact_match(self):
        gateway = FakeRealQueryGateway(orders=ORDER_ROWS)
        redis_client, service = _service(gateway)

        reply = _rpc(redis_client, service, "real_query_orders",
                     {"user_order_id": "bq:abc:def001"})

        self.assertEqual([row["user_order_id"] for row in reply["data"]],
                         ["bq:abc:def001"])

    def test_unknown_user_order_id_is_an_empty_list_not_an_error(self):
        gateway = FakeRealQueryGateway(orders=ORDER_ROWS)
        redis_client, service = _service(gateway)

        reply = _rpc(redis_client, service, "real_query_orders",
                     {"user_order_id": "no-such-coid"})

        self.assertTrue(reply["ok"], reply["error"])
        self.assertEqual(reply["data"], [])

    def test_unparseable_status_normalizes_to_zero(self):
        snapshot = OrderSnapshot(
            order_sys_id="s", user_order_id="u", stock_code="600000.SH",
            action="BUY", volume=1, traded_volume=0, status="", price=1.0)

        row = _real_order_row(snapshot)

        self.assertEqual(row["status"], 0)
        self.assertIsInstance(row["status"], int)


class RealQueryDealsTest(unittest.TestCase):
    def test_normalizes_deal_rows_to_contract_shape(self):
        gateway = FakeRealQueryGateway(trades=DEAL_ROWS)
        redis_client, service = _service(gateway)

        reply = _rpc(redis_client, service, "real_query_deals", {})

        self.assertTrue(reply["ok"], reply["error"])
        self.assertIsInstance(reply["data"], list)      # 裸 list
        self.assertEqual(len(reply["data"]), 1)
        row = reply["data"][0]
        self.assertEqual(set(row.keys()), DEAL_ROW_KEYS)
        self.assertEqual(row["trade_id"], "deal-1")     # Rust 成交幂等键
        self.assertEqual(row["order_sys_id"], "sys123")
        self.assertEqual(row["user_order_id"], "bq:abc:def001")
        self.assertEqual(row["stock_code"], "600000.SH")
        self.assertEqual(row["action"], "BUY")
        self.assertIsInstance(row["volume"], int)
        self.assertEqual(row["volume"], 400)
        self.assertIsInstance(row["price"], float)
        self.assertEqual(row["price"], 12.34)
        self.assertEqual(row["traded_at"], "20261013093001")

    def test_deals_with_no_trades_is_an_empty_list(self):
        gateway = FakeRealQueryGateway()
        redis_client, service = _service(gateway)

        reply = _rpc(redis_client, service, "real_query_deals", {})

        self.assertTrue(reply["ok"], reply["error"])
        self.assertEqual(reply["data"], [])


class RealQueryMethodRegistrationTest(unittest.TestCase):
    def test_real_query_methods_are_read_methods(self):
        self.assertTrue({"real_query_orders", "real_query_deals"} <= READ_METHODS)

    def test_real_query_methods_are_not_order_methods(self):
        # 只读方法：不进 ORDER_METHODS，不需要 allow_order_methods 门控。
        self.assertFalse({"real_query_orders", "real_query_deals"} & ORDER_METHODS)

    def test_real_query_methods_follow_the_query_thread_policy(self):
        # get_trade_detail_data 只在 QMT 主线程有数据：必须与 query_orders /
        # query_trades 同线程路径（LISTENER_DEFERRED -> adjust drain 主线程）。
        self.assertTrue(
            {"real_query_orders", "real_query_deals"} <= LISTENER_DEFERRED_METHODS)

    def test_real_query_orders_requires_order_gateway(self):
        handlers = BigQmtRpcHandlers(
            account_id="acct",
            market_data=None,
            position_provider=None,
            order_gateway=None,
        )
        with self.assertRaises(RuntimeError):
            handlers.handle("real_query_orders", {})
        with self.assertRaises(RuntimeError):
            handlers.handle("real_query_deals", {})


if __name__ == "__main__":
    unittest.main()

#coding:gbk
import json
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import (  # noqa: E402
    ORDER_METHODS,
    OUTCOME_CLAIMED_ORDER_METHODS,
    BigQmtRpcHandlers,
    RedisPubSubRpcService,
)
from test_redis_rpc import FakeMarketData, FakePositionProvider, FakeRedis  # noqa: E402


class PaperOrdersTest(unittest.TestCase):
    def setUp(self):
        self.handlers = BigQmtRpcHandlers(
            account_id="paper",
            market_data=None,
            position_provider=None,
            order_gateway=None,
        )
        self.payload = {
            "client_order_id": "c1",
            "code": "600000.SH",
            "side": "BUY",
            "quantity_shares": 100,
            "limit_price_cents": 1000,
        }

    def test_submit_is_idempotent(self):
        first = self.handlers.handle("paper_submit_order", self.payload)
        second = self.handlers.handle("paper_submit_order", self.payload)
        self.assertEqual(first["broker_order_id"], second["broker_order_id"])
        self.assertEqual(first["event_kind"], "paper_order_ack")

    def test_fill_updates_cumulative_and_rejects_overflow(self):
        self.handlers.handle("paper_submit_order", self.payload)
        first = self.handlers.handle(
            "paper_fill_order", {"client_order_id": "c1", "quantity_shares": 40}
        )
        self.assertEqual(first["cumulative_filled_shares"], 40)
        second = self.handlers.handle(
            "paper_fill_order", {"client_order_id": "c1", "quantity_shares": 60}
        )
        self.assertEqual(second["cumulative_filled_shares"], 100)
        with self.assertRaises(ValueError):
            self.handlers.handle(
                "paper_fill_order", {"client_order_id": "c1", "quantity_shares": 1}
            )

    def test_cancel_and_account_snapshot_do_not_call_real_orders(self):
        self.handlers.handle("paper_submit_order", self.payload)
        cancelled = self.handlers.handle("paper_cancel_order", {"client_order_id": "c1"})
        self.assertEqual(cancelled["event_kind"], "paper_cancel_ack")
        snapshot = self.handlers.handle("paper_account_snapshot", {})
        self.assertEqual(snapshot["orders_enabled"], False)
        self.assertEqual(snapshot["paper_orders_enabled"], True)


class PaperRequestOutcomeTest(unittest.TestCase):
    """paper submit/cancel are claimed like order writes, so a caller that
    timed out can ask get_request_outcome what became of the request (#303
    accounting, end to end through process_request)."""

    def setUp(self):
        redis_client = FakeRedis()
        handlers = BigQmtRpcHandlers(
            account_id="paper",
            market_data=FakeMarketData(),
            position_provider=FakePositionProvider(),
            order_gateway=None,
        )
        self.redis_client = redis_client
        self.service = RedisPubSubRpcService(redis_client, handlers, account_id="paper")
        self.payload = {
            "client_order_id": "c1",
            "code": "600000.SH",
            "side": "BUY",
            "quantity_shares": 100,
            "limit_price_cents": 1000,
        }

    def _reply(self, request_id):
        raw = self.redis_client.kv.get("bigqmt:rpc:resp:paper:%s" % request_id)
        return json.loads(raw) if raw else None

    def _ask_outcome(self, ask_id, request_id):
        self.service.process_request({
            "request_id": ask_id, "account_id": "paper",
            "method": "get_request_outcome", "params": {"request_id": request_id}})
        return self._reply(ask_id)

    def test_paper_submit_outcome_recorded(self):
        self.service.process_request({
            "request_id": "p1", "account_id": "paper",
            "method": "paper_submit_order", "params": dict(self.payload)})

        reply = self._ask_outcome("q1", "p1")

        self.assertTrue(reply["ok"], reply["error"])
        self.assertNotEqual(reply["data"]["state"], "unknown")
        self.assertEqual("settled", reply["data"]["state"])
        self.assertTrue(reply["data"]["response"]["ok"])

    def test_paper_cancel_outcome_recorded(self):
        self.service.process_request({
            "request_id": "p2", "account_id": "paper",
            "method": "paper_submit_order", "params": dict(self.payload)})
        self.service.process_request({
            "request_id": "p3", "account_id": "paper",
            "method": "paper_cancel_order", "params": {"client_order_id": "c1"}})

        reply = self._ask_outcome("q2", "p3")

        self.assertTrue(reply["ok"], reply["error"])
        self.assertNotEqual(reply["data"]["state"], "unknown")
        self.assertEqual("settled", reply["data"]["state"])

    def test_paper_submit_still_not_a_real_order_method(self):
        # Boundary: paper methods live in the outcome-claim set only. They
        # must never enter ORDER_METHODS, the real order permission set.
        self.assertNotIn("paper_submit_order", ORDER_METHODS)
        self.assertNotIn("paper_cancel_order", ORDER_METHODS)
        self.assertIn("paper_submit_order", OUTCOME_CLAIMED_ORDER_METHODS)
        self.assertIn("paper_cancel_order", OUTCOME_CLAIMED_ORDER_METHODS)


if __name__ == "__main__":
    unittest.main()

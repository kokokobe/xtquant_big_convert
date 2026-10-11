#coding:gbk
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers


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


if __name__ == "__main__":
    unittest.main()

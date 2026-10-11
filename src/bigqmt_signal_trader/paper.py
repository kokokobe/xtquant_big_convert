#coding:gbk
"""Deterministic paper order book for D2.1."""

import time
import uuid


def _now_ns():
    return int(time.time() * 1000000000)


class PaperOrderBook(object):
    def __init__(self):
        self.orders = {}
        self.fills = []
        self._order_sequence = 0

    def submit_order(self, payload):
        payload = payload or {}
        client_order_id = str(payload.get("client_order_id") or "").strip()
        if not client_order_id:
            raise ValueError("client_order_id is required")
        code = str(payload.get("code") or "").strip()
        side = str(payload.get("side") or "").upper()
        quantity = int(payload.get("quantity_shares") or 0)
        price = int(payload.get("limit_price_cents") or 0)
        if not code:
            raise ValueError("code is required")
        if side not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")
        if quantity <= 0:
            raise ValueError("quantity_shares must be positive")
        if price <= 0:
            raise ValueError("limit_price_cents must be positive")

        existing = self.orders.get(client_order_id)
        if existing is not None:
            if (
                existing["code"] != code
                or existing["side"] != side
                or existing["quantity_shares"] != quantity
                or existing["limit_price_cents"] != price
            ):
                raise ValueError("client_order_id reused with different payload")
            return self.order_event(existing, "paper_order_ack")

        self._order_sequence += 1
        order = {
            "client_order_id": client_order_id,
            "broker_order_id": "paper_%s" % uuid.uuid4().hex,
            "code": code,
            "side": side,
            "quantity_shares": quantity,
            "limit_price_cents": price,
            "cumulative_filled_shares": 0,
            "state": "ACKNOWLEDGED",
            "observed_at_ns": _now_ns(),
        }
        self.orders[client_order_id] = order
        return self.order_event(order, "paper_order_ack")

    def cancel_order(self, payload):
        payload = payload or {}
        client_order_id = str(payload.get("client_order_id") or "").strip()
        order = self.orders.get(client_order_id)
        if order is None:
            raise ValueError("order not found: %s" % client_order_id)
        if order["state"] == "FILLED":
            event = self.order_event(order, "paper_cancel_ack")
            event["reason_code"] = "CANCEL_AFTER_FULL_FILL"
            return event
        order["state"] = "CANCELLED"
        event = self.order_event(order, "paper_cancel_ack")
        event["reason_code"] = "CANCELLED"
        return event

    def fill_order(self, payload):
        payload = payload or {}
        client_order_id = str(payload.get("client_order_id") or "").strip()
        order = self.orders.get(client_order_id)
        if order is None:
            raise ValueError("order not found: %s" % client_order_id)
        if order["state"] in ("CANCELLED", "FILLED"):
            raise ValueError("order is terminal: %s" % client_order_id)
        quantity = int(payload.get("quantity_shares") or 0)
        price = int(payload.get("price_cents") or order["limit_price_cents"])
        if quantity <= 0:
            raise ValueError("quantity_shares must be positive")
        if order["cumulative_filled_shares"] + quantity > order["quantity_shares"]:
            raise ValueError("fill exceeds order quantity")
        order["cumulative_filled_shares"] += quantity
        if order["cumulative_filled_shares"] == order["quantity_shares"]:
            order["state"] = "FILLED"
        fill = {
            "fill_id": "paper_fill_%s" % uuid.uuid4().hex,
            "order": order,
            "quantity_shares": quantity,
            "price_cents": price,
            "cumulative_filled_shares": order["cumulative_filled_shares"],
            "observed_at_ns": _now_ns(),
        }
        self.fills.append(fill)
        return {
            "event_kind": "paper_fill",
            "client_order_id": client_order_id,
            "broker_order_id": order["broker_order_id"],
            "broker_fill_id": fill["fill_id"],
            "code": order["code"],
            "side": order["side"],
            "quantity_shares": quantity,
            "price_cents": price,
            "cumulative_filled_shares": order["cumulative_filled_shares"],
            "observed_at_ns": fill["observed_at_ns"],
        }

    def query_order(self, payload):
        payload = payload or {}
        client_order_id = str(payload.get("client_order_id") or "").strip()
        order = self.orders.get(client_order_id)
        if order is None:
            raise ValueError("order not found: %s" % client_order_id)
        return self.order_event(order, "paper_order_status")

    def query_orders(self, payload=None):
        return [self.order_event(order, "paper_order_status") for order in self.orders.values()]

    def query_fills(self, payload=None):
        return [
            {
                "event_kind": "paper_fill",
                "client_order_id": fill["order"]["client_order_id"],
                "broker_order_id": fill["order"]["broker_order_id"],
                "broker_fill_id": fill["fill_id"],
                "quantity_shares": fill["quantity_shares"],
                "price_cents": fill["price_cents"],
                "cumulative_filled_shares": fill["cumulative_filled_shares"],
                "observed_at_ns": fill["observed_at_ns"],
            }
            for fill in self.fills
        ]

    def account_snapshot(self, payload=None):
        return {
            "account_alias": str((payload or {}).get("account_alias") or "paper"),
            "orders": len(self.orders),
            "fills": len(self.fills),
            "orders_enabled": False,
            "paper_orders_enabled": True,
        }

    def order_event(self, order, event_kind):
        return {
            "event_kind": event_kind,
            "client_order_id": order["client_order_id"],
            "broker_order_id": order["broker_order_id"],
            "code": order["code"],
            "side": order["side"],
            "quantity_shares": order["quantity_shares"],
            "limit_price_cents": order["limit_price_cents"],
            "cumulative_filled_shares": order["cumulative_filled_shares"],
            "state": order["state"],
            "observed_at_ns": _now_ns(),
        }

"""cash_in / deposit / withdraw must survive the whole chain: QMT row ->
AssetSnapshot -> RPC serialization -> client CompatObject, plus the Redis
cached-asset fallback.

Semantics: None means "the terminal did not report it" -- NOT zero. The
qplatform side must be able to tell a no-transfer day from an unreported
field, so unlike frozen_cash these never default to 0.0.
"""

import datetime as _dt
import json
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters import position_bigqmt
from bigqmt_signal_trader.adapters.position_bigqmt import BigQmtPositionProvider
from bigqmt_signal_trader.adapters.position_sync_redis import RedisPositionSyncSink
from bigqmt_signal_trader.models import AccountSnapshot, AssetSnapshot, PositionSnapshot
from bigqmt_signal_trader.redis_rpc import to_jsonable
from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader, StockAccount


class Row:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def _provider(row):
    def query(account_id, account_type, detail_type):
        return [row] if detail_type in ("ACCOUNT", "ASSET") else []

    return BigQmtPositionProvider(query)


class AssetSnapshotModelTest(unittest.TestCase):
    def test_defaults_keep_existing_positional_callers_working(self):
        snapshot = AssetSnapshot("acct", 100.0, 1000.0)

        self.assertEqual(snapshot.cash, 100.0)
        self.assertEqual(snapshot.total_asset, 1000.0)
        self.assertIsNone(snapshot.cash_in)
        self.assertIsNone(snapshot.deposit)
        self.assertIsNone(snapshot.withdraw)

    def test_carries_cash_flow_fields(self):
        snapshot = AssetSnapshot(
            "acct", 100.0, 1000.0, cash_in=8000.0, deposit=10000.0, withdraw=2000.0
        )

        self.assertEqual(snapshot.cash_in, 8000.0)
        self.assertEqual(snapshot.deposit, 10000.0)
        self.assertEqual(snapshot.withdraw, 2000.0)


if __name__ == "__main__":
    unittest.main()

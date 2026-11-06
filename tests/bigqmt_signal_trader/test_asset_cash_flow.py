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


class QmtCollectionTest(unittest.TestCase):
    def setUp(self):
        position_bigqmt._missing_field_reported.clear()

    def test_reads_cash_flow_fields_from_the_account_row(self):
        provider = _provider(Row(
            m_dAvailable=100.0, m_dBalance=1000.0,
            m_dCashIn=8000.0, m_dDeposit=10000.0, m_dWithdraw=2000.0,
        ))

        asset = provider.get_asset("acct")

        self.assertEqual(asset.cash_in, 8000.0)
        self.assertEqual(asset.deposit, 10000.0)
        self.assertEqual(asset.withdraw, 2000.0)

    def test_accepts_negative_net_outflow(self):
        provider = _provider(Row(m_dAvailable=100.0, m_dBalance=1000.0, m_dCashIn=-3000.0))

        self.assertEqual(provider.get_asset("acct").cash_in, -3000.0)

    def test_unreported_fields_stay_none(self):
        provider = _provider(Row(m_dAvailable=100.0, m_dBalance=1000.0))
        asset = provider.get_asset("acct")

        self.assertIsNone(asset.cash_in)
        self.assertIsNone(asset.deposit)
        self.assertIsNone(asset.withdraw)

    def test_missing_cash_in_reports_what_the_row_actually_has(self):
        import io
        import contextlib

        provider = _provider(Row(m_dAvailable=100.0, m_dBalance=1000.0, m_dWhateverElse=1.0))
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            provider.get_asset("acct")
        output = buffer.getvalue()

        self.assertIn("cash_in not found", output)
        self.assertIn("m_dWhateverElse", output)

    def test_missing_field_is_reported_once_not_per_call(self):
        import io
        import contextlib

        provider = _provider(Row(m_dAvailable=100.0, m_dBalance=1000.0))
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            for _ in range(5):
                provider.get_asset("acct")

        self.assertEqual(buffer.getvalue().count("cash_in not found"), 1)


if __name__ == "__main__":
    unittest.main()

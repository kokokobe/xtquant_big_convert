#encoding:utf-8
"""实桥 probe 执行器（任务17 人工闸门）：只读，≤120s，不碰任何下单方法。

用法（沙箱 python）：
  python tools/run_intraday_probe.py --account 52625295 \
      --codes qplatform/tests/artifacts/intraday_acceptance/probe_codes.json \
      --duration 60 --out <feed_contract.json 路径>

pipe_name 默认 bigqmt_rpc（桥配置未覆盖时）；endpoint/account/codes 全部显式。
"""
import argparse
import base64
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bigqmt_signal_trader.transports.pipe_push_channel import (
    PipeQuotePushChannel, pipe_events_path,
)  # noqa: E402
from bigqmt_signal_trader.transports.pipe_transport import NamedPipeTransport  # noqa: E402
from probe_intraday_quotes import run_probe  # noqa: E402


class PipePushSampler(object):
    """events 推送线消费者：probe 期间收帧（只读，不触发任何下单）。"""

    def __init__(self, account_id, pipe_name):
        self.channel = PipeQuotePushChannel(
            connect_address=pipe_events_path(pipe_name, account_id),
            print_prefix="[intraday-probe-push]")

    def start(self, on_frame):
        self.channel.start_subscriber([], on_frame)

    def stop(self):
        self.channel.stop()


class PipeRpcClient(object):
    """适配 run_probe 期望的 client.call(method, params) 接口。"""

    def __init__(self, account_id, pipe_name):
        self.transport = NamedPipeTransport(
            account_id=account_id, pipe_name=pipe_name,
            print_prefix="[intraday-probe]")
        self._seq = 0

    def call(self, method, params=None):
        self._seq += 1
        request = {
            "schema_version": 1,
            "request_id": "probe-%s-%d" % (method, self._seq),
            "account_id": self.transport.account_id,
            "method": method,
            "params": dict(params or {}),
        }
        return self.transport.send_request(request, 30.0)

    def stop(self):
        try:
            self.transport.stop()
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", required=True)
    ap.add_argument("--codes", required=True, help="probe_codes.json")
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--pipe-name", default="bigqmt_rpc")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    codes = json.load(open(args.codes, encoding="utf-8"))["codes"]
    client = PipeRpcClient(args.account, args.pipe_name)
    push = PipePushSampler(args.account, args.pipe_name)
    try:
        result = run_probe(client, codes, args.duration, push=push)
    finally:
        client.stop()
    contract = {
        "probed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "pipe_name": args.pipe_name,
        "account_id": args.account,
        "duration_s": args.duration,
        "codes_count": len(codes),
        "probe_result": result,
        "field_mapping_version": 0,
        "readiness": "MANUAL_REVIEW_PENDING",
    }
    out = os.path.abspath(args.out)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(contract, f, ensure_ascii=False, indent=1)
    print("wrote", out)
    print(json.dumps({k: result[k] for k in
                      ("subscription_ok", "feed_ready", "readiness_blockers")},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()

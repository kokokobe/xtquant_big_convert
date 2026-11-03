#encoding:utf-8
"""live paper 联调驱动：订阅+保活指定时长，供 Rust 端消费 events 管道。

只读订阅，不发单。用法：
  python tools/live_paper_driver.py --account 52625295 --duration 320
"""
import argparse
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tools"))

from bigqmt_signal_trader.transports.pipe_transport import NamedPipeTransport  # noqa: E402

codes_path = os.path.join(ROOT, "..", "qplatform", "tests", "artifacts",
                          "intraday_acceptance", "probe_codes.json")
codes = json.load(open(codes_path, encoding="utf-8"))["codes"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", default="52625295")
    ap.add_argument("--duration", type=int, default=320)
    ap.add_argument("--pipe-name", default="bigqmt_rpc")
    args = ap.parse_args()

    transport = NamedPipeTransport(account_id=args.account,
                                   pipe_name=args.pipe_name,
                                   print_prefix="[live-driver]")
    seq = [0]

    def call(method, params):
        seq[0] += 1
        req = {"schema_version": 1,
               "request_id": "live-%s-%d" % (method, seq[0]),
               "account_id": args.account,
               "method": method,
               "params": dict(params or {})}
        return transport.send_request(req, 30.0)

    started = time.time()
    try:
        call("ping", {})
        print("[live-driver] ping ok")
        r = call("subscribe_whole_quote",
                 {"client_id": "intraday-paper", "sub_id": "paper", "codes": codes})
        print("[live-driver] subscribe:", json.dumps(r, default=str)[:120].encode("gbk", "replace").decode("gbk"))
        deadline = started + args.duration
        last_keepalive = time.time()
        while time.time() < deadline:
            time.sleep(1.0)
            if time.time() - last_keepalive >= 10.0:
                call("quote_keepalive",
                     {"client_id": "intraday-paper", "sub_id": "paper"})
                last_keepalive = time.time()
        print("[live-driver] duration done")
    finally:
        try:
            call("unsubscribe_whole_quote",
                 {"client_id": "intraday-paper", "sub_id": "paper"})
            print("[live-driver] unsubscribed")
        except Exception as exc:
            print("[live-driver] unsubscribe failed: %s" % exc)
        transport.stop()


if __name__ == "__main__":
    main()

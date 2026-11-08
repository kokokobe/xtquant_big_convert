#encoding:utf-8
"""R2 只读桥探针：通过命名管道连活桥，只发只读 RPC，不下单不写数据。

用法（在 xtquant_big_convert 仓库根，用桥同账号的 Python 解释器）：
    python tools/r2_readonly_probe.py
输出 JSON receipt 到 stdout。
"""
import json
import sys
import time

sys.path.insert(0, r"D:\quantitative_qmt\xtquant_big_convert\src")
from bigqmt_signal_trader.transports.pipe_transport import (  # noqa: E402
    DEFAULT_PIPE_NAME,
    NamedPipeTransport,
)

ACCOUNT = "52625295"
# 仅白名单只读方法；明确不含任何 order/cancel/passorder 写方法
READONLY_METHODS = ("ping", "get_deployment_info")


def call(client, method, params=None, timeout=20.0):
    request = {
        "schema_version": 1,
        "request_id": "r2probe-%s-%d" % (method, int(time.time() * 1000)),
        "account_id": ACCOUNT,
        "method": method,
        "params": dict(params or {}),
    }
    return client.send_request(request, timeout)


def main():
    client = NamedPipeTransport(
        account_id=ACCOUNT, pipe_name=DEFAULT_PIPE_NAME,
        print_prefix="[r2probe]")
    receipt = {"schema": "qplatform.bridge-readonly-probe.v1",
               "account_id": ACCOUNT, "transport": "pipe",
               "called": [], "ok": True}
    for method in READONLY_METHODS:
        try:
            response = call(client, method)
            receipt["called"].append({
                "method": method,
                "ok": bool(response.get("ok", True)),
                "error": response.get("error"),
                "data_preview": json.dumps(
                    response.get("data", response),
                    ensure_ascii=False, default=str)[:300],
            })
        except Exception as exc:  # noqa: BLE001 - 探针要把失败写进回执
            receipt["ok"] = False
            receipt["called"].append({"method": method, "ok": False,
                                      "error": str(exc)[:300]})
    print(json.dumps(receipt, ensure_ascii=False, indent=1))
    return 0 if receipt["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

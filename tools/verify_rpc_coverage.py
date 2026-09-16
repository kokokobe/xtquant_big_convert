#encoding:utf-8
r"""verify_rpc_coverage.py -- 分层验证 flat 生成文件里桥的全部 RPC 方法可用性。

用法（在 xtquant_big_convert/ 下，建议用沙箱 python）:
  静态层（不连桥）:
    & "D:\python-lib-gf\python.exe" tools\verify_rpc_coverage.py --static
  实弹层（QMT 里的桥 transport=shm 跑着时）:
    & "D:\python-lib-gf\python.exe" tools\verify_rpc_coverage.py --live --account 52625295

三层验证各管一段，合起来才是"所有函数都可以使用"的完整回答:
  L1 静态: 每个白名单方法都有 _handle_<method> 实现（包源码 + flat 生成文件各查一遍）
  L2 离线: tests/bigqmt_signal_trader/ 的 pytest 组（run_all_tests.py）
  L3 实弹: 对着活桥逐方法发空参请求
      ok=True              -> PASS   handler 返回了数据
      ok=False             -> REACHABLE（方法已路由到 handler，空参下报参数错是预期）
      TransportTimeout/Err -> UNREACHABLE（这一条才是"不能用"）
ORDER_METHODS 默认被 allow_order_methods=False 挡住，返回 ok=False "order rpc
methods are disabled" —— 同样算 REACHABLE（证明白名单门在工作）。
"""
import argparse
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
sys.path.insert(0, SRC)

FLAT = os.path.join(SRC, "BIGQMT_DRYRUN_NO_REDIS_FLAT_ALL_IN_ONE.py")


def _load_registries():
    from bigqmt_signal_trader.redis_rpc import (
        READ_METHODS, ORDER_METHODS, METHOD_ALIASES, BigQmtRpcHandlers,
        MARKET_DATA_METHODS)
    from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
    return (set(READ_METHODS), set(ORDER_METHODS), dict(METHOD_ALIASES),
            BigQmtRpcHandlers, set(MARKET_DATA_METHODS), BigQmtMarketDataProvider)


def run_static():
    (read_methods, order_methods, aliases, handlers_cls,
     market_data_methods, market_data_cls) = _load_registries()
    problems = []
    print("== L1 静态: 白名单方法 vs 分发实现 ==")
    print("READ_METHODS=%d (其中 market_data 适配器分发 %d) ORDER_METHODS=%d aliases=%d"
          % (len(read_methods), len(read_methods & market_data_methods),
             len(order_methods), len(aliases)))
    # handle() 的真实分发顺序: _handle_<m> -> MARKET_DATA_METHODS 走适配器 -> 报未实现
    for name in sorted(read_methods | order_methods):
        if hasattr(handlers_cls, "_handle_%s" % name):
            continue
        if name in market_data_methods:
            if not hasattr(market_data_cls, name):
                problems.append("适配器缺 %s" % name)
            continue
        problems.append("无分发路径: %s" % name)
    for alias, target in sorted(aliases.items()):
        if target not in read_methods | order_methods:
            problems.append("别名 %s -> %s 不在白名单" % (alias, target))
    print("包源码: %s" % ("全部命中" if not problems else problems))

    if os.path.isfile(FLAT):
        flat_src = open(FLAT, "rb").read().decode("utf-8", "replace")
        missing = [name for name in sorted(read_methods | order_methods)
                   if ("_handle_%s" % name) not in flat_src]
        print("flat 生成文件 (%d KB): %s"
              % (os.path.getsize(FLAT) // 1024,
                 "全部 handler 已嵌入" if not missing else "缺 %s" % missing))
        if missing:
            problems.extend("flat 缺 _handle_%s" % m for m in missing)
    else:
        print("flat 文件不存在，跳过生成层检查:", FLAT)
    print("L1 结论: %s" % ("PASS" if not problems else "FAIL (%d)" % len(problems)))
    return 0 if not problems else 1


def run_live(account, per_call_timeout=8.0, report_path=None, skip=(),
             only_handlers=False):
    from bigqmt_signal_trader.redis_rpc import (
        READ_METHODS, ORDER_METHODS, MARKET_DATA_METHODS)
    from bigqmt_signal_trader.transports.shm_transport import SharedMemoryTransport

    read_methods = sorted(READ_METHODS)
    skip = set(skip or ())
    if only_handlers:
        # 原生 xtdata 依赖族（MARKET_DATA_METHODS）：在本终端上对不可达的行情
        # 服务每次调用付 60-90s 连接罚金（#143），还会把后续请求排队陪葬。
        # 它们单独用长超时测；这一轮只扫 handlers 直属方法。
        native = set(MARKET_DATA_METHODS) - skip
        skip = skip | native
        print("== L3 实弹: 只扫 handler 直属方法（跳过原生 xtdata 族 %d 个）=="
              % len(native))
    print("== 对活桥逐方法发空参请求 (account=%s, %d 个只读方法, skip=%d) =="
          % (account, len(read_methods), len(skip)))
    report = None
    if report_path:
        report = open(report_path, "w", encoding="utf-8")

    def emit(line):
        print(line)
        if report is not None:
            report.write(line + "\n")
            report.flush()

    client = SharedMemoryTransport(account_id=account, print_prefix="[verify]")
    counts = {"PASS": 0, "REACHABLE": 0, "UNREACHABLE": 0}
    unreachable = []
    consecutive_timeouts = 0
    t0 = time.time()

    def call_once(name, timeout=None):
        request = {
            "schema_version": 1,
            "request_id": "verify-%s-%d" % (name, int(time.time() * 1000)),
            "account_id": account,
            "method": name,
            "params": {},
        }
        return client.send_request(request, timeout or per_call_timeout)

    try:
        # ping 探活先行: 首次给 120s —— 上一轮若被原生 xtdata 慢调用堵过，
        # 积压队列排空可能要一两分钟
        try:
            response = call_once("ping", timeout=120.0)
            if response.get("ok"):
                emit("  ping                               PASS  桥在线 (version=%s)"
                     % response.get("version"))
            else:
                emit("  ping                               REACHABLE %s" % response.get("error"))
        except Exception as exc:
            emit("  ping                               UNREACHABLE %s" % type(exc).__name__)
            emit("L3 结论: FAIL -- 桥不响应, 检查 QMT 里桥是否在跑 / 账号是否一致")
            return 1

        for name in read_methods:
            if name == "ping":
                counts["PASS"] += 1
                continue
            if name in skip:
                counts["REACHABLE"] += 1
                emit("  {:<36} SKIP        (已知终端级超慢, 单独验证)".format(name))
                continue
            try:
                response = call_once(name)
            except Exception as exc:
                counts["UNREACHABLE"] += 1
                unreachable.append((name, "%s: %s" % (type(exc).__name__, exc)))
                consecutive_timeouts += 1
                emit("  {:<36} UNREACHABLE  {}".format(name, type(exc).__name__))
                if consecutive_timeouts >= 5:
                    emit("!! 连续 %d 次不可达 -- 桥的 adjust 线程疑似被某个 handler "
                         "卡住 (drain 模式单线程), 后面的请求只会排队超时, 提前熔断。"
                         % consecutive_timeouts)
                    emit("!! 卡住的方法大概率是上一个 REACHABLE/PASS 之后、第一个 "
                         "UNREACHABLE 的那个 (本条: %s)" % name)
                    break
                continue
            consecutive_timeouts = 0
            if response.get("ok"):
                counts["PASS"] += 1
                emit("  {:<36} PASS".format(name))
            else:
                counts["REACHABLE"] += 1
                error = str(response.get("error") or "")[:80]
                emit("  {:<36} REACHABLE    {}".format(name, error))
        # order 白名单门抽 3 个
        if consecutive_timeouts == 0:
            for name in sorted(ORDER_METHODS)[:3]:
                try:
                    response = call_once(name)
                except Exception as exc:
                    counts["UNREACHABLE"] += 1
                    unreachable.append((name, repr(exc)))
                    emit("  {:<36} UNREACHABLE  {}".format(name, type(exc).__name__))
                    continue
                error = str(response.get("error") or "")
                counts["REACHABLE"] += 1
                emit("  {:<36} REACHABLE    {}{}".format(
                    name, "(order gate) " if "disabled" in error else "", error[:60]))
    finally:
        client.stop()
    emit("---- 汇总 (%.1fs): PASS=%d REACHABLE=%d UNREACHABLE=%d"
         % (time.time() - t0, counts["PASS"], counts["REACHABLE"],
            counts["UNREACHABLE"]))
    if unreachable:
        emit("!! 不可达清单:")
        for name, why in unreachable:
            emit("   %s  %s" % (name, why))
    verdict = "PASS" if not unreachable else "FAIL"
    emit("L3 结论: %s" % verdict)
    if report is not None:
        report.close()
    return 0 if not unreachable else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--static", action="store_true", help="静态层: 方法表 vs 实现 vs flat 嵌入")
    parser.add_argument("--live", action="store_true", help="实弹层: 对活桥逐方法发空参请求")
    parser.add_argument("--account", default="", help="账号 id（live 用）")
    parser.add_argument("--only-handlers", action="store_true",
                        help="跳过原生 xtdata 依赖族（MARKET_DATA_METHODS），只扫 handler 直属方法")
    parser.add_argument("--timeout", type=float, default=8.0, help="单方法超时秒数（live 用）")
    parser.add_argument("--skip", default="", help="逗号分隔的方法名，跳过不测（已知终端级超慢的）")
    parser.add_argument("--report", default=os.path.join(HERE, "rpc_coverage_report.txt"),
                        help="逐行结果落盘文件（live 用）")
    args = parser.parse_args()
    if not args.static and not args.live:
        args.static = True
    rc = 0
    if args.static:
        rc |= run_static()
    if args.live:
        rc |= run_live(args.account, per_call_timeout=args.timeout,
                       report_path=args.report,
                       skip={m.strip() for m in args.skip.split(",") if m.strip()},
                       only_handlers=args.only_handlers)
    return rc


if __name__ == "__main__":
    sys.exit(main())

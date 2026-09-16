#encoding:utf-8
r"""板块/系统函数能力探针：在 QMT 里枚举 ContextInfo 与原生 xtdata 的真实函数面。

用途：对照迅投官方文档（dict.thinktrader.net/innerApi），找出"文档有、本终端没有"
的函数，终结"到底是没有还是没接"的争论。
跑法：QMT 编辑器里本策略直接"运行"（无需行情订阅），看 [sector_probe] 输出。
"""
def init(ContextInfo):
    print("[sector_probe] ====== 1. ContextInfo 板块相关方法 ======")
    names = sorted(m for m in dir(ContextInfo) if "sector" in m.lower())
    for n in names:
        print("  ContextInfo.%s" % n)
    if not names:
        print("  (无任何 sector 相关属性)")
    print("[sector_probe] ----- 显式 hasattr 检查 -----")
    for n in ("get_sector_list", "get_sector", "get_stock_list_in_sector",
              "create_sector", "create_sector_folder", "add_sector",
              "remove_sector", "add_stock_to_sector", "remove_stock_from_sector",
              "reset_sector_stock_list", "download_history_data",
              "get_history_trade_detail_data", "call_formula",
              "get_formula_result", "subscribe_formula"):
        print("  hasattr(ContextInfo, %-34s) = %s" % ("'%s'," % n, hasattr(ContextInfo, n)))

    print("[sector_probe] ====== 2. 原生 xtdata 板块相关 ======")
    try:
        from xtquant import xtdata
        xn = sorted(m for m in dir(xtdata) if "sector" in m.lower())
        for n in xn:
            print("  xtdata.%s" % n)
        if not xn:
            print("  (无任何 sector 相关属性)")
        print("  hasattr(xtdata, 'get_sector_list') = %s" % hasattr(xtdata, "get_sector_list"))
        print("  xtdata 版本/路径: %s" % getattr(xtdata, "__file__", "?"))
    except Exception as e:
        print("  原生 xtdata 导入失败: %s: %s" % (type(e).__name__, e))

    print("[sector_probe] ====== 3. ContextInfo 全量 dir()（对照官方文档用） ======")
    all_names = sorted(dir(ContextInfo))
    print("  共 %d 个属性" % len(all_names))
    line = []
    for n in all_names:
        line.append(n)
        if len(line) >= 6:
            print("  " + "  ".join(line))
            line = []
    if line:
        print("  " + "  ".join(line))
    print("[sector_probe] ====== 结束 ======")


def handlebar(C):
    pass

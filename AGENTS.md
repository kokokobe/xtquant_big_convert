# xtquant_big_convert（BIGQMT 桥）项目须知

> 本文件是 AI 侧工作约定，沉淀 2026-09-15 深入排查后的完整认知。上游是开源项目
> `github.com/litaolemo/xtquant_big_convert`（v0.3.43，MIT），本仓库内是本地扩展版
> （新增 shm 传输等）。主仓库总纲见 `D:\quantitative_qmt\AGENTS.md`，两者冲突时以
> 更新者为准并互相回写。

## 这个项目是什么

把券商版 QMT 终端变成**本地 RPC 服务器**：桥驻留在 QMT 进程内（只有那里能调
QMT API），把行情 / 五档 / 持仓 / 资金 / 下单 / 下载任务 / 期权分析暴露成方法调用；
外部 python 客户端经可插拔传输层调它。另有 MiniQMT 兼容适配层（`xtquant_compat`）
和回测包（`bigqmt_backtest`）。

```
外部 python 客户端 ──transport──> QMT 进程内的桥（RPC 服务 + handlers）
                                    ├─ BigQmtRpcHandlers → QMT API
                                    ├─ 行情推送通道（PUB/SUB，独立于 RPC 线）
                                    └─ FormulaServer 直读快速路径（QMT 自带, :58600）
```

## 目录结构

```
xtquant_big_convert/
├── src/
│   ├── bigqmt_signal_trader/           # 主包
│   │   ├── transports/                 # ★ 可插拔传输层（本仓库改造重点）
│   │   │   ├── base.py                 # RpcTransport 契约 + deliver()
│   │   │   ├── factory.py              # build_transport + transport_supports_drain
│   │   │   ├── redis_transport.py      # redis pubsub+list（上游默认）
│   │   │   ├── zmq_transport.py        # ROUTER/DEALER 回环 TCP（no-redis 默认）
│   │   │   ├── pipe_transport.py       # 命名管道（非 TCP 备选）
│   │   │   ├── shm_transport.py        # ★ 2026-09-15 新实现：零 TCP（见下）
│   │   │   └── mysql_transport.py      # mysql 表轮询
│   │   ├── redis_rpc.py                # 名字带 redis 实际是传输无关的 RPC 框架
│   │   ├── bigqmt_signal_trader_strategy.py  # QMT 入口 init/handlebar/adjust 全局接线
│   │   ├── quote_push_channel.py       # 行情推送通道（Redis/ZMQ 两实现；shm 无）
│   │   ├── quote_subscription_manager.py
│   │   ├── adapters/                   # market/order(DryRun+live)/position/redis_common
│   │   ├── xtquant_compat.py           # MiniQMT 兼容客户端 shim（外部侧）
│   │   └── ...                         # exec_events/order_watch/multi_account/...
│   ├── bigqmt_backtest/                # 回测包
│   ├── xtquant/                        # xtquant shim
│   ├── bigqmt_signal_trader_strategy.py 等顶层胶水模块（py-modules）
│   └── BIGQMT_DRYRUN_NO_REDIS_FLAT_ALL_IN_ONE.py  # ★ 构建产物，勿手改
├── tools/build_no_redis_single_file_flat.py  # ★ 构建脚本
├── tests/bigqmt_signal_trader/         # pytest 测试（~150 文件）
├── run_all_tests.py                    # 分组跑全量（signal_trader/backtest；--live 需 QMT）
└── bigqmt_no_redis/                    # 旧的模块化 shell + 独立 zmq_transport 副本（历史）
```

## 部署形态（本机实测，2026-09-15）

| 位置 | 内容 |
|---|---|
| `D:\广发证券QMT量化交易系统 - 交易端\python\BIGQMT_BRIDGE.py` | QMT 侧策略（导入后 QMT 加密），加载 flat all-in-one |
| `D:\...\python\bigqmt_signal_trader_local_config.py` | **外置私有配置（账号/凭据，勿提交）**。构建脚本保证：`sys.modules` 里已有外置模块时**外置优先于 flat 内嵌**（防覆盖手改） |
| `D:\...\python\logs\bigqmt-pid<pid>.log` | 桥的落盘日志（`logging_setup._resolve_log_dir` 优先选 QMT python 目录；实际日志主要走 print 进 FormulaOutput，文件多为 0 B） |
| `src/bigqmt_signal_trader_client_config.example.py` | 外部客户端配置样例（transport 必须与 QMT 侧一致） |

当前生效配置要点（`bigqmt_signal_trader_local_config.py`）：
- `"transport": 'shm'`（2026-09-15 从 zmq 切换，原因见下）
- **drain 模式**：`rpc_background_threads: False` + `schedule_adjust("adjust", 100ms)`。
  硬约束：`get_trade_detail_data` **只在 QMT 主线程有数据**，所以交易/查询类方法
  绝不能搬到后台线程执行 —— 请求由 adjust tick 调 `drain_request_queue` 拉取，
  inline 方法当场处理，其余进 pending 队列由 `drain_pending` 消化
- `rpc_allow_order_methods: False`（远程下单关闭，DRYRUN 语义）

## 传输层：为什么有 shm，为什么 zmq/redis 在 QMT 里是死路

QMT 终端审计：`CStrategyTradeManager` 以 10s 节拍扫进程 TCP 表，白名单外对端
（**含 127.0.0.1 回环**）即记 `illegal IP` 并同毫秒 stopAllStrategy 全面板杀。
见主仓库 AGENTS.md 网络 IO 行。

| 传输 | wire | QMT 面板判定 |
|---|---|---|
| redis | TCP 6379 | ✗ 必被抓（9-12/13/14/15 多次实锤） |
| zmq | 回环 TCP：ROUTER/DEALER + libzmq signaler **自连 TCP** | ✗ 必被抓。`ipc://` Windows 不可用（`Protocol not supported`）。15:12:52 抓 59298、19:44:56 抓 62619 都是 signaler 对 |
| pipe | 命名管道 | 理论合规（非 TCP），未在面板长跑验证 |
| **shm** | mmap 命名 section + 内核事件/互斥体 | ✓ 零 socket，TCP 表里不存在；跨进程 RPC 实测 **0.15ms** |

**pyzmq 任何形态都不要进 QMT 进程**（inproc 也要建 Context → IO/reaper 线程 →
signaler 回环自连；`Context.instance()` 不随策略停止回收，留僵尸连接持续触发，
须重启 QMT 清除）。

## shm 传输（transports/shm_transport.py）要点

- 原语：`mmap.mmap(-1, size, tagname=...)`（pagefile 支撑、不落盘）+
  `CreateEventW`（自动复位事件）/ `CreateMutexW`（请求环发布顺序化），
  纯 stdlib ctypes。原型已在 QMT 面板实证：`D:\quantitative_qmt\src\mem_share_probe.py` v4
- 命名：`{prefix}_req{,_evt,_mtx}_{account}` 服务端共享；`{prefix}_rsp{,_evt}_{account}_{client16hex}`
  **每线程私有应答线**（#186 shm 版：共享环会被在途请求互相覆盖）
- 线格式协议 v1：header 64B（`BQSH`/version/slot_count/slot_size/write_seq/lost）
  + slot（seq 8B + request_id 32B + len/flags + payload）。**几何是协议常量，不吃配置**
  ——REQ 64×64KB，RSP 4×4MB；改尺寸必须升 version
- 回复路由：`request["reply_shm"]/["reply_evt"]` 随信封走（shm 版 ROUTER identity）
- 双模式：`background_threads=True` 监听线程等事件（推送 0.0ms）；`=False` 纯 drain
  （adjust 每 tick 非阻塞拉序号，**无线程**）——本机部署用 drain 模式
- 行情推送：shm **没有**推送通道，`build_quote_subscription_service` 对 shm 显式返回
  None（不要悄悄落 RedisQuotePushChannel(None)）
- 客户端在另一进程/另一台 python（py3.14 等）直接用同模块，两端协议常量一致即可

## 构建 / 发布

```
& "D:\python-lib-gf\python.exe" tools/build_no_redis_single_file_flat.py
# → WROTE src/BIGQMT_DRYRUN_NO_REDIS_FLAT_ALL_IN_ONE.py (48 包 + 4 顶层)
```

- 改 `src/` 源码后必须重建 flat，再把 flat 拷到 QMT 重新导入（QMT 端加密）
- `tests/test_single_file_build.py` 校验构建（14 项）；版本号在
  `pyproject.toml` + `src/bigqmt_signal_trader/version.py`，CHANGELOG.md 按版本记
- flat 的 shell 自带 importlib 挂载（定制 `__import__` 解析相对导入）、模块清理解绑
  socket、`"[bigqmt_shell] ... loaded from ..."` 打的是**名义挂载路径**，不是真实磁盘目录

## 测试

```
& "D:\python-lib-gf\python.exe" -m pytest tests/bigqmt_signal_trader/test_shm_transport.py -q
& "D:\python-lib-gf\python.exe" run_all_tests.py            # 分组全量（--live 需 QMT）
```

- 沙箱 python `D:\python-lib-gf\python.exe`（3.6.8）可直接跑离线测试；**py3.6 兼容是
  铁律**（无 3.7+ 语法、无 `multiprocessing.shared_memory`——shm 正因此手写 mmap 环）
- 目录布局（2026-09-15 标准化）：
  - `tests/bigqmt_signal_trader/` 主包单测、`tests/bigqmt_backtest/` 回测单测
  - `tests/infra/` 工程设施测试（构建/版本/依赖/shim 导入/credit 报告）
  - `tests/live/` 需活桥的测试（`test_order_concurrency_live.py` 默认 skip，设
    `BIGQMT_LIVE_TEST=1` 运行；`test_all_apis.py`）
- `run_all_tests.py` 与多个上游测试用了 py3.7+ 的
  `subprocess.capture_output/text`——沙箱跑前已改成 `PIPE/universal_newlines`
- **沙箱 3.6 预存在失败清单**（52+26，与本仓库改动无关，勿重复排查）：
  - `test_qmt_cli_*` 26 ERROR：`qmt-trader/scripts/qmt.py` 用
    `from __future__ import annotations`（py3.7+ 语法），3.6 直接 SyntaxError
  - `test_compat_method_coverage` ShimReaches 4：xtquant_compat 往 xtdata 注入
    包装器依赖 3.7+ 导入语义（get_bvol 等 wrapper 未注册）
  - `test_divid_factors_frame` 10 / `test_xtquant_compat` PartialMarker 4：
    pandas 0.22 帧行为差异
  - `test_transports` Mysql 1：沙箱无 dbutils
  - 其余：idna 预载 2、bundled_xtconstant 2、quote_heartbeat 1、option 边界 1
- 传输层新实现必须：实现 `drain_request_queue`（否则 adjust 掉进 redis 兜底，见
  factory 注释的 pipe 教训）+ 过 `test_transport_selection.py` 的决策断言
  （`_resolve_background_threads`/`_transport_can_drain` 问传输类，不问名单）

## 近期改动（2026-09-15）

1. **shm_transport.py 实现**（原占位 TODO）：零 TCP RPC，跨进程 0.15ms；24 项单测 +
   跨进程测试全过；`transport_supports_drain` 补 shm 分支
2. **quote_subscription_manager**：shm 下行情推送服务显式禁用（返回 None），不再静默
   落 None redis 客户端
3. **构建脚本两处修复**：`_load_local_config` 改为**外置配置优先**（原版内嵌占位块
   YOUR_ACCOUNT_ID 遮蔽外置文件，桥一直以占位账号+强制 zmq 在跑）；transport 强制
   逻辑放行显式 `"shm"`（其余仍强制 zmq，防手滑跌进 redis 触发审计）
4. **面板验证通过（20:18 起）**：`[bigqmt_rpc] transport=shm` + `shm started
   req=bigqmt_shm_req_52625295`（drain 模式），主日志零新增 illegal IP、桥连续
   存活跨多个 10s 审计节拍，adjust 节拍稳定 100ms。此前 zmq 部署每次 ~2s 内被杀
   （19:44:56 抓 62619、20:05:59 抓 15560 对）。注意：外置 local_config 生效后
   账号从占位符 YOUR_ACCOUNT_ID 变为真实 52625295，**外部客户端配置必须同步**
   （transport=shm + account_id=52625295）
5. **rid 截断 bug 修复 + 全量覆盖率实弹通过（深夜）**：`write_slot` 把 request_id
   截到 31 字节而客户端用完整串等值比对——超过 31 字符的 rid（含生产端
   uuid4().hex 32 位）应答"已写环但永远匹配不上"；单测全绿是因为测试 rid 都短。
   修复为前缀比对 + 完整性由解码后 `response["request_id"]` 兜底 + 长 rid 回归
   测试。最终覆盖率：**141 个测试对象 = 22 PASS + 119 REACHABLE + 2 已归因超时**
   （get_ticks 是 #143 慢族且把 passorder 队列陪葬；order 白名单门由
   cancel_order/cancel_orders_batch 的 "not allowed" 证明工作正常）。工具：
   `tools/verify_rpc_coverage.py`（--static / --live --only-handlers --skip ...），
   报告落盘 `tools/rpc_coverage_report*.txt`
6. **能力矩阵权威入口 = `probe_capabilities` RPC**（tools/check_passorder_and_caps.py
   是现成调用样例）：qmt_globals（passorder/cancel/get_trade_detail_data=true；
   download_history_data/down_history_data/get_history_trade_detail_data=false
   ——下载族与历史成交查询在本终端未注入）、global_namespace（板块写族全 false）、
   contextinfo_methods（行情读取全 true）、credit_probe（信用查询可用 rows=0）。
   "哪些方法用不了"一律先调它，不要再猜权限

## RPC 方法可用性验证（tools/verify_rpc_coverage.py，2026-09-15 深夜）

三层验证工具已建：`--static`（方法表 vs 实现 vs flat 嵌入）/ `--live`（对活桥逐方法
发空参请求，结果落盘 tools/rpc_coverage_report*.txt）。实测结论：

- **传输层 100% 可靠**：87 条请求、lost=0、每条的应答最终都写入应答环（外部可直接
  mmap dump 验证）；"超时"全是服务端处理慢，不是丢消息
- **分发层 100%**：140 个白名单方法全有实现路径（86 个 MARKET_DATA_METHODS 走
  适配器分发，其余走 `_handle_*`）
- **运行时三类**：
  - ✅ 即答可用：ping/get_asset/持仓/委托等核心查询（~50 个 handler 方法）
  - ⏱ **原生 xtdata 依赖族（~86 个 MARKET_DATA_METHODS）**：本终端连不上行情服务
    （#143），板块写/下载/公式引擎/成分股等每次调用付 **60-90s 连接罚金**后返回
    失败——终端能力缺陷，不是桥 bug。逐个实弹测试会拖垮整轮（单线程 drain 排队
    传导），验证时整族跳过
  - ⏳ **异步回调族（信用/融资融券 ~14 个）**：handler 等 QMT 回调，回调不落地阻塞
    35-90s（#202 已知，代码注释明说"adjust 线程 = 等待会把它堵死"）

## RPC 参数用法手册（2026-09-16 带参实弹，tools/param_manual_sweep.py）

**判定规则**：PASS=真实数据返回；BIZ=端到端通但业务层拒绝（终审结论）；
TIMEOUT=数据服务依赖（#143 挂 60-90s，慎调）；**空参 TypeError ≠ 不可用，
必须带参实测**。

### ✅ 即答可用（本地数据/纯计算，drain 模式安全）

| 方法 | 参数形态 | 备注 |
|---|---|---|
| get_market_data_ex | stock_list, period, start_time, end_time | 行情主通道，DataFrame 回包 |
| get_full_tick | stock_list | 五档/tick |
| get_positions / get_asset / query_orders / query_trades / query_execution_snapshot / query_account_infos / query_account_status / get_position_statistics / sync_positions / query_stock_position(stock_code) | 常规 | 账户/持仓/委托全通 |
| submit_order / passorder / cancel_order | 见 ORDER_METHODS | 模拟柜台 60 并发实测 |
| get_trading_dates(market, start, end, count) / get_trading_calendar / get_market_last_trade_date(market) | market="SH" | 104KB 日历 |
| get_financial_data(stock_list, table_list=["Capital"], start_time, end_time) | **stock_list 批量**（不是 stock_code） | 框架通；值需终端数据管理先补财务数据 |
| get_north_finance_change(start_time, end_time) | 日期 | 北向资金 489B |
| get_stock_list_in_sector(sector_name) | sector_name="沪深300" | 5222 成分 |
| get_sector_list(**allow_fallback=True**) | 逃逸参数 | 13 知名板块；不含自建板块 |
| get_hkt_exchange_rate(market="HK") | market | 真实港币汇率 0.88/0.83 |
| get_hkt_statistics / get_option_undl_data / get_option_detail_data(+batch) / get_main_contract / get_his_contract_list / get_his_index_data / get_contract_expire_date / get_contract_multiplier | 各自签名 | 期权/合约族 |
| is_suspended_stock(stock_code) | stock_code | false=未停牌 |
| get_value_by_order_id(order_id) | order_id | 需真实委托号 |
| get_divid_factors(stock_code) / get_turnover_rate / get_turn_over_rate / get_weight_in_index / get_risk_free_rate / get_date_location / get_raw_financial_data(field_list, stock_list, ...) | 各自签名 | 全通 |
| get_universe() | 无参 | 返回 []（桥策略未 set_universe） |
| download_history_data(stock_code, period, start, end) / download_history_data2(stock_list, ...) | 单票/批量 | 本地已有数据秒回；**新数据内联调用会死锁，必须走异步任务** |
| submit_download_history_data(2)(stock_list, period, start, end) | job_id 秒回 | 异步任务（download_jobs_enabled=True） |
| get_download_status(job_id) / wait_download(job_id) | job_id | 轮询/等待任务 |
| download_holiday_data() / download_his_st_data(...) | 无参/简单 | PASS |
| datetime_to_timetag / timetag_to_datetime | 时间字符串 | 纯计算 |
| ping / probe_capabilities / probe_order_identity / get_deployment_info / reload_deployment / reload_status | 无参 | 系统 |

### ⏳ 数据服务依赖（TIMEOUT 20s 窗口，实际挂 60-90s，慎调）

| 方法 | 备注 |
|---|---|
| is_fund / is_stock / is_future(stock_code) | 看似简单，内部查品种库 → 挂 |
| get_instrument_detail / get_instrumentdetail(stock_code) | 合约详情查品种库 → 挂 |
| get_ticks(codes) | tick 数据服务 → 挂 |
| get_largecap / get_midcap / get_smallcap(stock_list) | 市值分类查服务 → 挂 |
| get_scale_and_stock(stock_value, ...) | 同上 |
| get_local_data / get_market_data（**用 get_market_data_ex 替代**） | 原生路径 |
| get_sector_list（不带 allow_fallback）/ get_sector_info / get_sector_list 相关读 | 原生路径 |
| get_holder_num / get_option_list / get_option_iv / get_option_undl / get_factor_data / get_float_caps / get_cb_info / get_close_price / get_last_close / get_last_volume / get_svol / get_bvol / get_total_share / get_trade_times / get_longhubang / get_markets / get_open_date / get_ipo_info / get_his_st_data / get_hkt_details / bsm_iv / bsm_price / download_cb_data / gen_factor_index | 原生 xtdata 阻塞族（快扫 TIMEOUT 名单） |

### ❌ 终端缺失（BIZ 终审，NotImplementedError/RuntimeError）

| 方法 | 判决 |
|---|---|
| call_formula / subscribe_formula / unsubscribe_formula / get_formula_result | ContextInfo 无公式系统 |
| download_etf_info / download_financial_data / download_financial_data2 | ContextInfo 无（财务数据用终端 数据管理 UI 补，get_financial_data 读） |
| download_history_contracts / download_index_weight / download_sector_data / get_l2_transaction / subscribe_l2thousand | needs native xtdata SDK |
| get_his_option_list / get_his_option_list_batch | ContextInfo 无 |
| add_sector / add_stock_to_sector / reset_sector_stock_list | #143：报成功但板块没变（写通道坏） |
| get_finance(stock_code) | **QMT 实现缺陷**：方法存在但内部引用缺失属性（AttributeError），编辑器/交易面板上下文差异待查 |
| stockcode_in_rzrk | ContextInfo 无 |
| subscribe_whole_quote / unsubscribe_whole_quote / quote_keepalive / quote_subscription_status / quote_unsubscribe_all | shm 部署无全推推送通道（quote push 显式禁用） |
| get_scale_and_rank(index_name) | 分发通；QMT 返回数据待验证（签名已实测校准为单参数） |
| 信用/两融异步族 14 个 | #202 阻塞 |
- **教训**：连续 UNREACHABLE 时先 dump 请求环/应答环（`_Ring` 外部可读）区分"服务端
  没处理"vs"应答迟到"，不要急着加超时——本次三轮"失败"实为测试超时 < 服务端固有
  延迟，且上一轮的慢请求积压会污染下一轮
- **market_data 族快扫（tools/sweep_market_data.py，ping 探空 + 20s 实弹）**：90 个
  方法 = 6 PASS + 44 REACHABLE（空参 TypeError/NotImplementedError = 分发通）+
  40 TIMEOUT（原生 xtdata 阻塞族，本终端无法服务）。PASS 的有 get_market_data_ex/
  get_option_undl_data/get_turnover_rate/get_risk_free_rate/download_holiday_data/
  download_his_st_data；TIMEOUT 名单见 tools/market_data_sweep_report.txt。
  **下单链路**：submit_order 60 并发全受理进模拟柜台，60/60 唯一 id，资金/持仓
  零变化；并发下单注意 #304（0.3.44 drain 每拍限时 + 过期拒绝）
- **⚠️ rpc_background_threads=True 在本终端不可用（2026-09-16 实测回退）**：
  切后台线程模式后 ping 110ms（listener 未即时处理）、get_asset 返回 cash=1000.0
  （真实 63464.87——**数据正确性错误**，比慢更严重）、submit 3.1s、query_orders
  30s 超时级联劣化。**保持 False（drain 模式）**：三天稳定、数据正确。代价是
  RPC 往返 ~50-95ms（adjust 100ms tick），对信号推送（总线亚毫秒）无影响。
   交易上下文方法本就由 LISTENER_DEFERRED_METHODS 强制走 adjust，切换无收益。
   另注意：sim 柜台实测中出现过 cash=1000.0 异常，排查时先核对终端委托/成交记录
- **⚠️ 同步下载在 drain 线程上 = 死锁（2026-09-16 14:46 实测）**：内联调用
   `download_history_data`/`down_history_data` 下载**本地没有的新数据**时，
  adjust 线程永久冻结（ping 冻结、cadence 停摆，QMT 下载完成回调落在被阻塞的
  同一线程——#202 同款机制），只能重启桥解卡。本地已有数据的增量下载秒回
  无碍。**下载必须走异步任务队列**：`download_jobs_enabled: True` +
  `submit_download_history_data`（秒回 job_id）→ `get_download_status` 轮询 →
  `get_market_data_ex` 验证落地；内联 download RPC 在 drain 模式下视为禁用
- **✅ 进程间发布总线（pub_bus.py）已实测双向打通（09-16）**：QMT→外部经
  `bus_publish` RPC（2/2 收到）；外部→QMT 外部 BusPublisher 直发 +
  `bus_inbox_drain` 取（2/2）。发布微秒级；RPC 外壳往返 ~50-95ms（drain 100ms
  tick 所致，总线推送本身亚毫秒）。多订阅者=独立事件+独立游标；环覆盖有
  `__bus_lost__` 通告。工具：tools/test_bus_e2e.py。外部→QMT 的消息若触发
  下单，必须转 pending 队列在 adjust tick 执行（#252）
- **REACHABLE ≠ 不可用**：空参 TypeError 只说明分发通。带真实参数实测
  `download_history_data`（513300.SH 1d 两周）→ ok=True 且 get_market_data_ex
  读回 11 个交易日 OHLC——下载族走 ContextInfo 通道可用（tools/
  test_download_history.py），qmt_globals 的 false 只代表全局函数未注入，
  ContextInfo 方法在。**下结论前必须带参实测**
- **TIMEOUT ≠ 不可用，可能是超时窗口 < 原生挂起时长**：get_sector_list 实测
  `allow_fallback=true` 秒回 13 个知名板块名 → get_stock_list_in_sector("沪深A股")
  5222 成分 ✓（#130 教训：适配器默认拒绝返回假列表，逃逸参数是显式设计的）。
  用户引用的迅投通用文档里 get_sector_list 是系统函数，但本终端 ContextInfo
  只有 create_sector/get_sector/get_stock_list_in_sector 三个板块方法（probe 实锤）
- **穷举探针（src/sector_capability_probe.py，QMT 编辑器跑）一锤定音**：本终端
  ContextInfo 共 142 属性，板块相关只有 create_sector/get_sector/
  get_stock_list_in_sector；get_sector_list=False、板块写 6 函数=False、
  download_history_data=False、call_formula 族=False（与 BIZ 判决完全一致）；
  原生 xtdata（D:\python-lib-gf\...\xtquant\xtdata.py）有 get_sector_list/
  add_sector/remove_sector/download_sector_data 但连不上行情服务（#143）。
  **官方文档是迅投通用规格，广发内嵌 ContextInfo 是阉割版——判定函数有无以
  穷举探针为准，勿引文档**。附带发现：ContextInfo 有 get_universe/set_universe
  属性（与"广发不支持 set_universe"的旧记录有出入，属性存在≠生效）、get_finance、
  load_stk_list、get_scale_and_rank、get_largecap/midcap/smallcap、get_product_*
  等桥未暴露的方法，按需接入

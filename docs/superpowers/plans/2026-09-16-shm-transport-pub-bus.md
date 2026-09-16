# 实施计划：shm 零 TCP 传输、发布总线与验证工具链

日期：2026-09-16（当日完成）
规格：docs/superpowers/specs/2026-09-16-shm-transport-pub-bus-design.md
复盘：docs/superpowers/postmortems/2026-09-16-download-deadlock-bg-mode-rollback.md

## 阶段 0：原语探针（先行验证，避免建在沙滩上）

| 步骤 | 内容 | 验收 |
|---|---|---|
| 0.1 | src/mem_share_probe.py v4：mmap tagname / 事件 / 互斥体 / 跨进程 | QMT 面板实测：跨进程共享 ✓ 事件推送 0.0ms ✓ |

结果：三个原语全部面板可用 → 才进入实现。（注：zmq ipc 被 probe 否定——Protocol not supported + signaler 回环 TCP 会被审计杀。）

## 阶段 1：shm 传输

| 步骤 | 内容 | 验收 |
|---|---|---|
| 1.1 | transports/shm_transport.py 实现（环形缓冲/互斥体发布/每线程应答线/双模式） | 单测全绿 |
| 1.2 | factory 接入（transport="shm"）+ transport_supports_drain 分支 | test_transport_selection |
| 1.3 | 跨进程实测 | test_shm_transport_crossproc 0.15ms |
| 1.4 | QMT 面板部署 + 零 illegal IP + 延迟实测 | FormulaOutput 日志 + netstat |

## 阶段 2：发布总线

| 步骤 | 内容 | 验收 |
|---|---|---|
| 2.1 | pub_bus.py（Publisher/Subscriber/注册表/游标/丢包通告） | 单测 7 项 |
| 2.2 | RPC：bus_publish / bus_inbox_start / bus_inbox_drain | test_pub_bus::RpcHandlerTest |
| 2.3 | 外部 CLI：tools/bus_subscribe.py + e2e 工具 | tools/test_bus_e2e.py 双向 2/2+2/2 |

## 阶段 3：下载任务内存模式

| 步骤 | 内容 | 验收 |
|---|---|---|
| 3.1 | download_jobs redis_client=None 回退（内存注册表+后台 worker） | 单测 5 项（完成/进度/失败/拒绝/参数） |
| 3.2 | _download_job_redis None 语义 + handler download_func 透传 | live：submit 秒回 → 数据落地 11 交易日 |

## 阶段 4：ContextInfo 方法面补全

| 步骤 | 内容 | 验收 |
|---|---|---|
| 4.1 | 穷举探针（sector_capability_probe.py）实锤终端函数面 | 用户在 QMT 编辑器运行，输出对照官方文档 |
| 4.2 | market_bigqmt 12 方法 + MARKET_DATA_METHODS 注册 | 带参实弹：get_finance 框架通（值需终端补数据）、is_suspended_stock false 等 |
| 4.3 | 已知不可用族显式化（NotImplementedError 判决，不静默） | BIZ 终审 18 项 |

## 阶段 5：验证工具链与测试树

| 步骤 | 内容 | 验收 |
|---|---|---|
| 5.1 | verify_rpc_coverage.py（静态+实弹三层） | 141 方法全覆盖 |
| 5.2 | sweep_market_data.py / adapt_params_sweep.py / param_manual_sweep.py | 90 方法逐个归因 |
| 5.3 | 测试树标准化：tests/infra + tests/live（skip 守卫） | run_all_tests.py 全量 |
| 5.4 | py3.6 兼容修复（capture_output/text/PEP562） | 沙箱全绿 |

## 阶段 6：调优与定案

| 步骤 | 内容 | 验收 |
|---|---|---|
| 6.1 | schedule_adjust_interval 10ms + drain_max_items 50 | ping 1.85-8.91ms（前 50-110ms） |
| 6.2 | 订单族 LISTENER_DEFERRED 补全 | bg 模式下 submit 不再阻塞接收线程 |
| 6.3 | background_threads=True 终审 → 回退 False 定案 | 验证清单 + postmortem |

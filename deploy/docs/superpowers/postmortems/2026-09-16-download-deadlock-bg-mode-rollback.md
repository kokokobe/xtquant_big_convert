# 复盘：下载死锁、background_threads 回退与两次误判

日期：2026-09-16（事件当日）
关联：specs/2026-09-16-shm-transport-pub-bus-design.md / AGENTS.md 网络 IO 与验证章节

## 事件时间线（2026-09-15 ~ 09-16）

| 时间 | 事件 | 证据 |
|---|---|---|
| 09-12 ~ 09-15 多次 | 桥（zmq/redis 传输）被审计杀：illegal IP 6379 / 临时端口 | XtClient_*.log onIllegalIPFind |
| 09-15 14:19 | sector 穷举探针（编辑器） | ContextInfo 142 属性穷举 |
| 09-15 14:46 | **内联下载死锁**：adjust 线程冻结，cadence 停摆 20min+ | ping 冻结 + FormulaOutput cadence 停在 14:46 |
| 09-15 15:12 | mem_share_probe v1 触发 zmq illegal IP（59298）→ 全面板被杀 | 主日志 43713 行 |
| 09-15 15:38/16:36/17:03 | shm 探针 v2-v4（无 zmq）→ 全绿 | 零新增 illegal |
| 09-15 19:44/20:05 | 桥以 transport=zmq 重启 → 2s 内被杀（15560/62619） | 主日志 + netstat socketpair |
| 09-15 20:18 | transport=shm 首次上线 → 零 illegal、稳定 | FormulaOutput |
| 09-16 | bg 模式验证 → 异常 → 回退 False 定案 | 见下文 |

## 事件 1：内联同步下载死锁

**现象**：`download_history_data`（600036.SH 新数据）RPC 调用 120s/300s 超时；cadence 从 14:46:11 起永久冻结；ping 全部超时。

**根因**：同步下载函数（`down_history_data` 注入全局）从 **drain/adjust 线程**调用——QMT 的下载完成回调需要回到该线程投递，而该线程正阻塞在下载等待里 → 死锁。与 #202（信用查询回调同款机制）同类：**等待回调的调用绝不能放在回调投递的目标线程上**。

**修复**：下载任务内存模式——redis_client=None 时任务入进程内注册表，由后台守护线程逐只执行（挂起只挂 worker，桥本体免疫）；`pump_download_jobs` 内存模式返回 None。

**误判记录**：第一次归因为"原生 xtdata 连接罚金（60-90s）"——方向对但机制错（是死锁不是慢查询）；两次超时测试（120s/300s）期间桥持续死锁，复测数据全部失真。**教训：死锁怀疑成立时，先重启恢复环境再测，不要在 wedged 环境上堆数据。**

## 事件 2：background_threads=True 回退

**现象**：切 True 后 ping 108-110ms（预期亚毫秒）、query_orders 30s+ 超时、submit 3.1s。

**归因过程**：
1. 先误判 cash=1000.0 为数据错误 → 用户核实为**其他终端自动买入的真实账户状态** → 再误判为"模拟柜台执行测试单" → 最终确认：**账户是多终端共享的，其他终端的自动交易在动资金**。两次误判的共同根源：没把"账户被外部共享"纳入事实清单。
2. ping 108ms 的根因：**QMT 进程内 GIL 调度**——listener 线程事件唤醒后仍需等 QMT 主侧让出 GIL，实测每次 ≈ 1 个 adjust tick。上游 #104 已量化（"each time a background thread acquires the GIL costs ~1 adjust tick (~100ms)"），且上游自己因此做了 drain 每拍限时（#183/#188/#304）——**drain 模式就是上游对 GIL 问题的官方答案**，我不该绕开它。

**定案**：`rpc_background_threads=False`。真正的问题不是"线程模式坏了"，是 **QMT 进程内任何后台线程都拿不到亚毫秒**——实时性需求由总线（外部进程自己执行，无 GIL 竞争）承接。

**遗留改进（已实施）**：submit_order/cancel_order/passorder 不在 LISTENER_DEFERRED 名单——bg 模式下会在接收线程内联执行并阻塞 3s（结算等待）级联。已补入名单：订单族一律回 adjust 线程（与 #252 语义一致）。

## 附带 bug（验证工具链挖出的）

| bug | 根因 | 修复 |
|---|---|---|
| shm 应答 rid 截断 31 字节 vs 客户端完整串比对 | uuid4().hex 32 位也超长；单测 rid 全短，漏测 | 前缀比对 + 解码后完整校验 + 长 rid 回归测试 |
| 注册表/ring mmap 生命周期 | section 随最后句柄关闭销毁，订阅者关闭映射后下一个访问者新建全空段 | 发布方/订阅方长期持有映射 |
| test_xtquant_shim_import 用 py3.7+ subprocess 参数 | capture_output/text 是 3.7+ | PIPE/universal_newlines |
| xtquant shim 用 PEP 562 __getattr__（3.7+） | 沙箱 3.6 不支持 | __class__ 替换法 |
| flat 内嵌占位配置遮蔽外置文件 | _load_local_config 原设计强制内嵌 | 外置文件优先 + 陈旧模块防护 |

## 判定方法论（沉淀）

1. **零 TCP 断言要双重验证**：代码层"没有 socket 调用" + 终端日志"零新增 illegal IP"，缺一不可
2. **REACHABLE ≠ 不可用**：空参 TypeError 只证明分发通——必须带参实测（download_history_data 误判教训）
3. **TIMEOUT ≠ 坏了**：先 dump 请求环/应答环区分"没处理"vs"应答迟到"；再排除外部共享资源的争抢窗口（其他终端交易波次）
4. **账户是多终端共享的**：cash/持仓随时被外部改变——任何"数据异常"先核对终端面板再怀疑代码
5. **性能数据要标注测量环境**：QMT 进程外的亚毫秒（无 GIL 竞争）不能外推到 QMT 进程内（实测 ~100ms）

# 设计规格：shm 零 TCP 传输、进程间发布总线与 ContextInfo 方法面补全

日期：2026-09-16
状态：已实现并实测验收
关联：AGENTS.md（xtquant_big_convert）/ 主仓库 AGENTS.md 网络 IO 行 / 上游 #143 #202 #104 #252 #304

## 1. 背景与硬约束

| 约束 | 来源 | 影响 |
|---|---|---|
| QMT 审计扫描：10s 节拍扫进程 TCP 表，白名单外对端（含回环）即 stopAllStrategy 全面板杀 | 2026-09-14/15 多次实锤（illegal IP: 6379/59298/62619） | 传输层**禁止任何 socket**（含 zmq ipc 的 signaler 回环自连） |
| get_trade_detail_data 只在主策略线程上下文有数据 | 本地配置注释 + #252 | 交易上下文方法必须留在 adjust 线程执行 |
| 后台线程抢 GIL ≈ 100ms/tick（#104 量化） | 上游 zmq 传输注释 | 后台线程模式无延迟收益（实测终审回退） |
| 沙箱 Python 3.6.8 | 终端内置 | 无 3.7+ 语法、无 multiprocessing.shared_memory |
| 账户 52625295 为多终端共享账户，其他终端自动交易 | 用户核实 | 下单测试与其他终端策略互相影响；账户状态（cash）随时可能被外部改变 |

## 2. 设计

### 2.1 shm RPC 传输（transports/shm_transport.py）

替代 redis/zmq 的同机传输。零 socket：mmap 命名 section（pagefile 支撑）+ 内核事件 + 互斥体。

**协议 v1（几何为常量，双端不一致直接报错）**：
- 请求环：64 槽 × 64KB，多写者（总线互斥体守护 seq 分配→写帧→header 推进的临界区）
- 应答环：每客户端线程私有 section（4 槽 × 4MB）+ 私有事件，名字随请求信封走（`reply_shm`/`reply_evt`，shm 版 ROUTER identity）
- 帧：seq u64（0=空，最后盖戳）+ request_id 31B + length/flags + payload（≥16KB 自动 zlib）
- 帧容量超限：明确报错/错误信封，不静默截断

**关键决策**：
- rid 只存 31 字节 → 客户端比对用同长前缀，完整一致性由解码后 `response["request_id"]` 兜底（uuid4().hex 32 位也会截断，等值比对永远失配——覆盖率实测踩过）
- 应答事件 auto-reset + 100ms 轮询片兜底：单订阅者真推送，多订阅者有界滞后
- `LISTENER_DEFERRED_METHODS` 补入订单族（submit_order/cancel_order/passorder 等 5 个）：订单的结算等待在接收线程上会阻塞 3s+ 并级联（实测），一律回 adjust 线程执行

**双模式**：`background_threads=True` 事件驱动 listener；`=False` 纯 drain（adjust tick 调 `drain_request_queue`）。**本部署定案 False**（见 postmortem：QMT 进程内 GIL 使后台线程无延迟收益）。

### 2.2 进程间发布总线（pub_bus.py）

redis pub/sub 的同机等价物。环形缓冲（64 槽 × 64KB，topic+seq+payload）+ 注册表（16 槽订阅者事件名）+ 总线互斥体。

- 发布：写帧（互斥体内）→ 遍历注册表 SetEvent 每个订阅者事件。微秒级，QMT 回调线程/handlebar/外部进程均可直调
- 订阅：登记独立事件（1:1 唤醒）+ 独立游标（只收订阅后的消息）；`listen()` 生成器 = redis pubsub.listen 体验
- 丢包可检测：环覆盖时游标跳变 → `__bus_lost__` 通告（丢失数量可测）
- **生命周期铁律**：ring/registry 的 mmap 由发布方与订阅方**长期持有**——section 随最后一个句柄关闭销毁，关闭后下一个访问者会新建全空段（实测坑）
- 语义边界：at-most-once、默认不持久化、同机同登录会话；慢消费者靠覆盖检测知情

### 2.3 下载任务内存模式（download_jobs.py）

Big QMT 的历史数据补充 = 注入全局 `down_history_data`（v10 生产同款通道，实测可用）。**内联同步调用在 drain 线程上死锁**（下载完成回调落在被阻塞线程，2026-09-16 14:46 cadence 冻结实测）→ `redis_client=None` 时回退：

- 进程内注册表 + 后台守护线程逐只下载（挂起只挂 worker，桥本体免疫）
- 进度按只更新（done/total/state），失败保留进度与错误
- `pump_download_jobs` 内存模式返回 None（worker 自驱动，无需 tick 泵）

### 2.4 ContextInfo 方法面补全（12 个 RPC）

穷举探针（src/sector_capability_probe.py）实锤终端存在而桥未暴露：get_finance、get_universe、get_scale_and_rank（实测单参数）、get_scale_and_stock、get_largecap/midcap/smallcap、is_suspended_stock、stockcode_in_rzrk、is_fund/is_stock/is_future。全部走 `_call_context` 通道，注册进 MARKET_DATA_METHODS。

### 2.5 构建脚本修正

- 外置 `bigqmt_signal_trader_local_config.py` **优先于**内嵌占位块（原占位块 YOUR_ACCOUNT_ID 遮蔽外置文件，桥以占位账号+zmq 运行）
- `transport=shm` 显式放行（其余仍强制 zmq——no-redis 构建的身份）
- GBK 兼容：flat 以终端编码写出，源码禁用 GBK 外字符（↔ 等）

## 3. 调优定案

| 参数 | 定案值 | 依据 |
|---|---|---|
| schedule_adjust_interval | **10nMilliSecond** | ping 50-110ms → 1.85-8.91ms（27-60 倍） |
| drain_max_items | 50 | 单拍吞吐上限 ×2.5 |
| download_job_chunk_size / max_wall_seconds | 20 / 1.0s | 下载每拍推进更多 |
| rpc_background_threads | **False 定案** | GIL：后台线程无延迟收益（#104 + 实测终审） |

## 4. 验证数据（全部实测）

- shm RPC 跨进程往返 0.15ms；总线推送亚毫秒；总线双向 e2e 2/2 + 2/2
- 141 个 RPC 方法全覆盖验证：22 PASS + 119 REACHABLE（含带参升级 31 PASS）+ 2 归因超时
- 60 并发下单 4 单/s 全受理进模拟柜台，60/60 唯一 id；真单路径经用户核实成交（cash 63464.87→1000.0）
- 调优后 ping avg 1.85-8.91ms；查询 15-16ms 稳定
- 稳定性：零审计违规（多天）、零消息丢失（87+ 请求环 dump 实证）

## 5. 边界与非目标

- Windows-only、同机同登录会话；跨机部署用上游 redis/zmq
- 公式系统 / 板块写 / L2 / 部分 download：**终端 ContextInfo 阉割**（穷举探针实锤），桥无法补——数据需求走外部源
- 全推行情推送：shm 无推送通道，quote push 显式禁用（订阅类 RPC 返回明确错误）
- 订单路径为真网关（status=SUBMITTED=真 passorder）：测试下单会进真实（共享）账户

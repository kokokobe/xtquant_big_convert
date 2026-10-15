#coding:gbk
"""exec 推送环写端（D2.2-R1：order/deal 回调实时进 SHM，供 Rust 侧唤醒读取）。

为什么存在
----------
BigQMT 的 order_callback / deal_callback（委托/成交实时主推，仅实盘模式生效）
此前的产物只进 redis/quote-push 主推；shm 部署没有 redis，rust_engine 只能
轮询 QMT 本地缓存（get_trade_detail_data），本地缓存刷新滞后于柜台。本模块
把回调现场直接写进一个命名共享内存环 + SetEvent，rust 侧
``ShmRing::open(section, event, mutex, 256, 16384)`` + ``poll_frames()`` +
``wait_event(timeout_ms)`` 即可亚毫秒级拿到委托/成交事实。

协议（.superpowers/sdd/2026-10-14-d2-2-r1-exec-push/protocol.md，两端口逐字
一致，不得偏离）：

* 段名 ``{prefix}_exec_{acct}``、事件 ``{prefix}_exec_evt_{acct}``、互斥体
  ``{prefix}_exec_mtx_{acct}``（prefix 默认 ``bigqmt_shm``；acct 经 shm_transport
  同款 ``_sanitize``）；
* 帧格式逐字复用 ``transports/shm_transport.py`` 请求线：header ``<4sIIIQI``
  （magic ``b"BQSH"``, version=2, slot_count, slot_size, write_seq, lost）+
  slot 头 48B（seq u64, id char[32] 截 31 字节, length u32, flags u32, payload）；
* 几何 slot_count=256, slot_size=16*1024 —— 协议常量，不吃配置；
* payload：UTF-8 JSON 单行 ``{"kind":"order"|"deal", "row":{...}}``，row 字段
  由调用方按 Task 11 契约归一化好，本模块只包信封不做字段加工。

写端纪律（回调线程零阻塞零抛错）：写端复用请求线的帧写入原语（互斥体内
seq 分配 + 帧写入 + header bump 一个临界区），每帧写完 SetEvent 一次；任何
异常不上抛，丢帧计入内部 ``drops`` 计数器。
"""

import ctypes
import json
import struct

from .transports.shm_transport import (
    _HDR_BYTES,
    _MUTEX_WAIT_MS,
    _WAIT_ABANDONED,
    _WAIT_OBJECT_0,
    _Ring,
    _kernel32,
    _sanitize,
)

# -- 协议几何（两端常量一致；改几何必须两端同步升 version） --------------------
EXEC_SLOT_COUNT = 256
EXEC_SLOT_BYTES = 16 * 1024
EXEC_SECTION_TOTAL = _HDR_BYTES + EXEC_SLOT_COUNT * EXEC_SLOT_BYTES
# slot 的 id 字段（char[32]）：exec 环不按 id 路由应答，固定填 "exec"。
EXEC_FRAME_ID = "exec"
DEFAULT_SHM_PREFIX = "bigqmt_shm"


class ExecPushError(Exception):
    """exec 推送环初始化 / 写帧失败。"""


class ExecPushPublisher(object):
    """exec 环的单写端：publish_order / publish_deal 进环 + SetEvent。

    段/事件/互斥体按名字幂等（CreateEventW/CreateMutexW 对同名对象返回既有
    句柄，mmap tagname 同名映射同一 pagefile section），策略重跑/重复 init
    不会重复建段；write_seq 每帧从 header 现读 +1，跨 publisher 实例保持
    单调，Rust 读端游标因此永不回退。
    """

    def __init__(self, shm_prefix, account_id, print_prefix="[bigqmt_exec_push]"):
        self.print_prefix = str(print_prefix or "[bigqmt_exec_push]")
        self.account_id = str(account_id or "")
        # 丢帧计数（写不进环的帧：互斥体超时 / 帧超容量 / 段损坏）。回调线程
        # 不抛错，掉帧只能从这里（以及 header lost）看出来。
        self.drops = 0
        self.published = 0
        prefix = str(shm_prefix or DEFAULT_SHM_PREFIX)
        acct = _sanitize(account_id)
        self.section_name = "%s_exec_%s" % (prefix, acct)
        self.event_name = "%s_exec_evt_%s" % (prefix, acct)
        self.mutex_name = "%s_exec_mtx_%s" % (prefix, acct)
        self._ring = _Ring(
            self.section_name, EXEC_SECTION_TOTAL, EXEC_SLOT_COUNT, EXEC_SLOT_BYTES)
        self._k32 = _kernel32()
        # 自动复位、初始无信号 —— 与请求线事件同款：多次 SetEvent 合并成
        # 一次唤醒，读端醒来后全量 poll（丢信号只损延迟不损消息）。
        self._event = self._k32.CreateEventW(None, 0, 0, self.event_name)
        if not self._event:
            raise ExecPushError(
                "CreateEventW(%s) failed: error=%s"
                % (self.event_name, ctypes.get_last_error()))
        self._mutex = self._k32.CreateMutexW(None, 0, self.mutex_name)
        if not self._mutex:
            raise ExecPushError(
                "CreateMutexW(%s) failed: error=%s"
                % (self.mutex_name, ctypes.get_last_error()))

    # -- 发布 ---------------------------------------------------------------
    def publish_order(self, row):
        """order 契约行入环。任何异常吞掉并计 drops（不向回调线程抛错）。"""
        self._publish("order", row)

    def publish_deal(self, row):
        """deal 契约行入环。任何异常吞掉并计 drops（不向回调线程抛错）。"""
        self._publish("deal", row)

    def _publish(self, kind, row):
        try:
            payload = json.dumps(
                {"kind": kind, "row": dict(row or {})},
                ensure_ascii=False, default=str).encode("utf-8")
            self._write_frame(payload)
            self.published += 1
        except Exception:
            self.drops += 1

    def _write_frame(self, payload):
        # 发布顺序化与 shm 请求线 send_request 同款：互斥体内 seq 分配 + 帧
        # 写入 + header bump 是一个临界区，读端看到 write_seq=N 即知
        # slot <= N 每帧完整（seq 戳最后落，读端永不见半帧）。
        state = int(self._k32.WaitForSingleObject(
            ctypes.c_void_p(self._mutex), _MUTEX_WAIT_MS))
        if state not in (_WAIT_OBJECT_0, _WAIT_ABANDONED):
            raise ExecPushError("exec ring mutex wait failed (%d)" % state)
        try:
            seq = self._ring.write_seq() + 1
            self._ring.write_slot(seq, EXEC_FRAME_ID, payload)
            self._ring._bump_seq(seq)
            # D2.3: sync writer-side drop counter into the header lost
            # field (u32 LE @24); reader computes delta against its baseline.
            self._ring._map.seek(24)
            self._ring._map.write(struct.pack('<I', int(self.drops) & 0xFFFFFFFF))
        finally:
            self._k32.ReleaseMutex(self._mutex)
        self._k32.SetEvent(self._event)

    # -- 生命周期 -----------------------------------------------------------
    def close(self):
        """释放句柄与映射。段由最后一个映射者消失（pagefile 支撑）。"""
        for handle in (self._mutex, self._event):
            if handle:
                try:
                    self._k32.CloseHandle(handle)
                except Exception:
                    pass
        self._mutex = None
        self._event = None
        if self._ring is not None:
            self._ring.close()
            self._ring = None

    def __repr__(self):
        return "<%s account_id=%r section=%s drops=%d published=%d>" % (
            self.__class__.__name__, self.account_id, self.section_name,
            self.drops, self.published)

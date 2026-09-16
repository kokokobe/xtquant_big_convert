#encoding:utf-8
r"""进程间发布总线（QMT 桥 <-> 外部脚本）：mmap 环形缓冲 + 每订阅者命名事件。

定位：redis pub/sub 的同机等价物，零 TCP（QMT 审计扫描在 TCP 表里看不到它）。
原语实测：mem_share_probe v4（跨进程 section 共享 + 事件推送 0.0ms）。

拓扑
----
* 发布方（QMT 桥内 handlebar/deal_callback/RPC，或外部进程直连）：
  在共享环形缓冲里发布 (topic, payload) 帧 + 逐个唤醒已注册订阅者的事件。
* 订阅方（每实例一条独立线）：启动时在注册表里登记自己的事件名 →
  等自己的事件（auto-reset, 1:1 唤醒）→ 按游标读新帧 → 按 topic 过滤 → yield。

语义（对照 redis pub/sub）
--------------------------
* 推送延迟 0.1ms 级；at-most-once、默认不持久化（调用方可自行写 sqlite）
* 慢消费者：环（64 槽）被覆盖时游标跳变 → 订阅方收到 __bus_lost__ 通告，
  丢失数量可检测——不会静默错乱
* 多订阅者：各自独立游标 + 独立事件，互不干扰
* 跨机器：不支持（同机同登录会话；Local 命名空间）

帧格式（协议 v1，几何为常量）
----------------------------
section header 64B: magic b"BQPB" / version u32 / slot_count u32 / slot_size u32
                    / write_seq u64 / lost u32
slot: seq u64（0=空） / topic char[32] / length u32 / flags u32 / payload

注册表（订阅者事件名登记）：16 槽 × 64B（state u32 + name 60B），总线互斥体守护。

payload 为 JSON（ensure_ascii=False → utf-8）；超容量直接报错（调用方自行压缩）。
"""
import ctypes
import json
import mmap
import os
import struct
import threading
import time
import uuid

HDR_BYTES = 64
MAGIC = b"BQPB"
VERSION = 1
SLOT_HDR_BYTES = 48          # seq(8) + topic(32) + length(4) + flags(4)
SLOT_COUNT = 64
SLOT_BYTES = 64 * 1024
TOTAL_BYTES = HDR_BYTES + SLOT_COUNT * SLOT_BYTES
FLAG_ZLIB = 0x1
ZLIB_MIN_BYTES = 16 * 1024
TOPIC_BYTES = 32
CAPACITY = 64                # 注册表容量 = 最大并发订阅者

_HDR_FMT = "<4sIIIQI"
_WAIT_OBJECT_0 = 0
_MUTEX_WAIT_MS = 5000


def _kernel32():
    if os.name != "nt":
        raise RuntimeError("pub bus is Windows-only (os.name=%r)" % os.name)
    dll = ctypes.WinDLL("kernel32", use_last_error=True)
    dll.CreateEventW.restype = ctypes.c_void_p
    dll.CreateEventW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_wchar_p]
    dll.OpenEventW.restype = ctypes.c_void_p
    dll.OpenEventW.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_wchar_p]
    dll.SetEvent.restype = ctypes.c_int
    dll.SetEvent.argtypes = [ctypes.c_void_p]
    dll.WaitForSingleObject.restype = ctypes.c_uint32
    dll.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    dll.CreateMutexW.restype = ctypes.c_void_p
    dll.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
    dll.ReleaseMutex.restype = ctypes.c_int
    dll.ReleaseMutex.argtypes = [ctypes.c_void_p]
    dll.CloseHandle.restype = ctypes.c_int
    dll.CloseHandle.argtypes = [ctypes.c_void_p]
    return dll


EVENT_MODIFY_STATE = 0x0002


def _sanitize(account_id):
    text = str(account_id or "")
    cleaned = "".join(ch for ch in text if ch.isalnum() or ch in "-_.")
    return cleaned or "default"


def bus_names(account_id, prefix="bigqmt_bus"):
    """总线的内核对象名（发布方/订阅方/外部工具共用）。"""
    acct = _sanitize(account_id)
    return {
        "ring": "%s_ring_%s" % (prefix, acct),
        "mutex": "%s_mtx_%s" % (prefix, acct),
        "subs": "%s_subs_%s" % (prefix, acct),
    }


class BusError(RuntimeError):
    pass


class _Ring(object):
    """共享环形缓冲：多写者（互斥体守护发布）+ 多读者（各自游标）。"""

    def __init__(self, name):
        self.name = name
        self._map = mmap.mmap(-1, TOTAL_BYTES, tagname=name)
        if self._map.read(4) != MAGIC:
            self._map.seek(0)
            self._map.write(struct.pack(_HDR_FMT, MAGIC, VERSION,
                                        SLOT_COUNT, SLOT_BYTES, 0, 0))
            self._map.flush()
        magic, ver, sc, ss, wseq, lost = struct.unpack_from(_HDR_FMT, self._map, 0)
        if magic != MAGIC or ver != VERSION:
            self.close()
            raise BusError("bus section %r magic/version mismatch: %r v%d"
                           % (name, magic, ver))
        if HDR_BYTES + sc * ss != TOTAL_BYTES:
            self.close()
            raise BusError("bus section %r geometry mismatch" % name)

    def close(self):
        try:
            self._map.close()
        except Exception:
            pass

    def write_seq(self):
        return struct.unpack_from(_HDR_FMT, self._map, 0)[4]

    def _slot_offset(self, seq):
        return HDR_BYTES + ((seq - 1) % SLOT_COUNT) * SLOT_BYTES

    def read_frame(self, seq):
        off = self._slot_offset(seq)
        slot_seq = struct.unpack_from("<Q", self._map, off)[0]
        if slot_seq != seq:
            return None
        topic = self._map[off + 8: off + 40].split(b"\x00")[0].decode("utf-8", "replace")
        length, flags = struct.unpack_from("<II", self._map, off + 40)
        if length == 0 or length > SLOT_BYTES - SLOT_HDR_BYTES:
            return None
        payload = self._map[off + 48: off + 48 + length]
        if flags & FLAG_ZLIB:
            import zlib
            payload = zlib.decompress(payload)
        return topic, payload

    def write_frame(self, seq, topic, payload):
        off = self._slot_offset(seq)
        flags = 0
        body = payload
        if len(body) >= ZLIB_MIN_BYTES:
            import zlib
            body = zlib.compress(body, 1)
            flags |= FLAG_ZLIB
        capacity = SLOT_BYTES - SLOT_HDR_BYTES
        if len(body) > capacity:
            raise BusError("bus frame too large: %d > %d bytes" % (len(body), capacity))
        tb = topic.encode("utf-8")[:TOPIC_BYTES - 1]
        self._map[off + SLOT_HDR_BYTES: off + SLOT_HDR_BYTES + len(body)] = body
        struct.pack_into("<II", self._map, off + 40, len(body), flags)
        self._map[off + 8: off + 8 + len(tb)] = tb
        struct.pack_into("<Q", self._map, off, seq)


class BusPublisher(object):
    """发布方：publish(topic, data) → 环形缓冲 + 唤醒全部已注册订阅者。

    线程/进程安全：帧发布在总线互斥体内完成（seq 分配、槽写入、header 推进
    是一个临界区），QMT 回调线程与外部进程可并发发布。
    """

    name = "bus"

    def __init__(self, account_id, prefix="bigqmt_bus"):
        self.account_id = str(account_id or "")
        self._names = bus_names(account_id, prefix)
        self._k32 = _kernel32()
        self._ring = None
        self._mutex = None
        self._subs_map = None     # 注册表映射：发布方持有，保证注册表不随订阅者
        self._sub_events = {}     # 事件名 -> handle（OpenEventW 缓存）  关闭而销毁
        self._seq = 0

    def _ensure(self):
        if self._ring is None:
            self._ring = _Ring(self._names["ring"])
            self._mutex = self._k32.CreateMutexW(None, 0, self._names["mutex"])
            if not self._mutex:
                raise BusError("CreateMutexW failed: %d" % ctypes.get_last_error())
            self._seq = self._ring.write_seq()
        if self._subs_map is None:
            # 发布方长期持有注册表映射——否则最后一个订阅者关闭映射的瞬间，
            # pagefile section 销毁，下一个订阅者会新建全空注册表（实测坑）。
            self._subs_map = mmap.mmap(-1, 64 + CAPACITY * 64,
                                       tagname=self._names["subs"])

    def _signal_subscribers(self):
        """遍历注册表，唤醒每个已登记订阅者（句柄按名字缓存）。"""
        subs = self._subs_map
        for i in range(CAPACITY):
            off = 64 + i * 64
            state = struct.unpack_from("<I", subs, off)[0]
            if state != 1:
                continue
            name = subs[off + 4: off + 64].split(b"\x00")[0].decode("utf-8", "replace")
            if not name:
                continue
            handle = self._sub_events.get(name)
            if handle is None:
                handle = self._k32.OpenEventW(EVENT_MODIFY_STATE, 0, name)
                if not handle:
                    continue          # 订阅者已退出：跳过
                self._sub_events[name] = handle
            self._k32.SetEvent(handle)

    def publish(self, topic, data):
        """发布一条消息。data 为 dict/str/bytes（JSON 序列化）。返回全局 seq。"""
        self._ensure()
        if isinstance(data, (dict, list)):
            payload = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        elif isinstance(data, str):
            payload = data.encode("utf-8")
        elif isinstance(data, bytes):
            payload = data
        else:
            payload = str(data).encode("utf-8")
        topic = str(topic or "default")
        state = self._k32.WaitForSingleObject(ctypes.c_void_p(self._mutex), _MUTEX_WAIT_MS)
        if state not in (0, 0x80):     # WAIT_OBJECT_0 / WAIT_ABANDONED
            raise BusError("bus publish mutex timeout")
        try:
            seq = self._ring.write_seq() + 1
            self._ring.write_frame(seq, topic, payload)
            struct.pack_into("<Q", self._ring._map, 16, seq)
            self._ring._map.flush()
        finally:
            self._k32.ReleaseMutex(self._mutex)
        self._signal_subscribers()
        return seq


_PUBLISHERS = {}
_PUBLISHERS_LOCK = threading.Lock()


def get_publisher(account_id, prefix="bigqmt_bus"):
    """进程内单例（按账号）：QMT 桥的 RPC handler 与信号生产者共用。"""
    key = "%s|%s" % (prefix, _sanitize(account_id))
    with _PUBLISHERS_LOCK:
        pub = _PUBLISHERS.get(key)
        if pub is None:
            pub = BusPublisher(account_id, prefix=prefix)
            _PUBLISHERS[key] = pub
        return pub


class InboxReceiver(object):
    """后台线程订阅总线 → 有界收件箱；drain() 取走全部累积消息。

    QMT 桥内用法：RPC `bus_inbox_start` 启动（幂等）/ `bus_inbox_drain` 拉取。
    注意：消息若要触发下单，必须转入 pending 队列在 adjust tick 执行
    （get_trade_detail_data 只在主线程有数据，#252）——接收线程只做入箱。
    """

    def __init__(self, account_id, topics=None, prefix="bigqmt_bus",
                 maxlen=1000, poll_seconds=0.05):
        import collections
        self._inbox = collections.deque(maxlen=int(maxlen))
        self._lock = threading.Lock()
        self._topics = set(topics) if topics else None
        self._sub = BusSubscriber(account_id, prefix=prefix,
                                  poll_seconds=poll_seconds)
        self._thread = None

    def start(self):
        if self._thread is not None:
            return
        self._sub.subscribe()
        topics = self._topics

        def _run():
            try:
                for topic, data, seq in self._sub.listen(topics=topics):
                    with self._lock:
                        self._inbox.append({"topic": topic, "data": data,
                                            "seq": seq, "ts": time.time()})
            except Exception:
                pass     # 守护收件线程: 桥停止时静默退出

        self._thread = threading.Thread(target=_run,
                                        name="bigqmt-bus-inbox", daemon=True)
        self._thread.start()

    def drain(self):
        with self._lock:
            items = list(self._inbox)
            self._inbox.clear()
        return items

    def stop(self):
        self._sub.close()


_INBOX_RECEIVERS = {}
_INBOX_RECEIVERS_LOCK = threading.Lock()


def get_inbox_receiver(account_id, topics=None, prefix="bigqmt_bus"):
    """进程内单例（按账号+topic 集）：桥内接收外部→QMT 通知的收件箱。"""
    key = "%s|%s|%s" % (prefix, _sanitize(account_id),
                        ",".join(sorted(topics or ())))
    with _INBOX_RECEIVERS_LOCK:
        rx = _INBOX_RECEIVERS.get(key)
        if rx is None:
            rx = InboxReceiver(account_id, topics=topics, prefix=prefix)
            _INBOX_RECEIVERS[key] = rx
        return rx


class BusSubscriber(object):
    """订阅方：subscribe() 后 iterate listen() —— redis pubsub.listen 体验。

    每实例登记一个独立事件（1:1 唤醒）+ 独立游标（从登记时刻的新消息开始）。
    多订阅者互不干扰；环覆盖时收到 __bus_lost__ 通告（丢失数量在消息里）。
    """

    def __init__(self, account_id, prefix="bigqmt_bus",
                 poll_seconds=0.05, client_name=None):
        self.account_id = str(account_id or "")
        self._names = bus_names(account_id, prefix)
        self.poll_seconds = max(float(poll_seconds), 0.02)
        self.client_name = client_name or ("sub-%s" % uuid.uuid4().hex[:8])
        self._k32 = _kernel32()
        self._ring = None
        self._event = None
        self._mutex = None
        self._slot_index = None
        self._cursor = 0
        self._closed = False

    def subscribe(self):
        """登记事件 + 声明游标起点（只收订阅之后发布的消息）。"""
        if self._ring is not None:
            return
        self._ring = _Ring(self._names["ring"])
        self._mutex = self._k32.CreateMutexW(None, 0, self._names["mutex"])
        event_name = "%s_sub_evt" % self._names["subs"]
        self._event_name = "%s_%s" % (event_name, self.client_name)
        self._event = self._k32.CreateEventW(None, 0, 0, self._event_name)
        if not self._event:
            raise BusError("CreateEventW failed: %d" % ctypes.get_last_error())
        # 注册表映射长期持有（发布方也要能读到）——否则最后一个持映射者关闭
        # 的瞬间 section 销毁，下一个订阅者看到的是全空新表（实测坑）。
        self._subs_map = mmap.mmap(-1, 64 + CAPACITY * 64,
                                   tagname=self._names["subs"])
        # 在注册表里占一个槽（写自己的事件名），供发布方唤醒
        state = self._k32.WaitForSingleObject(
            ctypes.c_void_p(self._mutex), _MUTEX_WAIT_MS)
        if state not in (0, 0x80):
            raise BusError("bus registry mutex timeout")
        try:
            slot = None
            for i in range(CAPACITY):
                off = 64 + i * 64
                if struct.unpack_from("<I", self._subs_map, off)[0] == 0:
                    slot = i
                    break
            if slot is None:
                raise BusError("bus subscriber registry full (%d)" % CAPACITY)
            name_b = self._event_name.encode("utf-8")[:59]
            self._subs_map[off + 4: off + 4 + len(name_b)] = name_b
            struct.pack_into("<I", self._subs_map, off, 1)
            self._slot_index = slot
            self._subs_map.flush()
        finally:
            self._k32.ReleaseMutex(self._mutex)
        self._cursor = self._ring.write_seq()

    def _drain(self, topics):
        """读游标之后的新帧，yield (topic, payload_dict, seq)。"""
        target = self._ring.write_seq()
        out = []
        scan_from = max(self._cursor + 1, target - SLOT_COUNT + 1)
        lost = scan_from - (self._cursor + 1)
        if lost > 0:
            out.append(("__bus_lost__", {"lost": lost, "up_to": scan_from - 1},
                        self._cursor))
        for seq in range(scan_from, target + 1):
            frame = self._ring.read_frame(seq)
            if frame is None:
                continue
            topic, payload = frame
            if topics and topic not in topics:
                continue
            try:
                text = payload.decode("utf-8")
                data = json.loads(text)
            except Exception:
                data = text
            out.append((topic, data, seq))
        self._cursor = target
        return out

    def listen(self, topics=None):
        """生成器：持续产出 (topic, data, seq)。Ctrl+C / close() 退出。"""
        if self._ring is None:
            self.subscribe()
        while not self._closed:
            self._k32.WaitForSingleObject(
                ctypes.c_void_p(self._event),
                int(self.poll_seconds * 1000))
            for topic, data, seq in self._drain(topics):
                yield topic, data, seq

    def unsubscribe(self):
        """从注册表摘除自己的事件名（总线槽位回收）。"""
        if self._slot_index is None or self._mutex is None or self._subs_map is None:
            return
        state = self._k32.WaitForSingleObject(
            ctypes.c_void_p(self._mutex), _MUTEX_WAIT_MS)
        if state in (0, 0x80):
            try:
                struct.pack_into("<I", self._subs_map,
                                 64 + self._slot_index * 64, 0)
                self._subs_map.flush()
            except Exception:
                pass
            self._k32.ReleaseMutex(self._mutex)
        self._slot_index = None

    def close(self):
        self._closed = True
        self.unsubscribe()
        for handle in (self._event,):
            if handle:
                try:
                    self._k32.CloseHandle(ctypes.c_void_p(handle))
                except Exception:
                    pass
        self._event = None
        if self._subs_map is not None:
            try:
                self._subs_map.close()
            except Exception:
                pass
            self._subs_map = None
        if self._ring is not None:
            self._ring.close()
            self._ring = None

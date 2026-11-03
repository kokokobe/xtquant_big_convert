# coding: utf-8
"""Server->client event push over a dedicated named pipe (#408).

The RPC pipe (pipe_transport) is request/response with single-threaded I/O
per connection -- a WriteFile concurrent with a pending ReadFile blocks
forever on a synchronous handle. Interleaving server-initiated frames there
would break that serialization, so push gets its own pipe with exactly one
direction -- the server only writes, clients only read -- and no handle
ever sees concurrent read+write.

Same publish/subscriber interface as ZmqQuotePushChannel, so exec events
(order/trade/order_error/cancel_error) and whole-quote frames ride it
unchanged. The subscriber loops a blocking ReadFile with reconnect, filters
topics by prefix (zmq SUBSCRIBE semantics), and owns the same per-subscriber
stop Event as the zmq channel (#403): a join timeout can never leak a zombie
that a later start revives.

任务13（有界隔离）：publish 线程只做一次 JSON 编码并入每客户端有界
队列（帧数+字节双上限），绝不执行 WriteFile、绝不等待；每客户端一个
独立 writer 线程做带截止时间的 WriteFile，watchdog 用 CancelIoEx 取消
超时在途写；队列满/写超时关闭该连接并计入 backpressure_status——
慢读者不可能阻塞 QMT 回调线程。每帧携带 meta={stream_epoch,stream_seq}
（epoch 为启动生成的 opaque 字符串，seq 每原始发布递增），旧读者忽略
meta 不受影响。
"""

import ctypes
import json
import queue
import threading
import time

from .pipe_transport import (
    _BUFFER_BYTES,
    _CONNECT_POLL_SECONDS,
    _kernel32,
    pipe_path,
    DEFAULT_PIPE_NAME,
    ERROR_BROKEN_PIPE,
    ERROR_FILE_NOT_FOUND,
    ERROR_INVALID_HANDLE,
    ERROR_MORE_DATA,
    ERROR_OPERATION_ABORTED,
    ERROR_PIPE_BUSY,
    ERROR_PIPE_CONNECTED,
    GENERIC_READ,
    GENERIC_WRITE,
    INVALID_HANDLE_VALUE,
    OPEN_EXISTING,
    PIPE_READMODE_MESSAGE,
    PIPE_TYPE_MESSAGE,
    PIPE_ACCESS_DUPLEX,
    PIPE_UNLIMITED_INSTANCES,
    PIPE_WAIT,
)


def pipe_events_path(name=DEFAULT_PIPE_NAME, account_id=""):
    """The event-push pipe path, derived from the RPC pipe name so both
    sides compute it from the same config block without discovery (#408)."""
    return pipe_path(str(name or DEFAULT_PIPE_NAME) + "_events", account_id)


class _Client(object):
    """One connected writer: its pipe handle, bounded queue and thread."""

    __slots__ = ("handle", "lock", "queue", "queue_bytes", "thread",
                 "disconnected", "writing_since")

    def __init__(self, handle, queue_capacity):
        self.handle = handle
        self.lock = threading.Lock()
        self.queue = queue.Queue(maxsize=queue_capacity)
        self.queue_bytes = 0
        self.thread = None
        self.disconnected = False
        self.writing_since = 0.0


class PipeQuotePushChannel(object):
    """Named-pipe push channel: bind_address publishes, connect_address subscribes."""

    def __init__(self, bind_address=None, connect_address=None,
                 print_prefix="[bigqmt_quote_push]",
                 queue_capacity=256, write_timeout_seconds=1.0,
                 max_frame_bytes=1048576, max_clients=8,
                 max_client_queue_bytes=8388608):
        self.bind_address = bind_address
        self.connect_address = connect_address
        self.print_prefix = print_prefix
        self.queue_capacity = max(1, int(queue_capacity))
        self.write_timeout_seconds = float(write_timeout_seconds)
        self.max_frame_bytes = max(1, int(max_frame_bytes))
        self.max_clients = max(1, int(max_clients))
        self.max_client_queue_bytes = max(1, int(max_client_queue_bytes))
        self._running = False
        self._accept_thread = None
        self._sub_handles = {}
        self._sub_handles_lock = threading.Lock()
        self._sub_thread = None
        self._subscriber_stop = None
        self._stale_subscriber_threads = []
        self.subscriber_join_seconds = 1.0
        self._client_handle = None
        self._client_handle_lock = threading.Lock()
        # -- 任务13：有界发布状态 --------------------------------------
        self._clients = {}          # handle -> _Client
        self._stream_epoch = ""
        self._stream_seq = 0
        self._test_write_fn = None  # 私有测试接缝，不导出给 RPC
        self._overflows = 0
        self._rejected_oversize = 0
        self._write_timeouts = 0
        self._disconnected_clients = 0
        self._watchdog_thread = None

    def _dll(self):
        return _kernel32()

    def _last_error(self):
        return ctypes.get_last_error()

    # -- server side -------------------------------------------------------
    def start_publisher(self):
        if not self.bind_address:
            raise ValueError("bind_address is required to start a publisher")
        self._running = True
        import uuid
        self._stream_epoch = uuid.uuid4().hex
        self._stream_seq = 0
        self._overflows = 0
        self._rejected_oversize = 0
        self._write_timeouts = 0
        self._disconnected_clients = 0
        self._test_client = None
        thread = threading.Thread(
            target=self._accept_loop, name="bigqmt-quote-push-pipe-accept")
        thread.daemon = True
        thread.start()
        self._accept_thread = thread
        watchdog = threading.Thread(
            target=self._watchdog_loop, name="bigqmt-quote-push-pipe-watchdog")
        watchdog.daemon = True
        watchdog.start()
        self._watchdog_thread = watchdog

    def _accept_loop(self):
        dll, _ = self._dll()
        while self._running:
            # DUPLEX, not OUTBOUND: the client's SetNamedPipeHandleState
            # (message read mode) needs GENERIC_WRITE on its handle, and an
            # OUTBOUND pipe refuses that with ACCESS_DENIED (err=5). The wire
            # is still one-directional in practice -- clients never write.
            handle = dll.CreateNamedPipeW(
                self.bind_address, PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
                PIPE_UNLIMITED_INSTANCES, _BUFFER_BYTES, _BUFFER_BYTES, 0, None)
            if handle == INVALID_HANDLE_VALUE:
                if not self._running:
                    return
                time.sleep(_CONNECT_POLL_SECONDS)
                continue
            connected = dll.ConnectNamedPipe(handle, None)
            if not connected and self._last_error() != ERROR_PIPE_CONNECTED:
                try:
                    dll.CloseHandle(handle)
                except Exception:
                    pass
                continue
            if not self._running:
                try:
                    dll.CloseHandle(handle)
                except Exception:
                    pass
                return
            self._register_client(handle)

    def _register_client(self, handle):
        """每个连接一个有界队列 + 独立 writer 线程；超 max_clients 拒绝。"""
        with self._sub_handles_lock:
            if len(self._clients) >= self.max_clients:
                # 超额连接：立即关闭，不接受（登记仅发生在成功分支）
                try:
                    self._dll()[0].CloseHandle(handle)
                except Exception:
                    pass
                return
            client = _Client(handle, self.queue_capacity)
            self._clients[handle] = client
            self._sub_handles[handle] = threading.Lock()
        client.thread = threading.Thread(
            target=self._writer_loop, args=(client,),
            name="bigqmt-quote-push-pipe-writer")
        client.thread.daemon = True
        client.thread.start()

    def _writer_loop(self, client):
        # 仅由 disconnected（含 stop() 置位）终止：_install_test_writer 可在
        # start_publisher 之前注册，writer 需存活到 stop，不随 _running 退出
        while not client.disconnected:
            try:
                payload = client.queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if payload is None or client.disconnected:
                break  # None = stop() 哨兵：排空后的有界退出
            client.queue_bytes -= len(payload)
            ok = self._write_frame(client, payload)
            if not ok:
                break
        self._drop_client(client)

    def _write_frame(self, client, payload):
        """带截止时间的一次写出；超时由 watchdog 用 CancelIoEx 取消。"""
        if client.disconnected:
            return False
        client.writing_since = time.time()
        try:
            if client.handle is None:
                # 合成测试客户端：写函数可阻塞，watchdog 不适用
                return bool(self._test_write_fn(payload))
            dll, wintypes = self._dll()
            written = wintypes.DWORD()
            with client.lock:
                ok = dll.WriteFile(client.handle, payload, len(payload),
                                   ctypes.byref(written), None)
            return bool(ok)
        except Exception:
            return False
        finally:
            client.writing_since = 0.0

    def _install_test_writer(self, fn):
        """测试接缝：注册一个无句柄合成客户端，其 writer 调用 fn 而非
        WriteFile——不经真实管道即可验证背压/断开语义（不导出给 RPC）。"""
        self._test_write_fn = fn
        if getattr(self, "_test_client", None) is not None:
            return
        client = _Client(None, self.queue_capacity)
        self._test_client = client
        with self._sub_handles_lock:
            self._clients[None] = client
        client.thread = threading.Thread(
            target=self._writer_loop, args=(client,),
            name="bigqmt-quote-push-pipe-test-writer")
        client.thread.daemon = True
        client.thread.start()

    def _watchdog_loop(self):
        """写超时检测：CancelIoEx 取消在途 WriteFile，writer 落败即断开。"""
        while self._running:
            now = time.time()
            with self._sub_handles_lock:
                clients = list(self._clients.values())
            for client in clients:
                started = client.writing_since
                if client.handle is None:
                    continue  # 合成客户端无在途 I/O 可取消
                if started and now - started > self.write_timeout_seconds:
                    self._write_timeouts += 1
                    try:
                        self._dll()[0].CancelIoEx(client.handle, None)
                    except Exception:
                        pass
            time.sleep(0.05)

    def publish(self, topic, data):
        """冻结一帧并入各客户端有界队列；不 WriteFile、不等待、不阻塞回调。

        返回 False 表示本帧被拒（超尺寸）——队列满不假装完整，直接断开
        慢读者并计入 backpressure_status。
        """
        if not self._running:
            return False
        self._stream_seq += 1
        payload = json.dumps(
            {"topic": str(topic), "data": data,
             "meta": {"stream_epoch": self._stream_epoch,
                      "stream_seq": self._stream_seq}},
            ensure_ascii=False, default=str).encode("utf-8")
        if len(payload) > self.max_frame_bytes:
            self._rejected_oversize += 1
            return False
        with self._sub_handles_lock:
            clients = list(self._clients.values())
        if not clients:
            return True  # 无订阅者 no-op（订阅侧负责打底/新鲜度检查）
        for client in clients:
            if client.disconnected:
                continue
            try:
                client.queue.put_nowait(payload)
                client.queue_bytes += len(payload)
            except queue.Full:
                self._overflows += 1
                self._disconnect_client(client)
        return True

    def _disconnect_client(self, client):
        """满队列/写失败：标记断开并取消在途 I/O，writer 自行收尾。"""
        client.disconnected = True
        self._disconnected_clients += 1
        if client.handle is not None:
            try:
                self._dll()[0].CancelIoEx(client.handle, None)
            except Exception:
                pass

    def _drop_client(self, client):
        with self._sub_handles_lock:
            self._clients.pop(client.handle, None)
            self._sub_handles.pop(client.handle, None)
        if client.handle is None:
            return  # 合成客户端无句柄可关
        try:
            self._dll()[0].DisconnectNamedPipe(client.handle)
        except Exception:
            pass
        try:
            self._dll()[0].CloseHandle(client.handle)
        except Exception:
            pass

    def _drop_handle(self, handle):
        dll, _ = self._dll()
        with self._sub_handles_lock:
            self._sub_handles.pop(handle, None)
            client = self._clients.pop(handle, None)
        if client is not None:
            client.disconnected = True
        try:
            dll.DisconnectNamedPipe(handle)
        except Exception:
            pass
        try:
            dll.CloseHandle(handle)
        except Exception:
            pass

    def backpressure_status(self):
        """背压可观测状态：溢出/超尺寸拒绝/写超时/断开数/在队帧数/客户端数。"""
        with self._sub_handles_lock:
            clients = list(self._clients.values())
        queued = sum(c.queue.qsize() for c in clients)
        queued_bytes = sum(c.queue_bytes for c in clients)
        return {
            "overflows": self._overflows,
            "rejected_oversize": self._rejected_oversize,
            "write_timeouts": self._write_timeouts,
            "disconnected_clients": self._disconnected_clients,
            "queued_frames": queued,
            "queued_bytes": queued_bytes,
            "clients": len(clients),
            "stream_epoch": self._stream_epoch,
            "stream_seq": self._stream_seq,
        }

    def publisher_subscriber_count(self):
        """Connected clients right now (diagnostic)."""
        with self._sub_handles_lock:
            return len(self._sub_handles)

    # -- client side -------------------------------------------------------
    def start_subscriber(self, topics, on_msg):
        if not self.connect_address:
            raise ValueError("connect_address is required to start a subscriber")
        # Self-contained, same lifecycle as the zmq channel (#403): never
        # rely on the caller to stop() first, and a join timeout means "still
        # in its callback", not "leaked".
        self._stop_subscriber()
        self._running = True
        stopped = self._subscriber_stop = threading.Event()
        self._sub_thread = threading.Thread(
            target=self._sub_loop,
            args=(set(str(t) for t in (topics or [])), on_msg, stopped),
            name="bigqmt-quote-push-pipe-sub")
        self._sub_thread.daemon = True
        self._sub_thread.start()

    def _connect_client(self, stopped):
        dll, wintypes = self._dll()
        while not stopped.is_set():
            handle = dll.CreateFileW(self.connect_address,
                                     GENERIC_READ | GENERIC_WRITE,
                                     0, None, OPEN_EXISTING, 0, None)
            if handle != INVALID_HANDLE_VALUE:
                # The handle's read mode defaults to BYTE, where a message
                # larger than one ReadFile buffer comes back ok=TRUE in
                # buffer-sized slices and the JSON reassembly below decodes
                # each slice as its own (broken) event -- small frames work,
                # big frames vanish silently. MESSAGE mode is what surfaces
                # ERROR_MORE_DATA (the RPC pipe client sets it too).
                mode = wintypes.DWORD(PIPE_READMODE_MESSAGE)
                dll.SetNamedPipeHandleState(handle, ctypes.byref(mode), None, None)
                return handle
            err = self._last_error()
            if err == ERROR_PIPE_BUSY:
                dll.WaitNamedPipeW(self.connect_address, 200)
            else:
                # ERROR_FILE_NOT_FOUND: the publisher is not up (yet). Retry
                # at the same cadence as the RPC pipe's connect.
                time.sleep(_CONNECT_POLL_SECONDS)
        return None

    def _read_one_message(self, dll, wintypes, handle, buf):
        """One complete message, or None on close/abort (caller decides)."""
        read = wintypes.DWORD()
        chunks = []
        while True:
            ok = dll.ReadFile(handle, buf, _BUFFER_BYTES,
                              ctypes.byref(read), None)
            if read.value:
                chunks.append(ctypes.string_at(buf, read.value))
            if ok:
                return b"".join(chunks)
            err = self._last_error()
            if err == ERROR_MORE_DATA:
                continue
            if err in (ERROR_OPERATION_ABORTED, ERROR_INVALID_HANDLE,
                       ERROR_BROKEN_PIPE):
                return None
            return None

    def _sub_loop(self, topics, on_msg, stopped):
        dll, wintypes = self._dll()
        buf = ctypes.create_string_buffer(_BUFFER_BYTES)
        try:
            while not stopped.is_set():
                handle = self._connect_client(stopped)
                if handle is None:
                    return
                with self._client_handle_lock:
                    self._client_handle = handle
                try:
                    while not stopped.is_set():
                        raw = self._read_one_message(dll, wintypes, handle, buf)
                        if raw is None:
                            # Server restart (reconnect) or stop() (the loop
                            # condition exits).
                            break
                        try:
                            event = json.loads(raw.decode("utf-8"))
                        except Exception:
                            continue
                        topic = str(event.get("topic") or "")
                        if topics and not any(topic.startswith(t) for t in topics):
                            continue
                        try:
                            on_msg(topic, event.get("data"))
                        except Exception as exc:
                            print("%s subscriber callback failed: %s"
                                  % (self.print_prefix, exc))
                finally:
                    with self._client_handle_lock:
                        if self._client_handle is handle:
                            self._client_handle = None
                    try:
                        dll.CloseHandle(handle)
                    except Exception:
                        pass
                if not stopped.is_set():
                    time.sleep(_CONNECT_POLL_SECONDS)
        except Exception:
            return

    def _stop_subscriber(self):
        event = self._subscriber_stop
        if event is not None:
            event.set()
        with self._client_handle_lock:
            handle = self._client_handle
        if handle is not None:
            try:
                dll, _ = self._dll()
                # Wake the blocking ReadFile so the loop sees the set Event;
                # CloseHandle on a handle with pending I/O would block until
                # the read completes (the RPC pipe's stop() learned this the
                # hard way).
                dll.CancelIoEx(handle, None)
            except Exception:
                pass
        thread = self._sub_thread
        if thread is not None and thread.is_alive():
            thread.join(self.subscriber_join_seconds)
            if thread.is_alive():
                self._stale_subscriber_threads.append(thread)
                print("%s subscriber did not stop within %.1fs; its stop "
                      "event is set and it will exit when its callback "
                      "returns (tracked, not abandoned)"
                      % (self.print_prefix, self.subscriber_join_seconds))
        self._sub_thread = None
        self._subscriber_stop = None

    def active_subscribers(self):
        """Live subscriber threads right now; healthy is 0 or 1 (#403)."""
        alive = []
        for thread in self._stale_subscriber_threads:
            if thread.is_alive():
                alive.append(thread)
        self._stale_subscriber_threads = alive
        current = self._sub_thread
        return len(alive) + (1 if current is not None and current.is_alive() else 0)

    # -- both --------------------------------------------------------------
    def stop(self):
        self._running = False
        self._stop_subscriber()
        if self.bind_address:
            # 有界 flush：先让 writer 排空已入队帧（健康 writer 毫秒级），
            # 限时 join 后才强制断开——stop 不丢健康队列里的帧，也不被慢写拖死
            with self._sub_handles_lock:
                clients = list(self._clients.values())
            threads = []
            for client in clients:
                try:
                    client.queue.put_nowait(None)
                except queue.Full:
                    pass
                if client.thread is not None and client.thread.is_alive():
                    threads.append(client.thread)
            for thread in threads:
                thread.join(1.0)
            for client in clients:
                client.disconnected = True
                if client.handle is not None:
                    try:
                        self._dll()[0].CancelIoEx(client.handle, None)
                    except Exception:
                        pass
            try:
                dll, _ = self._dll()
                # Unblock the accept loop: ConnectNamedPipe only returns once
                # somebody connects, so connect to our own pipe once (the RPC
                # transport's stop() uses the same trick).
                handle = dll.CreateFileW(self.bind_address,
                                         GENERIC_READ | GENERIC_WRITE,
                                         0, None, OPEN_EXISTING, 0, None)
                if handle != INVALID_HANDLE_VALUE:
                    dll.CloseHandle(handle)
            except Exception:
                pass
        with self._sub_handles_lock:
            handles = list(self._sub_handles)
        for handle in handles:
            self._drop_handle(handle)
        thread = self._accept_thread
        if thread is not None and thread.is_alive():
            thread.join(1.0)
        self._accept_thread = None
        watchdog = self._watchdog_thread
        if watchdog is not None and watchdog.is_alive():
            watchdog.join(0.5)
        self._watchdog_thread = None

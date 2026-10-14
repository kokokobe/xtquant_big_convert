#encoding:utf-8
"""flat 预加载在 QMT 受限内建环境下必须存活（D2.2-R1 flat 预加载崩溃回归）。

0.3.64 真机（2026-10-14）：exec_push 把 transports 链的首次执行拖进了策略文件
exec 期，而 QMT 沙箱在该阶段 ``import builtins`` 拿到的是被削过名字的存根
（缺 ``RuntimeError`` / ``Exception`` 等异常类）。``_load_local_module`` 用
``dict(_builtins.__dict__)`` 构建模块命名空间，于是懒加载链上第一条模块级
内建名引用 —— ``transports/base.py`` 的 ``class TransportError(RuntimeError)``
—— 直接 ``NameError``；连 ``_load_exec_push`` 的 ``except Exception`` 兜底都
在模块命名空间里解析失败，异常一路穿透 shell 顶层兜底，整个桥起不来。

同阶段唯一被证明完整可用的内建命名空间是 shell exec globals 里的
``__builtins__``（shell 自己的 ``except Exception`` 处理器就跑在它上面）。
修复后模块命名空间的内建名从那里取，``import builtins`` 并入兜底；健康解释
器里两者相同，行为不变。

这两个测试在真实构建产物上复现 QMT 环境（``sys.modules["builtins"]`` 被换成
缺异常类的存根、exec globals 的 ``__builtins__`` 完整），钉住：
1. 预加载全程无 NameError、无 preload failed；
2. 加载器确实从 shell exec globals 取内建名（防止未来重建时把修复删掉）。
"""

import io
import os
import subprocess
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # tests/infra/ -> 工程根
TOOLS = os.path.join(ROOT, "tools")

_built = []


def build_flat():
    """Run the real no-redis flat builder once and return (path, source)."""
    if _built:
        return _built[0]
    handle, out_path = tempfile.mkstemp(suffix="_no_redis_flat.py")
    os.close(handle)
    env = dict(os.environ, BIGQMT_BUILD_OUT=out_path)
    completed = subprocess.run(
        [sys.executable, os.path.join(TOOLS, "build_no_redis_single_file_flat.py")],
        cwd=TOOLS, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if completed.returncode != 0:
        raise AssertionError("flat build failed:\n%s"
                             % completed.stdout.decode("utf-8", "replace"))
    with io.open(out_path, encoding="gbk") as stream:
        source = stream.read()
    _built.append((out_path, source))
    return _built[0]


def _make_stub_builtins(drop=None):
    """QMT 式存根：默认保留类型 / 函数，剥掉全部异常类。

    ``drop`` 可指定只剥部分异常类（双存根降级场景用：真机 concern 形态是
    存根保留 Exception，仅缺 transports/base.py 类基需要的 RuntimeError /
    TimeoutError）。ModuleNotFoundError / NameError 一律保留——加载器自己的
    except 子句和模块级 from-import 兜底依赖它们，缺了就不是在测降级路径。
    """
    import builtins as real_builtins
    drop = set(drop) if drop is not None else None
    stub = types.ModuleType("builtins")
    for name, value in vars(real_builtins).items():
        if (isinstance(value, type) and issubclass(value, BaseException)
                and name not in ("BaseException", "Exception", "NameError",
                                 "ModuleNotFoundError", "ImportError")
                and (drop is None or name in drop)):
            continue
        stub.__dict__[name] = value
    return stub


class FlatPreloadRestrictedBuiltinsTest(unittest.TestCase):
    def setUp(self):
        self._builtins_stub = _make_stub_builtins()
        self._saved_builtins = sys.modules.get("builtins")
        self._saved_import_module = __import__("importlib").import_module
        self._saved_reload = __import__("importlib").reload

    def tearDown(self):
        import importlib
        importlib.import_module = self._saved_import_module
        importlib.reload = self._saved_reload
        if self._saved_builtins is not None:
            sys.modules["builtins"] = self._saved_builtins
        else:
            sys.modules.pop("builtins", None)
        for name in list(sys.modules):
            if name.startswith("bigqmt"):
                sys.modules.pop(name, None)

    def _exec_flat_under_qmt_like_sandbox(self, dual_stub=False):
        path, source = build_flat()
        code = compile(source, path, "exec")
        import builtins as real_builtins
        if dual_stub:
            # concern ① 形态：exec globals 的 __builtins__ 与 import builtins
            # 双双是存根（保留 Exception，缺 RuntimeError / TimeoutError），
            # 合并快照仍缺 transports/base.py 类基 -> 走 _load_exec_push 降级。
            shell_builtins = dict(vars(_make_stub_builtins(
                drop=("RuntimeError", "TimeoutError"))))
            sys.modules["builtins"] = _make_stub_builtins(
                drop=("RuntimeError", "TimeoutError"))
        else:
            shell_builtins = dict(vars(real_builtins))
            sys.modules["builtins"] = self._builtins_stub
        exec_globals = {
            "__name__": "bigqmt_flat_under_test",
            "__file__": path,
            # shell 自己的帧用这份内建名（QMT exec globals 的角色）。
            "__builtins__": shell_builtins,
        }
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            exec(code, exec_globals)  # 崩溃会直接抛 NameError
        return buffer.getvalue(), exec_globals

    def test_preload_survives_stubbed_builtins_module(self):
        """0.3.64 真机崩溃签名：模块命名空间里 RuntimeError / Exception 解析不到。"""
        output, _exec_globals = self._exec_flat_under_qmt_like_sandbox()
        self.assertNotIn("preload failed", output, output)
        self.assertNotIn("load failed", output, output)
        self.assertIn("bigqmt_signal_trader.exec_push", sys.modules)
        exec_push = sys.modules["bigqmt_signal_trader.exec_push"]
        self.assertTrue(hasattr(exec_push, "ExecPushPublisher"))
        # transports 链在预加载期被 exec_push 拉起，异常基类必须可解析。
        base = sys.modules["bigqmt_signal_trader.transports.base"]
        self.assertTrue(hasattr(base, "TransportError"))

    def test_dual_stub_degrades_to_exec_push_disabled(self):
        """concern ① 降级路径：exec globals 与 import builtins 双存根（保留
        Exception、缺 RuntimeError / TimeoutError）-> 合并快照仍缺类基 ->
        transports/base NameError 被 _load_exec_push 的 except Exception 接住
        -> 打印降级标志、_exec_push=None，桥照常起、推送环禁用。"""
        output, exec_globals = self._exec_flat_under_qmt_like_sandbox(dual_stub=True)
        self.assertIn("exec push ring is disabled", output, output)
        # 降级消息自带 "exec_push load failed"（这是 _load_exec_push 的正常
        # 留痕，不是预加载崩溃）——预加载不崩的判据是 shell 顶层兜底没触发。
        self.assertNotIn("bridge preload failed", output, output)
        self.assertNotIn("exec_events preload failed", output, output)
        # shell 后续正常完成：版本留痕、账号配置、init 生成。
        self.assertRegex(output, r"bigqmt_signal_trader \d+\.\d+\.\d+ loaded")
        self.assertIn("local account config loaded", output, output)
        self.assertTrue(callable(exec_globals.get("init")))
        # 推送环写端降级为 None。
        self.assertIsNone(exec_globals.get("_exec_push"))

    def test_loader_sources_module_builtins_from_shell_exec_globals(self):
        """加载器必须从 shell exec globals 的 __builtins__ 取内建名（修复本体），
        不能退回只信 ``import builtins`` 的旧写法。"""
        _path, source = build_flat()
        self.assertIn("_SHELL_BUILTINS_NAMES", source)
        self.assertIn("module_builtins = dict(_SHELL_BUILTINS_NAMES)", source)


if __name__ == "__main__":
    unittest.main()

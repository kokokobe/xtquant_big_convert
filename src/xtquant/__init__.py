"""Optional xtquant import shim backed by Big QMT Redis RPC.

Put this package before the real xtquant package on PYTHONPATH only when the
caller intentionally wants Big QMT RPC compatibility.
"""

# xtconstant / xttype are pure definitions and safe to import eagerly.
#
# xtdata and xttrader are NOT: both reach back into
# bigqmt_signal_trader.xtquant_compat, which itself does
# ``from xtquant.xtconstant import *``. Importing them here closes the loop --
# ``import bigqmt_signal_trader`` then fails with "partially initialized
# module", and only in that direction, so whether it breaks depends on which
# package the caller happens to import first.
#
# Resolved lazily: ``xtquant.xtdata`` and ``from xtquant import xttrader``
# both still work, but the import runs after xtquant_compat has finished
# initialising rather than in the middle of it.
#
# py3.6 note (2026-09-15): module-level ``__getattr__`` is PEP 562 (3.7+).
# The QMT sandbox ships 3.6, so the lazy hook is installed by swapping the
# module's ``__class__`` instead -- the same effect on every supported
# version, and ``dir()`` keeps advertising the lazy submodules.
import sys as _sys
import types as _types

from . import xtconstant, xttype

__all__ = ["xtconstant", "xtdata", "xttrader", "xttype"]

_LAZY_SUBMODULES = ("xtdata", "xttrader")


def _lazy_getattr(name):
    if name in _LAZY_SUBMODULES:
        import importlib

        module = importlib.import_module("." + name, __name__)
        globals()[name] = module      # resolve once, then it is a plain attribute
        return module
    raise AttributeError("module %r has no attribute %r" % (__name__, name))


def _lazy_dir():
    return sorted(set(list(globals()) + list(_LAZY_SUBMODULES)))


class _LazyXtquantModule(_types.ModuleType):
    """Module class carrying the lazy-submodule hook (PEP 562 for py3.6)."""

    def __getattr__(self, name):
        return _lazy_getattr(name)

    def __dir__(self):
        return _lazy_dir()


# Swap the class of THIS module object. Works on py3.6 and 3.7+ alike; on
# 3.7+ it simply supersedes module-level __getattr__, so only one mechanism
# exists and both stay honest.
_sys.modules[__name__].__class__ = _LazyXtquantModule

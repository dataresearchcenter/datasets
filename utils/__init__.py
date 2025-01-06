from importlib import import_module
from typing import Any, TypeAlias

from servicelayer.extensions import get_entry_point

Data: TypeAlias = dict[str, Any]


def get_method(method_name: str):
    # method A: via a named Python entry point
    func = get_entry_point("memorious.operations", method_name)
    if func is not None:
        return func
    # method B: direct import from a module
    if ":" not in method_name:
        raise ValueError("Unknown method: %s", method_name)
    package, method = method_name.rsplit(":", 1)
    module = import_module(package)
    return getattr(module, method)

from __future__ import annotations

from inspect import isawaitable
from typing import Any


async def call_lifecycle(obj: Any, method: str) -> None:
    fn = getattr(obj, method, None)
    if callable(fn):
        result = fn()
        if isawaitable(result):
            await result

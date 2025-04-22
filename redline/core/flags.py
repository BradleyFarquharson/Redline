from __future__ import annotations

from enum import Enum, auto


class Status(Enum):
    OK = auto()
    WARN = auto()
    FAIL = auto()


def evaluate(delta: float, warn: float, fail: float) -> Status:
    """Absolute‑MB delta -> Status."""
    d = abs(delta)
    if d >= fail:
        return Status.FAIL
    if d >= warn:
        return Status.WARN
    return Status.OK 
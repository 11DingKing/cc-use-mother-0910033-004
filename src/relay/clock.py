"""时钟与时间工具。

所有领域判断只依赖注入的时钟，生产用 ``SystemClock``，测试与演示用
``FakeClock``，从而让"超时、批窗口、重启回放"具备确定性。
时间统一使用 UTC ISO-8601 字符串（尾缀 ``Z``），可按字典序比较。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

ISO_SUFFIX = "Z"


def now_iso(dt: datetime) -> str:
    """把 UTC 时间转为可字典序比较的 ISO 字符串。"""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + ISO_SUFFIX


def parse_iso(value: str) -> datetime:
    """解析 ISO-8601 字符串，``Z`` 结尾按 UTC 处理。"""
    if not isinstance(value, str):
        raise ValueError("时间必须是 ISO-8601 字符串")
    text = value.strip()
    if text.endswith(ISO_SUFFIX):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"无法解析时间：{value}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class Clock:
    """时钟协议。"""

    def now(self) -> datetime:
        raise NotImplementedError

    def now_iso(self) -> str:
        return now_iso(self.now())


class SystemClock(Clock):
    """系统 UTC 时钟。"""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FakeClock(Clock):
    """可手动推进的假时钟，用于确定性测试与演示。"""

    def __init__(self, start: datetime | str | None = None) -> None:
        if start is None:
            current = datetime.now(timezone.utc)
        elif isinstance(start, str):
            current = parse_iso(start)
        else:
            current = start.astimezone(timezone.utc)
        self._now = current

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float = 0, **kwargs: float) -> datetime:
        """推进若干秒（或 timedelta 关键字，如 minutes=）。"""
        delta = timedelta(seconds=seconds, **kwargs)
        self._now = self._now + delta
        return self._now

    def set(self, value: datetime | str) -> None:
        self._now = parse_iso(value) if isinstance(value, str) else value.astimezone(timezone.utc)

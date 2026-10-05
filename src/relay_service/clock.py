"""时间抽象：生产用系统时钟，测试用可拨快的假时钟。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol

ISO_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    """真实 UTC 时钟。"""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FakeClock:
    """测试时钟：初始固定，可手动推进。"""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 10, 5, 10, 0, 0, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> datetime:
        self._now += timedelta(seconds=seconds)
        return self._now

    def move_to(self, value: datetime | str) -> datetime:
        self._now = parse_ts(value) if isinstance(value, str) else value.astimezone(timezone.utc)
        return self._now


def to_iso(value: datetime) -> str:
    """固定宽度的 UTC 字符串，可直接按字典序比较。"""
    return value.astimezone(timezone.utc).strftime(ISO_FMT)


def parse_ts(value: str | datetime) -> datetime:
    """接受 ISO 字符串（含结尾 Z）或 datetime，统一返回带时区的 UTC datetime。"""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

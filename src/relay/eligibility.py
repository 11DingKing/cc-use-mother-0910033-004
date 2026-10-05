"""候选筛选：资格、距离与连续服务限制。

筛选规则（任一不满足即剔除）：

1. 候选处于可应召状态（``available=True``，未被本事件其他邀请占用）。
2. 具备岗位要求的全部资格（``required_qualifications`` 为子集）。
3. 连续服务限制：``consecutive_shifts < max_consecutive_shifts``。
4. 距离不超过 ``max_distance_m``（岗位可给出活动坐标与候选坐标的直线距离，
   未提供坐标时使用候选档案上的 ``distance_m``）。

排序：距离升序优先，距离相同按连续服务班次升序，再按候选编号保证全序确定性。
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable

EARTH_RADIUS_M = 6_371_000.0


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """两点直线（球面）距离，单位米。"""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlng / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


@dataclass(frozen=True)
class RankedCandidate:
    candidate_id: str
    distance_m: float
    reason_rank: tuple

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "distance_m": round(self.distance_m, 1),
        }


def effective_distance(candidate: dict[str, Any], location: dict[str, Any] | None) -> float:
    """活动给出坐标且候选有坐标时按直线距离，否则用档案距离。"""
    if (
        location
        and candidate.get("lat") is not None
        and candidate.get("lng") is not None
        and location.get("lat") is not None
        and location.get("lng") is not None
    ):
        return haversine_m(
            candidate["lat"], candidate["lng"], location["lat"], location["lng"]
        )
    return float(candidate["distance_m"])


def is_eligible(
    candidate: dict[str, Any],
    *,
    required_qualifications: Iterable[str],
    max_distance_m: float | None,
    location: dict[str, Any] | None,
    exclude_ids: set[str] | None = None,
) -> tuple[bool, str]:
    """返回（是否合格, 原因）。原因仅在不合格时非空。"""
    if exclude_ids and candidate["id"] in exclude_ids:
        return False, "already_invited"
    if not candidate.get("available", True):
        return False, "unavailable"
    missing = set(required_qualifications) - set(candidate.get("qualifications", []))
    if missing:
        return False, "missing_qualification:" + ",".join(sorted(missing))
    if candidate.get("consecutive_shifts", 0) >= candidate.get("max_consecutive_shifts", 6):
        return False, "consecutive_limit"
    distance = effective_distance(candidate, location)
    if max_distance_m is not None and distance > max_distance_m:
        return False, "too_far"
    return True, ""


def rank_candidates(
    candidates: list[dict[str, Any]],
    *,
    required_qualifications: Iterable[str] = (),
    max_distance_m: float | None = None,
    location: dict[str, Any] | None = None,
    exclude_ids: set[str] | None = None,
) -> tuple[list[RankedCandidate], list[dict[str, Any]]]:
    """筛选并排序候选。

    返回（合格有序列表, 被剔除候选及原因列表，供接口透明展示）。
    """
    required = set(required_qualifications)
    chosen: list[tuple[float, int, str]] = []
    rejected: list[dict[str, Any]] = []
    for c in candidates:
        ok, reason = is_eligible(
            c,
            required_qualifications=required,
            max_distance_m=max_distance_m,
            location=location,
            exclude_ids=exclude_ids,
        )
        distance = effective_distance(c, location)
        if not ok:
            rejected.append({"candidate_id": c["id"], "reason": reason})
            continue
        chosen.append((distance, int(c.get("consecutive_shifts", 0)), c["id"]))
    chosen.sort(key=lambda item: (item[0], item[1], item[2]))
    ranked = [
        RankedCandidate(
            candidate_id=cid,
            distance_m=distance,
            reason_rank=(distance, shifts, cid),
        )
        for distance, shifts, cid in chosen
    ]
    rejected.sort(key=lambda item: item["candidate_id"])
    return ranked, rejected

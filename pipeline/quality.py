"""质量检查模块：定义并执行数据质量规则。"""

import json
import logging
from typing import Callable

from .database import get_connection

logger = logging.getLogger(__name__)


# ── 质量规则定义 ──
# 每条规则: (rule_name, check_func)
# check_func(row) -> str | None  返回 None 表示通过，否则返回问题描述


def _rule_required_fields(row: dict) -> str | None:
    """必填字段完整性检查：title 和 mal_id 必须存在且非空。"""
    if not row.get("mal_id"):
        return "mal_id is missing or zero"
    if not row.get("title") or not str(row["title"]).strip():
        return "title is missing or empty"
    return None


def _rule_score_range(row: dict) -> str | None:
    """评分值域检查：score 若存在则必须在 [0, 10] 范围内。"""
    score = row.get("score")
    if score is not None and score != 0:
        try:
            s = float(score)
            if s < 0 or s > 10:
                return f"score {s} out of range [0, 10]"
        except (ValueError, TypeError):
            return f"score '{score}' is not a valid number"
    return None


def _rule_year_validity(row: dict) -> str | None:
    """年份合法性检查：year 若存在则必须在 [1900, 2030] 范围内。"""
    year = row.get("year")
    if year is not None:
        try:
            y = int(year)
            if y < 1900 or y > 2030:
                return f"year {y} out of expected range [1900, 2030]"
        except (ValueError, TypeError):
            return f"year '{year}' is not a valid integer"
    return None


def _rule_episodes_non_negative(row: dict) -> str | None:
    """集数非负检查：episodes 若存在则必须 >= 0。"""
    episodes = row.get("episodes")
    if episodes is not None:
        try:
            e = int(episodes)
            if e < 0:
                return f"episodes {e} is negative"
        except (ValueError, TypeError):
            return f"episodes '{episodes}' is not a valid integer"
    return None


QUALITY_RULES: list[tuple[str, Callable]] = [
    ("required_fields", _rule_required_fields),
    ("score_range", _rule_score_range),
    ("year_validity", _rule_year_validity),
    ("episodes_non_negative", _rule_episodes_non_negative),
]


def run_quality_checks(mal_ids: list[int] | None = None) -> dict:
    """
    对 mal_anime 表中的记录执行质量检查。

    Args:
        mal_ids: 要检查的 ID 列表。None 表示检查所有尚未匹配的记录。

    Returns:
        {"passed": [mal_id, ...], "failed": {mal_id: [(rule, detail), ...]}}
    """
    passed = []
    failed = {}

    with get_connection() as conn:
        if mal_ids is not None:
            placeholders = ",".join("?" * len(mal_ids))
            rows = conn.execute(
                f"SELECT * FROM mal_anime WHERE mal_id IN ({placeholders})", mal_ids
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM mal_anime").fetchall()

        # 先清除这些 ID 的旧质量问题记录
        ids_to_check = [dict(r)["mal_id"] for r in rows]
        if ids_to_check:
            placeholders = ",".join("?" * len(ids_to_check))
            conn.execute(
                f"DELETE FROM quality_issues WHERE mal_id IN ({placeholders})", ids_to_check
            )

        for row in rows:
            row_dict = dict(row)
            issues = []
            for rule_name, check_fn in QUALITY_RULES:
                detail = check_fn(row_dict)
                if detail:
                    issues.append((rule_name, detail))
                    conn.execute(
                        "INSERT INTO quality_issues (mal_id, rule_name, detail) VALUES (?, ?, ?)",
                        (row_dict["mal_id"], rule_name, detail),
                    )

            if issues:
                failed[row_dict["mal_id"]] = issues
            else:
                passed.append(row_dict["mal_id"])

    logger.info("Quality check: %d passed, %d failed", len(passed), len(failed))
    return {"passed": passed, "failed": failed}

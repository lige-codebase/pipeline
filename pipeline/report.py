"""处理报告模块：生成本轮 + 累计统计。"""

import logging
from .database import get_connection

logger = logging.getLogger(__name__)


def generate_report(round_stats: dict | None = None) -> str:
    """
    生成处理报告。

    Args:
        round_stats: 本轮 pipeline 运行中收集的统计数据：
            {
                "fetched": int,
                "quality_passed": int,
                "quality_failed": int,
                "exact_matched": int,
                "fuzzy_matched": int,
                "unmatched": int,
            }

    Returns:
        格式化的报告字符串
    """
    lines = []
    lines.append("=" * 60)
    lines.append("  Pipeline 处理报告")
    lines.append("=" * 60)

    # ── 本轮统计 ──
    if round_stats:
        lines.append("")
        lines.append("【本轮统计】")
        lines.append(f"  新增获取数据:     {round_stats.get('fetched', 0)}")
        lines.append(f"  质量检查通过:     {round_stats.get('quality_passed', 0)}")
        lines.append(f"  质量检查未通过:   {round_stats.get('quality_failed', 0)}")
        lines.append(f"  精确 ID 匹配:     {round_stats.get('exact_matched', 0)}")
        lines.append(f"  模糊标题匹配:     {round_stats.get('fuzzy_matched', 0)}")
        lines.append(f"  未匹配:           {round_stats.get('unmatched', 0)}")

    # ── 累计统计 ──
    with get_connection() as conn:
        total_anime = conn.execute("SELECT COUNT(*) as c FROM mal_anime").fetchone()["c"]
        total_issues = conn.execute(
            "SELECT COUNT(DISTINCT mal_id) as c FROM quality_issues"
        ).fetchone()["c"]
        total_matched = conn.execute(
            "SELECT COUNT(*) as c FROM match_result WHERE status = 'matched'"
        ).fetchone()["c"]
        total_unmatched = conn.execute(
            "SELECT COUNT(*) as c FROM match_result WHERE status = 'unmatched'"
        ).fetchone()["c"]
        total_review = conn.execute(
            "SELECT COUNT(*) as c FROM match_result WHERE status = 'review'"
        ).fetchone()["c"]
        total_match_records = total_matched + total_unmatched + total_review

        # 置信度区间分布
        confidence_dist = conn.execute("""
            SELECT
                SUM(CASE WHEN confidence >= 0.95 THEN 1 ELSE 0 END) as high,
                SUM(CASE WHEN confidence >= 0.85 AND confidence < 0.95 THEN 1 ELSE 0 END) as medium,
                SUM(CASE WHEN confidence > 0 AND confidence < 0.85 THEN 1 ELSE 0 END) as low,
                SUM(CASE WHEN confidence = 0 THEN 1 ELSE 0 END) as none
            FROM match_result
        """).fetchone()

    lines.append("")
    lines.append("【累计统计】")
    lines.append(f"  总处理条目数:     {total_anime}")
    lines.append(f"  有质量问题条目:   {total_issues}")
    lines.append(f"  已匹配:           {total_matched}")
    lines.append(f"  未匹配:           {total_unmatched}")
    lines.append(f"  待审查:           {total_review}")
    if total_match_records > 0:
        match_rate = total_matched / total_match_records * 100
        lines.append(f"  总匹配率:         {match_rate:.1f}%")
    else:
        lines.append(f"  总匹配率:         N/A")

    lines.append("")
    lines.append("【置信度区间分布】")
    if confidence_dist:
        lines.append(f"  [0.95, 1.0]  高置信度:  {confidence_dist['high'] or 0}")
        lines.append(f"  [0.85, 0.95) 中置信度:  {confidence_dist['medium'] or 0}")
        lines.append(f"  (0, 0.85)    低置信度:  {confidence_dist['low'] or 0}")
        lines.append(f"  0            无匹配:    {confidence_dist['none'] or 0}")

    lines.append("")
    lines.append("=" * 60)

    report = "\n".join(lines)
    return report

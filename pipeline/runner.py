"""Pipeline 主流程：编排所有阶段，支持断点恢复。"""

import logging
from typing import Optional

from .database import init_db, get_connection, get_checkpoint, set_checkpoint, get_unmatched_passed_ids
from .fetcher import fetch_incremental
from .quality import run_quality_checks
from .matcher import run_matching
from .exporter import export_jsonl
from .report import generate_report

logger = logging.getLogger(__name__)


def run_pipeline(
    max_pages: int = 2,
    skip_fetch: bool = False,
    skip_match: bool = False,
    export: bool = True,
    export_path: Optional[str] = None,
    source: str = "api",
    csv_path: Optional[str] = None,
    max_items: Optional[int] = None,
):
    """
    执行端到端增量 pipeline。

    流程: fetch → quality check → match → export → report

    断点恢复策略（每个阶段都会执行，各自内部实现增量/幂等）:
    - fetch 阶段: checkpoint 记录已获取的最后页码/CSV行号，重启从断点继续加载
    - quality 阶段: 幂等操作，对所有未匹配的记录重跑无副作用
    - match 阶段: 通过 get_unmatched_passed_ids() 自动排除已匹配 ID，
                  match_stage checkpoint 记录子阶段（exact/fuzzy），从断点继续

    Args:
        max_pages: API 模式下本轮最多获取的页数 (每页 25 条)
        skip_fetch: 跳过获取阶段（用于只重跑后续阶段）
        skip_match: 跳过匹配阶段
        export: 是否在最后导出 JSONL
        export_path: 自定义导出路径
        source: 数据源类型 "api" 或 "csv"
        csv_path: CSV 模式下的文件/目录路径
        max_items: CSV 模式下本轮最多加载的条目数
    """
    logger.info("Pipeline starting…")
    init_db()

    round_stats = {
        "fetched": 0,
        "quality_passed": 0,
        "quality_failed": 0,
        "exact_matched": 0,
        "fuzzy_matched": 0,
        "unmatched": 0,
    }

    # ── Stage 1: Fetch ──
    # fetch 内部通过 checkpoint 实现增量加载（记录页码/CSV行号），
    # 重启后自动从断点继续获取，因此无需在 runner 层跳过。
    fetched_ids = []
    if skip_fetch:
        logger.info("── Stage 1: Fetch (skipped) ──")
    else:
        logger.info("── Stage 1: Fetch (%s mode) ──", source)
        with get_connection() as conn:
            set_checkpoint(conn, "pipeline_stage", "fetch")
        for item in fetch_incremental(
            max_pages=max_pages,
            source=source,
            csv_path=csv_path,
            max_items=max_items,
        ):
            fetched_ids.append(item["mal_id"])
        round_stats["fetched"] = len(fetched_ids)
        logger.info("Fetched %d items this round.", len(fetched_ids))

    # ── Stage 2: Quality Check ──
    # quality check 是幂等操作，对所有未匹配的记录重跑不会产生副作用。
    logger.info("── Stage 2: Quality Check ──")
    with get_connection() as conn:
        set_checkpoint(conn, "pipeline_stage", "quality")
    with get_connection() as conn:
        unmatched = get_unmatched_passed_ids(conn)
    all_ids_to_check = list(set(fetched_ids + unmatched))

    if all_ids_to_check:
        qc_result = run_quality_checks(all_ids_to_check)
        round_stats["quality_passed"] = len(qc_result["passed"])
        round_stats["quality_failed"] = len(qc_result["failed"])
    else:
        logger.info("No new items to quality check.")

    # ── Stage 3: Match ──
    # match 内部通过 get_unmatched_passed_ids() 自动排除已匹配的 ID，
    # match_stage checkpoint 记录子阶段（exact/fuzzy），重启后从断点继续。
    if skip_match:
        logger.info("── Stage 3: Matching (skipped) ──")
    else:
        logger.info("── Stage 3: Cross-source Matching ──")
        with get_connection() as conn:
            set_checkpoint(conn, "pipeline_stage", "match")

        # 只匹配通过质量检查且尚未匹配的
        with get_connection() as conn:
            ids_to_match = get_unmatched_passed_ids(conn)

        if ids_to_match:
            try:
                match_result = run_matching(ids_to_match)
                round_stats["exact_matched"] = len(match_result.get("exact", {}))
                round_stats["fuzzy_matched"] = len(match_result.get("fuzzy", {}))
                round_stats["unmatched"] = len(match_result.get("unmatched", []))
            except Exception as e:
                logger.error("Matching stage failed: %s. Will continue to export/report.", e)
        else:
            logger.info("No items pending matching.")

    # ── Stage 4: Export ──
    if export:
        logger.info("── Stage 4: Export ──")
        with get_connection() as conn:
            set_checkpoint(conn, "pipeline_stage", "export")
        export_jsonl(output_path=export_path)

    # ── Stage 5: Report ──
    logger.info("── Stage 5: Report ──")
    report = generate_report(round_stats)
    print(report)

    # 标记本轮 pipeline 完成
    with get_connection() as conn:
        set_checkpoint(conn, "pipeline_stage", "done")

    logger.info("Pipeline complete.")
    return round_stats

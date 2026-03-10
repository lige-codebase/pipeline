#!/usr/bin/env python3
"""CLI 入口：运行 anime metadata pipeline。"""

import argparse
import logging
import sys


def main():
    parser = argparse.ArgumentParser(
        description="动画元数据增量处理与跨源匹配 Pipeline"
    )
    parser.add_argument(
        "--max-pages", type=int, default=2,
        help="API 模式下本轮最多获取的页数，每页 25 条 (default: 2)",
    )
    parser.add_argument(
        "--source", type=str, default="api", choices=["api", "csv"],
        help="数据源类型: api=Jikan API, csv=本地 CSV 文件 (default: api)",
    )
    parser.add_argument(
        "--csv-path", type=str, default=None,
        help="CSV 模式下的文件路径或包含 CSV 的目录路径",
    )
    parser.add_argument(
        "--max-items", type=int, default=None,
        help="CSV 模式下本轮最多加载的条目数",
    )
    parser.add_argument(
        "--skip-fetch", action="store_true",
        help="跳过数据获取阶段",
    )
    parser.add_argument(
        "--skip-match", action="store_true",
        help="跳过跨源匹配阶段",
    )
    parser.add_argument(
        "--no-export", action="store_true",
        help="跳过 JSONL 导出",
    )
    parser.add_argument(
        "--export-path", type=str, default=None,
        help="自定义 JSONL 导出路径",
    )
    parser.add_argument(
        "--export-only", action="store_true",
        help="仅导出已有数据，不运行 pipeline",
    )
    parser.add_argument(
        "--export-status", type=str, default=None,
        choices=["matched", "unmatched", "review"],
        help="导出时按匹配状态过滤",
    )
    parser.add_argument(
        "--report-only", action="store_true",
        help="仅生成报告，不运行 pipeline",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="输出详细日志",
    )

    args = parser.parse_args()

    # 配置日志
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    from pipeline.database import init_db

    if args.export_only:
        from pipeline.exporter import export_jsonl
        init_db()
        path = export_jsonl(
            output_path=args.export_path,
            status_filter=args.export_status,
        )
        print(f"Exported to: {path}")
        return

    if args.report_only:
        from pipeline.report import generate_report
        init_db()
        report = generate_report()
        print(report)
        return

    from pipeline.runner import run_pipeline
    run_pipeline(
        max_pages=args.max_pages,
        skip_fetch=args.skip_fetch,
        skip_match=args.skip_match,
        export=not args.no_export,
        export_path=args.export_path,
        source=args.source,
        csv_path=args.csv_path,
        max_items=args.max_items,
    )


if __name__ == "__main__":
    main()

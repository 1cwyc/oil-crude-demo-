"""Command-line interface for the tanker processing stages."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys

from .config import load_config, resolve_date_range
from .pipeline import (
    StageOutputConflict,
    build_registry,
    doctor,
    export_csv,
    filter_positions,
    plan,
    render_heatmap,
    run_pipeline,
    sample_positions,
)


DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "configs" / "tanker_pipeline_20250715.json"


def _add_dates(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--date", help="单日，格式 YYYY-MM-DD。")
    parser.add_argument("--from-date", help="起始日期，格式 YYYY-MM-DD；默认读取配置。")
    parser.add_argument("--to-date", help="结束日期，格式 YYYY-MM-DD；默认读取配置。")


def _add_stage_controls(parser: argparse.ArgumentParser, *, source_rows: bool = False) -> None:
    parser.add_argument("--force", action="store_true", help="原子重建不再匹配当前输入/配置的派生输出。")
    parser.add_argument("--dry-run", action="store_true", help="只显示计划，不读取 .dat 内容。")
    if source_rows:
        parser.add_argument(
            "--max-source-rows", type=int,
            help="仅用于小样本调试：限制每个 .dat 解码的逻辑行数；正式处理不要设置。",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="复用现有 DuckDB AIS 解码器，生成油轮登记、轨迹、三小时样本与热力图。"
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="流程 JSON 配置文件。")
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="核查路径、日期、文件大小与执行顺序。")
    _add_dates(plan_parser)

    doctor_parser = subparsers.add_parser("doctor", help="只读核查依赖、内置解码器、输入和输出磁盘。")
    _add_dates(doctor_parser)

    registry_parser = subparsers.add_parser("build-registry", help="解码 STA 并建立年度油轮 MMSI 登记。")
    _add_dates(registry_parser)
    _add_stage_controls(registry_parser, source_rows=True)

    position_parser = subparsers.add_parser("filter-positions", help="逐日解码 POS，并在同一查询中筛出油轮。")
    _add_dates(position_parser)
    _add_stage_controls(position_parser, source_rows=True)

    sample_parser = subparsers.add_parser("sample", help="为每艘油轮选择最接近三小时时刻的有效位置。")
    _add_dates(sample_parser)
    _add_stage_controls(sample_parser)

    csv_parser = subparsers.add_parser("export-csv", help="将选定日期的三小时样本导出为 CSV。")
    _add_dates(csv_parser)
    _add_stage_controls(csv_parser)

    heatmap_parser = subparsers.add_parser("heatmap", help="从三小时样本绘制网格密度热力图。")
    _add_dates(heatmap_parser)
    _add_stage_controls(heatmap_parser)

    run_parser = subparsers.add_parser("run", help="按正确顺序运行所有启用阶段。")
    _add_dates(run_parser)
    _add_stage_controls(run_parser, source_rows=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    try:
        config = load_config(args.config)
        dates = resolve_date_range(
            config,
            single_date=getattr(args, "date", None),
            start_date=getattr(args, "from_date", None),
            end_date=getattr(args, "to_date", None),
        )
        if args.command == "doctor":
            result = doctor(config, dates)
        elif args.command == "plan":
            result = plan(config, dates)
        elif args.command == "build-registry":
            result = build_registry(
                config, dates, force=args.force, dry_run=args.dry_run,
                max_source_rows=args.max_source_rows,
            )
        elif args.command == "filter-positions":
            result = filter_positions(
                config, dates, force=args.force, dry_run=args.dry_run,
                max_source_rows=args.max_source_rows,
            )
        elif args.command == "sample":
            result = sample_positions(config, dates, force=args.force, dry_run=args.dry_run)
        elif args.command == "export-csv":
            result = export_csv(config, dates, force=args.force, dry_run=args.dry_run)
        elif args.command == "heatmap":
            result = render_heatmap(config, dates, force=args.force, dry_run=args.dry_run)
        else:
            result = run_pipeline(
                config, dates, force=args.force, dry_run=args.dry_run,
                max_source_rows=args.max_source_rows,
            )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (ValueError, FileNotFoundError, StageOutputConflict, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

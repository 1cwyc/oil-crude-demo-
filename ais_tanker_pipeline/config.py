"""Configuration helpers for the AIS tanker pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Iterator
from zoneinfo import ZoneInfo


_MEMORY_RE = re.compile(r"\d+(?:\.\d+)?(?:KB|MB|GB|TB)", re.IGNORECASE)


@dataclass(frozen=True)
class PipelineConfig:
    """Validated, immutable view over the JSON configuration."""

    path: Path
    data: dict[str, Any]

    @property
    def output_root(self) -> Path:
        return self.resolve_path(self.data["output_root"])

    @property
    def decoder_project_root(self) -> Path | None:
        value = self.data.get("decoder_project_root")
        return self.resolve_path(value) if value else None

    def resolve_path(self, value: str | Path) -> Path:
        """Resolve relative paths from the JSON file directory for portability."""
        path = Path(os.path.expandvars(str(value))).expanduser()
        if not path.is_absolute():
            path = self.path.parent / path
        return path.resolve()

    @property
    def config_hash(self) -> str:
        canonical = json.dumps(self.data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @property
    def default_start(self) -> date:
        return date.fromisoformat(self.data["date_range"]["from"])

    @property
    def default_end(self) -> date:
        return date.fromisoformat(self.data["date_range"]["to"])

    @property
    def tanker_types(self) -> tuple[int, ...]:
        return tuple(int(value) for value in self.data["tanker_classification"]["ship_types"])

    @property
    def sampling_timezone(self) -> ZoneInfo:
        return ZoneInfo(self.data["sampling"]["timezone"])

    def input_path(self, kind: str, day: date) -> Path:
        key = "sta" if kind.lower() in {"sta", "static"} else "pos"
        pattern = self.data["input_patterns"][key]
        return self.resolve_path(
            pattern.format(
                date=day.isoformat(),
                year=f"{day.year:04d}",
                month=f"{day.month:02d}",
                day=f"{day.day:02d}",
                yyyymm=f"{day.year:04d}{day.month:02d}",
                yyyymmdd=f"{day.year:04d}{day.month:02d}{day.day:02d}",
            )
        )


def load_config(path: str | Path) -> PipelineConfig:
    config_path = Path(path).resolve()
    data = json.loads(config_path.read_text(encoding="utf-8"))
    _validate(data)
    return PipelineConfig(path=config_path, data=data)


def _validate(data: dict[str, Any]) -> None:
    required = {
        "output_root",
        "date_range",
        "input_patterns",
        "tanker_classification",
        "sampling",
        "quality",
        "duckdb",
    }
    missing = sorted(required.difference(data))
    if missing:
        raise ValueError(f"配置缺少字段：{', '.join(missing)}")

    start = date.fromisoformat(data["date_range"]["from"])
    end = date.fromisoformat(data["date_range"]["to"])
    if end < start:
        raise ValueError("date_range.to 不能早于 date_range.from。")
    for kind in ("sta", "pos"):
        if kind not in data["input_patterns"]:
            raise ValueError(f"input_patterns 缺少 {kind}。")

    tanker_types = [int(value) for value in data["tanker_classification"]["ship_types"]]
    if not tanker_types or any(value < 0 or value > 255 for value in tanker_types):
        raise ValueError("tanker_classification.ship_types 必须是 0–255 范围内的非空列表。")
    if data["tanker_classification"].get("policy", "any_observed") != "any_observed":
        raise ValueError("当前版本只支持 any_observed 油轮识别策略。")

    hours = [int(value) for value in data["sampling"]["hours"]]
    if not hours or len(hours) != len(set(hours)) or any(value < 0 or value > 23 for value in hours):
        raise ValueError("sampling.hours 必须是 0–23 范围内的不重复小时列表。")
    tolerance = int(data["sampling"]["tolerance_seconds"])
    if tolerance < 0 or tolerance >= 10800:
        raise ValueError("sampling.tolerance_seconds 必须在 0–10799 秒之间。")
    ZoneInfo(data["sampling"]["timezone"])

    memory_limit = str(data["duckdb"].get("memory_limit", "12GB"))
    if not _MEMORY_RE.fullmatch(memory_limit):
        raise ValueError("duckdb.memory_limit 必须类似 12GB 或 8000MB。")
    if int(data["duckdb"].get("threads", 4)) < 1:
        raise ValueError("duckdb.threads 必须至少为 1。")

    heatmap = data.get("heatmap", {})
    if heatmap:
        extent = heatmap.get("extent", [-180.0, 180.0, -90.0, 90.0])
        if len(extent) != 4 or not (extent[0] < extent[1] and extent[2] < extent[3]):
            raise ValueError("heatmap.extent 必须是 [xmin, xmax, ymin, ymax]。")
        if float(heatmap.get("bin_size_degrees", 0.5)) <= 0:
            raise ValueError("heatmap.bin_size_degrees 必须大于 0。")


def iter_dates(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def resolve_date_range(
    config: PipelineConfig,
    *,
    single_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[date]:
    if single_date and (start_date or end_date):
        raise ValueError("--date 不能与 --from-date/--to-date 同时使用。")
    if single_date:
        start = end = date.fromisoformat(single_date)
    else:
        start = date.fromisoformat(start_date) if start_date else config.default_start
        end = date.fromisoformat(end_date) if end_date else config.default_end
    if end < start:
        raise ValueError("结束日期不能早于开始日期。")
    return list(iter_dates(start, end))


def target_epochs(config: PipelineConfig, day: date) -> list[tuple[int, int, str]]:
    """Return (hour, Unix epoch, ISO timestamp) for one local sampling day."""
    timezone = config.sampling_timezone
    result: list[tuple[int, int, str]] = []
    for hour in sorted(int(value) for value in config.data["sampling"]["hours"]):
        moment = datetime(day.year, day.month, day.day, hour, tzinfo=timezone)
        result.append((hour, int(moment.timestamp()), moment.isoformat()))
    return result


def is_tanker_type(ship_type: int | None, tanker_types: tuple[int, ...]) -> bool:
    return ship_type is not None and int(ship_type) in tanker_types

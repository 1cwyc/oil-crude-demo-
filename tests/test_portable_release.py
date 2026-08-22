from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

import duckdb

from ais_tanker_pipeline.config import is_tanker_type, load_config, target_epochs
from ais_tanker_pipeline.pipeline import (
    PIPELINE_VERSION,
    StageOutputConflict,
    _check_existing,
    _file_signature,
    build_registry,
    doctor,
    export_csv,
    filter_positions,
    render_heatmap,
    sample_positions,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_ROOT = PROJECT_ROOT / "sample_data"


def config_data(output_root: Path, timezone_name: str = "UTC") -> dict:
    return {
        "pipeline_name": "portable release test",
        "output_root": str(output_root),
        "date_range": {"from": "2026-03-02", "to": "2026-03-02"},
        "input_patterns": {
            "sta": str(SAMPLE_ROOT / "STA_OK_{date}.dat"),
            "pos": str(SAMPLE_ROOT / "POS_OK_{date}.dat"),
        },
        "tanker_classification": {
            "policy": "any_observed",
            "ship_types": [80, 81, 82, 83, 84, 89],
        },
        "sampling": {
            "timezone": timezone_name,
            "hours": [0],
            "tolerance_seconds": 300,
            "require_complete_window": False,
        },
        "quality": {"exclude_zero_zero": True},
        "duckdb": {
            "memory_limit": "1GB",
            "threads": 2,
            "temp_directory": str(output_root / "_tmp"),
        },
        "heatmap": {
            "metric": "sample_count",
            "extent": [110, 120, 25, 35],
            "bin_size_degrees": 0.25,
            "dpi": 100,
        },
    }


class PortableReleaseTests(unittest.TestCase):
    def test_manifest_detects_modified_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config_data(root / "out")), encoding="utf-8")
            config = load_config(config_path)
            target = root / "result.parquet"
            target.write_bytes(b"original")
            manifest_path = root / "manifest.json"
            inputs: list[dict] = []
            parameters = {"case": "output-integrity"}
            manifest_path.write_text(
                json.dumps(
                    {
                        "status": "complete",
                        "pipeline_version": PIPELINE_VERSION,
                        "inputs": inputs,
                        "parameters": parameters,
                        "outputs": [_file_signature(target)],
                    }
                ),
                encoding="utf-8",
            )
            self.assertTrue(
                _check_existing(
                    targets=[target],
                    manifest_path=manifest_path,
                    config=config,
                    inputs=inputs,
                    parameters=parameters,
                    force=False,
                )
            )
            target.write_bytes(b"modified-output")
            with self.assertRaises(StageOutputConflict):
                _check_existing(
                    targets=[target],
                    manifest_path=manifest_path,
                    config=config,
                    inputs=inputs,
                    parameters=parameters,
                    force=False,
                )

    def test_tanker_codes_and_timezone_boundary(self) -> None:
        types = (80, 81, 82, 83, 84, 89)
        self.assertTrue(all(is_tanker_type(value, types) for value in types))
        self.assertTrue(all(not is_tanker_type(value, types) for value in (79, 85, 86, 87, 88, 90)))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = config_data(root / "out", "Asia/Shanghai")
            data["date_range"] = {"from": "2025-07-15", "to": "2025-07-15"}
            path = root / "config.json"
            path.write_text(json.dumps(data), encoding="utf-8")
            _, epoch, _ = target_epochs(load_config(path), date(2025, 7, 15))[0]
            self.assertEqual(
                datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(),
                "2025-07-14T16:00:00+00:00",
            )

    def test_bundled_decoder_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "output"
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config_data(output)), encoding="utf-8")
            config = load_config(config_path)
            days = [date(2026, 3, 2)]

            report = doctor(config, days)
            self.assertEqual(report["decoder_mode"], "bundled")
            self.assertTrue(report["ready"])
            registry = build_registry(config, days)
            self.assertEqual(registry["registries"][0]["tankers"], 1)
            positions = filter_positions(config, days)
            self.assertEqual(positions["days"][0]["records"], 3)
            samples = sample_positions(config, days)
            sample_path = Path(samples["days"][0]["target"])
            row = duckdb.sql(
                "SELECT mmsi, line_number, candidate_count, absolute_offset_seconds "
                f"FROM parquet_scan('{sample_path.as_posix()}')"
            ).fetchone()
            self.assertEqual(row, (123456789, 1, 3, 0))
            self.assertEqual(export_csv(config, days)["records"], 1)
            self.assertEqual(render_heatmap(config, days)["occupied_grid_cells"], 1)

            self.assertEqual(build_registry(config, days)["registries"][0]["action"], "skipped")
            self.assertEqual(filter_positions(config, days)["days"][0]["action"], "skipped")
            self.assertEqual(sample_positions(config, days)["days"][0]["action"], "skipped")


if __name__ == "__main__":
    unittest.main()

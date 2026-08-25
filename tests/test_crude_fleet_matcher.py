from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

import duckdb
import pandas as pd
import yaml

from ais_tanker_pipeline.fleet.crude_fleet_matcher import match_crude_fleet_samples
from ais_tanker_pipeline.fleet.crude_fleet_matcher import build_crude_fleet_matches
from ais_tanker_pipeline.fleet.crude_fleet_matcher import run_crude_fleet_matcher
from ais_tanker_pipeline.fleet.crude_fleet_matcher import main
from ais_tanker_pipeline.fleet.config import load_crude_fleet_matcher_config


def write_parquet(path: Path, rows: list[tuple[object, ...]], columns: list[str]) -> None:
    connection = duckdb.connect()
    try:
        connection.register("rows", pd.DataFrame(rows, columns=columns))
        connection.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        connection.close()


class CrudeFleetMatcherTests(unittest.TestCase):
    def test_cli_dry_run_reads_only_matcher_config(self) -> None:
        """Fails if planning opens missing AIS partitions before the operator requests a build."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "matcher.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "reference_path": str(root / "not-opened-reference.parquet"),
                        "samples_root": str(root / "not-opened-ais"),
                        "output_root": str(root / "derived"),
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = main(["--config", str(config_path), "--month", "2025-09", "--dry-run"])

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue())["action"], "would_build")

    def test_loads_only_the_three_matcher_paths(self) -> None:
        """Fails if the matcher host config accepts undeclared policy or input fields."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "matcher.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "reference_path": str(root / "reference.parquet"),
                        "samples_root": str(root / "ais"),
                        "output_root": str(root / "derived"),
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            config = load_crude_fleet_matcher_config(config_path)

        self.assertEqual(config.reference_path, (root / "reference.parquet").resolve())
        self.assertEqual(config.samples_root, (root / "ais").resolve())
        self.assertEqual(config.output_root, (root / "derived").resolve())
        self.assertEqual(len(config.config_hash), 64)

    def test_prefers_imo_then_uses_only_unambiguous_mmsi_fallback(self) -> None:
        """Fails if a conflicting or ambiguous MMSI can override physical IMO identity."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            samples = root / "samples.parquet"
            output = root / "matches.parquet"
            write_parquet(
                reference,
                [
                    ("imo:9424209", "9424209", 111),
                    ("imo:9468853", "9468853", 222),
                    ("imo:9224805", "9224805", 333),
                    ("imo:9258167", "9258167", 333),
                ],
                ["crude_vessel_id", "imo", "mmsi"],
            )
            write_parquet(
                samples,
                [
                    (999, 1000, "9424209"),
                    (222, 2000, None),
                    (111, 3000, "9468853"),
                    (333, 4000, None),
                ],
                ["mmsi", "target_time_s", "registry_imo"],
            )

            report = match_crude_fleet_samples(reference, samples, output)
            connection = duckdb.connect()
            try:
                rows = connection.execute(
                    "SELECT mmsi, target_time_s, crude_vessel_id, match_method FROM read_parquet(?) ORDER BY target_time_s",
                    [str(output)],
                ).fetchall()
            finally:
                connection.close()

        self.assertEqual(report["counts"], {"matched_rows": 3, "imo_matches": 2, "mmsi_matches": 1})
        self.assertEqual(rows, [
            (999, 1000, "imo:9424209", "imo"),
            (222, 2000, "imo:9468853", "mmsi"),
            (111, 3000, "imo:9468853", "imo"),
        ])

    def test_rejects_duplicate_three_hour_sample_keys(self) -> None:
        """Fails closed rather than publishing a sidecar with duplicate AIS keys."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            samples = root / "samples.parquet"
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(
                samples,
                [(111, 1000, "9424209"), (111, 1000, "9424209")],
                ["mmsi", "target_time_s", "registry_imo"],
            )

            with self.assertRaisesRegex(ValueError, "duplicate three-hour AIS keys"):
                match_crude_fleet_samples(reference, samples, root / "matches.parquet")

    def test_rejects_samples_without_the_registry_imo_contract_field(self) -> None:
        """Fails closed if an AIS schema drift would silently disable IMO-priority matching."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            samples = root / "samples.parquet"
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(samples, [(111, 1000)], ["mmsi", "target_time_s"])

            with self.assertRaisesRegex(ValueError, "registry_imo"):
                match_crude_fleet_samples(reference, samples, root / "matches.parquet")

    def test_rejects_samples_with_a_noninteger_mmsi_contract_type(self) -> None:
        """Fails closed if a string MMSI would change the three-hour key contract."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            samples = root / "samples.parquet"
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(samples, [("111", 1000, "9424209")], ["mmsi", "target_time_s", "registry_imo"])

            with self.assertRaisesRegex(ValueError, "mmsi must be INTEGER or BIGINT"):
                match_crude_fleet_samples(reference, samples, root / "matches.parquet")

    def test_publishes_month_sidecar_manifest_and_skips_identical_inputs(self) -> None:
        """Fails if a match sidecar lacks a deterministic monthly identity or provenance."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            samples = root / "samples.parquet"
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(samples, [(111, 1000, "9424209")], ["mmsi", "target_time_s", "registry_imo"])

            first = build_crude_fleet_matches(reference, samples, root / "derived", "2025-09", config_hash="a" * 64)
            second = build_crude_fleet_matches(reference, samples, root / "derived", "2025-09", config_hash="a" * 64)
            output = Path(first["output_path"])
            manifest = Path(first["manifest_path"])
            manifest_exists = manifest.exists()
            connection = duckdb.connect()
            try:
                schema = connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(output)]
                ).fetchall()
            finally:
                connection.close()

        self.assertEqual(first["action"], "built")
        self.assertEqual(second["action"], "skipped")
        self.assertEqual(output.parts[-5:], ("enrichment", "crude_fleet_matches", "year=2025", "month=09", "crude_fleet_matches.parquet"))
        self.assertTrue(manifest_exists)
        self.assertEqual([column[0] for column in schema], ["mmsi", "target_time_s", "crude_vessel_id", "match_method"])

    def test_runner_reads_the_requested_month_partition_only(self) -> None:
        """Fails if monthly matching scans a neighboring AIS month or expects a copied samples table."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            september = root / "ais" / "samples_3h" / "timezone=UTC" / "year=2025" / "month=09" / "day=01"
            october = root / "ais" / "samples_3h" / "timezone=UTC" / "year=2025" / "month=10" / "day=01"
            september.mkdir(parents=True)
            october.mkdir(parents=True)
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(september / "samples.parquet", [(111, 1000, "9424209")], ["mmsi", "target_time_s", "registry_imo"])
            write_parquet(october / "samples.parquet", [(999, 2000, "9424209")], ["mmsi", "target_time_s", "registry_imo"])
            config_path = root / "matcher.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {"reference_path": str(reference), "samples_root": str(root / "ais"), "output_root": str(root / "derived")}
                ),
                encoding="utf-8",
            )

            report = run_crude_fleet_matcher(load_crude_fleet_matcher_config(config_path), "2025-09")

        self.assertEqual(report["counts"]["matched_rows"], 1)


if __name__ == "__main__":
    unittest.main()

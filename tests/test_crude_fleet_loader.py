from __future__ import annotations

import csv
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

import duckdb
import yaml

from ais_tanker_pipeline.artifacts import OutputConflict
from ais_tanker_pipeline.fleet.crude_fleet_loader import (
    build_crude_fleet_reference,
    load_crude_fleet,
    main,
    run_crude_fleet_loader,
)
from ais_tanker_pipeline.fleet.config import load_crude_fleet_config


class CrudeFleetLoaderTests(unittest.TestCase):
    def test_cli_dry_run_reads_only_the_host_config(self) -> None:
        """Fails if planning tries to open the fleet CSV before the user runs a build."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "fleet.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {"fleet_csv": str(root / "not-opened.csv"), "output_root": str(root / "derived")},
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = main(["--config", str(config_path), "--dry-run"])

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue())["action"], "would_build")

    def test_loads_the_minimal_host_configuration(self) -> None:
        """Fails if the loader accepts paths outside its explicit two-path contract."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "fleet.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {"fleet_csv": str(root / "fleet.csv"), "output_root": str(root / "derived")},
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            config = load_crude_fleet_config(config_path)

        self.assertEqual(config.fleet_csv, (root / "fleet.csv").resolve())
        self.assertEqual(config.output_root, (root / "derived").resolve())
        self.assertEqual(len(config.config_hash), 64)

    def test_runner_records_the_host_configuration_hash(self) -> None:
        """Fails if a published reference cannot be tied to its host configuration."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "fleet.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])
                writer.writerow(["ORIENTAL ARK", "9424209", "636014465", "340", "60", "320054"])
            config_path = root / "fleet.yaml"
            config_path.write_text(
                yaml.safe_dump({"fleet_csv": str(source), "output_root": str(root / "derived")}),
                encoding="utf-8",
            )
            config = load_crude_fleet_config(config_path)

            report = run_crude_fleet_loader(config)
            manifest = json.loads(Path(report["manifest_path"]).read_text(encoding="utf-8"))

        self.assertEqual(manifest["config_hash"], config.config_hash)

    def test_loads_chinese_source_headers_into_imo_stable_records(self) -> None:
        """Fails if source-field normalization or IMO-based identity is removed."""
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])
                writer.writerow(["ORIENTAL ARK", "9424209", "636014465", "340", "60", "320054"])

            records, summary = load_crude_fleet(source)

        self.assertEqual(summary.source_rows, 1)
        self.assertEqual(summary.reference_rows, 1)
        self.assertEqual(records[0].crude_vessel_id, "imo:9424209")
        self.assertEqual(records[0].imo, "9424209")
        self.assertEqual(records[0].mmsi, 636014465)
        self.assertEqual(records[0].length_m, 340.0)
        self.assertEqual(records[0].breadth_m, 60.0)
        self.assertEqual(records[0].deadweight_t, 320054.0)

    def test_rejects_a_csv_missing_a_required_source_field(self) -> None:
        """Fails if a malformed fleet schema escapes as an implementation exception."""
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽"])
                writer.writerow(["ORIENTAL ARK", "9424209", "636014465", "340", "60"])

            with self.assertRaisesRegex(ValueError, "missing columns: dwt"):
                load_crude_fleet(source)

    def test_rejects_an_empty_fleet_csv(self) -> None:
        """Fails if an empty reference can be published with an indeterminate schema."""
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                csv.writer(stream).writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])

            with self.assertRaisesRegex(ValueError, "fleet CSV has no data rows"):
                load_crude_fleet(source)

    def test_rejects_an_imo_with_an_invalid_check_digit(self) -> None:
        """Fails if malformed IMO values can create a physical vessel identity."""
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])
                writer.writerow(["BAD IMO", "9424208", "636014465", "340", "60", "320054"])

            with self.assertRaisesRegex(ValueError, "invalid IMO"):
                load_crude_fleet(source)

    def test_deduplicates_identical_rows_for_the_same_imo(self) -> None:
        """Fails if duplicate source rows create duplicate physical vessels."""
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])
                row = ["ORIENTAL ARK", "9424209", "636014465", "340", "60", "320054"]
                writer.writerow(row)
                writer.writerow(row)

            records, summary = load_crude_fleet(source)

        self.assertEqual(summary.source_rows, 2)
        self.assertEqual(summary.reference_rows, 1)
        self.assertEqual([record.imo for record in records], ["9424209"])

    def test_rejects_a_nonpositive_vessel_dimension(self) -> None:
        """Fails if a record that makes later cargo arithmetic invalid is accepted."""
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])
                writer.writerow(["BAD WIDTH", "9424209", "636014465", "340", "0", "320054"])

            with self.assertRaisesRegex(ValueError, "breadth_m must be positive"):
                load_crude_fleet(source)

    def test_builds_a_strict_reference_parquet_and_manifest(self) -> None:
        """Fails if the loader publishes extra fields or lacks provenance evidence."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])
                writer.writerow(["ORIENTAL ARK", "9424209", "636014465", "340", "60", "320054"])

            report = build_crude_fleet_reference(source, root / "derived")
            output = Path(report["output_path"])
            manifest = json.loads(Path(report["manifest_path"]).read_text(encoding="utf-8"))
            connection = duckdb.connect()
            try:
                schema = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(output)]).fetchall()
                rows = connection.execute("SELECT * FROM read_parquet(?)", [str(output)]).fetchall()
            finally:
                connection.close()

        self.assertEqual([field[0] for field in schema], [
            "crude_vessel_id", "imo", "mmsi", "length_m", "breadth_m", "deadweight_t",
        ])
        self.assertEqual(rows, [("imo:9424209", "9424209", 636014465, 340.0, 60.0, 320054.0)])
        self.assertEqual(
            manifest["counts"],
            {"source_rows": 1, "reference_rows": 1, "ambiguous_mmsi": 0},
        )

    def test_rejects_a_conflicting_existing_reference_output(self) -> None:
        """Fails if changed fleet input silently overwrites a published reference table."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])
                writer.writerow(["ORIENTAL ARK", "9424209", "636014465", "340", "60", "320054"])
            build_crude_fleet_reference(source, root / "derived")
            with source.open("a", encoding="utf-8", newline="") as stream:
                csv.writer(stream).writerow(["SECOND", "9468853", "241411000", "340", "60", "319861"])

            with self.assertRaises(OutputConflict):
                build_crude_fleet_reference(source, root / "derived")

    def test_force_rebuilds_a_conflicting_reference_output(self) -> None:
        """Fails if force cannot replace an explicitly reviewed stale derived table."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])
                writer.writerow(["ORIENTAL ARK", "9424209", "636014465", "340", "60", "320054"])
            build_crude_fleet_reference(source, root / "derived")
            with source.open("a", encoding="utf-8", newline="") as stream:
                csv.writer(stream).writerow(["SECOND", "9468853", "241411000", "340", "60", "319861"])

            report = build_crude_fleet_reference(source, root / "derived", force=True)
            connection = duckdb.connect()
            try:
                rows = connection.execute("SELECT count(*) FROM read_parquet(?)", [report["output_path"]]).fetchone()[0]
            finally:
                connection.close()

        self.assertEqual(report["action"], "built")
        self.assertEqual(rows, 2)

    def test_skips_an_unchanged_published_reference(self) -> None:
        """Fails if an identical rerun needlessly rewrites the authoritative table."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])
                writer.writerow(["ORIENTAL ARK", "9424209", "636014465", "340", "60", "320054"])

            first = build_crude_fleet_reference(source, root / "derived")
            second = build_crude_fleet_reference(source, root / "derived")

        self.assertEqual(first["action"], "built")
        self.assertEqual(second["action"], "skipped")

    def test_records_duplicate_mmsi_as_reference_qc_without_dropping_imos(self) -> None:
        """Fails if an ambiguous MMSI silently removes or merges physical vessels."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "crude.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["船名", "imo", "MMSI", "长", "宽", "dwt"])
                writer.writerow(["FIRST", "9224805", "613612000", "300", "50", "200000"])
                writer.writerow(["SECOND", "9258167", "613612000", "300", "50", "200001"])

            report = build_crude_fleet_reference(source, root / "derived")
            manifest = json.loads(Path(report["manifest_path"]).read_text(encoding="utf-8"))

        self.assertEqual(report["counts"]["reference_rows"], 2)
        self.assertEqual(manifest["counts"].get("ambiguous_mmsi"), 1)


if __name__ == "__main__":
    unittest.main()

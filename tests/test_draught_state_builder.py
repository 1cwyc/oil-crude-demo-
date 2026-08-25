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

from ais_tanker_pipeline.draught.config import DraughtConfig, load_draught_config, month_range
from ais_tanker_pipeline.draught import draught_state_builder
from ais_tanker_pipeline.draught.draught_state_builder import (
    DraughtObservation,
    build_draught_states,
    main,
    read_matched_observations,
    run_draught_state_builder,
)


def write_parquet(path: Path, rows: list[tuple[object, ...]], columns: list[str]) -> None:
    connection = duckdb.connect()
    try:
        connection.register("rows", pd.DataFrame(rows, columns=columns))
        connection.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        connection.close()


def state_config() -> DraughtConfig:
    return DraughtConfig(
        Path("config.yaml"), Path("reference.parquet"), Path("static"), Path("derived"),
        (1.0, 30.0), 0.30, 48.0, 6.0, 3, {},
    )


class DraughtConfigTests(unittest.TestCase):
    def test_cli_dry_run_does_not_open_missing_parquet(self) -> None:
        """Fails if planning a draught build reads absent reference or static input."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "draught.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "reference_path": str(root / "missing-reference.parquet"),
                        "static_root": str(root / "missing-static"),
                        "output_root": str(root / "derived"),
                        "draught_valid_range_m": [1.0, 30.0],
                        "state_tolerance_m": 0.30,
                        "max_observation_gap_hours": 48,
                        "minimum_state_duration_hours": 6,
                        "minimum_state_observations": 3,
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = main(["--config", str(config_path), "--start-month", "2025-09", "--end-month", "2025-09", "--dry-run"])

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue())["action"], "would_build")

    def test_loads_the_complete_fixed_version_one_configuration(self) -> None:
        """Fails if a host config can silently change a state-building rule."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "draught.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "reference_path": str(root / "reference.parquet"),
                        "static_root": str(root / "static"),
                        "output_root": str(root / "derived"),
                        "draught_valid_range_m": [1.0, 30.0],
                        "state_tolerance_m": 0.30,
                        "max_observation_gap_hours": 48,
                        "minimum_state_duration_hours": 6,
                        "minimum_state_observations": 3,
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            config = load_draught_config(config_path)

        self.assertEqual(config.draught_valid_range_m, (1.0, 30.0))
        self.assertEqual(config.state_tolerance_m, 0.30)
        self.assertEqual(config.max_observation_gap_hours, 48.0)
        self.assertEqual(config.minimum_state_duration_hours, 6.0)
        self.assertEqual(config.minimum_state_observations, 3)
        self.assertEqual(len(config.config_hash), 64)

    def test_rejects_changed_version_one_tolerance(self) -> None:
        """Fails if a changed tolerance can alter state segmentation without a new algorithm version."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "draught.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "reference_path": str(root / "reference.parquet"),
                        "static_root": str(root / "static"),
                        "output_root": str(root / "derived"),
                        "draught_valid_range_m": [1.0, 30.0],
                        "state_tolerance_m": 0.31,
                        "max_observation_gap_hours": 48,
                        "minimum_state_duration_hours": 6,
                        "minimum_state_observations": 3,
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "version 1 requires state_tolerance_m=0.3"):
                load_draught_config(config_path)

    def test_enumerates_an_inclusive_month_range(self) -> None:
        """Fails if a requested end month is skipped when processing annual inputs."""
        self.assertEqual(month_range("2025-09", "2025-11"), ("2025-09", "2025-10", "2025-11"))


class DraughtObservationTests(unittest.TestCase):
    def test_yields_matched_observations_incrementally(self) -> None:
        """Fails if a monthly AIS query materializes every matched observation before reduction."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            static = root / "static.parquet"
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(
                static,
                [(111, 100, "9424209", 12.0, 0), (111, 200, "9424209", 12.1, 0)],
                ["mmsi", "receive_time_s", "imo", "draught_m", "dq_mask"],
            )

            reader = getattr(draught_state_builder, "iter_matched_observations", None)
            self.assertIsNotNone(reader)
            observations = reader(reference, [static], valid_range=(1.0, 30.0), tolerance_m=0.30)
            first = next(observations)

        self.assertEqual((first.crude_vessel_id, first.receive_time_s, first.draught_m), ("imo:9424209", 100, 12.0))

    def test_prefers_imo_and_filters_invalid_draught(self) -> None:
        """Fails if a conflicting MMSI can override IMO or zero draught becomes a stable observation."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            static = root / "static.parquet"
            write_parquet(
                reference,
                [("imo:9424209", "9424209", 111), ("imo:9468853", "9468853", 222)],
                ["crude_vessel_id", "imo", "mmsi"],
            )
            write_parquet(
                static,
                [(111, 100, "9468853", 12.0, 0), (222, 200, None, 0.0, 0)],
                ["mmsi", "receive_time_s", "imo", "draught_m", "dq_mask"],
            )

            observations = read_matched_observations(reference, [static], valid_range=(1.0, 30.0), tolerance_m=0.30)

        self.assertEqual([(item.crude_vessel_id, item.receive_time_s, item.draught_m) for item in observations], [("imo:9468853", 100, 12.0)])

    def test_merges_same_imo_same_time_reports_by_median(self) -> None:
        """Fails if a valid IMO's simultaneous reports stop the month instead of yielding their median."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            static = root / "static.parquet"
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(
                static,
                [(111, 100, "9424209", 12.0, 0), (222, 100, "9424209", 12.5, 0)],
                ["mmsi", "receive_time_s", "imo", "draught_m", "dq_mask"],
            )

            observations = read_matched_observations(reference, [static], valid_range=(1.0, 30.0), tolerance_m=0.30)

        self.assertEqual(
            [(item.crude_vessel_id, item.receive_time_s, item.draught_m) for item in observations],
            [("imo:9424209", 100, 12.25)],
        )

    def test_rejects_static_schema_without_imo_before_matching(self) -> None:
        """Fails if schema drift can silently bypass IMO-priority physical identity resolution."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            static = root / "static.parquet"
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(static, [(111, 100, 12.0, 0)], ["mmsi", "receive_time_s", "draught_m", "dq_mask"])

            with self.assertRaisesRegex(ValueError, "static AIS missing columns: imo"):
                read_matched_observations(reference, [static], valid_range=(1.0, 30.0), tolerance_m=0.30)

    def test_rejects_non_integral_mmsi_before_matching(self) -> None:
        """Fails if a text MMSI is implicitly coerced and changes the physical-identity join."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            static = root / "static.parquet"
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(
                static,
                [("111", 100, "9424209", 12.0, 0)],
                ["mmsi", "receive_time_s", "imo", "draught_m", "dq_mask"],
            )

            with self.assertRaisesRegex(ValueError, "static AIS wrong types: mmsi"):
                read_matched_observations(reference, [static], valid_range=(1.0, 30.0), tolerance_m=0.30)


class DraughtReducerTests(unittest.TestCase):
    def test_reduces_ordered_observations_without_requesting_a_global_length(self) -> None:
        """Fails if the reducer materializes the entire monthly observation stream to sort it."""
        class OrderedStream:
            def __init__(self) -> None:
                self._items = iter(
                    [
                        DraughtObservation("imo:9424209", 0, 12.0),
                        DraughtObservation("imo:9424209", 3 * 3600, 12.1),
                        DraughtObservation("imo:9424209", 6 * 3600, 12.2),
                    ]
                )

            def __iter__(self) -> "OrderedStream":
                return self

            def __next__(self) -> DraughtObservation:
                return next(self._items)

            def __length_hint__(self) -> int:
                raise AssertionError("global materialization is not allowed")

        states = build_draught_states(OrderedStream(), state_config())

        self.assertEqual([(item.state_start_s, item.state_end_s) for item in states], [(0, 6 * 3600)])

    def test_publishes_a_six_hour_three_observation_stable_state(self) -> None:
        """Fails if a qualifying stable state is discarded or its median is not preserved."""
        states = build_draught_states(
            [
                DraughtObservation("imo:9424209", 0, 12.0),
                DraughtObservation("imo:9424209", 3 * 3600, 12.2),
                DraughtObservation("imo:9424209", 6 * 3600, 12.1),
            ],
            state_config(),
        )

        self.assertEqual(len(states), 1)
        self.assertEqual(
            (states[0].crude_vessel_id, states[0].state_start_s, states[0].state_end_s, states[0].draught_median_m),
            ("imo:9424209", 0, 6 * 3600, 12.1),
        )

    def test_does_not_bridge_a_gap_larger_than_forty_eight_hours(self) -> None:
        """Fails if stale static reports create a false continuous draught state."""
        states = build_draught_states(
            [
                DraughtObservation("imo:9424209", 0, 12.0),
                DraughtObservation("imo:9424209", 3 * 3600, 12.0),
                DraughtObservation("imo:9424209", 6 * 3600, 12.0),
                DraughtObservation("imo:9424209", 55 * 3600, 12.0),
            ],
            state_config(),
        )

        self.assertEqual([(item.state_start_s, item.state_end_s) for item in states], [(0, 6 * 3600)])


class DraughtArtifactTests(unittest.TestCase):
    def test_records_same_imo_conflict_median_summary_in_manifest(self) -> None:
        """Fails if a wide simultaneous IMO spread is merged but cannot be audited from the manifest."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            static = root / "static" / "year=2025" / "month=09" / "date=2025-09-01" / "static.parquet"
            static.parent.mkdir(parents=True)
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(
                static,
                [
                    (111, 0, "9424209", 5.1, 0),
                    (222, 0, "9424209", 5.7, 0),
                    (111, 3 * 3600, "9424209", 5.4, 0),
                    (111, 6 * 3600, "9424209", 5.4, 0),
                ],
                ["mmsi", "receive_time_s", "imo", "draught_m", "dq_mask"],
            )
            config = DraughtConfig(
                root / "config.yaml", reference, root / "static", root / "derived", (1.0, 30.0), 0.30, 48.0, 6.0, 3,
                {"test": "config"},
            )

            result = run_draught_state_builder(config, "2025-09", "2025-09")
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))

        self.assertEqual(manifest["counts"]["imo_timestamp_conflict_merged_groups"], 1)
        self.assertAlmostEqual(manifest["counts"]["imo_timestamp_conflict_merged_max_spread_m"], 0.6)
        self.assertEqual(manifest["algorithm_version"], "1.1.0")

    def test_publishes_month_state_sidecar_and_skips_identical_inputs(self) -> None:
        """Fails if stable states are not published as a minimal idempotent monthly artifact."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = root / "reference.parquet"
            static = root / "static" / "year=2025" / "month=09" / "date=2025-09-01" / "static.parquet"
            static.parent.mkdir(parents=True)
            write_parquet(reference, [("imo:9424209", "9424209", 111)], ["crude_vessel_id", "imo", "mmsi"])
            write_parquet(
                static,
                [(111, 0, "9424209", 12.0, 0), (111, 3 * 3600, "9424209", 12.1, 0), (111, 6 * 3600, "9424209", 12.2, 0)],
                ["mmsi", "receive_time_s", "imo", "draught_m", "dq_mask"],
            )
            config = DraughtConfig(
                root / "config.yaml", reference, root / "static", root / "derived", (1.0, 30.0), 0.30, 48.0, 6.0, 3,
                {"test": "config"},
            )

            first = run_draught_state_builder(config, "2025-09", "2025-09")
            second = run_draught_state_builder(config, "2025-09", "2025-09")
            output = Path(first["output_paths"][0])
            connection = duckdb.connect()
            try:
                schema = connection.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(output)]).fetchall()
            finally:
                connection.close()

        self.assertEqual(first["action"], "built")
        self.assertEqual(second["action"], "skipped")
        self.assertEqual([row[0] for row in schema], ["draught_state_id", "crude_vessel_id", "state_start_s", "state_end_s", "draught_median_m"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import duckdb
import pandas
import yaml

from ais_tanker_pipeline.routes.config import load_trajectory_config
from ais_tanker_pipeline.routes.voyage_trajectory_builder import run_voyage_trajectory_builder


def _write_parquet(path: Path, rows: list[tuple[object, ...]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.register("rows", pandas.DataFrame(rows, columns=columns))
        connection.execute("COPY rows TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        connection.close()


def _config(root: Path) -> Path:
    path = root / "trajectory.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "output_root": str(root / "derived"),
                "voyages_root": str(root / "derived" / "voyages" / "crude_voyages"),
                "events_root": str(root / "derived" / "events" / "loading_unloading_events"),
                "samples_root": str(root / "samples_3h" / "timezone=UTC"),
                "matches_root": str(root / "derived" / "enrichment" / "crude_fleet_matches"),
                "max_segment_gap_hours": 24,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


class VoyageTrajectoryTests(unittest.TestCase):
    def test_keeps_only_matched_valid_points_inside_event_window(self) -> None:
        """The public route sidecar must consist only of chronological original AIS points."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            start = 1_756_684_800
            _write_parquet(
                root / "derived" / "voyages" / "crude_voyages" / "year=2025" / "month=09" / "voyages.parquet",
                [("voyage:1", "imo:1", "event:load", "event:unload", start + 32_400)],
                ["voyage_id", "crude_vessel_id", "load_event_id", "unload_event_id", "unload_end_s"],
            )
            _write_parquet(
                root / "derived" / "events" / "loading_unloading_events" / "year=2025" / "month=09" / "events.parquet",
                [
                    ("event:load", "accepted", "load", "imo:1", start - 7_200, start),
                    ("event:unload", "accepted", "unload", "imo:1", start + 21_600, start + 32_400),
                ],
                ["event_id", "event_status", "event_kind", "crude_vessel_id", "event_start_s", "event_end_s"],
            )
            _write_parquet(
                root / "samples_3h" / "timezone=UTC" / "year=2025" / "month=09" / "date=2025-09-01" / "samples.parquet",
                [
                    (111, start - 10_800, 49.0, 24.0, True, 0),
                    (111, start, 50.0, 25.0, True, 0),
                    (111, start + 10_800, 51.0, 25.5, True, 0),
                    (111, start + 21_600, 52.0, 26.0, True, 0),
                    (111, start + 5_400, 99.0, 99.0, False, 0),
                ],
                ["mmsi", "target_time_s", "longitude_deg", "latitude_deg", "is_hard_valid", "dq_mask"],
            )
            _write_parquet(
                root / "derived" / "enrichment" / "crude_fleet_matches" / "year=2025" / "month=09" / "matches.parquet",
                [(111, start + offset, "imo:1", "imo") for offset in (-10_800, 0, 10_800, 21_600)],
                ["mmsi", "target_time_s", "crude_vessel_id", "match_method"],
            )

            report = run_voyage_trajectory_builder(load_trajectory_config(_config(root)), month="2025-09")

            connection = duckdb.connect()
            try:
                points = connection.execute(
                    "SELECT * FROM read_parquet(?, hive_partitioning = false) ORDER BY point_index", [report["points_path"]]
                ).fetchall()
                qc = connection.execute("SELECT * FROM read_parquet(?, hive_partitioning = false)", [report["qc_path"]]).fetchall()
            finally:
                connection.close()
        self.assertEqual(
            points,
            [
                ("voyage:1", 0, start, 50.0, 25.0),
                ("voyage:1", 1, start + 10_800, 51.0, 25.5),
                ("voyage:1", 2, start + 21_600, 52.0, 26.0),
            ],
        )
        self.assertEqual(qc, [("voyage:1", 3, 1.0, 10_800, "complete")])

    def test_omits_an_identity_conflict_instead_of_drawing_an_arbitrary_route(self) -> None:
        """Two MMSIs for one physical voyage/time cannot be selected as a real AIS trajectory."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            start = 1_756_684_800
            _write_parquet(
                root / "derived" / "voyages" / "crude_voyages" / "year=2025" / "month=09" / "voyages.parquet",
                [("voyage:1", "imo:1", "event:load", "event:unload", start + 21_600)],
                ["voyage_id", "crude_vessel_id", "load_event_id", "unload_event_id", "unload_end_s"],
            )
            _write_parquet(
                root / "derived" / "events" / "loading_unloading_events" / "year=2025" / "month=09" / "events.parquet",
                [("event:load", "accepted", "load", "imo:1", start - 1, start), ("event:unload", "accepted", "unload", "imo:1", start + 10_800, start + 21_600)],
                ["event_id", "event_status", "event_kind", "crude_vessel_id", "event_start_s", "event_end_s"],
            )
            _write_parquet(
                root / "samples_3h" / "timezone=UTC" / "year=2025" / "month=09" / "date=2025-09-01" / "samples.parquet",
                [(111, start, 50.0, 25.0, True, 0), (222, start, 120.0, 39.0, True, 0)],
                ["mmsi", "target_time_s", "longitude_deg", "latitude_deg", "is_hard_valid", "dq_mask"],
            )
            _write_parquet(
                root / "derived" / "enrichment" / "crude_fleet_matches" / "year=2025" / "month=09" / "matches.parquet",
                [(111, start, "imo:1", "imo"), (222, start, "imo:1", "imo")],
                ["mmsi", "target_time_s", "crude_vessel_id", "match_method"],
            )
            report = run_voyage_trajectory_builder(load_trajectory_config(_config(root)), month="2025-09")
            connection = duckdb.connect()
            try:
                point_count = connection.execute("SELECT count(*) FROM read_parquet(?, hive_partitioning = false)", [report["points_path"]]).fetchone()[0]
                qc = connection.execute("SELECT voyage_id, sample_count, route_status FROM read_parquet(?, hive_partitioning = false)", [report["qc_path"]]).fetchall()
            finally:
                connection.close()
        self.assertEqual(point_count, 0)
        self.assertEqual(qc, [("voyage:1", 0, "identity_conflict")])


if __name__ == "__main__":
    unittest.main()

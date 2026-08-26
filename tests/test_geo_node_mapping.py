from __future__ import annotations

import csv
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import yaml

from ais_tanker_pipeline.geo.config import load_geo_config
from ais_tanker_pipeline.geo.geo_registry_builder import build_port_zones
from ais_tanker_pipeline.geo.node_mapping import build_zone_node_map


def _config(root: Path, overseas_cluster_radius_km: float = 250) -> Path:
    path = root / "geo.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "wpi_csv": str(root / "wpi.csv"),
                "output_root": str(root / "derived"),
                "port_zone_radius_km": 75,
                "overseas_cluster_radius_km": overseas_cluster_radius_km,
                "china_groups": {
                    "cn_bohai_rim": [120.0, 38.5],
                    "cn_yangtze_delta": [121.3, 31.2],
                    "cn_southeast_coast": [118.6, 24.7],
                    "cn_pearl_river_delta": [113.7, 22.6],
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


class ZoneNodeMapTests(unittest.TestCase):
    def test_publishes_one_mapping_per_zone_and_the_four_china_groups(self) -> None:
        """A missing zone mapping or China group would make monthly OD assignment ambiguous."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (root / "wpi.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(
                    stream,
                    fieldnames=["INDEX_NO", "REGION_NO", "PORT_NAME", "COUNTRY", "LONGITUDE", "LATITUDE"],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {"INDEX_NO": "10", "REGION_NO": "1", "PORT_NAME": "CN PORT", "COUNTRY": "CN", "LONGITUDE": "120", "LATITUDE": "38"},
                        {"INDEX_NO": "20", "REGION_NO": "2", "PORT_NAME": "OVERSEAS PORT", "COUNTRY": "SA", "LONGITUDE": "50", "LATITUDE": "25"},
                    ]
                )
            config = load_geo_config(_config(root))
            build_port_zones(config)

            report = build_zone_node_map(config)

            connection = duckdb.connect()
            try:
                mappings = connection.execute(
                    "SELECT * FROM read_parquet(?) ORDER BY zone_id", [report["map_path"]]
                ).fetchall()
                nodes = connection.execute(
                    "SELECT node_id, node_kind FROM read_parquet(?) ORDER BY node_id", [report["nodes_path"]]
                ).fetchall()
            finally:
                connection.close()

        self.assertEqual(
            mappings,
            [
                ("zone:wpi:10", "cn_bohai_rim", "china_group_nearest"),
                ("zone:wpi:20", "overseas:wpi:20", "overseas_radius_cluster"),
            ],
        )
        self.assertEqual(
            nodes,
            [
                ("cn_bohai_rim", "china_group"),
                ("cn_pearl_river_delta", "china_group"),
                ("cn_southeast_coast", "china_group"),
                ("cn_yangtze_delta", "china_group"),
                ("overseas:wpi:20", "overseas_function_area"),
            ],
        )

    def test_manifest_failure_restores_the_previous_nodes_and_mapping_pair(self) -> None:
        """A failed publication must not leave downstream readers with a mixed artifact pair."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (root / "wpi.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(
                    stream,
                    fieldnames=["INDEX_NO", "REGION_NO", "PORT_NAME", "COUNTRY", "LONGITUDE", "LATITUDE"],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {"INDEX_NO": "10", "REGION_NO": "1", "PORT_NAME": "CN PORT", "COUNTRY": "CN", "LONGITUDE": "120", "LATITUDE": "38"},
                        {"INDEX_NO": "20", "REGION_NO": "2", "PORT_NAME": "OVERSEAS PORT", "COUNTRY": "SA", "LONGITUDE": "50", "LATITUDE": "25"},
                    ]
                )
            config = load_geo_config(_config(root))
            build_port_zones(config)
            first = build_zone_node_map(config)
            old_nodes = Path(first["nodes_path"]).read_bytes()
            old_map = Path(first["map_path"]).read_bytes()

            (root / "wpi.csv").write_text(
                "INDEX_NO,REGION_NO,PORT_NAME,COUNTRY,LONGITUDE,LATITUDE\n"
                "10,1,CN PORT,CN,120,38\n"
                "20,2,OVERSEAS PORT,SA,60,25\n",
                encoding="utf-8",
            )
            build_port_zones(config, force=True)
            with patch(
                "ais_tanker_pipeline.geo.node_mapping.write_json_atomic",
                side_effect=OSError("manifest storage unavailable"),
            ):
                with self.assertRaisesRegex(OSError, "manifest storage unavailable"):
                    build_zone_node_map(config, force=True)

            self.assertEqual(Path(first["nodes_path"]).read_bytes(), old_nodes)
            self.assertEqual(Path(first["map_path"]).read_bytes(), old_map)

    def test_existing_recovery_backup_leaves_no_partial_output(self) -> None:
        """A blocked recovery must not leave a discoverable partial mapping member."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "wpi.csv").write_text(
                "INDEX_NO,REGION_NO,PORT_NAME,COUNTRY,LONGITUDE,LATITUDE\n"
                "10,1,CN PORT,CN,120,38\n",
                encoding="utf-8",
            )
            config = load_geo_config(_config(root))
            build_port_zones(config)
            nodes_parent = config.output_root / "network_v1" / "geo" / "network_nodes"
            nodes_parent.mkdir(parents=True)
            (nodes_parent / "network_nodes.backup.parquet").write_bytes(b"previous recovery")

            with self.assertRaisesRegex(Exception, "recovery backup"):
                build_zone_node_map(config)

            self.assertEqual(list(config.output_root.glob("network_v1/geo/**/*.partial-*")), [])


if __name__ == "__main__":
    unittest.main()

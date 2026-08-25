from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
import csv

import duckdb

import yaml

from ais_tanker_pipeline.geo.config import load_geo_config
from ais_tanker_pipeline.geo.geo_registry_builder import build_port_zones


class GeoConfigTests(unittest.TestCase):
    def test_loads_exact_geo_configuration_and_hashes_all_rules(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "geo.yaml"
            path.write_text(yaml.safe_dump({
                "wpi_csv": str(root / "wpi.csv"), "output_root": str(root / "derived"),
                "port_zone_radius_km": 75, "overseas_cluster_radius_km": 250,
                "china_groups": {
                    "cn_bohai_rim": [120.0, 38.5], "cn_yangtze_delta": [121.3, 31.2],
                    "cn_southeast_coast": [118.6, 24.7], "cn_pearl_river_delta": [113.7, 22.6],
                },
            }), encoding="utf-8")

            config = load_geo_config(path)

        self.assertEqual(config.port_zone_radius_km, 75.0)
        self.assertEqual(config.overseas_cluster_radius_km, 250.0)
        self.assertEqual(tuple(config.china_groups), ("cn_bohai_rim", "cn_yangtze_delta", "cn_southeast_coast", "cn_pearl_river_delta"))
        self.assertEqual(len(config.config_hash), 64)

    def test_rejects_unknown_configuration_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "geo.yaml"
            path.write_text(yaml.safe_dump({"wpi_csv": "wpi.csv", "output_root": "derived", "port_zone_radius_km": 75,
                "overseas_cluster_radius_km": 250, "china_groups": {}, "extra": True}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "exactly"):
                load_geo_config(path)


class PortZoneArtifactTests(unittest.TestCase):
    def test_builds_minimal_wpi_port_reference_and_zones_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "wpi.csv"
            with source.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=["INDEX_NO", "REGION_NO", "PORT_NAME", "COUNTRY", "LONGITUDE", "LATITUDE", "OIL_DEPTH"])
                writer.writeheader()
                writer.writerows([
                    {"INDEX_NO": "10", "REGION_NO": "1", "PORT_NAME": "ALPHA", "COUNTRY": "CN", "LONGITUDE": "120", "LATITUDE": "38", "OIL_DEPTH": "K"},
                    {"INDEX_NO": "20", "REGION_NO": "2", "PORT_NAME": "BETA", "COUNTRY": "SA", "LONGITUDE": "50", "LATITUDE": "25", "OIL_DEPTH": ""},
                ])
            config_path = root / "geo.yaml"
            config_path.write_text(yaml.safe_dump({"wpi_csv": str(source), "output_root": str(root / "derived"), "port_zone_radius_km": 75,
                "overseas_cluster_radius_km": 250, "china_groups": {"cn_bohai_rim": [120, 38.5], "cn_yangtze_delta": [121.3, 31.2], "cn_southeast_coast": [118.6, 24.7], "cn_pearl_river_delta": [113.7, 22.6]}}), encoding="utf-8")

            first = build_port_zones(load_geo_config(config_path))
            second = build_port_zones(load_geo_config(config_path))
            connection = duckdb.connect()
            try:
                reference_schema = [row[0] for row in connection.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [first["port_reference_path"]]).fetchall()]
                zones = connection.execute("SELECT zone_id, port_id, radius_km FROM read_parquet(?, hive_partitioning=false) ORDER BY port_id", [first["port_zones_path"]]).fetchall()
            finally:
                connection.close()

        self.assertEqual(first["action"], "built")
        self.assertEqual(second["action"], "skipped")
        self.assertEqual(reference_schema, ["port_id", "wpi_index_no", "port_name", "country_code", "longitude_deg", "latitude_deg", "source_region_no", "has_oil_depth"])
        self.assertEqual(zones, [("zone:wpi:10", "wpi:10", 75.0), ("zone:wpi:20", "wpi:20", 75.0)])

    def test_rejects_duplicate_wpi_index_numbers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "wpi.csv"
            source.write_text("INDEX_NO,REGION_NO,PORT_NAME,COUNTRY,LONGITUDE,LATITUDE\n10,1,A,CN,120,30\n10,2,B,SA,50,25\n", encoding="utf-8")
            config_path = root / "geo.yaml"
            config_path.write_text(yaml.safe_dump({"wpi_csv": str(source), "output_root": str(root / "derived"), "port_zone_radius_km": 75,
                "overseas_cluster_radius_km": 250, "china_groups": {"cn_bohai_rim": [120, 38.5], "cn_yangtze_delta": [121.3, 31.2], "cn_southeast_coast": [118.6, 24.7], "cn_pearl_river_delta": [113.7, 22.6]}}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate INDEX_NO"):
                build_port_zones(load_geo_config(config_path))

    def test_preserves_locatable_wpi_port_with_unknown_country(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "wpi.csv"
            source.write_text("INDEX_NO,REGION_NO,PORT_NAME,COUNTRY,LONGITUDE,LATITUDE\n10,1,ALPHA,,120,30\n", encoding="utf-8")
            config_path = root / "geo.yaml"
            config_path.write_text(yaml.safe_dump({"wpi_csv": str(source), "output_root": str(root / "derived"), "port_zone_radius_km": 75,
                "overseas_cluster_radius_km": 250, "china_groups": {"cn_bohai_rim": [120, 38.5], "cn_yangtze_delta": [121.3, 31.2], "cn_southeast_coast": [118.6, 24.7], "cn_pearl_river_delta": [113.7, 22.6]}}), encoding="utf-8")

            report = build_port_zones(load_geo_config(config_path))
            country = duckdb.connect().execute("SELECT country_code FROM read_parquet(?)", [report["port_reference_path"]]).fetchone()[0]

        self.assertEqual(country, "ZZ")

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import yaml

from ais_tanker_pipeline.geo.config import load_geo_config


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

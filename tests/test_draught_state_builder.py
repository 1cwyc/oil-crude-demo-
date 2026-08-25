from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import yaml

from ais_tanker_pipeline.draught.config import load_draught_config, month_range


class DraughtConfigTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()

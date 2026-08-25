from __future__ import annotations

import unittest

from ais_tanker_pipeline.events.event_detector_3h import EventCandidate, detect_events


class EventDetectorTests(unittest.TestCase):
    def test_accepts_load_only_with_stable_draught_change_and_six_hour_port_stop(self) -> None:
        candidate = EventCandidate("imo:1", "before", "after", 10.0, 14.2, 10_000, 40_000, "wpi:10", 12_000, 33_600, 120.0, 30.0)
        events = detect_events([candidate], low_speed_minimum_hours=6, supplementary_change_m=1.5, standard_change_m=3.0)
        self.assertEqual([(event.event_kind, event.event_status, event.port_id) for event in events], [("load", "accepted", "wpi:10")])

    def test_rejects_small_change_when_stop_is_short(self) -> None:
        candidate = EventCandidate("imo:1", "before", "after", 10.0, 12.0, 10_000, 20_000, "wpi:10", 12_000, 15_000, 120.0, 30.0)
        self.assertEqual(detect_events([candidate], low_speed_minimum_hours=6, supplementary_change_m=1.5, standard_change_m=3.0), [])

"""Rule-only core for stable-draught loading and unloading acceptance."""
from __future__ import annotations

from dataclasses import dataclass
from ais_tanker_pipeline.artifacts import canonical_hash


@dataclass(frozen=True)
class EventCandidate:
    crude_vessel_id: str; before_state_id: str; after_state_id: str
    before_draught_m: float; after_draught_m: float; state_end_s: int; next_state_start_s: int
    port_id: str | None; stop_start_s: int | None; stop_end_s: int | None
    longitude_deg: float | None; latitude_deg: float | None


@dataclass(frozen=True)
class AcceptedEvent:
    event_id: str; event_status: str; event_kind: str; crude_vessel_id: str; port_id: str
    event_start_s: int; event_end_s: int; event_longitude_deg: float; event_latitude_deg: float
    before_draught_state_id: str; after_draught_state_id: str; before_draught_m: float; after_draught_m: float


def detect_events(candidates: list[EventCandidate], *, low_speed_minimum_hours: float, supplementary_change_m: float, standard_change_m: float) -> list[AcceptedEvent]:
    """Accept only physically directional, port-linked stable-state transitions."""
    accepted: list[AcceptedEvent] = []
    for item in candidates:
        if item.port_id is None or None in (item.stop_start_s, item.stop_end_s, item.longitude_deg, item.latitude_deg):
            continue
        change = item.after_draught_m - item.before_draught_m
        if abs(change) < supplementary_change_m or item.stop_end_s - item.stop_start_s < low_speed_minimum_hours * 3600:
            continue
        if item.next_state_start_s - item.state_end_s > 96 * 3600:
            continue
        kind = "load" if change > 0 else "unload"
        event_id = "event:" + canonical_hash([item.crude_vessel_id, item.before_state_id, item.after_state_id, kind, item.stop_start_s, item.stop_end_s])[:24]
        accepted.append(AcceptedEvent(event_id, "accepted", kind, item.crude_vessel_id, item.port_id, item.stop_start_s, item.stop_end_s, item.longitude_deg, item.latitude_deg, item.before_state_id, item.after_state_id, item.before_draught_m, item.after_draught_m))
    return accepted

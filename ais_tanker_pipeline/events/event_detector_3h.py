"""Rule-only core for stable-draught loading and unloading acceptance."""
from __future__ import annotations

from dataclasses import dataclass
import argparse
import json
import math
import os
from pathlib import Path
import sys

import duckdb
import pandas
import yaml
from ais_tanker_pipeline.artifacts import canonical_hash
from ais_tanker_pipeline.artifacts import OutputConflict, file_signature, partial_path, read_manifest, sha256_file, write_json_atomic


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


def _distance_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    p = math.pi / 180
    a = math.sin((lat2-lat1)*p/2)**2 + math.cos(lat1*p)*math.cos(lat2*p)*math.sin((lon2-lon1)*p/2)**2
    return 12742 * math.asin(math.sqrt(a))


def _nearest_port(lon: float, lat: float, ports: list[tuple[str, float, float]], radius_km: float) -> str | None:
    ranked = sorted((_distance_km(lon, lat, x, y), port_id) for port_id, x, y in ports)
    if not ranked or ranked[0][0] > radius_km or (len(ranked) > 1 and abs(ranked[0][0]-ranked[1][0]) < 1e-9):
        return None
    return ranked[0][1]


def _read_config(path: str | Path) -> dict[str, object]:
    source = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    required = {"samples_glob", "matches_path", "states_path", "port_reference_path", "output_root", "port_zone_radius_km", "low_speed_max_kn", "minimum_stop_hours", "supplementary_change_m", "standard_change_m"}
    if not isinstance(source, dict) or set(source) != required:
        raise ValueError("event config must contain exactly the version 1 fields")
    return source


def _candidates(config: dict[str, object]) -> list[EventCandidate]:
    con = duckdb.connect()
    try:
        ports = con.execute("SELECT port_id, longitude_deg, latitude_deg FROM read_parquet(?, hive_partitioning=false)", [str(config["port_reference_path"])]).fetchall()
        query = """
        WITH states AS (
          SELECT *, lead(draught_state_id) OVER w after_id, lead(draught_median_m) OVER w after_d,
          lead(state_start_s) OVER w after_start
          FROM read_parquet(?, hive_partitioning=false)
          WINDOW w AS (PARTITION BY crude_vessel_id ORDER BY state_start_s)
        ), pairs AS (
          SELECT * FROM states WHERE after_id IS NOT NULL AND abs(after_d-draught_median_m)>=?
        ), samples AS (
          SELECT m.crude_vessel_id, s.target_time_s, s.longitude_deg, s.latitude_deg, s.sog_kn
          FROM read_parquet(?, hive_partitioning=false) s
          JOIN read_parquet(?, hive_partitioning=false) m USING (mmsi, target_time_s)
          WHERE s.longitude_deg IS NOT NULL AND s.latitude_deg IS NOT NULL AND s.sog_kn IS NOT NULL
        ), stopped AS (
          SELECT p.crude_vessel_id, p.draught_state_id, p.after_id, p.draught_median_m, p.after_d,
          p.state_end_s, p.after_start, min(s.target_time_s) stop_start, max(s.target_time_s) stop_end,
          median(s.longitude_deg) median_lon, median(s.latitude_deg) median_lat
          FROM pairs p JOIN samples s ON s.crude_vessel_id=p.crude_vessel_id
            AND s.target_time_s BETWEEN p.state_end_s-43200 AND p.after_start+43200
            AND s.sog_kn<=?
          GROUP BY ALL HAVING max(s.target_time_s)-min(s.target_time_s)>=?
        ) SELECT * FROM stopped
        """
        rows = con.execute(query, [str(config["states_path"]), float(config["supplementary_change_m"]), str(config["samples_glob"]), str(config["matches_path"]), float(config["low_speed_max_kn"]), float(config["minimum_stop_hours"])*3600]).fetchall()
    finally:
        con.close()
    result=[]
    for vessel,before,after,before_d,after_d,state_end,after_start,start,end,lon,lat in rows:
        port_id=_nearest_port(float(lon),float(lat),ports,float(config["port_zone_radius_km"]))
        result.append(EventCandidate(vessel,before,after,float(before_d),float(after_d),int(state_end),int(after_start),port_id,int(start),int(end),float(lon),float(lat)))
    return result


def run_event_detector(config_path: str | Path, *, force: bool=False) -> dict[str, object]:
    config=_read_config(config_path); root=Path(str(config["output_root"])).resolve()
    event_path=root/"events"/"loading_unloading_events"/"year=2025"/"month=09"/"loading_unloading_events.parquet"; manifest_path=root/"reports"/"manifests"/"event_detector_3h_2025-09.json"
    inputs=[Path(str(config[k])).resolve() for k in ("matches_path","states_path","port_reference_path")]
    input_sigs=[{**file_signature(p),"sha256":sha256_file(p)} for p in inputs]
    existing=read_manifest(manifest_path)
    if isinstance(existing,dict) and existing.get("config_hash")==canonical_hash(config) and existing.get("inputs")==input_sigs and event_path.is_file() and existing.get("output",{}).get("sha256")==sha256_file(event_path):
        return {"action":"skipped","events_path":str(event_path),"manifest_path":str(manifest_path),"counts":existing["counts"]}
    if (event_path.exists() or manifest_path.exists()) and not force: raise OutputConflict("event output already exists; inspect it before rebuilding")
    events=detect_events(_candidates(config),low_speed_minimum_hours=float(config["minimum_stop_hours"]),supplementary_change_m=float(config["supplementary_change_m"]),standard_change_m=float(config["standard_change_m"]))
    frame=pandas.DataFrame([tuple(x.__dict__.values()) for x in events],columns=list(AcceptedEvent.__dataclass_fields__))
    event_path.parent.mkdir(parents=True,exist_ok=True); temporary=partial_path(event_path); con=duckdb.connect()
    try:
        con.register("events",frame); con.execute("COPY (SELECT * FROM events ORDER BY crude_vessel_id,event_start_s) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",[str(temporary)])
    finally: con.close()
    os.replace(temporary,event_path)
    counts={"accepted_events":len(events),"loads":sum(x.event_kind=="load" for x in events),"unloads":sum(x.event_kind=="unload" for x in events)}
    write_json_atomic(manifest_path,{"status":"complete","module_name":"event_detector_3h","algorithm_version":"1.0.0","config_hash":canonical_hash(config),"inputs":input_sigs,"output":{**file_signature(event_path),"sha256":sha256_file(event_path)},"counts":counts})
    return {"action":"built","events_path":str(event_path),"manifest_path":str(manifest_path),"counts":counts}


def main(argv: list[str] | None=None) -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--config",required=True); parser.add_argument("--force",action="store_true"); args=parser.parse_args(argv)
    try: print(json.dumps(run_event_detector(args.config,force=args.force),ensure_ascii=False)); return 0
    except (OSError,ValueError,OutputConflict) as exc: print(f"ERROR: {exc}",file=sys.stderr); return 2


if __name__ == "__main__":
    raise SystemExit(main())

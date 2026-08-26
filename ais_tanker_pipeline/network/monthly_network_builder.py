from __future__ import annotations
import argparse,json,os,re,sys
from pathlib import Path
import duckdb
from ais_tanker_pipeline.artifacts import OutputConflict,file_signature,partial_path,read_manifest,sha256_file,write_json_atomic
from ais_tanker_pipeline.network.config import NetworkConfig,load_network_config

ALGORITHM_VERSION='1.0.0'
EDGE_COLUMNS=['network_month','origin_node_id','destination_node_id','estimated_cargo_t','voyage_count']
FLOW_COLUMNS=['network_month','node_id','export_cargo_t','import_cargo_t','export_voyage_count','import_voyage_count']

def _month(month:str)->tuple[str,str]:
    if not re.fullmatch(r'\d{4}-\d{2}',month) or not 1<=int(month[-2:])<=12: raise ValueError('month must be YYYY-MM')
    return month[:4],month[-2:]
def _files(path:Path)->list[Path]: return sorted(p for p in path.glob('*.parquet') if p.is_file() and '.partial-' not in p.name)
def _sigs(paths:list[Path])->list[dict[str,object]]: return [{**file_signature(p),'sha256':sha256_file(p)} for p in paths]
def _write_query(connection,query,target):
    temp=partial_path(target);temp.parent.mkdir(parents=True,exist_ok=True)
    connection.execute(f"COPY ({query}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",[str(temp)]);return temp

def build_monthly_network(config:NetworkConfig,*,month:str,force:bool=False)->dict[str,object]:
    year,number=_month(month); root=config.output_root
    voyages=_files(root/'voyages'/'crude_voyages'/f'year={year}'/f'month={number}')
    events=sorted(p for p in (root/'events'/'loading_unloading_events').rglob('*.parquet') if p.is_file() and '.partial-' not in p.name)
    zones=root/'geo'/'port_zones'/'port_zones.parquet'
    mapping=root/'network_v1'/'geo'/f'period={config.mapping_period}'/'zone_node_map'/'zone_node_map.parquet'
    nodes=root/'network_v1'/'geo'/f'period={config.mapping_period}'/'network_nodes'/'network_nodes.parquet'
    if not voyages or not events or not all(p.is_file() for p in (zones,mapping,nodes)): raise FileNotFoundError('monthly network input missing')
    edges_path=root/'network_v1'/'monthly_od_edges'/f'year={year}'/f'month={number}'/'monthly_od_edges.parquet'
    flows_path=root/'network_v1'/'monthly_node_flows'/f'year={year}'/f'month={number}'/'monthly_node_flows.parquet'
    manifest=root/'reports'/'manifests'/f'monthly_network_builder_{month}.json'; targets=[edges_path,flows_path]
    inputs=_sigs([*voyages,*events,zones,mapping,nodes]); existing=read_manifest(manifest)
    if isinstance(existing,dict) and existing.get('status')=='complete' and existing.get('algorithm_version')==ALGORITHM_VERSION and existing.get('config_hash')==config.config_hash and existing.get('inputs')==inputs and all(p.is_file() for p in targets) and existing.get('outputs')==_sigs(targets):
        return {'action':'skipped','edges_path':str(edges_path),'flows_path':str(flows_path),'manifest_path':str(manifest),'counts':existing['counts']}
    if (any(p.exists() for p in targets) or manifest.exists()) and not force: raise OutputConflict('monthly network output already exists; inspect it before rebuilding')
    c=duckdb.connect(); c.execute("SET TimeZone='UTC'")
    try:
        base=f'''WITH v AS (SELECT * FROM read_parquet({[str(p) for p in voyages]!r}, hive_partitioning=false)), e AS (SELECT * FROM read_parquet({[str(p) for p in events]!r}, hive_partitioning=false)), mapped AS (SELECT '{month}'::VARCHAR network_month, om.node_id origin_node_id, dm.node_id destination_node_id, v.voyage_id,v.estimated_cargo_t FROM v JOIN e le ON le.event_id=v.load_event_id JOIN e ue ON ue.event_id=v.unload_event_id JOIN read_parquet('{str(zones)}') oz ON oz.port_id=le.port_id JOIN read_parquet('{str(mapping)}',hive_partitioning=false) om ON om.zone_id=oz.zone_id JOIN read_parquet('{str(zones)}') dz ON dz.port_id=ue.port_id JOIN read_parquet('{str(mapping)}',hive_partitioning=false) dm ON dm.zone_id=dz.zone_id WHERE le.event_status='accepted' AND ue.event_status='accepted' AND v.estimated_cargo_t>0 AND strftime(to_timestamp(v.unload_end_s),'%Y-%m')='{month}') '''
        edges_q=base+"SELECT network_month,origin_node_id,destination_node_id,sum(estimated_cargo_t)::DOUBLE estimated_cargo_t,count(*)::BIGINT voyage_count FROM mapped GROUP BY ALL ORDER BY 1,2,3"
        flows_q=base+"SELECT network_month,node_id,sum(export_cargo_t)::DOUBLE export_cargo_t,sum(import_cargo_t)::DOUBLE import_cargo_t,sum(export_voyage_count)::BIGINT export_voyage_count,sum(import_voyage_count)::BIGINT import_voyage_count FROM (SELECT network_month,origin_node_id node_id,estimated_cargo_t export_cargo_t,0::DOUBLE import_cargo_t,1::BIGINT export_voyage_count,0::BIGINT import_voyage_count FROM mapped UNION ALL SELECT network_month,destination_node_id,0::DOUBLE,estimated_cargo_t,0::BIGINT,1::BIGINT FROM mapped) GROUP BY ALL ORDER BY 1,2"
        staged_edges=_write_query(c,edges_q,edges_path);staged_flows=_write_query(c,flows_q,flows_path)
        cols=lambda p:[r[0] for r in c.execute('DESCRIBE SELECT * FROM read_parquet(?,hive_partitioning=false)',[str(p)]).fetchall()]
        if cols(staged_edges)!=EDGE_COLUMNS or cols(staged_flows)!=FLOW_COLUMNS: raise RuntimeError('monthly output schema failed')
        totals=c.execute('SELECT (SELECT sum(estimated_cargo_t) FROM read_parquet(?,hive_partitioning=false)),(SELECT sum(export_cargo_t) FROM read_parquet(?,hive_partitioning=false)),(SELECT sum(import_cargo_t) FROM read_parquet(?,hive_partitioning=false)),(SELECT sum(voyage_count) FROM read_parquet(?,hive_partitioning=false)),(SELECT count(*) FROM read_parquet(?,hive_partitioning=false))',[str(staged_edges),str(staged_flows),str(staged_flows),str(staged_edges),str(staged_edges)]).fetchone()
        if not all(value is not None for value in totals[:3]) or any(abs(float(totals[0])-float(v))>1e-6 for v in totals[1:3]): raise RuntimeError('monthly cargo conservation failed')
        for target,staged in zip(targets,[staged_edges,staged_flows]): target.parent.mkdir(parents=True,exist_ok=True);os.replace(staged,target)
        counts={'edges':int(totals[4]),'voyages':int(totals[3]),'estimated_cargo_t':float(totals[0])}; outputs=_sigs(targets)
        write_json_atomic(manifest,{'status':'complete','module_name':'monthly_network_builder','algorithm_version':ALGORITHM_VERSION,'config_hash':config.config_hash,'inputs':inputs,'outputs':outputs,'counts':counts,'month':month,'mapping_period':config.mapping_period})
    finally: c.close()
    return {'action':'built','edges_path':str(edges_path),'flows_path':str(flows_path),'manifest_path':str(manifest),'counts':counts}
def main(argv=None)->int:
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--month',required=True);p.add_argument('--dry-run',action='store_true');p.add_argument('--force',action='store_true');a=p.parse_args(argv)
    try:
        config=load_network_config(a.config);year,number=_month(a.month)
        report={'action':'would_build'} if a.dry_run else build_monthly_network(config,month=a.month,force=a.force);print(json.dumps(report));return 0
    except (OSError,ValueError,RuntimeError,OutputConflict) as e: print(f'ERROR: {e}',file=sys.stderr);return 2
if __name__=='__main__':raise SystemExit(main())

from __future__ import annotations
import argparse,json,math
from pathlib import Path
import duckdb
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from matplotlib.cm import ScalarMappable
from matplotlib.lines import Line2D
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from ais_tanker_pipeline.network.config import load_network_config

def render(config,month):
    y,m=month.split('-');root=config.output_root
    flows=root/'network_v1'/'monthly_node_flows'/f'year={y}'/f'month={m}'/'monthly_node_flows.parquet'
    points=root/'routes'/'voyage_trajectory_points'/f'year={y}'/f'month={m}'/'voyage_trajectory_points.parquet'
    qc=root/'routes'/'voyage_trajectory_qc'/f'year={y}'/f'month={m}'/'voyage_trajectory_qc.parquet'
    nodes=root/'network_v1'/'geo'/f'period={config.mapping_period}'/'network_nodes'/'network_nodes.parquet'
    voyages=root/'voyages'/'crude_voyages'/f'year={y}'/f'month={m}'/'crude_voyages.parquet'
    c=duckdb.connect();
    try:
        node_rows=c.execute('''SELECT n.node_id,n.node_kind,n.longitude_deg,n.latitude_deg,f.export_cargo_t,f.import_cargo_t FROM read_parquet(?,hive_partitioning=false) n JOIN read_parquet(?,hive_partitioning=false) f USING(node_id)''',[str(nodes),str(flows)]).fetchall()
        route_rows=c.execute('''SELECT p.voyage_id,p.target_time_s,p.longitude_deg,p.latitude_deg,v.estimated_cargo_t FROM read_parquet(?,hive_partitioning=false) p JOIN read_parquet(?) v USING(voyage_id) JOIN read_parquet(?,hive_partitioning=false) q USING(voyage_id) WHERE q.route_status <> 'identity_conflict' ORDER BY 1,2''',[str(points),str(voyages),str(qc)]).fetchall()
    finally:c.close()
    cargos=np.array([r[4] for r in route_rows if r[4]>0],float);norm=LogNorm(vmin=max(float(np.quantile(cargos,.05)),1),vmax=float(np.quantile(cargos,.99)));cmap=plt.get_cmap('plasma_r')
    fig=plt.figure(figsize=(18,9),facecolor='white');ax=plt.axes(projection=ccrs.PlateCarree());ax.set_global();ax.set_facecolor('white')
    ax.add_feature(cfeature.LAND,facecolor='#dddddd',edgecolor='#bcbcbc',linewidth=.35,zorder=0);ax.add_feature(cfeature.BORDERS,edgecolor='#c7c7c7',linewidth=.25,zorder=1);ax.coastlines(resolution='110m',color='#aaaaaa',linewidth=.35,zorder=1);ax.set_axis_off()
    last={}
    for vid,t,lon,lat,cargo in route_rows:
        prev=last.get(vid)
        if prev and t-prev[0]<=86400 and abs(lon-prev[1])<=5 and abs(lat-prev[2])<=5:
            ax.plot([prev[1],lon],[prev[2],lat],transform=ccrs.PlateCarree(),color=cmap(norm(cargo)),alpha=.42,linewidth=.25+2.4*norm(cargo),zorder=2)
        last[vid]=(t,lon,lat)
    values=[]
    for node_id,kind,lon,lat,exported,imported in node_rows:
        if kind=='china_group': color='#1464d2';value=exported+imported
        elif exported>imported: color='#d73027';value=exported
        else: color='#1a9850';value=imported
        values.append((lon,lat,color,value))
    maxv=max(v[3] for v in values)
    for lon,lat,color,value in values: ax.scatter(lon,lat,s=15+160*math.sqrt(value/maxv),transform=ccrs.PlateCarree(),c=color,edgecolors='white',linewidths=.55,zorder=4,alpha=.96)
    ax.legend(handles=[Line2D([0],[0],marker='o',color='w',markerfacecolor='#1464d2',label='China port groups',markersize=8),Line2D([0],[0],marker='o',color='w',markerfacecolor='#d73027',label='Net export areas',markersize=8),Line2D([0],[0],marker='o',color='w',markerfacecolor='#1a9850',label='Net import areas',markersize=8)],loc='lower left',frameon=False,fontsize=10)
    cb=fig.colorbar(ScalarMappable(norm=norm,cmap=cmap),ax=ax,orientation='horizontal',fraction=.035,pad=.035,aspect=45);cb.set_label('SCPC estimated cargo per voyage (t)')
    ax.set_title(f'Global crude-oil maritime network — {month}\nActual matched 3-hour AIS voyage trajectories',fontsize=15,pad=15)
    out=root/'visualizations'/'crude_od_network'/f'year={y}'/f'month={m}';out.mkdir(parents=True,exist_ok=True);png=out/f'crude_od_network_{month}.png';pdf=out/f'crude_od_network_{month}.pdf';fig.savefig(png,dpi=300,bbox_inches='tight');fig.savefig(pdf,bbox_inches='tight');plt.close(fig)
    return {'png_path':str(png),'pdf_path':str(pdf),'nodes':len(values),'route_points':len(route_rows)}
def main(argv=None):
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--month',required=True);a=p.parse_args(argv);print(json.dumps(render(load_network_config(a.config),a.month)));return 0
if __name__=='__main__':raise SystemExit(main())

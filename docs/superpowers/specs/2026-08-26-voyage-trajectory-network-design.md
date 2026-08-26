# 真实 AIS 航迹、月度/年度原油网络与全球制图 PRD

## 1. 目标与边界

本设计把已经识别的宏观原油航次连接到其真实三小时 AIS 位置序列，建立可复用的月度和年度加权有向网络，并以论文级全球地图展示真实航迹。首个真实数据验收月为 `2025-09`；当 `2025-07` 至 `2026-06` 全部输入可用时，使用完全相同的接口生成十二个月网络和一个年度网络。

本设计不改变原油船识别、吃水状态、事件识别、SCPC 货量公式或既有航次接受规则。它不把 OD 直线、大圆弧、人工航线或插值点称为真实 AIS 航迹。

## 2. 已核实事实

- `2025-09` 有 4,937,332 条三小时 AIS 样本、680,295 条原油船匹配记录、631 条已接受航次和 233 条探索性 OD 边。
- 每一条该月航次的装载完成至卸载开始窗口都落在 2025-09；全部有至少 3 个匹配的真实三小时 AIS 点，625 条至少有 5 个点。
- 全分辨率 `positions/tanker` 有约 11.36 亿行，且含一个零字节 `.partial-*.parquet` 残留；它不是本模块的常规输入。
- 真实 `geo/network_nodes` 只有节点坐标；没有 `port_id -> node_id` 的权威映射。现有边表由未提交的探索性聚类逻辑产生，不能作为可复用网络模块的权威实现。
- 三小时样本有唯一候选键 `(mmsi, target_time_s)`，并含 UTC 时刻、经纬度、硬有效性和位置质量字段；fleet match 使用同一键并提供 `crude_vessel_id`。

## 3. 方案选择

选择方案 A：仅使用实际匹配的三小时 AIS 点。

1. 每个航次从装载事件 `event_end_s` 到卸载事件 `event_start_s` 之间读取同一 `crude_vessel_id` 的真实样本。
2. 不在港口事件坐标与首末 AIS 点之间补线；不跨数据缺口连线；不进行地图几何平滑或避陆推断。
3. 绘图把每一条航次的真实分段轨迹作为视觉线层；正式网络边仍按航次货量在 OD 表中聚合。

不采用全分辨率位置作为主输入：它比三小时样本大约三个数量级，且不会提高宏观网络图的可复核性。全分辨率位置仍可由后续独立的高风险航次审计模块按需读取。

## 4. 模块划分与依赖顺序

每个模块一个分支和 PR；真实数据和主机 YAML 均在 Git 外。

1. `geo_node_mapping_builder`：在已批准的港区/节点语义下发布权威 `zone_node_map`。这是月度网络的阻断前置条件。
2. `voyage_trajectory_builder`：发布实际三小时 AIS 航迹点和最小轨迹 QC。
3. `monthly_network_builder`：发布唯一的版本化月度节点流和 OD 边表。
4. `annual_network_builder`：仅在十二个完整月度网络存在时汇总年度节点流和 OD 边表。
5. `crude_od_map_renderer`：消费正式网络、航次和实际轨迹点，输出月度或年度全球地图。

除通用 artifact/manifest 能力外，模块不共享业务逻辑；下游只通过稳定 ID 和已发布的 Parquet 合同连接。

## 5. 权威输入合同

### 5.1 区—节点映射前置物

`geo/zone_node_map/zone_node_map.parquet` 必须只含：

| 字段 | 类型 | 规则 |
| --- | --- | --- |
| `zone_id` | VARCHAR | 非空、唯一，引用 `geo/port_zones` |
| `node_id` | VARCHAR | 非空，引用 `geo/network_nodes` |
| `mapping_method` | VARCHAR | 固定、版本化的映射方法标识 |

一个有效港区只能映射一个节点。中国四大港口群由批准的边界/配置映射；海外功能区的聚类半径和节点规则必须进入配置哈希。不能在网络模块中临时根据港口距离重新聚类。

### 5.2 航迹模块输入

- `voyages/crude_voyages/year=YYYY/month=MM/*.parquet`：最少读取 `voyage_id`、`crude_vessel_id`、`load_event_id`、`unload_event_id`、`unload_end_s`。
- `events/loading_unloading_events/**/*.parquet`：最少读取 `event_id`、`event_status`、`event_start_s`、`event_end_s`。
- `samples_3h/timezone=UTC/**/*.parquet`：最少读取 `mmsi`、`target_time_s`、`longitude_deg`、`latitude_deg`、`is_hard_valid`、`dq_mask`。
- `enrichment/crude_fleet_matches/year=YYYY/month=MM/*.parquet`：最少读取 `mmsi`、`target_time_s`、`crude_vessel_id`。

航迹读取可跨月，以完整航次时间窗为准；输入根目录必须覆盖该窗口。路径发现只匹配完成态文件名，绝不读取 `.partial-*` 文件。

### 5.3 网络模块输入

- 已接受 `crude_voyages`。
- 对应的 accepted load/unload events，用于 `port_id -> zone_id -> node_id` 双连接。
- `port_zones`、权威 `zone_node_map`、`network_nodes`。

网络月永远取 `unload_end_s` 的 UTC 自然月。月度网络不会从图片、航迹几何或已有探索性边表反推节点映射。

## 6. 输出合同

所有新产物写入外部派生根目录的 `network_v1` 或 `routes` 新路径，不覆盖当前探索性 `trade/monthly_od_edges`。

### 6.1 真实轨迹点

`routes/voyage_trajectory_points/year=YYYY/month=MM/voyage_trajectory_points.parquet`：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `voyage_id` | VARCHAR | 航次主键 |
| `point_index` | BIGINT | 同航次按 UTC 递增的序号 |
| `target_time_s` | BIGINT | 原始三小时目标时刻 |
| `longitude_deg` | DOUBLE | 原始有效 AIS 经度 |
| `latitude_deg` | DOUBLE | 原始有效 AIS 纬度 |

它是过滤后的航次轨迹 sidecar，不复制完整 AIS 位置表，不重复货量、MMSI、速度或船舶字段。

`routes/voyage_trajectory_qc/year=YYYY/month=MM/voyage_trajectory_qc.parquet`：

`voyage_id`、`sample_count`、`coverage_fraction`、`max_gap_s`、`route_status`。`route_status` 仅允许 `complete`、`gapped`、`no_points`、`window_not_covered`。它是审计表，不是概率或 confidence 表。

### 6.2 月度网络

`network_v1/monthly_node_flows/year=YYYY/month=MM/monthly_node_flows.parquet` 严格仅：

`network_month`、`node_id`、`export_cargo_t`、`import_cargo_t`、`export_voyage_count`、`import_voyage_count`。

`network_v1/monthly_od_edges/year=YYYY/month=MM/monthly_od_edges.parquet` 严格仅：

`network_month`、`origin_node_id`、`destination_node_id`、`estimated_cargo_t`、`voyage_count`。

### 6.3 年度网络

`network_v1/annual_node_flows/period=YYYY-MM_YYYY-MM/annual_node_flows.parquet` 严格仅：

`network_period`、`node_id`、`export_cargo_t`、`import_cargo_t`、`export_voyage_count`、`import_voyage_count`。

`network_v1/annual_od_edges/period=YYYY-MM_YYYY-MM/annual_od_edges.parquet` 严格仅：

`network_period`、`origin_node_id`、`destination_node_id`、`estimated_cargo_t`、`voyage_count`。

年度值是配置中明确列出的连续 12 个 UTC 月度表的和；首个研究年度为 `2025-07_2026-06`。缺少、重复、schema 不一致或月份不属于该配置序列时失败关闭。

### 6.4 图片

`visualizations/crude_od_network/year=YYYY/month=MM/` 或 `visualizations/crude_od_network/period=YYYY-MM_YYYY-MM/`：

- `crude_od_network_YYYY-MM.png` 和 `.pdf`，或 `crude_od_network_YYYY-MM_YYYY-MM.png` 和 `.pdf`。
- 伴随 JSON manifest，记录输入 SHA256、配置哈希、Cartopy/Matplotlib 版本、绘制航次数、分段数、节点类别计数和货量范围。

## 7. 算法与 QC

### 7.1 航迹重建

1. 对每条航次精确连接两个 accepted events；事件类型或船舶不一致、时间窗非正、重复 voyage ID 均失败关闭。
2. 在 `load.event_end_s <= target_time_s <= unload.event_start_s` 内连接 fleet match 和三小时 AIS；仅接受 `is_hard_valid=true`、有限且物理范围内的经纬度。
3. 轨迹点按 `(target_time_s, mmsi)` 排序。同一航次同一时刻多点或时间逆序失败关闭。
4. 预期三小时槽数为 `floor((unload_start_s - load_end_s)/10800)+1`；`coverage_fraction=sample_count/expected_slots`。最大间隔超过配置的 `max_segment_gap_hours`（初始值 24）时，`route_status=gapped`。
5. `no_points` 和 `window_not_covered` 不得伪造路线；它们保留在 QC 表和 manifest。绘图仅画真实、连续的分段。

### 7.2 月度与年度聚合

月度 builder 只接受能通过双端节点映射且 `estimated_cargo_t > 0` 的航次。它在 DuckDB 内按 OD 聚合；不把全球数据加载进 Pandas。节点流从同一航次集直接聚合，保证边表和节点流总货量守恒。

年度 builder 读取配置明确的 12 个完整月度分区并按 node/OD 聚合。它不重算航次、不重画路径、不把未完成年度误报为年度结果。

### 7.3 全球地图

- Cartopy `PlateCarree` 底图：白色海洋、浅灰陆地、低对比海岸线和国界；无网格、坐标轴或经纬度标签。
- 每条可绘制航次使用其真实 AIS 点段；相邻点超过阈值时分段，不跨缺口连线。
- 航次线的颜色、宽度和透明度按该航次 `estimated_cargo_t` 的对数尺度映射。地图 caption 明确这是航次 SCPC 货量；正式 OD 边权仍在网络边表中。
- 节点颜色：`china_group` 固定蓝色；其他节点在当前月/年 `export_cargo_t > import_cargo_t` 时红色，反之绿色。中国节点以总吞吐量缩放，红色节点以出口量缩放，绿色节点以进口量缩放。
- 提供节点类型图例和以吨为单位的连续货量 colorbar。自环货量不从网络表删除；它不具有可跨海航迹时仅通过节点吞吐量体现，并在 manifest 计数。

## 8. 配置、manifest 与运行

每个模块使用独立、未版本化主机 YAML；版本化 example YAML 只使用环境变量路径。所有阈值和年份/月范围进入 canonical config hash。

每个 CLI 支持 `--dry-run`、幂等跳过和经人工核查后的 `--force`。派生输出、manifest 和 partial/backup 均使用现有原子发布能力。输入 schema、输出 schema、行数、键唯一性、非空关键值和 SHA256 都是发布门槛。

新增依赖为 Cartopy 及其受支持的 PyProj/Shapely 运行时；依赖版本同时写入 `requirements.txt` 和 `environment.yml`。GeoPandas 和 NetworkX 不进入本阶段，因为没有空间叠加或网络指标计算需求。

## 9. 测试和真实验收

### 合成测试

- 航迹时间窗、跨月读取、AIS/match 键连接、排序、重复/缺失/越界坐标和间隙切段。
- 港区到节点的一对一映射和映射缺失失败关闭。
- 月度边、节点流、货量守恒、UTC 卸载月、年度完整十二月门禁。
- 地图在无显示器环境写出 PNG/PDF，带正确节点类别、colorbar 和 manifest；测试不下载真实数据。
- idempotency、force、输出损坏、partial 恢复和 manifest 不匹配。

### 2025-09 真实验收

1. 预检只读验证 30 个完成态三小时样本分区、fleet match、events、voyages 和权威 zone-node map。
2. 生成实际轨迹点/QC；验证 631 条航次均有真实点、轨迹点数与 QC 一致、没有人造点。
3. 生成新的月度网络；验证 OD 和节点流货量守恒、节点 ID 都可引用、输入航次数与输出 voyage_count 守恒。
4. 生成 300 DPI PNG/PDF；人工核验中国为蓝、净出口为红、净进口为绿、边为真实 AIS 分段且无经纬度调试轴。
5. 年度 CLI 在其余 11 个月未齐备时必须返回受控错误；数据齐备后再生成完整年度网络和年度图。

## 10. 风险与不实施范围

- 三小时点不能表达港内微观航线；本研究目标是宏观航线，故不使用全分辨率位置。
- AIS 覆盖缺口会产生断线，不允许用大圆弧补齐。缺口比例和最大间隔通过 QC/manifest 公开。
- `geo_node_mapping_builder` 是当前交付的必要前置模块；未发布其权威映射时，新的月度/年度网络不应运行。
- 本阶段不实施海峡穿越判定、航线分类、国家统计验证、路径优化、在线服务、爬虫、DWT 模型或个体船舶人工身份复核。

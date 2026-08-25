# 2025-09 港区与网络节点生成 PRD

## 1. 目标与边界

为原油船三小时 AIS 的停留与装卸事件提供唯一、可复现的候选港区；在已接受的装卸事件出现后，再生成正式网络节点。正式网络只保留中国四大港口群和由实际装卸港激活的海外原油功能区。

不以“原油船靠港”本身认定装卸；靠港、锚泊、补给、维修和压载均可能发生。只有 `event_detector_3h` 的吃水、停留和速度规则接受的事件才可激活网络节点。

不实施爬虫、在线地图服务、人工身份档案或对原始 AIS 的修改。WPI 原始 CSV、真实 Parquet 和主机 YAML 均不进入 Git。

## 2. 已核实事实与设计判断

### 已核实

- 已上传 WPI CSV 有 3,669 个港口，字段含 `INDEX_NO`、`REGION_NO`、`PORT_NAME`、`COUNTRY`、`LATITUDE`、`LONGITUDE`、`OIL_DEPTH`、`CARGOWHARF` 与 `CARGO_ANCH`。
- 2025-09 有 4,937,332 条三小时样本，680,295 条匹配原油船样本，足以在 DuckDB 中进行批量候选关联。
- NGA WPI 是公开可下载的全球港口位置和属性基准；本项目将其作为来源点位，而非作为装卸事实。

### 设计判断

1. 每个 WPI 港口生成一个半径 75 km 的候选 `port_zone`；若多个候选命中，事件总是选择球面距离最小的港口，距离并列则失败关闭。
2. 中国港口按离四个固定群中心最近的规则归入 `cn_bohai_rim`、`cn_yangtze_delta`、`cn_southeast_coast`、`cn_pearl_river_delta`。这保证正式中国节点恒为四个；不会另建 `china_other` 节点。
3. 海外节点仅由接受装卸事件关联到的 WPI 港口生成。将这些活动海外港口按 250 km 球面连通分量聚合；节点 ID 为其成员最小 WPI ID 的稳定哈希，名称为成员港口/国家的确定性摘要。这是“海外功能区”的可复现粗粒度定义，阈值进入配置和 manifest 哈希。
4. 没有通过事件规则的港口不会进入正式节点表或边表。

## 3. 输入与输出合同

主机 YAML：

```yaml
wpi_csv: "${AIS_GEO_ROOT}/world_port_index.csv"
output_root: "${AIS_DERIVED_ROOT}"
port_zone_radius_km: 75
overseas_cluster_radius_km: 250
china_groups:
  cn_bohai_rim: [120.0, 38.5]
  cn_yangtze_delta: [121.3, 31.2]
  cn_southeast_coast: [118.6, 24.7]
  cn_pearl_river_delta: [113.7, 22.6]
```

阶段一输入仅为 WPI CSV，输出：

- `geo/port_reference/port_reference.parquet`：`port_id`、`wpi_index_no`、`port_name`、`country_code`、`longitude_deg`、`latitude_deg`、`source_region_no`、`has_oil_depth`。
- `geo/port_zones/port_zones.parquet`：`zone_id`、`port_id`、`longitude_deg`、`latitude_deg`、`radius_km`。

阶段二额外读取已接受事件中的 `event_id`、`event_status`、`port_id`；输出：

- `geo/network_nodes/network_nodes.parquet`：`node_id`、`node_name`、`node_kind`、`longitude_deg`、`latitude_deg`。
- `geo/zone_node_map/zone_node_map.parquet`：`port_id`、`node_id`。

正式 Parquet 不落盘原始 WPI 其他字段、事件 ID、置信度或冗余坐标。

## 4. QC、幂等与验收

- WPI 必须有唯一、非空、正整数 `INDEX_NO`、非空名称、有限且在物理范围内的经纬度；错误阻断。来源国家为空但坐标和港名有效时保留，并以 `ZZ` 明确标注未知，不从港名推测国家。
- 阶段一输出 ID 唯一、坐标有限、半径严格为配置值；阶段二只接受唯一 accepted-event `port_id`，不存在港口或距离聚类冲突均阻断。
- 输入、配置和输出 SHA256 写入 manifest；相同运行幂等跳过；既有不一致产物失败关闭，`--force` 原子重建。
- 真实 2025-09 验收需报告 WPI 行数、候选港数、被接受事件激活的港数、中国四群节点数、海外功能区数，以及所有输出的 SHA256。

## 5. 风险与不实施范围

- 75 km 点位港区是宏观候选范围，不能替代码头边界；事件规则的低速/停留/吃水门槛负责防止误入网络。
- 250 km 是海外功能区聚合尺度，不代表行政区或贸易统计国；其敏感性仅在后续验证中讨论，不在第一版引入概率/置信区间。
- WPI 缺漏或坐标误差可能使事件无港区匹配；该事件不得伪造节点，按事件规则拒绝。

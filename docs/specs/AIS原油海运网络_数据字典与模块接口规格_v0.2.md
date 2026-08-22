# AIS原油海运网络数据字典与模块接口规格 v0.2

> 状态：依据用户评审意见形成的精简设计基线；尚未进入代码开发。
>
> 目标：在现有 `AIS_Tanker_3H_Pipeline`（Python + Parquet + DuckDB）上增量扩展，重建 2025-07 至 2026-06 的原油航次、月度贸易网络和关键通道约束层。
>
> 正式网络：只生成“中国四大港口群精细化、海外原油功能区粗粒度化”的月度有向加权网络。

## 1. 本次修订结论

### 1.1 三小时样本可以作为主识别数据，但不能理解为无条件充分

经重新核对参考论文，原规则是：

- 吃水变化大于 3 m；
- 吃水变化点航速不高于 2 kn；
- 相邻吃水变化点之间至少 12 h、航行距离至少 30 km；
- 每个航次至少 120 个轨迹点。论文明确说明，120 点来自“最长 6 min 更新间隔下，12 h 至少有 120 个 AIS 点”的数据条件。

因此，本研究采用以下迁移原则：

1. `samples_3h_enriched` 是装卸候选、航次分段、港区停留和航路识别的正式主输入。
2. 不能把论文的 120 点直接改成某个固定的三小时点数；改为“实际三小时样本数 / 理论三小时样本数”的覆盖规则。
3. 装卸识别比较的是事件前后稳定吃水状态，不是相邻两个三小时点的单次差值。
4. 三小时分辨率足以支持月度贸易量和 OD 网络重建，但事件代表时刻通常只能精确到约 3 h；月末和月初附近事件必须写入质量标记。
5. 全分辨率位置不再是每个事件的强制输入，只用于规则校准、月界事件、轨迹稀疏事件和抽样核验。

### 1.2 “尽量宽松”采用召回优先的确定性规则，不采用遍布全流程的概率分数

正式规则分为两种接受路径：

- 标准路径：`|吃水变化| > 3 m` 且变化附近 `SOG <= 2 kn`。
- 补充路径：`1.5 m <= |吃水变化| <= 3 m`，同时满足同一停留段持续至少 6 h、变化后状态至少持续两个三小时样本，并有原油港区或原油功能区证据。

事件只保存 `accepted/rejected`、命中的规则编号和质量位掩码，不保存人为加权得到的连续“置信度”。无法判定的记录保留在候选表中，不进入正式网络。

需要直接指出的边界：放宽规则只能补偿 AIS 漏报和抽样遗漏，不能用来补偿宏观统计中的管道运输。如果某国官方进出口量包含陆上管道，AIS 海运量天然可能更低；通过放宽船舶规则强行追平总量会产生系统性高估。

### 1.3 字段精简原则

- 来源表已有字段不改写；新增表只保存后续模块实际读取的字段。
- 可由同表字段直接计算的值原则上不落盘，例如 `state_duration_s`、`draught_change_m`、L/B、L×B。
- 版本、输入哈希、配置哈希和生成时间写在模块 manifest，不在每一行重复。
- 不传播逐记录概率、上下界和平均置信度；模型评估指标写入独立报告。
- 原始来源定位字段只保留在最靠近来源的表中，下游通过稳定 ID 关联。
- 同一概念只有一个权威表；其他模块引用 ID，不复制整套属性。

## 2. 已知事实、推断和待核验项

### 2.1 已核实或由用户确认

- 12 个月现有 output 的字段结构相同，后续在另一台电脑执行。
- 现有正式数据为 Zstandard Parquet，并使用 DuckDB 查询。
- 现有油轮候选口径是 AIS 船型 80–89。
- `static_shards` 含带时间戳的 `draught_m`；登记表中的 `latest_draught_m` 只是摘要。
- DWT 只用于选择区块系数 `Cb`，不作为 SCPC 公式中的连续输入。
- 正式网络节点采用中国四大港口群和海外粗粒度功能区；海峡属于航路约束，不属于供需节点。

### 2.2 合理推断和设计判断

- 三小时样本适合宏观航次重建，但不适合声称装卸时刻达到分钟级精度。
- 对本课题而言，确定性等级预测和质量标记比逐表传播概率更易复现、也更符合最终验证方法。
- 正式批处理应继续以 DuckDB 扫描 Parquet；不应另建一套把全球轨迹全部载入 Pandas/GeoPandas 内存的主流程。

### 2.3 当前电脑无法核实

- 另一台电脑上的实际目录、文件命名和代码结构。
- ChinaPorts 当前查询接口、返回字段名称、是否需要 JavaScript 渲染及实际命中率。
- 27,762 艘一月候选船中长、宽、DWT 的实际覆盖率。

这些项目只影响适配器和配置，不改变本数据合同。开发前在执行电脑上完成一次轻量核验即可。

## 3. 架构原则与可复用组件

### 3.1 优先复用

| 能力 | 采用方案 | 复用边界 |
|---|---|---|
| 分区扫描、窗口计算、时间关联、聚合 | 现有 DuckDB + Parquet | 不再创建第二套 ETL 引擎 |
| 点落区、航迹与海峡相交、球面距离 | DuckDB `spatial` 扩展 | 距离使用球面/椭球函数，不能把经纬度平面距离当米 |
| ChinaPorts 请求调度 | Scrapy | 复用 Retry、HTTP cache、AutoThrottle、`JOBDIR`；不自写并发调度器 |
| DWT 七分类 | scikit-learn | 复用分类器、分层交叉验证、混淆矩阵和分类报告；不自写机器学习算法 |
| 小规模空间文件编制与人工核验 | GeoPandas/Shapely/QGIS | 只处理港区和通道配置，不承载全球轨迹主计算 |
| 网络指标 | NetworkX | 只读取月度边表计算指标，不参与航次重建 |

开源依据：

- DuckDB Spatial 已提供 `ST_Intersects`、`ST_Distance_Sphere`、`ST_Distance_Spheroid` 等函数：<https://duckdb.org/docs/current/core_extensions/spatial/functions>
- Scrapy 已提供持久化任务目录、重试、HTTP 缓存和自动限速：<https://docs.scrapy.org/en/latest/topics/settings.html>、<https://docs.scrapy.org/en/master/topics/autothrottle.html>
- scikit-learn 已提供分层交叉验证和标准分类评估：<https://scikit-learn.org/stable/modules/cross_validation.html>、<https://scikit-learn.org/stable/modules/generated/sklearn.metrics.classification_report.html>

### 3.2 不作为正式主依赖

- MovingPandas 可用于少量轨迹的可视化和停留识别原型，但其 Pandas/GeoPandas 内存模型不适合作为本项目全球三小时样本的正式计算主干。
- 不建立通用工作流平台、消息队列、在线服务、仪表盘或微服务。
- 不实现自定义缓存框架、自定义分类算法、自定义空间数据库。

### 3.3 复用现有仓库的前置检查

在另一台电脑开始任何模块前，先搜索现有项目是否已有以下能力：配置加载、日志、Parquet 写入、DuckDB 连接、分区路径生成、manifest、稳定哈希、质量位掩码、命令行入口和测试夹具。已有能力通过公共接口扩展；不得复制一份相似工具到新目录。

## 4. 版本与溯源合同

### 4.1 表级 manifest

每个模块每次运行生成一个 JSON manifest，至少包含：

| 字段 | 作用 |
|---|---|
| `run_id` | 唯一运行编号 |
| `module_name` | 模块名 |
| `algorithm_version` | 代码/规则版本 |
| `input_paths` | 输入文件或分区 |
| `input_fingerprint` | 输入 schema 与文件清单哈希 |
| `config_hash` | 配置哈希 |
| `output_paths` | 输出位置 |
| `row_count` | 输出行数 |
| `min_time_s`、`max_time_s` | 时间边界；无时间表可为空 |
| `qc_counts` | 各质量标记计数 |
| `created_at_utc` | 生成时间 |

这些字段不再重复写入每一行 Parquet。

### 4.2 稳定 ID

- `internal_vessel_id`：有效 IMO 优先；无有效 IMO 时使用 MMSI 身份段。
- `draft_state_id`：船舶 ID + 状态起止时刻 + 状态规则版本的哈希。
- `event_id`：船舶 ID + 事件类型 + 事件窗口的哈希。
- `voyage_id`：装载事件 ID + 卸载事件 ID 的哈希。
- `zone_id`、`node_id`、`route_id`、`chokepoint_id`：由版本化配置持久维护。

## 5. 现有输入的最小读取合同

下表只列新模块实际读取的字段，不要求删除原 output 中其他字段。

### 5.1 `registry/tanker_registry`

| 字段 | 用途 |
|---|---|
| `mmsi` | 候选船主键 |
| `is_tanker` | 候选过滤 |
| `observed_ship_types` | 船型集合 |
| `ship_type_conflict` | 身份/船型质量标记 |
| `latest_imo` | ChinaPorts 优先查询、跨月身份合并和 MMSI 复用核验 |
| `first_static_time_s`、`last_static_time_s` | 身份段时间范围 |

`latest_ship_type` 和 `latest_ship_name` 不复制到任何新增正式表。前者已被 `observed_ship_types` 覆盖；后者只在原表中保留。`latest_callsign`、`latest_draught_m`、`latest_destination` 不进入新流水线。

### 5.2 `registry/static_shards`

| 字段 | 用途 |
|---|---|
| `mmsi`、`imo` | 身份关联 |
| `ship_name`、`callsign` | 只用于冲突核验，不向下游复制 |
| `ship_type` | 船型冲突核验 |
| `receive_time_s`、`draught_m` | 时变吃水观测 |
| `source_file`、`line_number`、`dq_mask` | 行级溯源和质量 |

### 5.3 `samples_3h`

| 字段 | 用途 |
|---|---|
| `mmsi`、`target_time_s` | 主键和排序 |
| `pos_time_s` | 实际位置时刻 |
| `longitude_deg`、`latitude_deg` | 停留、港区、航路和距离 |
| `sog_kn`、`cog_deg`、`navigation_status` | 停留和运动状态 |
| `is_hard_valid`、`dq_mask`、`absolute_offset_seconds` | 位置质量 |

`target_time_local`、`timezone`、`candidate_count`、`true_heading` 和 `rot` 不进入新模块核心合同。

### 5.4 `positions/tanker`

不是正式全量输入。只在以下情况下按船舶和时间窗读取：规则校准样本、月界事件、三小时覆盖不足事件、货量异常事件和人工抽查样本。

## 6. 核心数据表

### 6.1 船舶身份 `enrichment/vessel_identity`

主键：`internal_vessel_id`。

| 字段 | 类型 | 作用 |
|---|---|---|
| `internal_vessel_id` | VARCHAR | 全流程统一船舶 ID |
| `canonical_imo` | INTEGER | 校验通过的 IMO，可空 |
| `primary_mmsi` | BIGINT | 主 MMSI |
| `all_mmsi` | BIGINT[] | 该身份包含的 MMSI |
| `first_seen_s` | BIGINT | 身份段起点 |
| `last_seen_s` | BIGINT | 身份段终点 |
| `identity_method` | VARCHAR | `imo_exact/mmsi_segment/manual` |
| `identity_qc_mask` | BIGINT | IMO/MMSI/时间冲突 |

船名、呼号和证据 JSON 不进入该表；需要核验时回查 `static_shards`。

### 6.2 ChinaPorts 采集层

#### `enrichment/chinaports_query_state`

运行状态表，不属于研究结果。单机批处理不设计租约和多工作器字段。

| 字段 | 作用 |
|---|---|
| `query_id` | 稳定任务 ID |
| `internal_vessel_id` | 船舶 ID |
| `query_key_type`、`query_key_value` | `imo/mmsi` 及查询值 |
| `status` | `pending/success/not_found/missing_dwt/retry/dead` |
| `attempt_count` | 尝试次数 |
| `last_http_status`、`last_error_code` | 最近失败原因 |
| `updated_at_utc` | 更新时间 |

断点请求队列、去重、重试和限速由 Scrapy 复用能力负责，不重复实现 `priority`、`lease_owner`、`lease_until`、`next_attempt_at`。

#### `enrichment/chinaports_raw_index`

| 字段 | 作用 |
|---|---|
| `response_id` | 原始响应 ID |
| `query_id` | 查询外键 |
| `retrieved_at_utc` | 抓取时间 |
| `http_status` | HTTP 状态 |
| `content_hash_sha256` | 响应内容哈希 |
| `raw_object_path` | 压缩原始响应路径 |
| `parser_version` | 解析器版本 |
| `parse_status` | `success/empty/schema_changed/error` |

#### `enrichment/vessel_particulars_observed`

业务键：`internal_vessel_id + source_name + response_id`。

| 字段 | 作用 |
|---|---|
| `internal_vessel_id`、`source_name` | 船舶和来源 |
| `source_imo`、`source_mmsi`、`source_ship_name` | 匹配核验 |
| `length_m_observed`、`breadth_m_observed` | 模型输入 |
| `deadweight_t_observed` | DWT 标签 |
| `match_method` | `imo/mmsi/combined` |
| `field_qc_mask` | 身份、尺度和单位冲突 |
| `response_id` | 原始响应外键 |

吃水、总吨、净吨、页面呼号和连续匹配置信度不保存，因为当前 DWT 等级模型和后续公式不读取它们。

### 6.3 DWT 等级层

DWT 分级与参考论文一致，区间统一左闭右开：

| `dwt_class_id` | DWT（千吨） | `cb_reference` |
|---|---:|---:|
| `DWT_00_040` | <40 | 0.790 |
| `DWT_040_060` | [40,60) | 0.800 |
| `DWT_060_080` | [60,80) | 0.817 |
| `DWT_080_120` | [80,120) | 0.832 |
| `DWT_120_200` | [120,200) | 0.843 |
| `DWT_200_300` | [200,300) | 0.845 |
| `DWT_300_INF` | >=300 | 0.850 |

#### `models/dwt_training_dataset`

| 字段 | 作用 |
|---|---|
| `internal_vessel_id` | 样本主键 |
| `length_m`、`breadth_m` | 唯一正式特征 |
| `deadweight_t_label` | 原始观测标签 |
| `dwt_class_label` | 七分类标签 |
| `label_source` | 标签来源 |

L/B、L×B 在训练时即时生成，不落盘；吃水分位数、建造年、总吨和设计簇暂不加入第一版。

#### `models/dwt_class_predictions`

| 字段 | 作用 |
|---|---|
| `internal_vessel_id` | 船舶主键 |
| `dwt_class_id` | 最终等级 |
| `prediction_method` | `observed/model/manual` |
| `is_extrapolation` | 长宽是否超出训练样本范围 |

模型类别概率只进入模型评估文件，不写入正式预测表，也不计算概率加权 `Cb`。SCPC 模块根据 `dwt_class_id` 从唯一的 DWT 等级配置读取 `Cb`，不在预测表重复保存。

### 6.4 吃水状态层

#### `draught/draught_observations_clean`

| 字段 | 作用 |
|---|---|
| `internal_vessel_id`、`mmsi` | 身份 |
| `receive_time_s` | 观测时间 |
| `draught_m_raw`、`draught_m_clean` | 原值和清洗值 |
| `draft_qc_mask` | 空值、物理异常、尖峰、身份冲突、稀疏 |
| `source_file`、`line_number` | 行级溯源 |

滚动中位数、MAD 和 0.1 m 规范化值是算法中间量，不落盘。

#### `draught/draught_states`

| 字段 | 作用 |
|---|---|
| `draft_state_id` | 平台 ID |
| `internal_vessel_id` | 船舶 |
| `state_start_s`、`state_end_s` | 右开状态区间 |
| `draft_median_m` | 代表吃水 |
| `observation_count` | 支撑观测数 |
| `state_qc_mask` | 稀疏、陈旧和边界标记 |

持续时间、前后状态 ID、状态差和状态置信度均可通过窗口函数得到，不落盘。

#### `draught/samples_3h_draught_link`

这是 sidecar 关联表，不复制现有三小时位置字段。

| 字段 | 作用 |
|---|---|
| `mmsi`、`target_time_s` | 与原三小时样本连接的主键 |
| `internal_vessel_id` | 统一身份 |
| `matched_draught_m` | 匹配吃水 |
| `draft_state_id` | 状态外键 |
| `draft_match_method` | `interval/bidirectional/previous/unmatched/transition` |
| `draft_qc_mask` | 匹配质量原因 |

所有模块通过 DuckDB 视图连接 `samples_3h` 与该 sidecar 表，不生成第二份完整三小时样本。

### 6.5 港口、锚地和节点层

#### `geo/port_reference`

| 字段 | 作用 |
|---|---|
| `port_reference_id` | 来源对象 ID |
| `source_name`、`source_object_id` | WPI/UNLOCODE/OSM/港口机构来源 |
| `port_name`、`country_iso2`、`unlocode` | 基本标识 |
| `longitude_deg`、`latitude_deg` | 参考点 |
| `oil_terminal_evidence`、`anchorage_evidence` | 功能证据 |

来源版本和许可写入数据源 manifest，不逐行重复。

#### `geo/port_zones`

| 字段 | 作用 |
|---|---|
| `zone_id`、`zone_name` | 空间区标识 |
| `zone_type` | `terminal/port/anchorage/sts/sbm/waiting_area` |
| `parent_port_id` | 上级港口，可空 |
| `country_iso2`、`unlocode` | 国家和港口代码 |
| `crude_function` | `export/import/both/unknown` |
| `geometry` | EPSG:4326 多边形 |
| `geometry_method` | `source/buffer/ais_cluster/manual` |
| `source_reference_ids` | 来源对象 ID 数组 |

#### `geo/network_nodes`

| 字段 | 作用 |
|---|---|
| `node_id` | 正式节点 ID |
| `node_name_zh`、`node_name_en` | 名称 |
| `node_level` | `china_port_group/global_functional_region` |
| `geometry` | 节点范围 |

中国节点固定为 `CN_BOHAI`、`CN_YRD`、`CN_SE_COAST`、`CN_SW_COAST`。海外节点沿用 v0.1 的波斯湾、西非、美洲、俄罗斯各出口区、欧洲、南亚、东北亚、东南亚和大洋洲等功能区。

#### `geo/zone_node_map`

仅保留 `zone_id`、`node_id`、`mapping_method`。同一配置版本中一个有效 `zone_id` 只能映射到一个正式节点。

### 6.6 事件层

#### `events/port_calls`

| 字段 | 作用 |
|---|---|
| `port_call_id` | 停留段 ID |
| `internal_vessel_id`、`zone_id` | 船舶和空间区 |
| `arrival_time_s`、`departure_time_s` | 到离时间 |
| `median_sog_kn` | 停留速度证据 |
| `sample_count` | 三小时样本数 |
| `port_call_qc_mask` | 时间缺口、边界和空间冲突 |

停留时长由到离时间计算；不保存最小速度、连续置信度和覆盖率。

#### `events/loading_unloading_events`

宽松规则生成的完整候选集，供核验和调整规则，不直接形成网络。

| 字段 | 作用 |
|---|---|
| `event_id` | 事件 ID |
| `internal_vessel_id` | 船舶 |
| `event_type` | `loading/unloading` |
| `event_start_s`、`event_end_s` | 变化窗口 |
| `pre_state_id`、`post_state_id` | 前后吃水状态 |
| `zone_id`、`node_id` | 空间映射，可空 |
| `rule_id` | 命中的候选规则 |
| `event_status` | `accepted/rejected` |
| `event_qc_mask` | 稀疏、月界、非港区、规则冲突 |

代表经纬度和前后吃水通过状态表及事件窗口内样本回查，不重复保存。

### 6.7 航次与 SCPC 层

#### `voyages/crude_voyages`

| 字段 | 作用 |
|---|---|
| `voyage_id` | 航次 ID |
| `internal_vessel_id` | 船舶 |
| `load_event_id`、`unload_event_id` | 事件外键 |
| `estimated_cargo_t` | SCPC 估计原油量 |
| `route_distance_nm` | 三小时航迹距离 |
| `sample_count` | 航次三小时点数 |
| `is_crude_voyage` | 是否进入正式原油网络 |
| `crude_basis` | `crude_terminal/crude_port/manual` |
| `voyage_qc_mask` | 货量、边界、覆盖和配对异常 |

起止时间、节点、吃水、L、B、DWT 等级和 Cb 均通过事件表、身份表、船舶参数表和预测表连接，不在航次表重复。

SCPC 计算合同：

`cargo_t = [rho_load * L * B * loaded_draught * Cb - rho_unload * L * B * ballast_draught * Cb] / 1000`

其中 `loaded_draught` 是装载后状态吃水，`ballast_draught` 是卸载后状态吃水。`cargo_t <= 0` 不进入正式网络；超过对应 DWT 等级上界的结果标记异常，不静默截断。

### 6.8 月度网络和国家验证中间表

#### `trade/monthly_node_flows`

仅保留 `month`、`node_id`、`export_cargo_t`、`import_cargo_t`、`export_voyage_count`、`import_voyage_count`。

#### `trade/monthly_od_edges`

仅保留 `network_month`、`origin_node_id`、`destination_node_id`、`estimated_cargo_t`、`voyage_count`。主月份为卸载完成月。

#### `validation/country_month_flows`

该表不是第二套网络，只用于与国家统计验证。保留 `month`、`country_iso2`、`flow_type`、`ais_cargo_t`、`voyage_count`。国家从装卸事件对应 `port_zones.country_iso2` 聚合，在海外功能区聚合前生成。

#### `validation/country_month_comparison`

保留 `month`、`country_iso2`、`flow_type`、`ais_cargo_t`、`official_cargo_t`、`official_source_id`。误差、比值和相关系数在验证报告中计算，不作为重复派生列落盘。

#### `validation/official_source_catalog`

每个官方数据来源只保存一行：`official_source_id`、`source_name`、`source_url_or_file_hash`、`commodity_code`、`transport_scope`、`quantity_basis`、`time_basis`。国家—月份比较表只引用来源 ID，避免重复保存统计口径。

### 6.9 航路与关键通道层

#### `routes/chokepoint_catalog`

保留 `chokepoint_id`、中英文名、`geometry`、`chokepoint_type`、`has_maritime_alternative`、`baseline_capacity_t_month`、`capacity_basis`。容量是后续中断优化的约束，不能删除。

#### `routes/route_catalog`

保留 `route_id`、起止节点、`route_type`、`geometry`、`distance_nm`、`baseline_sailing_days`、`draft_limit_m`、`baseline_capacity_t_month`、`fuel_cost_index`、`emission_index`、`risk_score`。后三项直接服务多目标函数。

#### `routes/route_chokepoint_map`

仅保留 `route_id`、`chokepoint_id`、`passage_order`、`intersection_method`。

#### `routes/voyage_route_assignment`

仅保留 `voyage_id`、`route_id`、`assignment_method`、`observed_chokepoints`。不保存连续分配置信度和偏差距离。

## 7. 三小时事件识别规则

### 7.1 输入视图

按 `internal_vessel_id, target_time_s` 连接：

1. 原 `samples_3h`；
2. `samples_3h_draught_link`；
3. `port_zones` 和 `zone_node_map`。

### 7.2 停留段

- 相邻有效样本间隔不超过 6 h 时允许属于同一停留段。
- 港区内样本或相邻点球面距离较小且 `SOG <= 2 kn` 的样本形成停留证据。
- 持续至少 6 h 的停留段可支撑 1.5–3 m 补充路径；大于 3 m 的标准路径仍要求变化附近有低速点。
- 锚地、STS、SBM 和港外等候区分别保留，不自动归并为码头装卸。

### 7.3 吃水变化事件

1. 使用变化前后两个稳定状态的中位吃水。
2. 变化后状态至少持续两个三小时样本；只有一个样本时可保留候选，但不得自动接受。
3. 正变化判为装载，负变化判为卸载。
4. 同一停留段内的多次人工吃水更新合并为一个变化窗口，避免重复计量。
5. 港区匹配用于定位和原油判定；非港区显著变化保留候选并标记，不伪造最近港口。

### 7.4 航次分段

- 一个已接受装载事件与其后第一个逻辑一致的已接受卸载事件构成候选航次。
- 航次持续时间至少 12 h，球面航迹距离至少 30 km。
- 理论三小时样本数为 `floor((end-start)/3h)+1`；正式航次至少有 3 个实际样本，且覆盖率不低于配置阈值，初始建议 50%。覆盖率只在判定时计算，不写入正式航次表。
- 月界航次允许跨月；仅研究总边界无法闭合的航次排除出正式网络并记入质量报告。

### 7.5 全分辨率位置的独立核验模块

该模块不参与日常全量运行，只读取候选时间窗：

- 距月界不超过 3 h；
- 三小时覆盖不足；
- SCPC 货量超过 DWT 等级上界；
- 每月按规则类型分层抽取的人工核验样本。

核验结果用于修改下一版规则配置或人工覆盖表，不在同一次运行中无记录地改变判定。

## 8. DWT 模型与验证

### 8.1 第一版模型

- 输入仅为船长、船宽及训练时即时生成的 L/B、L×B。
- 输出仅为七级 DWT 类别。
- 先比较一条简单尺度规则基线和一个 scikit-learn 树模型；仅当树模型在分层交叉验证中明显改善宏平均 F1 和相邻等级错误率时才采用树模型。
- 不预测精确连续 DWT，不把类别概率传给 SCPC。

### 8.2 船舶等级验收

独立模型报告保留：训练样本数、各类样本数、混淆矩阵、各类 precision/recall/F1、宏平均 F1、相邻等级错误率和外推船数。它们不进入逐船预测表。

### 8.3 国家宏观验证

使用用户已取得的实际进出口数据，比较重点国家的：

- 研究期累计规模：AIS/官方比值和绝对差；
- 月度变化：相关系数及趋势图；
- 主要国家排序：Spearman 等级相关和前若干名重合度。

官方数据必须增加来源级元数据：统计口径、是否仅海运、是否含管道、商品编码、净重/毛重和时间口径。若含管道且无法剔除，该国只作为“官方总量上界”比较，不用来调整 AIS 事件阈值。

## 9. 独立模块接口

| 模块 | 唯一职责 | 输入 | 输出 |
|---|---|---|---|
| `schema_gate` | 核对可读取字段 | 一个月完整 schema + 其余月 schema 指纹 | 审计摘要 |
| `identity_resolution` | 统一 IMO/MMSI 身份 | registry、static_shards | vessel_identity |
| `chinaports_labeling` | 获取长宽和 DWT 标签 | vessel_identity | query_state、raw_index、particulars |
| `dwt_classification` | 训练并预测七级 DWT | particulars | training_dataset、predictions、评估报告 |
| `draught_state_builder` | 清洗静态吃水并构建状态 | static_shards、identity | observations_clean、draught_states |
| `sample_draught_linker` | 给三小时样本关联状态 | samples_3h、draught_states | samples_3h_draught_link |
| `geo_registry_builder` | 构建港区、节点和映射 | WPI、UNLOCODE、OSM、人工配置 | port_reference、zones、nodes、map |
| `event_detector_3h` | 识别停留和装卸事件 | 三小时输入视图 | port_calls、loading_unloading_events |
| `fullres_event_audit` | 对少量事件进行独立核验 | 候选窗口、全分辨率位置 | 核验报告/人工覆盖表 |
| `voyage_builder` | 配对航次并计算 SCPC | accepted events、船舶参数、DWT、密度 | crude_voyages |
| `monthly_network_builder` | 生成唯一正式月度网络 | crude_voyages、events、nodes | node_flows、OD edges |
| `country_validation_builder` | 生成国家比较和报告 | voyages、zones、官方统计 | country flows、验证报告 |
| `route_layer_builder` | 构建候选航路和通道约束 | 三小时航迹、nodes、chokepoints | route tables |

每个模块只写自己的输出目录，不能修改上游表；相同输入和配置必须得到相同主键和统计值。

## 10. 轻量 schema gate

由于用户已确认 12 个月字段结构一致，开发时不再对每个月生成一份冗长报告：

1. 选一个完整月份执行 `DESCRIBE` 并与第 5 节最小合同比较。
2. 对其余月份只计算列名、类型和顺序的 schema 指纹。
3. 指纹一致即通过；不一致时只报告差异月和差异字段。
4. 必要字段缺失时阻断对应模块，不影响无关模块。

## 11. 配置和目录边界

### 11.1 必要配置

```text
configs/
├─ schema/required_fields.yaml
├─ dwt/dwt_classes.yaml
├─ draught/draught_rules.yaml
├─ events/event_rules_3h.yaml
├─ geo/port_zones.geojson
├─ geo/network_nodes.geojson
├─ geo/zone_node_map.csv
├─ routes/chokepoints.geojson
└─ routes/route_rules.yaml
```

不再单独设置多个内容重叠的置信度配置。所有数值阈值只能出现在配置中，并进入 manifest 哈希。

### 11.2 建议源码边界

在读取另一台电脑的真实仓库前，不指定现有文件的修改位置。新增职责建议如下：

```text
src/ais_tanker_pipeline/
├─ core/              # 复用配置、路径、manifest、稳定ID、Parquet写入
├─ identity/
├─ enrichment/chinaports/
├─ dwt/
├─ draught/
├─ geo/
├─ events/
├─ voyages/
├─ trade/
├─ validation/
└─ routes/
```

`core` 只能放被两个以上模块实际使用且语义完全一致的基础能力；不得把业务规则堆入通用工具。每个业务目录只公开一个清晰入口和一个数据合同，内部实现可替换而不影响下游。

## 12. 质量验收

- 所有正式表主键重复数为 0。
- 同船吃水状态区间不重叠。
- 已接受装载事件前后吃水上升，卸载事件前后吃水下降。
- 每个正式航次仅由一个装载事件和一个卸载事件组成，装载早于卸载。
- 每个 `voyage_id` 只向一个正式月度 OD 边贡献一次货量。
- 月度 OD 边货量之和等于同月正式航次货量之和。
- 每月报告候选船数、IMO 率、长宽率、DWT 标签率、DWT 预测率、有效吃水率、三小时吃水关联率、候选/接受事件数、完整航次数、港区映射率和航路分配率。
- 原始 AIS 文件哈希和现有正式 Parquet 行数不得被新增模块改变。

## 13. 任务分级与实施顺序

### 第一类：不做就无法实现目标

1. 读取真实仓库并复用已有基础组件。
2. 轻量 schema gate 和统一船舶身份。
3. ChinaPorts 长宽/DWT 标签、七级 DWT 模型。
4. 吃水状态、三小时关联、三小时事件和完整航次。
5. 港区—节点映射、SCPC 货量、月度网络。
6. 国家宏观验证。
7. 航路—海峡映射，为中断优化提供输入。

### 第二类：有明显收益但独立实施

- 用全分辨率位置核验少量高风险事件。
- 用 AIS 停留簇修正港区边界。
- 对 DWT 边界等级补抓训练样本。
- 在确有数据时加入海水密度的时空变化。

### 第三类：当前砍掉

- 全量位置逐行附加吃水。
- 逐表概率、置信区间和平均置信度传播。
- 精确连续 DWT 回归。
- 统一粒度全球对照网络。
- 全球逐泊位人工建模。
- 实时服务、消息队列、微服务、仪表盘和自研通用框架。
- 验证码、登录限制或访问控制绕过。

## 14. v0.1 字段删除与保留摘要

| v0.1 内容 | v0.2 处理 | 原因 |
|---|---|---|
| `latest_ship_type` | 不读取、不复制 | 被 `observed_ship_types` 覆盖 |
| `latest_ship_name` | 仅留在原输入 | 后续可回查，不需复制 |
| `latest_imo` | 保留读取 | ChinaPorts 查询和身份合并必需 |
| 每行四个公共审计字段 | 移到 manifest | 避免全表重复 |
| 七类概率、`class_confidence`、`cb_lower/upper` | 删除 | 最终只需要等级和固定 Cb |
| 各类 `*_confidence` 和平均置信度 | 删除 | 改用规则状态和 QC 原因 |
| `cargo_lower_t`、`cargo_upper_t` | 删除 | 当前验证不使用区间估计 |
| 滚动中位数、MAD、状态持续时间等中间量 | 不落盘 | 可重算且下游不直接读取 |
| 完整 `samples_3h_enriched` 副本 | 改为 sidecar 表 | 避免复制大字段 |
| 全分辨率位置作为全量事件输入 | 改为独立抽查模块 | 三小时样本成为正式主路线 |
| 独立 STS 复杂评分表 | 第一版不设 | 海上事件先保留候选和空间类型，避免过早扩展 |
| 国家宏观验证 | 提升为必做模块 | 与用户最终验证方法一致 |

## 15. 最小质量位定义

位掩码只表达可复核的异常原因，不表达连续置信度。第一版固定如下；新增位只能追加，不能改变已有位含义。

| 字段 | 位值与含义 |
|---|---|
| `identity_qc_mask` | 1=IMO格式无效；2=同一身份段IMO冲突；4=MMSI时间重叠冲突 |
| `field_qc_mask` | 1=查询标识不一致；2=长度或宽度异常；4=DWT异常；8=重复响应字段冲突 |
| `draft_qc_mask` | 1=空值或0；2=物理范围异常；4=局部尖峰；8=身份冲突；16=时间上下文稀疏；32=变化过渡窗 |
| `state_qc_mask` | 1=支撑观测稀疏；2=研究边界截断；4=状态证据陈旧 |
| `port_call_qc_mask` | 1=样本时间缺口；2=研究/月度边界；4=空间区冲突 |
| `event_qc_mask` | 1=变化前状态稀疏；2=变化后状态稀疏；4=距月界不超过3 h；8=未匹配港区；16=空间区冲突；32=速度证据缺失 |
| `voyage_qc_mask` | 1=研究边界未闭合；2=三小时覆盖不足；4=SCPC货量非正；8=货量超过DWT等级上界；16=事件配对冲突；32=原油依据不足 |

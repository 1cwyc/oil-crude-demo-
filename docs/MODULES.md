# 模块运行与交接索引

本文件帮助另一台电脑上的 Codex 快速确定模块边界。完整字段定义、主键、枚举和规则以 [数据字典与模块接口规格 v0.2](specs/AIS原油海运网络_数据字典与模块接口规格_v0.2.md) 为唯一权威来源；这里不复制完整字段表。

当前下列模块均为规划中的增量模块，尚未实现正式入口。实现任一模块前必须检查真实仓库、浏览相关开源实现、编写独立 PRD 并取得用户确认。

### `schema_gate`

- **Function:** 核对一个完整月份的实际 schema，并比较其余月份的 schema 指纹。
- **Prerequisites:** 12 个月 output 路径可读；用户已确认月间结构应一致。
- **Inputs:** `registry/tanker_registry`、`registry/static_shards`、`samples_3h`，以及按需读取的 `positions/tanker` Parquet。
- **Fields read:** 只读取列名、DuckDB 类型、顺序、分区键和文件清单；不扫描业务值。
- **Outputs:** 轻量 schema 审计摘要；只报告差异月份和差异字段。
- **Configuration:** `configs/schema/required_fields.yaml`。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 对应模块的必要字段缺失或类型不兼容。
- **Acceptance:** 基准月 `DESCRIBE` 通过；其余月份指纹一致，或差异被准确列出。
- **Downstream consumers:** 全部增量模块。

### `identity_resolution`

- **Function:** 将 IMO、MMSI 与时间段统一成稳定 `internal_vessel_id`。
- **Prerequisites:** `schema_gate` 已通过身份相关字段。
- **Inputs:** `tanker_registry`、`static_shards`。
- **Fields read:** registry 的 `mmsi`、`is_tanker`、`observed_ship_types`、`ship_type_conflict`、`latest_imo`、`first_static_time_s`、`last_static_time_s`；static 的 `mmsi`、`imo`、`ship_name`、`callsign`、`ship_type`、`receive_time_s`。
- **Outputs:** `enrichment/vessel_identity`。
- **Configuration:** 身份有效性和冲突规则配置。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 不可解释的 IMO 合并、MMSI 时间重叠或主键重复。
- **Acceptance:** `internal_vessel_id` 唯一；时间段合法；冲突进入 `identity_qc_mask`。
- **Downstream consumers:** ChinaPorts、DWT、吃水、事件和航次模块。

### `chinaports_labeling`

- **Function:** 通过合规网页爬虫获取船长、船宽和 DWT 训练标签，并保留响应溯源。
- **Prerequisites:** `vessel_identity`；目标公开页面允许访问；未遇到登录、验证码或访问控制绕过需求。
- **Inputs:** `enrichment/vessel_identity` 中的 `internal_vessel_id`、`canonical_imo`、`primary_mmsi`。
- **Fields read:** 页面实际可见的 IMO、MMSI、船名、船长、船宽和 DWT；页面选择器须在执行电脑实测后冻结。
- **Outputs:** `chinaports_query_state`、`chinaports_raw_index`、`vessel_particulars_observed`。
- **Configuration:** Scrapy 并发、AutoThrottle、Retry、HTTP cache、JOBDIR、请求间隔和解析器版本。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added. 页面选择器和可用字段还必须在执行电脑观察公开页面后校准。
- **Blocking conditions:** 页面 schema 改变、身份不一致、访问限制或来源条款不允许抓取。
- **Acceptance:** 可断点恢复；成功任务不重复请求；字段冲突可回查到 `response_id`。
- **Downstream consumers:** `dwt_classification`、`voyage_builder`。

### `dwt_classification`

- **Function:** 使用观测 DWT 标签和船舶尺度训练并预测七级 DWT 类别。
- **Prerequisites:** 可用的 `vessel_particulars_observed` 标签和尺度。
- **Inputs:** ChinaPorts 船舶参数观测表。
- **Fields read:** `internal_vessel_id`、`length_m_observed`、`breadth_m_observed`、`deadweight_t_observed`、`field_qc_mask`、`response_id`。
- **Outputs:** `models/dwt_training_dataset`、`models/dwt_class_predictions` 和独立评估报告。
- **Configuration:** `configs/dwt/dwt_classes.yaml`；模型选择和交叉验证配置。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 标签单位或尺度异常、类别边界不一致、训练与推断特征漂移。
- **Acceptance:** 报告各类 precision/recall/F1、宏平均 F1、相邻等级错误率和外推数；逐船表只保存最终等级和方法。
- **Downstream consumers:** `voyage_builder` 和 SCPC 计算。

### `draught_state_builder`

- **Function:** 清洗时变静态吃水并构建不重叠的稳定吃水状态。
- **Prerequisites:** `vessel_identity`；static schema 通过。
- **Inputs:** `static_shards`、`vessel_identity`。
- **Fields read:** `mmsi`、`imo`、`receive_time_s`、`draught_m`、`dq_mask`、`source_file`、`line_number` 和身份映射字段。
- **Outputs:** `draught_observations_clean`、`draught_states`。
- **Configuration:** `configs/draught/draught_rules.yaml`。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 关键来源字段缺失、同船时间逻辑错误或状态区间重叠。
- **Acceptance:** 清洗值可回查原记录；同船状态不重叠；质量位含义固定。
- **Downstream consumers:** `sample_draught_linker`、事件和航次模块。

### `sample_draught_linker`

- **Function:** 将稳定吃水状态作为 sidecar 关联到三小时样本，不复制位置大字段。
- **Prerequisites:** `samples_3h` 与 `draught_states` 可读。
- **Inputs:** `samples_3h`、`draught_states`。
- **Fields read:** samples 的 `mmsi`、`target_time_s`；states 的 `draft_state_id`、`internal_vessel_id`、`state_start_s`、`state_end_s`、`draft_median_m`、`state_qc_mask`。
- **Outputs:** `draught/samples_3h_draught_link`。
- **Configuration:** 回顾性匹配陈旧上限和变化窗口规则。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 三小时主键重复、身份映射重复或状态区间重叠。
- **Acceptance:** sidecar 主键与原样本一一对应；未匹配原因可解释；不生成完整样本副本。
- **Downstream consumers:** `event_detector_3h`、`voyage_builder`。

### `geo_registry_builder`

- **Function:** 构建港口参考、空间区、唯一正式网络节点和区—节点映射。
- **Prerequisites:** 已取得许可明确的 WPI、UN/LOCODE、OSM 或人工配置来源。
- **Inputs:** 外部港口参考、人工 GeoJSON/CSV；可选 AIS 停留簇校准结果。
- **Fields read:** 来源对象 ID、名称、国家、代码、经纬度、油码头/锚地证据和几何。
- **Outputs:** `port_reference`、`port_zones`、`network_nodes`、`zone_node_map`。
- **Configuration:** `configs/geo/port_zones.geojson`、`network_nodes.geojson`、`zone_node_map.csv`。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 一个 zone 映射多个正式节点、几何无效或来源许可不清。
- **Acceptance:** 中国四大港口群和海外功能区唯一；冲突不被静默覆盖。
- **Downstream consumers:** 事件、航次、网络、国家验证和航路模块。

### `event_detector_3h`

- **Function:** 以三小时样本为主识别停留、装载和卸载候选，并按规则接受或拒绝。
- **Prerequisites:** 三小时吃水关联视图和港区映射可用。
- **Inputs:** `samples_3h`、`samples_3h_draught_link`、`port_zones`、`zone_node_map`。
- **Fields read:** `internal_vessel_id`、`target_time_s`、`pos_time_s`、经纬度、`sog_kn`、`navigation_status`、位置质量字段、`draft_state_id`、匹配吃水、zone/node。
- **Outputs:** `events/port_calls`、`events/loading_unloading_events`。
- **Configuration:** `configs/events/event_rules_3h.yaml`。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 状态方向矛盾、重复事件、空间映射冲突或必要速度证据缺失。
- **Acceptance:** 使用前后稳定状态而非单点差；规则编号和 QC 可复核；候选与正式接受状态分开。
- **Downstream consumers:** 全分辨率审计、航次和国家验证。

### `fullres_event_audit`

- **Function:** 只对月界、覆盖不足、货量异常和分层抽样事件读取全分辨率位置进行独立核验。
- **Prerequisites:** 已生成三小时事件候选和核验抽样清单。
- **Inputs:** 候选事件窗口、`positions/tanker`。
- **Fields read:** `mmsi`、`pos_time_s`、经纬度、`sog_kn`、`navigation_status`、`is_hard_valid`、`dq_mask` 和溯源键。
- **Outputs:** 核验报告和版本化人工覆盖表，不改写原事件表。
- **Configuration:** 分层抽样、时间窗和高风险条件配置。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 全分辨率窗口缺失、身份不一致或审计结果无法关联候选事件。
- **Acceptance:** 每个结论能回查到事件和位置来源；同一次运行不无记录改变规则结果。
- **Downstream consumers:** 下一版事件规则和 `voyage_builder` 人工覆盖。

### `voyage_builder`

- **Function:** 配对接受的装卸事件，计算航迹距离和 SCPC 原油货量，生成正式候选航次。
- **Prerequisites:** 接受事件、船长船宽、DWT 等级/Cb、密度和三小时轨迹可用。
- **Inputs:** 事件表、船舶参数、DWT 预测、密度配置、三小时输入视图。
- **Fields read:** 船舶/事件 ID、事件时间和节点、前后状态 ID、状态吃水、长宽、`dwt_class_id`、密度、三小时经纬度。
- **Outputs:** `voyages/crude_voyages`。
- **Configuration:** DWT/Cb、密度、航次时长/距离/覆盖和原油依据规则。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 装卸配对重复、装载不早于卸载、货量非正或研究边界未闭合。
- **Acceptance:** 一个正式航次只含一个装载和一个卸载事件；SCPC 可由权威表重算；异常不静默截断。
- **Downstream consumers:** 月度网络、国家验证和航路分配。

### `monthly_network_builder`

- **Function:** 从正式原油航次生成唯一的月度节点流量和 OD 有向加权网络。
- **Prerequisites:** `crude_voyages`、接受事件和正式节点可用。
- **Inputs:** 航次、事件、`network_nodes`。
- **Fields read:** `voyage_id`、装卸事件 ID、`estimated_cargo_t`、是否正式原油航次、事件月份和 origin/destination node。
- **Outputs:** `trade/monthly_node_flows`、`trade/monthly_od_edges`。
- **Configuration:** 卸载完成月主口径和自环分析设置。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 航次重复贡献、节点缺失或月度货量不守恒。
- **Acceptance:** 同一航次只进入一条月度 OD 边；边货量和同月正式航次货量严格相等。
- **Downstream consumers:** 网络指标、多目标优化和报告。

### `country_validation_builder`

- **Function:** 在海外功能区聚合前生成国家月度 AIS 流量，并与用户提供的官方统计比较。
- **Prerequisites:** 航次、港区国家映射和来源元数据完整的官方统计可用。
- **Inputs:** `crude_voyages`、事件、`port_zones`、官方进出口表。
- **Fields read:** 月份、国家、流向、AIS 货量、航次数、官方货量、商品代码、运输/数量/时间口径和来源 ID。
- **Outputs:** `country_month_flows`、`country_month_comparison`、`official_source_catalog` 和验证报告。
- **Configuration:** 重点国家、商品口径和报告指标。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 官方单位不明、国家/流向键重复或来源口径缺失。
- **Acceptance:** 含管道且无法剔除的数据只作为总量上界；不反向调整 AIS 事件阈值追平总量。
- **Downstream consumers:** 方法验证和论文报告。

### `route_layer_builder`

- **Function:** 从三小时航迹构建候选航路、关键通道映射和逐航次航路分配，为多目标优化提供约束。
- **Prerequisites:** 正式航次、节点、关键通道配置和三小时轨迹可用。
- **Inputs:** 航次轨迹、`network_nodes`、`chokepoint_catalog`。
- **Fields read:** `voyage_id`、起止节点、三小时经纬度/时刻、货量、通道几何、容量、吃水限制、成本/排放/风险参数。
- **Outputs:** `route_catalog`、`route_chokepoint_map`、`voyage_route_assignment`。
- **Configuration:** `configs/routes/chokepoints.geojson`、`route_rules.yaml`。
- **Run entry:** Not implemented; the module requires its own approved PRD before an entry point is added.
- **Blocking conditions:** 航路 OD 与航次 OD 不一致、通道顺序矛盾或优化必需参数缺失。
- **Acceptance:** 每条候选航路可追踪 OD 和通道序列；容量、距离、时间、成本、排放和风险字段能直接服务多目标函数。
- **Downstream consumers:** 中断情景和多目标改道优化。

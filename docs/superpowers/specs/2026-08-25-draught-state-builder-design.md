# 原油船稳定吃水状态模块 PRD

> 状态：已实施并通过 2025-09 真实月验收。本文是 `draught_state_builder` 的唯一规格；不覆盖港区、装卸事件、航次、货量或网络。

## 1. 目标与任务分级

目标是在不复制静态 AIS 全表的前提下，将外部原油参考船与时变静态 AIS 吃水归并为可由事件模块连接的稳定吃水状态。

### 必须完成

- 以参考表的有效 IMO 优先、唯一 MMSI 兜底，将静态 AIS 映射至 `crude_vessel_id`。
- 对吃水和关键主键实施 fail-closed schema/QC gate。
- 生成同一物理船舶不重叠、可复算的稳定吃水状态。
- 使用 DuckDB + Parquet，输出最小状态表、manifest 和 CLI。

### 有明显收益但保持轻量

- 记录输入、配置和输出 SHA256；支持幂等、`--dry-run`、`--force` 与已验证 backup 恢复。
- 在 manifest 汇总归并后的有效观测数、输出状态数及同 IMO/同刻冲突归并审计。

### 当前不实施

- DWT 预测、载货状态分类、逐行置信度、人工 alias 审核。
- 完整静态 AIS 副本、全分辨率位置读取、插值或机器学习平滑。
- 跨船舶共享吃水模型和为追平官方统计调整阈值。

## 2. 已核实事实、设计判断与待验收项

### 已核实

- 2025-09 `registry/static_shards` 的单日 Parquet 含 `mmsi` (INTEGER)、`receive_time_s` (BIGINT)、`imo` (VARCHAR)、`draught_m` (DOUBLE)、`dq_mask`。
- 该月实际吃水非空值范围为 `0.0–25.5 m`；`0.0` 不能视为原油船有效装卸吃水。
- `reference/crude_vessels` 已以 IMO 作为物理船舶权威身份，参考 MMSI 可存在歧义。

### 设计判断

- `draught_m` 的有效物理范围固定为 `(1.0, 30.0] m`；下限排除常见 AIS 零值哨兵，上限只过滤显然错误。
- 连续观测间隔不超过 48 h、同段吃水极差不超过 0.30 m、至少持续 6 h 且至少 3 条观测，才构成稳定状态。
- 同一有效 IMO、同刻的有效吃水直接取中位数；其极差超过 0.30 m 时不阻断，而在 manifest 记录冲突归并汇总。稳定状态段的 0.30 m 容差保持严格。

### 实施后验收

- 原油参考船在每月静态 AIS 中的实际覆盖率。
- 有效吃水、稳定状态和短/不稳定片段的比例。
- 月边界附近因缺少前后稳定状态而无法供事件判断的数量。

## 3. 输入合同

主机 YAML 只包含：

```yaml
reference_path: "${AIS_DERIVED_ROOT}/reference/crude_vessels/crude_vessels.parquet"
static_root: "${AIS_MONTH_ROOT}/registry/static_shards"
output_root: "${AIS_DERIVED_ROOT}"
draught_valid_range_m: [1.0, 30.0]
state_tolerance_m: 0.30
max_observation_gap_hours: 48
minimum_state_duration_hours: 6
minimum_state_observations: 3
```

版本 1 固定上述阈值；未知、重复或缺失配置字段均失败关闭，规范化配置进入 manifest 哈希。算法版本为 `1.1.1`，用于标识同 IMO 同刻中位数归并及关键键 gate，进入 manifest 和状态 ID。

从参考表读取：`crude_vessel_id` (VARCHAR)、`imo` (VARCHAR)、`mmsi` (INTEGER)。`imo` 和 `crude_vessel_id` 必须非空且唯一；重复 MMSI 保留为歧义，不能用于兜底。

从请求的 UTC 月范围读取静态字段：`mmsi` (INTEGER)、`receive_time_s` (BIGINT)、`imo` (VARCHAR)、`draught_m` (DOUBLE)、`dq_mask` (UBIGINT)。所有日 Parquet 都须满足合同；空分区、NULL 的 MMSI/时间、重复键或类型漂移阻断运行。

## 4. 匹配、清洗与状态规则

1. 静态 `imo` 与参考 `imo` 精确匹配时优先；未命中时仅匹配参考表唯一 MMSI。
2. 相同 IMO 的不同静态 MMSI 统一为同一 `crude_vessel_id`；IMO/MMSI 冲突时 IMO 胜出；歧义 MMSI 未命中时丢弃。
3. 仅保留有限且处于 `(1.0, 30.0]` 的 `draught_m`。无效值不成为状态，也不修改原始静态表。
4. 对同一有效 IMO、同一 `receive_time_s` 的全部有效吃水报告直接取中位数；即使组内极差超过 0.30 m 也不阻断。极差超过 0.30 m 的这类组记为一次冲突归并，并记录其最大极差。没有有效 IMO 的 MMSI 兜底同刻组仍按原容差失败关闭；该归并不放宽后续稳定状态段的 0.30 m 容差。
5. 按物理船和 UTC 时间排序。相邻观测若间隔不超过 48 h 且扩展后本段吃水极差不超过 0.30 m，则属于同一候选段；否则切段。
6. 仅发布持续至少 6 h 且至少 3 条观测的候选段；状态区间为首末观测时刻闭区间，吃水为段内中位数。
7. `draught_state_id` 由算法版本、`crude_vessel_id`、首末时刻和中位数的规范化值确定性生成。状态不得重叠；任何重叠、空值或主键重复阻断发布。

## 5. 输出合同

输出按状态开始 UTC 月分区：

```text
draught/draught_states/year=YYYY/month=MM/draught_states.parquet
reports/manifests/draught_state_builder_YYYY-MM_YYYY-MM.json
```

正式 Parquet 严格仅含：

| 字段 | 类型 | 说明 |
|---|---|---|
| `draught_state_id` | VARCHAR | 稳定状态主键 |
| `crude_vessel_id` | VARCHAR | 连接原油参考船和三小时 sidecar |
| `state_start_s` | BIGINT | UTC epoch 秒，含端点 |
| `state_end_s` | BIGINT | UTC epoch 秒，且不早于开始 |
| `draught_median_m` | DOUBLE | 有效观测中位吃水 |

不落盘原始吃水、MMSI、IMO、观测数、极差、质量位、匹配方法或 confidence。它们只用于计算、测试和 manifest 汇总。

manifest 记录算法版本、请求月份、配置哈希、所有输入签名/SHA256、输出签名/SHA256、归并后的有效观测数、`imo_timestamp_conflict_merged_groups`、`imo_timestamp_conflict_merged_max_spread_m` 和输出状态数。

## 6. CLI、幂等与恢复

```powershell
python -m ais_tanker_pipeline.draught.draught_state_builder `
  --config $env:AIS_DRAUGHT_CONFIG `
  --start-month 2025-09 --end-month 2025-09 [--dry-run] [--force]
```

`--dry-run` 只解析配置和显示目标，不打开 Parquet。相同输入、范围、配置和输出 SHA256 幂等跳过；不一致输出失败关闭。`--force` 仅用于人工核查后的冲突重建。发布采用 temporary → target 与 manifest 原子写入：强制重建会把旧 manifest 有、而新 manifest 不再拥有的分区一并备份后删除；启动时若 target 已匹配当前 manifest，则清理遗留旧 backup，否则仅可依据当前 manifest SHA256 恢复已验证 backup，其他残留失败关闭。

## 7. TDD 与真实月验收

先写失败测试，至少覆盖：IMO 优先、唯一/歧义 MMSI、零值和越界吃水、同 IMO 同刻中位数归并及 manifest 审计、容差切段、48 h 间隙、6 h/3观测门槛、确定 ID、状态不重叠、schema gate、幂等、backup 恢复、发布完成后遗留旧 backup 清理，以及强制重建淘汰旧分区。

2025-09 验收要求：

- 所有输入日文件 schema 一致；
- 输出键重复、NULL、区间反转和同船状态重叠均为 0；
- manifest 输入数等于实际文件数，输出 SHA256 匹配；
- 报告原油静态匹配率、有效吃水数、状态数及剔除原因汇总；
- 不修改 `registry/static_shards`、`samples_3h`、原油参考表或现有正式 Parquet。

## 8. 风险与下游

- AIS 吃水可滞后或不更新：本模块只建立稳定状态，不将其解释为装卸事件。
- 月界缺少前后状态：事件模块拒绝证据不足的边界候选，不进行插值。
- 同 IMO 多 MMSI：由参考 IMO 物理身份统一；MMSI 歧义不作猜测。

下游 `event_detector_3h` 仅按 `crude_vessel_id` 和事件时刻连接本表。装卸方向、港区、航次、SCPC 货量和月度网络均不属于本模块。

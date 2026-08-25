# 三小时原油装卸事件检测 PRD

## 目标

在已匹配原油船三小时样本、稳定吃水状态和 WPI 候选港区之间识别接受的装载/卸载事件。停留本身不构成事件；必须存在方向一致的前后稳定吃水状态、低速停留与 75 km 内最近 WPI 港口证据。

## 输入与规则

- 样本与 fleet match 以 `(mmsi,target_time_s)` 精确连接，IMO 主身份只来自既有 matcher。
- 相邻稳定状态的吃水差绝对值 `>3m` 走标准路径；`1.5–3m` 走补充路径，必须停留至少 6h。两路径均须至少 6h 的低速 (`sog_kn<=1`) 港区样本，且状态间隔不超过 96h。
- 后状态更深为 `load`，更浅为 `unload`。低速位置的经纬中位数后投影到距离该中位位置最小的真实样本坐标；最近 WPI 端口必须在 75km 内，等距失败关闭。
- 事件 ID 由船、前后状态 ID、类型和时间窗口确定性哈希生成；同一相邻状态对只能产生一个事件。

## 输出

`events/loading_unloading_events/year=2025/month=09/loading_unloading_events.parquet` 仅含：`event_id`、`event_status`、`event_kind`、`crude_vessel_id`、`port_id`、`event_start_s`、`event_end_s`、`event_longitude_deg`、`event_latitude_deg`、`before_draught_state_id`、`after_draught_state_id`、`before_draught_m`、`after_draught_m`。

`events/port_calls/.../port_calls.parquet` 为相同接受停留的最小审计表：`port_call_id`、`crude_vessel_id`、`port_id`、`call_start_s`、`call_end_s`、`longitude_deg`、`latitude_deg`。

输出、输入和规则均由 SHA256 manifest 管理；无事件允许产生空但 schema 正确的输出。无港口、无低速证据、边界不完整和空间并列不写 accepted 事件。

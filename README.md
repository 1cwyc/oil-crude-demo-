# oil-crude-demo

面向宏观原油航次重建、月度海运贸易网络和关键通道约束研究的 AIS 数据处理项目。最终研究目标是在可复核的航次与网络基础上构建航路中断和改道的多目标优化模型。

## 研究边界

- 现有代码已实现油轮候选登记、全分辨率油轮位置筛选和三小时位置采样。
- 后续模块将按独立 PRD 实现船舶身份、DWT 等级、吃水状态、港区、装卸事件、航次、SCPC 货量、月度网络和航路约束。
- 三小时样本用于宏观航次重建；全分辨率位置用于高风险事件校准和抽查。
- 仓库不包含真实 AIS、ChinaPorts 抓取响应或研究生成结果。

## 当前能力

```text
STA .dat → 年度船舶/油轮登记
POS .dat + 油轮登记 → 油轮位置
油轮位置 → 三小时样本 → 可选 CSV/热力图
外部已接受装卸事件 + ERA5/WOA23 → event_seawater_density sidecar（event_density_matcher CLI）
外部原油船单 CSV → reference/crude_vessels（crude_fleet_loader CLI）
reference/crude_vessels + 三小时 AIS → 最小原油船 sidecar（crude_fleet_matcher CLI）
reference/crude_vessels + static AIS 吃水 → 稳定吃水状态 sidecar（draught_state_builder CLI）
```

`event_density_matcher` 与 `draught_state_builder` 已实现正式 CLI；前者只消费已存在的接受事件表，后者不复制 AIS 位置。`event_detector_3h`、`voyage_builder`、`country_validation_builder` 仍未实现。其余下游模块也保持独立 PRD 后实施的边界。

## 原油船单参考表

`crude_fleet_loader` 将外部船单规范化为唯一 IMO 主键的 `reference/crude_vessels`；它不读取或复制 AIS 位置。使用 [配置模板](configs/fleet/crude_fleet.example.yaml) 在仓库外创建主机 YAML：

```powershell
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.fleet.crude_fleet_loader --config $env:AIS_CRUDE_FLEET_CONFIG --dry-run
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.fleet.crude_fleet_loader --config $env:AIS_CRUDE_FLEET_CONFIG
```

相同输入会幂等跳过；输入、配置或输出不一致时失败关闭。仅在人工检查既有派生产物后可使用 `--force` 原子重建。

## 原油船三小时匹配 sidecar

`crude_fleet_matcher` 仅读取一个 UTC 月的既有 `samples_3h` 分区，按有效 IMO 优先、唯一 MMSI 兜底生成四列 sidecar，绝不复制位置字段。以 [配置模板](configs/fleet/crude_fleet_matcher.example.yaml) 在仓库外创建主机 YAML：

```powershell
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.fleet.crude_fleet_matcher --config $env:AIS_CRUDE_FLEET_MATCHER_CONFIG --month 2025-09 --dry-run
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.fleet.crude_fleet_matcher --config $env:AIS_CRUDE_FLEET_MATCHER_CONFIG --month 2025-09
```

输出为 `enrichment/crude_fleet_matches/year=YYYY/month=MM/crude_fleet_matches.parquet`；正式 Parquet 严格仅 `mmsi`、`target_time_s`、`crude_vessel_id`、`match_method`。相同输入幂等跳过，冲突失败关闭，人工核查后可使用 `--force`。

## 稳定吃水状态 sidecar

`draught_state_builder` 直接以权威原油船表按有效 IMO 优先、MMSI 兜底关联 static AIS；只消费 `mmsi`、`imo`、`receive_time_s`、`draught_m` 和 `dq_mask`，按 100,000 行 DuckDB 批次流式归约，绝不生成位置或完整样本副本。使用 [配置模板](configs/draught/draught.example.yaml) 在仓库外创建主机 YAML：

```powershell
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.draught.draught_state_builder --config $env:AIS_DRAUGHT_CONFIG --start-month 2025-09 --end-month 2025-09 --dry-run
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.draught.draught_state_builder --config $env:AIS_DRAUGHT_CONFIG --start-month 2025-09 --end-month 2025-09
```

输出为 `draught/draught_states/year=YYYY/month=MM/draught_states.parquet`，严格仅 `draught_state_id`、`crude_vessel_id`、`state_start_s`、`state_end_s`、`draught_median_m`；同一运行范围的 manifest 位于 `reports/manifests/`。输入 schema、没有有效 IMO 的同刻矛盾吃水、已有不一致产物均失败关闭；同一有效 IMO 的同刻有效吃水则取中位数，超过 0.3 m 的归并组数和最大极差仅记入 manifest。相同输入幂等跳过，人工核查后可使用 `--force`。

当前流程以 Python、DuckDB 和 Zstandard Parquet 为主。详细算法与限制见 [技术说明](docs/技术说明.md)。

## 仓库导航

- [Windows 运行说明](README_使用说明.md)
- [模块输入、输出和字段索引](docs/MODULES.md)
- [真实数据与输出边界](docs/DATA_BOUNDARIES.md)
- [双主机交接步骤](docs/HANDOFF.md)
- [贡献和 PR 工作流](CONTRIBUTING.md)
- [数据字典与模块接口规格 v0.2](docs/specs/AIS原油海运网络_数据字典与模块接口规格_v0.2.md)

## 合成数据快速自测

Windows PowerShell：

```powershell
.\01_setup_environment.ps1
.\04_run_self_test.ps1
```

也可以直接运行：

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .
```

自测只读取 `sample_data/` 中的合成记录，并把结果写入临时目录。

## 事件海水密度模块

最短入口（Windows PowerShell）：

```powershell
$env:AIS_DENSITY_CONFIG = 'D:\data\host-configs\density.yaml'
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.environment.event_density_matcher --config $env:AIS_DENSITY_CONFIG --dry-run
& .\.venv\Scripts\python.exe -m ais_tanker_pipeline.environment.event_density_matcher --config $env:AIS_DENSITY_CONFIG
```

以 [density.example.yaml](configs/environment/density.example.yaml) 作为主机配置模板。真实事件、ERA5、WOA23 和输出路径只存在于未版本化主机 YAML；模板的 `${AIS_ENV_ROOT}` 是主机环境变量，不得将真实路径或数据提交到仓库。

`--dry-run` 不打开事件或环境源文件。正常首次运行会执行 ERA5/WOA23 的 source schema gate，成功后生成严格三列的 `event_seawater_density.parquet` 与 manifest。相同输入会幂等跳过；只有人工检查冲突的派生输出后才能使用 `--force` 重建。

## 使用真实 AIS 数据

将真实 POS/STA 和所有输出放在仓库外，通过复制配置模板并修改 `input_patterns`、`output_root` 和 `duckdb.temp_directory` 指向本机数据盘。运行前先执行只读的 `doctor` 和 `plan`；不要把真实路径写回版本化配置。

## 开发与交接

`main` 受 GitHub Ruleset 保护。所有变更通过任务分支和 Pull Request 完成；两台主机按 [HANDOFF.md](docs/HANDOFF.md) 顺序接力。业务模块必须先完成独立 PRD、实施计划和测试，再进入实现。

## License

代码以 [MIT License](LICENSE) 发布。第三方代码和数据仍受各自许可约束；本许可证不授予无权再分发的数据。

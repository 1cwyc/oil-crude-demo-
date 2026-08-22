# AIS 油轮三小时采样独立运行包

本文件夹可以整体复制到另一台 Windows 电脑运行。它已内置此前核查代码中的 DuckDB AIS 解码逻辑，
不依赖原电脑的 `C:\Users\...\AIS-part\check` 目录，也不会修改原始 `.dat` 文件。

## 一、默认试验数据

默认配置为 `configs\tanker_pipeline_20250715.json`：

- POS：`F:\AIS\POS_OK_2025-07-15.dat`
- STA：`F:\AIS\STA_OK_2025-07-15.dat`
- 输出：`F:\AIS\output\tanker_2025-07-15`

> **本次仅是技术样例。** 单日 STA 只能识别 7 月 15 日当天曾发送有效静态船型的油轮，不能据此宣称覆盖
> 当天出现的全部油轮。正式研究应先用更宽时间窗或全年 STA 冻结年度油轮登记表，再筛选 7 月 15 日或全年 POS。
> 此外，UTC 00:00 的 ±30 分钟窗口需要 7 月 14 日 23:30–24:00 的位置数据；当前只有 7 月 15 日 POS 时，
> 00:00 样本会使用单边候选并在 manifest 中留下警告。

换电脑或换磁盘时，只需编辑该 JSON 中的三处路径：

1. `input_patterns.sta`
2. `input_patterns.pos`
3. `output_root`，以及同一输出目录下的 `duckdb.temp_directory`

## 二、最快运行方法

1. 将整个文件夹复制到目标电脑，不要只复制单个 `.py` 文件。
2. 安装 64 位 Python 3.11。
3. 双击 `01_setup_environment.bat`，程序会在本文件夹建立独立 `.venv` 并安装依赖。
4. 双击 `04_run_self_test.bat`，先用随包小型 `.dat` 验证全部阶段。
5. 双击 `02_doctor_and_plan.bat`，只读核查正式输入、磁盘和执行计划。
6. 确认无误后双击 `03_run_20250715.bat`，按提示输入 `RUN` 才会开始读取正式大文件。

如果单位电脑禁止双击脚本，可以在 PowerShell 中运行：

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\01_setup_environment.ps1
.\.venv\Scripts\python.exe .\run_pipeline.py doctor
.\.venv\Scripts\python.exe .\run_pipeline.py plan
.\.venv\Scripts\python.exe .\run_pipeline.py run
```

## 三、流程做什么

1. 直接读取 STA `.dat`，写出按日静态小分片。
2. 将同一年已有的静态分片聚合为年度船舶登记表和油轮 MMSI 登记表。
3. 逐日直接读取 POS `.dat`，在同一 DuckDB 查询中连接油轮登记表，只保存油轮位置。
4. 对每艘油轮的 0、3、6、9、12、15、18、21 点，在默认 ±30 分钟内选择时间最近的有效位置。
5. 写出三小时样本 Parquet、CSV 和经纬度网格密度热力图。

程序不会先生成全部船舶的完整 POS CSV/Parquet，因此正式年度流程的磁盘读写量明显低于“全量解码后再筛选”。

## 四、输出目录

```text
output_root/
├─ registry/
│  ├─ static_shards/year=YYYY/month=MM/date=YYYY-MM-DD/
│  ├─ vessel_registry/year=YYYY/
│  └─ tanker_registry/year=YYYY/
├─ positions/tanker/year=YYYY/month=MM/date=YYYY-MM-DD/
├─ samples_3h/timezone=UTC/year=YYYY/month=MM/date=YYYY-MM-DD/
├─ exports/
├─ heatmaps/
├─ reports/manifests/
└─ _tmp/
```

Parquet 是可复用的主结果；CSV 主要供人工查看或交给只支持表格的软件。全年数据默认建议直接用 Parquet 绘图，
避免生成一个体量很大的全年 CSV。

## 五、常用命令

```powershell
# 只读检查依赖、内置解码器、输入路径、输出磁盘
.\.venv\Scripts\python.exe .\run_pipeline.py doctor

# 只列计划和文件大小，不读取数据行
.\.venv\Scripts\python.exe .\run_pipeline.py plan

# 一次运行所有启用阶段
.\.venv\Scripts\python.exe .\run_pipeline.py run

# 分阶段运行
.\.venv\Scripts\python.exe .\run_pipeline.py build-registry
.\.venv\Scripts\python.exe .\run_pipeline.py filter-positions
.\.venv\Scripts\python.exe .\run_pipeline.py sample
.\.venv\Scripts\python.exe .\run_pipeline.py export-csv
.\.venv\Scripts\python.exe .\run_pipeline.py heatmap
```

配置不在默认位置时，加：

```powershell
.\.venv\Scripts\python.exe .\run_pipeline.py --config D:\my_config.json plan
```

已有输出与当前输入、程序版本或阶段参数不一致时，程序会停止。确认需要重建后才使用 `--force`：

```powershell
.\.venv\Scripts\python.exe .\run_pipeline.py run --force
```

## 六、全年运行

复制 `configs\full_year_template.json` 后修改路径和年份。全年 `run` 的顺序是：

1. 顺序读取全年 STA，建立每日静态分片；
2. 一次聚合年度油轮登记表；
3. 再顺序读取每天 POS；
4. 逐日采样并分区保存。

不要按天分别从空目录运行全年流程，否则早期日期可能还不知道后来才出现的油轮船型。完整年度研究应先让全年 STA
登记阶段完成，再筛选全部 POS。

年度登记表一旦因新增 STA 改变，应重建所有依赖它的历史位置和样本分区。当前版本面向“收齐并冻结年度登记表后批处理”，
不面向每天追加 STA 后混用新旧登记版本的运营式流程。

## 七、需要明确的研究假设

- 默认时区为 UTC。若改为 `Asia/Shanghai`，当地 00:00 对应前一 UTC 日，需要相邻日位置分区保证边界完整。
- 当前 2025-07-15 试验显式纳入 80–89 全部船型码；正式研究前仍需按数据提供方编码说明核验 85–88。
- `(0, 0)` 坐标默认剔除。
- 三小时采样不插值；容差内无记录时不生成虚假位置。
- 内置旧解码器目前仅为通信类型 1 的位置记录提供 MMSI，类型 2/3 的覆盖损失需在正式研究中量化。
- 默认热力图是经纬度规则网格、无底图的描述性图。论文使用前应确认研究区、投影、网格尺度和归一化。

## 八、故障处理

- `doctor.ready=false`：查看输出中的缺失依赖、文件或不可写目录。
- 依赖安装失败：确认网络、代理和 Python 为 64 位 3.11；随后重新运行安装脚本。
- 输出冲突：先检查 manifest 和配置变化，确认后再使用 `--force`。
- 运行中断：用同一配置重新运行；已完成且签名一致的阶段会自动跳过。
- 派生文件被截断或修改：程序会比较 manifest 中记录的大小和修改时间并停止，确认后使用 `--force` 重建。
- 磁盘空间不足：修改 `output_root` 与 `duckdb.temp_directory` 到空间更大的磁盘，再从新输出目录运行。

更详细的算法和质量控制说明见 `docs\技术说明.md`。

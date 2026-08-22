# oil-crude-demo 公共仓库治理与双主机接力 PRD

> 状态：设计已在会话中分章节确认，待用户审阅本文档后进入实施计划。
>
> 日期：2026-08-22
>
> 目标仓库：`git@github.com:1cwyc/oil-crude-demo-.git`

## 1. 背景与目标

本项目将现有本地仓库 `<repository-root>` 迁移到 GitHub 公共仓库，并将该公共仓库作为唯一代码源。使用者是同一人控制的两台主机和各自主机上的 Codex；两台主机使用独立 SSH 密钥，按顺序接力，不并行修改同一个任务。

本 PRD 的目标是建立一条可维护、可审计、可交接的研发路径：

1. `main` 由 GitHub 服务端规则保护，不接受日常直接推送。
2. 每项任务使用独立分支和 Pull Request；PR 同时承担变更审查、验证记录和跨主机交接。
3. 另一台主机仅依靠仓库文档、外部数据配置和真实 AIS 数据即可理解并运行项目。
4. 公共仓库不包含真实 AIS 原始数据、生成结果、抓取响应、凭据或主机个人信息。
5. 项目以 MIT License 开放，允许他人使用、修改和分发代码。

本 PRD 只设计仓库迁移与治理，不实现 AIS 业务模块。AIS 模块以已确认的 v0.2 数据字典与模块接口规格为业务基线。

## 2. 事实、判断与待核验项

### 2.1 已核实事实

- 本地仓库当前分支为 `main`，工作区干净，当前提交为 `bbb8459`。
- 当前远程 `origin` 是 `git@github.com:1cwyc/AI-.git`。
- 新远程 `git@github.com:1cwyc/oil-crude-demo-.git` 可通过 SSH 访问，当前没有 refs，是空仓库。
- 当前被跟踪内容约 43 KiB，包含源码、配置、文档、测试和两份小型合成 AIS 样本。
- 示例记录使用测试 MMSI 和 `TEST TANKER` 等标识，不是真实业务 AIS 数据。
- 已扫描当前工作树和提交历史，未发现 GitHub 令牌、私钥或常见 API 密钥。
- `ais_decoder/PROVENANCE.md` 含旧电脑用户名和绝对路径；它不是凭据，但没有公开必要。
- 两台主机由同一人使用同一个 GitHub 账号，每台主机使用独立 SSH 密钥。
- 用户已确认采用公共仓库方案和 MIT License，版权人使用 `1cwyc`。
- GitHub 官方文档说明，GitHub Free 的公共仓库可使用 protected branches 和 rulesets：
  - <https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches>
  - <https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets>

### 2.2 设计判断

- 服务器端 Ruleset 比本地 Git hook 更适合作为 `main` 防误推的权威机制；本地 hook 可被跳过且每台主机都要单独安装。
- 同一 GitHub 账号不能形成有意义的独立审批，因此 PR 不要求批准人数；质量由差异审阅、结构化 PR 记录和必需 CI 检查保证。
- 空仓库建立默认分支需要一次 bootstrap 直接推送；这是唯一允许的 `main` 直接推送例外。
- 公开前应先匿名化 `PROVENANCE.md`，避免将无业务价值的旧用户名和绝对路径永久写入新公共仓库历史。
- 业务全流程需要真实 AIS 数据，不适合放入公共 CI；CI 只运行可由源码、配置和合成样本完成的确定性检查。

### 2.3 实施时待核验

- GitHub 仓库网页上实际显示的 visibility 和默认分支状态。
- 当前环境是否已有可用于配置 Ruleset 的 GitHub CLI 登录会话。若没有，Ruleset 由用户在 GitHub 网页按本文清单配置，不索取或保存个人访问令牌。
- 当前轻量测试在干净环境中的准确依赖和运行命令。实施计划必须先运行现有测试，再决定 CI 命令，不凭文件名猜测。

这些待核验项影响操作方式，不改变治理目标。

## 3. 任务分级与范围

### 3.1 第一类：不做就无法实现目标

- 公开前敏感信息和数据边界复核。
- 匿名化旧主机路径。
- 将 `origin` 切换到新公共仓库并验证首次推送。
- 为 `main` 启用服务器端 Ruleset。
- 建立 MIT License、Codex 规则、贡献规范、PR 模板和双主机交接文档。
- 建立可在合成数据上运行的轻量 CI，并在稳定后设为必需检查。
- 导入已确认的 v0.2 数据规格，并提供面向执行者的模块索引。
- 验证直接推送被拒绝、任务分支可推送、PR 可合并。

### 3.2 第二类：有明显收益，纳入但保持最小实现

- `docs/DATA_BOUNDARIES.md`：明确外部数据和输出路径，降低误提交真实 AIS 的风险。
- CI 敏感信息与大文件检查：作为 `.gitignore` 之外的第二道检查。
- Squash merge 和线性历史：让按任务回溯和跨主机接力更清晰。

### 3.3 第三类：当前不实施

- 多人审批、CODEOWNERS 和复杂权限矩阵。
- 自建 Git 服务、通用工作流平台或发布系统。
- GPG 强制签名、自动发布、文档站点和依赖机器人。
- 在 GitHub Actions 中下载或处理真实 AIS 数据。
- 为仓库治理重复实现本地 pre-push hook。
- 自动创建或自动合并 PR。

## 4. 总体架构

```text
GitHub 公共仓库 1cwyc/oil-crude-demo-
                 │
        protected main + CI
                 │
        Pull Request（任务载体）
                 │
       ┌─────────┴─────────┐
       │                   │
     主机 A               主机 B
  独立 SSH 密钥        独立 SSH 密钥
  独立本地克隆        独立本地克隆
       └────── 顺序接力 ────┘
```

约束：

- GitHub 公共仓库是唯一权威代码源。
- 每台主机必须独立 `git clone`，不得复制另一台主机的 `.git` 目录。
- 任一时刻同一任务只由一台主机处理。
- 未完成任务以远程任务分支和 PR 为交接状态，不以聊天记忆或本地未提交文件为准。
- 上游表和原始数据不由新增模块修改；代码、配置和规格通过仓库流转，真实数据通过仓库外路径提供。

## 5. 标准任务工作流

### 5.1 开始任务

```powershell
git switch main
git fetch origin
git pull --ff-only origin main
git switch -c <type>/<task-slug>
```

分支类型仅使用：

- `docs/`：规格与说明；
- `feat/`：新功能；
- `fix/`：缺陷修复；
- `chore/`：依赖、配置和维护；
- `research/`：规则校准、实验和非生产性研究。

### 5.2 完成和交接

1. 运行与变更风险相匹配的验证。
2. 提交任务分支并推送到 `origin`。
3. 创建 PR，填写目标、输入输出、关联规格、验证结果、风险和交接状态。
4. 合并前检查 CI 和差异；默认使用 squash merge。
5. 合并后删除远程任务分支。
6. 下一台主机重新拉取 `main`，不得在旧分支上直接续写已合并任务。

### 5.3 禁止行为

- 日常直接推送 `main`。
- `git push --force` 或改写已公开历史。
- 提交真实 AIS、Parquet、DuckDB、抓取响应、环境文件、SSH 密钥和访问令牌。
- 在两台主机上同时修改同一个任务分支。
- 未经记录地绕过失败的测试或 Ruleset。

## 6. 仓库文档与单一职责

```text
/
├─ AGENTS.md
├─ CONTRIBUTING.md
├─ LICENSE
├─ README.md
├─ .gitignore
├─ .github/
│  ├─ pull_request_template.md
│  └─ workflows/quality.yml
├─ docs/
│  ├─ HANDOFF.md
│  ├─ MODULES.md
│  ├─ DATA_BOUNDARIES.md
│  ├─ specs/AIS原油海运网络_数据字典与模块接口规格_v0.2.md
│  └─ superpowers/specs/2026-08-22-public-repository-governance-design.md
└─ ais_decoder/PROVENANCE.md
```

- `AGENTS.md`：Codex 必须遵守的项目入口规则，只放执行约束和必读文档顺序。
- `CONTRIBUTING.md`：人和 Codex 共用的 Git、测试、提交与 PR 规范。
- `README.md`：项目目标、快速开始、目录导航和运行入口。
- `HANDOFF.md`：两台主机开始、暂停、交接、恢复和冲突处理命令。
- `MODULES.md`：各模块的功能、输入、输出、读取字段、依赖、命令和验收条件。
- `DATA_BOUNDARIES.md`：真实数据放置、路径配置、输出边界和禁止提交内容。
- v0.2 数据规格：字段和模块接口的权威业务合同。
- PR 模板：每项任务的结构化交接记录。
- `quality.yml`：只执行不依赖真实数据的自动检查。
- `PROVENANCE.md`：保留可维护来源说明，不保存个人用户名和绝对路径。

同一规则只在一个权威文件中定义；其他文档通过链接引用，不复制整段内容。`MODULES.md` 解释怎样使用字段，完整字段定义仍以 v0.2 数据规格为准。

## 7. 模块说明合同

`docs/MODULES.md` 中每个业务模块统一采用以下最小模板：

| 项目 | 要求 |
|---|---|
| 模块名与唯一职责 | 一句话说明该模块只负责什么 |
| 前置依赖 | 必须先完成的模块、配置或外部数据 |
| 输入表/文件 | 权威输入名称和路径模式 |
| 实际读取字段 | 仅列实现实际读取的字段及用途 |
| 输出表/文件 | 输出名称、主键、分区和用途 |
| 运行入口 | CLI 命令或 Python 入口 |
| 配置 | 读取的配置文件和必要键 |
| 阻断条件 | 遇到哪些问题必须停止 |
| 验收 | 可执行检查和预期结果 |
| 下游使用者 | 哪些模块读取该输出 |

第一版模块索引覆盖：`schema_gate`、`identity_resolution`、`chinaports_labeling`、`dwt_classification`、`draught_state_builder`、`sample_draught_linker`、`geo_registry_builder`、`event_detector_3h`、`fullres_event_audit`、`voyage_builder`、`monthly_network_builder`、`country_validation_builder` 和 `route_layer_builder`。

ChinaPorts 模块明确采用合规网页爬虫方式，不把未确认存在的官方 API 写成依赖。具体页面结构、访问约束和可用字段必须在真实执行环境校准后再冻结适配器。

## 8. Ruleset 设计

目标：`main`。

启用规则：

- Require a pull request before merging。
- Required approvals 设为 `0`。
- Block force pushes。
- Restrict deletions。
- Require linear history。
- Ruleset 状态为 Active。
- 不设置日常 bypass actor。

仓库默认合并方式保留 squash merge；其他合并方式可关闭以减少历史分叉。

原因：两台主机使用同一 GitHub 账号，要求独立批准会使自己的 PR 无法形成有效审批。PR 的价值是保存任务边界、差异、验证证据和交接记录；自动质量门禁由 CI 提供。

仓库所有者仍可编辑 Ruleset，这是个人账号的权限边界。紧急停用只能由用户手动执行，恢复后必须在相关 PR 中记录原因。

## 9. CI 设计

### 9.1 第一版检查

- 使用仓库声明的 Python 版本和依赖安装方法。
- 编译所有被跟踪的 Python 文件，检查语法。
- 解析被跟踪的 JSON 配置。
- 运行现有轻量测试；测试命令以实施时的基线执行结果为准。
- 检查私钥头、常见 GitHub 令牌格式和不应出现的环境文件。
- 检查被跟踪的大型 AIS、Parquet、DuckDB 和数据库文件。

### 9.2 边界

- CI 不访问真实 AIS 数据目录。
- CI 不访问 ChinaPorts 页面。
- CI 不将网络抓取成功作为合并条件。
- 需要真实数据的业务验收由执行电脑生成报告，并把命令和摘要写入 PR；原始数据和大结果不提交。

### 9.3 启用顺序

首次 `main` 建立后，先启用 PR 规则。治理 PR 合入并产生至少一次成功的 `quality` 检查后，再把唯一且稳定的检查名称设为 Required status check，避免引用不存在的任务导致仓库无法合并。

## 10. 数据、安全与许可证

公共仓库允许：

- 源码、测试和配置模板；
- 合成的最小 AIS 示例；
- 不含抓取正文的字段合同和解析测试夹具；
- 文档、质量摘要示例和不可逆的小型聚合示例。

公共仓库禁止：

- 真实 POS/STA 数据和可还原单船轨迹的业务数据；
- Parquet、DuckDB、SQLite、压缩抓取响应和大规模报告；
- `.env`、个人访问令牌、Cookie、SSH 密钥和浏览器会话；
- 写死的主机用户名、个人目录和临时文件路径；
- 无权再分发的第三方数据。

`.gitignore` 是便利防线，不是安全边界。公开前和 CI 中都要检查已跟踪文件；已经进入 Git 历史的敏感信息不能靠新增 `.gitignore` 消除。

许可证采用 MIT License，版权人 `1cwyc`。第三方代码和数据仍须保留各自许可与来源；MIT License 不能覆盖无权再许可的数据。

## 11. 迁移顺序

1. 重新核对本地 `main`、工作区和新远程为空。
2. 在公开 bootstrap 提交中匿名化 `PROVENANCE.md`；该提交不得包含其他业务变化。
3. 将 `origin` 从旧仓库改为 `git@github.com:1cwyc/oil-crude-demo-.git`。
4. 首次推送 `main` 并验证本地与远程提交一致。
5. 在 GitHub 网页确认仓库为 Public、默认分支为 `main`。
6. 立即建立 Active Ruleset，先启用 PR、删除保护、强推保护和线性历史。
7. 推送治理文档分支并创建首个治理 PR。
8. 治理 PR 合入后，确认 `quality` 成功运行。
9. 将 `quality` 设为必需状态检查。
10. 用测试分支验证保护规则和 PR 工作流。
11. 在第二台电脑按照 `HANDOFF.md` 独立克隆和验收。

旧仓库不再保留为本地 remote。是否删除或归档 GitHub 上的旧仓库不属于本次范围，避免误删历史。

## 12. 错误处理与恢复

- 工作区不干净：停止，不自动 stash、覆盖或丢弃用户修改。
- 新远程出现历史：停止，比较 refs 和文件，不强推。
- 扫描命中疑似敏感信息：停止公开，先区分真实凭据、测试字符串和文档示例。
- 首次推送失败：保持旧 remote URL 记录，不循环修改提交历史；先诊断 SSH、DNS 或权限。
- Ruleset 阻止预期操作：查看生效规则和 CI，不用 `--force` 或无记录地停用保护。
- CI 依赖失败：修复可复现环境；不能把真实 AIS 上传到 Actions 解决缺数问题。
- 两台主机发生并行变更：保留两个分支，用显式整合 PR 解决，不重置或覆盖任一主机历史。
- 第二台主机 `main` 分叉：停止开发，先 fetch 并查明本地独有提交；禁止直接 merge 一个来源不明的分叉。

## 13. 测试与验收

### 13.1 迁移验证

- `git remote -v` 只显示新仓库。
- 本地 `main` 与 `origin/main` 指向相同提交。
- 新仓库页面显示 Public、MIT License 和默认分支 `main`。
- `git fsck --full` 无对象错误。

### 13.2 安全验证

- 已跟踪文件中没有私钥、令牌、Cookie、`.env` 或个人绝对路径。
- 已跟踪文件中没有真实 AIS、Parquet、DuckDB 或抓取响应。
- 合成样本的标识和值明确为测试用途。

### 13.3 工作流验证

- 直接更新 `main` 被服务器拒绝。
- 强推或删除 `main` 被服务器拒绝。
- 普通任务分支能够推送。
- PR 模板自动加载，PR 能通过 squash merge 合入。
- `quality` 检查在仓库自带数据上成功，不访问真实 AIS。
- 第二台电脑可从空目录克隆，按文档完成环境检查和合成样本测试。

## 14. 风险与缓解

| 风险 | 影响 | 缓解措施 |
|---|---|---|
| 公共仓库意外泄露数据 | 隐私、许可和存储风险 | `.gitignore`、公开前扫描、CI 检查、DATA_BOUNDARIES |
| 同一账号缺少独立审批 | 无法由第二身份授权 | 0 审批 + PR 记录 + 必需 CI；不伪造多人审查 |
| 仓库所有者可修改规则 | 保护并非不可撤销 | Ruleset 默认 Active；紧急变更要求记录并恢复 |
| CI 与真实环境差异 | 合成测试通过但业务运行失败 | CI 只声明轻量保证；真实数据验收独立报告 |
| 文档重复后漂移 | 另一台 Codex 读取冲突规则 | 每项规则单一权威文件，其他文档链接引用 |
| 两台主机并行编辑 | 冲突和任务状态不明确 | 一个任务一个 PR、顺序接力、显式交接状态 |
| 第三方数据许可不清 | 无权公开数据 | 仓库不包含真实或第三方数据正文，只保存来源与合同 |

## 15. 开发和协作需求

- Git、OpenSSH 和可运行当前项目的 Python 环境。
- 每台主机独立 SSH 密钥并登记到同一 GitHub 账号。
- 每台主机从 GitHub 独立克隆。
- Git 提交作者姓名和邮箱在两台主机保持一致。
- 实际 AIS 数据通过主机本地配置提供，禁止写入版本控制。
- Codex 开始任务前依次读取 `AGENTS.md`、相关 PR、`MODULES.md`、v0.2 数据规格和目标模块测试。
- 任何业务模块开发仍须先浏览相关开源实现、编写模块 PRD、经用户确认后制定实施计划。

## 16. 完成定义

本项目治理迁移仅在以下条件全部满足时完成：

1. 新公共仓库成为唯一 `origin`，本地与远程 `main` 一致。
2. `main` 的 Active Ruleset 已通过实际拒绝测试证明生效。
3. MIT License、协作规则、PR 模板、交接文档、数据边界、模块索引和 v0.2 规格已通过 PR 合入。
4. `quality` 检查成功，并被配置为 `main` 的必需状态检查。
5. 仓库公开内容通过敏感信息和数据边界检查。
6. 第二台电脑能从 GitHub 独立克隆并按文档完成合成测试。

满足以上条件后，后续 AIS 模块才按独立 PRD 和任务 PR 逐项实施。

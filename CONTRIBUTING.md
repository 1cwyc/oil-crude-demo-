# 贡献与任务工作流

## 分支和提交

每项任务只使用一个分支：

- `docs/<task>`：规格和文档；
- `feat/<task>`：新功能；
- `fix/<task>`：缺陷修复；
- `chore/<task>`：依赖和维护；
- `research/<task>`：规则校准与非生产实验。

开始任务：

```powershell
git switch main
git fetch origin
git pull --ff-only origin main
git switch -c <type>/<task-name>
```

提交信息使用简短祈使句并带类型，例如 `feat: build draught states`、`fix: reject overlapping identity segments`。一个提交只表达一个可审查目的。

## 开发要求

1. 先读取当前 PR、模块索引、字段合同和相关测试。
2. 业务开发必须先完成经用户确认的 PRD 和实施计划。
3. 新行为或缺陷修复遵循红—绿—重构；先看到测试因预期原因失败。
4. 不做与任务无关的重构，不复制已有公共能力。
5. 阈值进入版本化配置，不散落在代码中。

## 提交前检查

在项目根目录运行：

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .
git diff --check
```

涉及真实 AIS 的模块还需运行该模块 PRD 指定的本地验收，并仅在 PR 中记录命令、摘要和报告路径；不得提交原始数据或大结果。

## Pull Request

```powershell
git push -u origin <type>/<task-name>
```

创建 PR 后完整填写模板：目标、规格、输入输出、读取字段、验证证据、数据边界、风险和交接状态。CI 为红色时不得合并。默认使用 squash merge，合并后删除任务分支。

禁止：

- 直接推送或强推 `main`；
- 绕过失败检查；
- 在两个主机上并行修改同一任务；
- 提交 `.venv`、真实数据、输出、Cookie、令牌或个人路径。

## 合并后

```powershell
git switch main
git fetch --prune origin
git pull --ff-only origin main
git branch -d <type>/<task-name>
```

如果分支不能安全删除或 `main` 不能快进，停止并按 [双主机交接说明](docs/HANDOFF.md) 排查，不使用强制删除或硬重置掩盖问题。

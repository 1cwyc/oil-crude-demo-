# 两台主机顺序接力

GitHub 公共仓库是唯一代码源，PR 是唯一任务载体。以下命令均在仓库根目录执行。

## 主机 A 开始任务

```powershell
git switch main
git fetch origin
git pull --ff-only origin main
git status --short --branch
git switch -c <type>/<task-name>
```

只有工作区干净且 `main` 能快进更新时才开始任务。

## 主机 A 暂停并交接开放 PR

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .
git diff --check
git status --short
git push -u origin <type>/<task-name>
```

在 PR 中记录：完成内容、未完成内容、输入输出、验证命令和结果、已知风险以及下一条建议命令。随后勾选“当前主机已停止修改该分支”，主机 A 不再提交。

## 主机 B 首次接入

在新的空目录中执行：

```powershell
git clone git@github.com:1cwyc/oil-crude-demo-.git
Set-Location .\oil-crude-demo-
.\01_setup_environment.ps1
.\04_run_self_test.ps1
```

不要复制主机 A 的 `.git`、`.venv`、输出目录或真实 AIS 数据。

## 主机 B 继续开放任务

仅在主机 A 已明确停止后执行：

```powershell
git fetch origin
git switch --track origin/<type>/<task-name>
git status --short --branch
```

如果本地已有该分支：

```powershell
git switch <type>/<task-name>
git pull --ff-only origin <type>/<task-name>
```

先阅读完整 PR，再继续计划中的下一步；不要重做已由提交和验证证据证明完成的工作。

## PR 合并后的下一任务

```powershell
git switch main
git fetch --prune origin
git pull --ff-only origin main
git branch -d <type>/<old-task-name>
git switch -c <type>/<new-task-name>
```

每个新任务使用新分支和新 PR。

## 异常处理

### 工作区不干净

运行 `git status` 和 `git diff`，识别修改归属。不要自动 stash、覆盖或删除；把状态写入 PR，等待原主机处理或用户决定。

### `main` 不能快进

```powershell
git fetch origin
git log --oneline --left-right main...origin/main
```

停止开发，查明本地独有提交。不得使用 `git reset --hard`、强推或来源不明的合并。

### CI 失败

读取失败任务的完整日志，在同一任务分支本地复现。修复后运行全部检查并推送；不得关闭 Ruleset 或合并红色检查。

### 两台主机误并行

两边立即停止并分别推送各自分支。用新的整合 PR 显式选择或合并变更，不覆盖任一分支历史。

### 远程分支陈旧

确认 PR 已合并且本地没有独有提交后，使用普通 `git branch -d` 和 GitHub 的 Delete branch；条件不满足时停止，不强制删除。

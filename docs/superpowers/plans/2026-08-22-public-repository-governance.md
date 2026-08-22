# oil-crude-demo Public Repository Governance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Safely migrate `<repository-root>` to the public GitHub repository `1cwyc/oil-crude-demo-`, protect `main`, and establish a documented PR-based relay workflow for two hosts.

**Architecture:** GitHub is the only authoritative code source. A protected `main` accepts changes through task branches and Pull Requests; a minimal GitHub Actions workflow runs deterministic tests on bundled synthetic data. Repository documents define one responsibility each, while real AIS data and generated outputs remain outside Git.

**Tech Stack:** Git, GitHub public repository and branch ruleset, PowerShell, Python 3.11, `unittest`, DuckDB 1.5.5, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-22-public-repository-governance-design.md`

## Global Constraints

- Resolve the repository path at runtime with `git rev-parse --show-toplevel`; do not publish a host-specific absolute path.
- New sole remote is exactly `git@github.com:1cwyc/oil-crude-demo-.git`.
- The old remote must not remain under another remote name.
- The repository is public and licensed under MIT with copyright holder `1cwyc`.
- Real AIS data, generated Parquet/DuckDB outputs, crawler responses, credentials, cookies, and machine-specific paths must not enter Git.
- The two hosts use separate SSH keys but the same GitHub account and work sequentially.
- Pull Requests require zero independent approvals because one account cannot independently approve its own work.
- The first public `main` push is the only direct-push bootstrap exception.
- After bootstrap, `main` requires PRs, blocks force pushes and deletion, and requires linear history.
- CI uses only tracked source, configuration, tests, and `sample_data`; it does not fetch real AIS or ChinaPorts pages.
- Existing business behavior is unchanged by this plan.
- Use `apply_patch` for repository file edits. Do not overwrite unrelated user changes.
- Before each commit, run `git diff --check` and the task-specific verification.

---

## File Map

### Files created

- `LICENSE`: MIT terms and copyright.
- `AGENTS.md`: required entry instructions for Codex on either host.
- `README.md`: public project landing page and links to authoritative documents.
- `CONTRIBUTING.md`: branch, commit, test, and PR rules.
- `docs/HANDOFF.md`: exact host-to-host relay procedure and recovery commands.
- `docs/DATA_BOUNDARIES.md`: allowed, ignored, and prohibited data classes.
- `docs/MODULES.md`: operational index for each AIS extension module.
- `docs/specs/AIS原油海运网络_数据字典与模块接口规格_v0.2.md`: approved field and module contract.
- `.github/pull_request_template.md`: PR task and handoff record.
- `.github/workflows/quality.yml`: deterministic public CI.
- `scripts/check_repository_safety.py`: scans Git-tracked paths and small text content.
- `tests/test_repository_safety.py`: unit tests for repository safety rules.

### Files modified

- `ais_decoder/PROVENANCE.md`: remove the former username and absolute source path.
- `.gitignore`: add crawler response/cookie and worktree exclusions only if absent.
- `README_使用说明.md`: replace the generic `C:\Users\...` path with a non-user-specific description if the safety check flags it.

### Files intentionally not modified

- `ais_tanker_pipeline/**`, `ais_decoder/*.py`, `run_pipeline.py`: no business-code changes.
- `sample_data/*.dat`: retain the existing small synthetic fixtures unchanged.
- Existing JSON production templates: retain generic `D:\AIS_DATA` and `D:\AIS_OUTPUT` examples.

---

### Task 1: Create the Public Bootstrap Commit and Move `origin`

**Files:**
- Modify: `ais_decoder/PROVENANCE.md`

**Interfaces:**
- Consumes: clean local `main` at commit `bbb8459`; empty SSH remote `git@github.com:1cwyc/oil-crude-demo-.git`.
- Produces: public `origin/main` containing the existing release plus anonymized decoder provenance.

- [ ] **Step 1: Verify the exact starting state without changing it**

Run in a visible PowerShell terminal:

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo status --short --branch
git -C $Repo rev-parse main
git -C $Repo remote -v
git ls-remote git@github.com:1cwyc/oil-crude-demo-.git
```

Expected:

- Worktree has no uncommitted files.
- `main` descends from `bbb8459`.
- Current `origin` still points to `1cwyc/AI-`.
- `git ls-remote` prints no refs for the new repository.

Stop if any expectation differs. Do not force-push or discard files.

- [ ] **Step 2: Preserve the governance branch, then switch to `main`**

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo log -1 --oneline docs/public-repository-governance
git -C $Repo switch main
```

Expected: the first command shows the committed design and plan; the second switches to `main`.

- [ ] **Step 3: Anonymize provenance with one focused edit**

Replace the source paragraph in `ais_decoder/PROVENANCE.md` with:

```markdown
The files `fast_duckdb.py`, `constants.py`, `parsers.py`, and `__init__.py` were
copied without algorithmic changes from the user's previously validated AIS
quality-check package. The original machine-specific source path is intentionally
not retained in this public repository.
```

Keep the paragraph explaining `_static_query` and `_position_query` unchanged.

- [ ] **Step 4: Verify the privacy edit and existing tests**

```powershell
$Repo = (git rev-parse --show-toplevel)
Select-String -Path "$Repo\ais_decoder\PROVENANCE.md" -Pattern 'Legion|C:\\Users\\' -Quiet
git -C $Repo diff --check
```

Expected:

- `Select-String` returns `False`.
- `git diff --check` prints nothing.

- [ ] **Step 5: Commit only the provenance change**

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo add -- ais_decoder/PROVENANCE.md
git -C $Repo diff --cached --name-only
git -C $Repo commit -m "docs: anonymize decoder provenance"
```

Expected staged file before commit: only `ais_decoder/PROVENANCE.md`.

- [ ] **Step 6: Replace `origin` and bootstrap public `main`**

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo remote set-url origin git@github.com:1cwyc/oil-crude-demo-.git
git -C $Repo remote -v
git -C $Repo push -u origin main
```

Expected: fetch and push URLs both use `oil-crude-demo-`; the push creates `origin/main`.

- [ ] **Step 7: Verify public remote integrity**

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo fetch origin
$Local = git -C $Repo rev-parse main
$Remote = git -C $Repo rev-parse origin/main
if ($Local -ne $Remote) { throw "main and origin/main differ" }
git -C $Repo fsck --full
$Repository = Invoke-RestMethod -Headers @{ 'User-Agent' = 'Codex' } `
  -Uri 'https://api.github.com/repos/1cwyc/oil-crude-demo-'
if ($Repository.private) { throw 'Repository is not public' }
```

Expected: hashes match, `git fsck` has no errors, and `private` is `False`.

- [ ] **Step 8: Rebase the governance branch onto the public bootstrap**

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo switch docs/public-repository-governance
git -C $Repo rebase main
git -C $Repo status --short --branch
```

Expected: clean governance branch with the design and plan on top of anonymized `main`.

---

### Task 2: Enable and Prove the Initial GitHub Ruleset

**Files:**
- No repository files changed.

**Interfaces:**
- Consumes: public `origin/main` from Task 1 and GitHub repository-admin access.
- Produces: an Active ruleset protecting `main`, verified by a rejected update to a temporary smoke-test branch governed by the same rule.

- [ ] **Step 1: Create the remote smoke-test branch before enabling the rule**

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo branch ruleset-smoke-test origin/main
git -C $Repo push -u origin ruleset-smoke-test
```

Expected: `origin/ruleset-smoke-test` exists at the same commit as `origin/main`.

- [ ] **Step 2: Configure the initial ruleset in the GitHub web UI**

Open repository **Settings → Rules → Rulesets → New branch ruleset** and enter:

```text
Ruleset name: protected-main
Enforcement status: Active
Target branches include:
  main
  ruleset-smoke-test
Bypass list: empty
Rules:
  Restrict deletions: enabled
  Require linear history: enabled
  Require a pull request before merging: enabled
  Required approvals: 0
  Block force pushes: enabled
```

Do not enable required status checks yet; the `quality` job does not exist on `main`.

- [ ] **Step 3: Verify the active ruleset through the public API**

```powershell
$Rules = Invoke-RestMethod -Headers @{ 'User-Agent' = 'Codex' } `
  -Uri 'https://api.github.com/repos/1cwyc/oil-crude-demo-/rulesets'
$Rules | Select-Object name,enforcement,target
```

Expected: one entry named `protected-main` with enforcement `active` and target `branch`.

- [ ] **Step 4: Prove direct updates are rejected without touching `main`**

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo switch ruleset-smoke-test
git -C $Repo commit --allow-empty -m "chore: test ruleset rejection"
git -C $Repo push origin ruleset-smoke-test
```

Expected: GitHub rejects the update because a Pull Request is required. The empty test commit remains local only.

- [ ] **Step 5: Remove the smoke target and branch cleanly**

In GitHub, edit `protected-main` so the target list contains only `main`. Save and verify the ruleset remains Active.

Then run:

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo switch docs/public-repository-governance
git -C $Repo branch -D ruleset-smoke-test
git -C $Repo push origin --delete ruleset-smoke-test
git -C $Repo fetch --prune origin
```

Expected: only `main` remains protected; the temporary local and remote branch are absent.

---

### Task 3: Add a Tested Repository Safety Scanner

**Files:**
- Create: `scripts/check_repository_safety.py`
- Create: `tests/test_repository_safety.py`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: repository path supplied by `--repo`; tracked paths from `git ls-files -z`.
- Produces: exit code `0` and `Repository safety check passed.` when clean; exit code `1` and one line per violation otherwise.

- [ ] **Step 1: Write failing unit tests for path and content rules**

Create `tests/test_repository_safety.py` with tests equivalent to:

```python
from pathlib import Path
import tempfile
import unittest

from scripts.check_repository_safety import inspect_tracked_file


class RepositorySafetyTests(unittest.TestCase):
    def inspect(self, relative: str, content: bytes = b"test") -> list[str]:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            return inspect_tracked_file(root, Path(relative))

    def test_allows_small_synthetic_ais_fixture(self) -> None:
        self.assertEqual(self.inspect("sample_data/POS_OK_2026-03-02.dat"), [])

    def test_rejects_raw_ais_outside_sample_data(self) -> None:
        self.assertTrue(self.inspect("data/POS_OK_2026-03-02.dat"))

    def test_rejects_generated_database(self) -> None:
        self.assertTrue(self.inspect("output/result.parquet"))

    def test_rejects_private_key_header(self) -> None:
        marker = ("-----" + "BEGIN " + "OPENSSH PRIVATE KEY" + "-----").encode()
        self.assertTrue(self.inspect("notes.txt", marker))

    def test_rejects_concrete_windows_user_path(self) -> None:
        path = ("C:" + "\\Users\\Alice\\AIS\\input.dat").encode()
        self.assertTrue(self.inspect("notes.txt", path))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the focused test and verify failure**

```powershell
$Repo = (git rev-parse --show-toplevel)
if (-not (Test-Path -LiteralPath "$Repo\.venv\Scripts\python.exe")) {
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$Repo\01_setup_environment.ps1"
}
$Python = "$Repo\.venv\Scripts\python.exe"
Push-Location $Repo
try { & $Python -m unittest tests.test_repository_safety -v } finally { Pop-Location }
```

Expected: import failure because `scripts.check_repository_safety` does not exist.

- [ ] **Step 3: Implement the minimal scanner**

Create `scripts/check_repository_safety.py` with these exact public interfaces:

```python
def inspect_tracked_file(repo: Path, relative_path: Path) -> list[str]:
    """Return deterministic violation messages for one tracked file."""

def tracked_files(repo: Path) -> list[Path]:
    """Return paths from `git ls-files -z` or raise RuntimeError."""

def scan_repository(repo: Path) -> list[str]:
    """Return sorted violations for all tracked files."""

def main(argv: list[str] | None = None) -> int:
    """Parse `--repo`, print results, and return 0 or 1."""
```

Implementation rules:

```python
BLOCKED_SUFFIXES = {
    ".parquet", ".duckdb", ".db", ".sqlite", ".sqlite3",
    ".arrow", ".feather", ".orc", ".zip", ".7z", ".rar",
}
MAX_SYNTHETIC_AIS_BYTES = 64 * 1024
MAX_TEXT_SCAN_BYTES = 1024 * 1024
```

- Reject `POS_OK_*.dat` and `STA_OK_*.dat` unless the path is directly below `sample_data/` and does not exceed 64 KiB.
- Reject any blocked suffix.
- Reject `.env`, `.pem`, `.key`, `.p12`, `.pfx`, and SSH private-key filenames.
- For files up to 1 MiB that decode as UTF-8, reject a private-key header, a concrete Windows user profile path, a concrete Unix home path, and common GitHub credential prefixes.
- Construct test credential/header strings from fragments so the scanner and its tests do not trigger themselves.
- Sort all messages by normalized relative path for deterministic CI output.

- [ ] **Step 4: Run the focused tests and repository scan**

```powershell
$Repo = (git rev-parse --show-toplevel)
$Python = "$Repo\.venv\Scripts\python.exe"
Push-Location $Repo
try {
  & $Python -m unittest tests.test_repository_safety -v
  & $Python scripts/check_repository_safety.py --repo .
} finally { Pop-Location }
```

Expected: unit tests pass. The repository scan may report only the known old generic user-path example; if so, replace it with `%USERPROFILE%` wording and rerun. No credential or data violation may be suppressed.

- [ ] **Step 5: Add missing ignore entries without duplicating existing rules**

Append only absent entries to `.gitignore`:

```gitignore
# Local Git worktrees
.worktrees/

# ChinaPorts crawler responses and authenticated browser state
crawler_raw/
cookies.json
storage_state.json
```

- [ ] **Step 6: Run all tests and commit**

```powershell
$Repo = (git rev-parse --show-toplevel)
$Python = "$Repo\.venv\Scripts\python.exe"
Push-Location $Repo
try {
  & $Python -m unittest discover -s tests -v
  & $Python scripts/check_repository_safety.py --repo .
  git diff --check
  git add -- scripts/check_repository_safety.py tests/test_repository_safety.py .gitignore README_使用说明.md
  git commit -m "test: enforce public repository data boundaries"
} finally { Pop-Location }
```

Expected: all tests and the scanner pass before commit. If `README_使用说明.md` was not changed, do not stage it.

---

### Task 4: Add the Public Project and Collaboration Documents

**Files:**
- Create: `LICENSE`
- Create: `AGENTS.md`
- Create: `README.md`
- Create: `CONTRIBUTING.md`
- Create: `docs/HANDOFF.md`
- Create: `docs/DATA_BOUNDARIES.md`

**Interfaces:**
- Consumes: governance spec and existing `README_使用说明.md`.
- Produces: one discoverable entry point for humans and Codex, plus exact branch and relay procedures.

- [ ] **Step 1: Add the MIT License**

Create `LICENSE` using the standard MIT text with:

```text
Copyright (c) 2026 1cwyc
```

Do not add custom restrictions; restrictions would conflict with the selected MIT license.

- [ ] **Step 2: Add the Codex entry contract**

Create `AGENTS.md` with these mandatory sections:

```markdown
# Codex project instructions

## Read before work
1. Read `README.md` and the active task PR.
2. Read `docs/MODULES.md` and the relevant v0.2 contract section.
3. Read the target module tests and configuration before proposing changes.

## Workflow
- Update `main` with `git pull --ff-only`.
- Use one task branch and one PR per task.
- Never work on the same task from both hosts simultaneously.
- Record commands and results in the PR before handoff.

## Data safety
- Never commit real AIS, generated outputs, crawler responses, credentials, cookies, or machine paths.
- Use only `sample_data` in repository tests and CI.
- Stop if an input schema differs from the documented minimum contract.

## Development gate
- Inspect existing code and relevant open-source projects first.
- Write and obtain approval for a module PRD before business implementation.
- Use tests before implementation and preserve upstream files.
```

Link detailed rules instead of copying `CONTRIBUTING.md` and `DATA_BOUNDARIES.md` in full.

- [ ] **Step 3: Add the public README**

Create `README.md` with these sections and links:

```markdown
# oil-crude-demo
## Purpose and research boundary
## Current capabilities
## Repository map
## Quick synthetic self-test
## Working with real AIS data
## Development and handoff
## License
```

State clearly that the final research goal is macro crude-voyage reconstruction and multi-objective route/disruption optimization. Link `README_使用说明.md` for the existing Windows pipeline, `docs/MODULES.md` for module contracts, and the v0.2 specification for fields.

- [ ] **Step 4: Add contribution and handoff procedures**

Create `CONTRIBUTING.md` with:

- Branch names `docs/`, `feat/`, `fix/`, `chore/`, `research/`.
- Start commands: switch `main`, fetch, pull `--ff-only`, create branch.
- Required local checks: full unittest, safety scanner, task-specific checks.
- Commit convention and prohibition on force push.
- PR template completion and squash merge.

Create `docs/HANDOFF.md` with separate command blocks for:

- Host A starts a task.
- Host A pauses and hands off an open PR.
- Host B clones for the first time.
- Host B resumes an open task only after Host A stops.
- Host B starts after a merged PR.
- Dirty worktree, diverged `main`, failed CI, and stale branch recovery.

All pull operations use `--ff-only`; recovery instructions must stop before any reset or destructive checkout.

- [ ] **Step 5: Add data boundaries**

Create `docs/DATA_BOUNDARIES.md` with an explicit table:

| Class | Git policy | Location |
|---|---|---|
| Source/tests/config templates | Track | Repository |
| Synthetic AIS fixtures <=64 KiB | Track | `sample_data/` |
| Real POS/STA | Never track | User-configured external volume |
| Parquet/DuckDB/reports | Never track | `output_root` outside repository |
| ChinaPorts raw responses/cookies | Never track | External crawler workspace |
| Secrets and SSH keys | Never track | OS credential locations |

Explain that `.gitignore` does not remove already tracked or historical content.

- [ ] **Step 6: Verify links, safety, and commit**

```powershell
$Repo = (git rev-parse --show-toplevel)
$Python = "$Repo\.venv\Scripts\python.exe"
Push-Location $Repo
try {
  & $Python scripts/check_repository_safety.py --repo .
  & $Python -m unittest discover -s tests -v
  git diff --check
  git add -- LICENSE AGENTS.md README.md CONTRIBUTING.md docs/HANDOFF.md docs/DATA_BOUNDARIES.md
  git commit -m "docs: define public collaboration workflow"
} finally { Pop-Location }
```

Expected: scanner and tests pass; Markdown links point to files that exist or are created in Task 5 on the same PR branch.

---

### Task 5: Import the Approved AIS Contract and Add the Module Index

**Files:**
- Create: `docs/specs/AIS原油海运网络_数据字典与模块接口规格_v0.2.md`
- Create: `docs/MODULES.md`

**Interfaces:**
- Consumes: approved source document `%USERPROFILE%\Documents\Codex\2026-08-22\ru-he\outputs\AIS原油海运网络_数据字典与模块接口规格_v0.2.md`.
- Produces: the repository's authoritative field contract and a concise operational module index.

- [ ] **Step 1: Copy the approved v0.2 contract without rewriting it**

Copy the approved file byte-for-byte into `docs/specs/`. Verify both SHA-256 hashes match before staging:

```powershell
$Source = Join-Path $env:USERPROFILE 'Documents\Codex\2026-08-22\ru-he\outputs\AIS原油海运网络_数据字典与模块接口规格_v0.2.md'
$Target = Join-Path $Repo 'docs\specs\AIS原油海运网络_数据字典与模块接口规格_v0.2.md'
Copy-Item -LiteralPath $Source -Destination $Target
if ((Get-FileHash $Source).Hash -ne (Get-FileHash $Target).Hash) { throw 'Specification copy mismatch' }
```

Do not silently edit the approved contract during import. Any later correction requires a separate docs PR.

- [ ] **Step 2: Create the module index using the fixed template**

For each of these modules, create one section in `docs/MODULES.md`:

```text
schema_gate
identity_resolution
chinaports_labeling
dwt_classification
draught_state_builder
sample_draught_linker
geo_registry_builder
event_detector_3h
fullres_event_audit
voyage_builder
monthly_network_builder
country_validation_builder
route_layer_builder
```

Each section must contain exactly these headings:

```markdown
### `<module_name>`
- **Function:**
- **Prerequisites:**
- **Inputs:**
- **Fields read:**
- **Outputs:**
- **Configuration:**
- **Run entry:**
- **Blocking conditions:**
- **Acceptance:**
- **Downstream consumers:**
```

Use only facts already defined in the v0.2 contract. For code or commands not implemented yet, write `Not implemented; the module requires its own approved PRD before an entry point is added.` This is a factual state marker, not an implementation placeholder.

For `chinaports_labeling`, specify a compliant Scrapy web crawler and explicitly state that page selectors and available fields are calibrated only after observing the live public page in the execution environment. Do not claim an official API exists.

- [ ] **Step 3: Validate coverage and avoid duplicated field dictionaries**

Run:

```powershell
$Modules = @(
  'schema_gate','identity_resolution','chinaports_labeling','dwt_classification',
  'draught_state_builder','sample_draught_linker','geo_registry_builder',
  'event_detector_3h','fullres_event_audit','voyage_builder',
  'monthly_network_builder','country_validation_builder','route_layer_builder'
)
$Text = Get-Content -Raw (Join-Path $Repo 'docs\MODULES.md')
foreach ($Module in $Modules) {
  if ($Text -notmatch [regex]::Escape("### ``$Module``")) { throw "Missing module: $Module" }
}
```

Then manually confirm `MODULES.md` links to the v0.2 table instead of copying full field tables.

- [ ] **Step 4: Run checks and commit**

```powershell
$Repo = (git rev-parse --show-toplevel)
$Python = "$Repo\.venv\Scripts\python.exe"
Push-Location $Repo
try {
  & $Python scripts/check_repository_safety.py --repo .
  git diff --check
  git add -- docs/specs/AIS原油海运网络_数据字典与模块接口规格_v0.2.md docs/MODULES.md
  git commit -m "docs: add AIS module and field contracts"
} finally { Pop-Location }
```

---

### Task 6: Add the PR Template and Deterministic GitHub Actions Check

**Files:**
- Create: `.github/pull_request_template.md`
- Create: `.github/workflows/quality.yml`

**Interfaces:**
- Consumes: `requirements.txt`, `tests/`, `sample_data/`, and `scripts/check_repository_safety.py`.
- Produces: a structured PR body and a unique required status context named `quality`.

- [ ] **Step 1: Create the Pull Request template**

Create `.github/pull_request_template.md` with unchecked boxes and these headings:

```markdown
## Objective
## Related specification or issue
## Inputs and fields read
## Outputs and interfaces changed
## Files changed
## Verification commands and results
## Real AIS data boundary
## Risks and known limitations
## Handoff status
- [ ] The current host has stopped modifying this branch.
- [ ] No real AIS data, generated outputs, crawler responses, or credentials are included.
- [ ] The receiving host can continue using only this PR and repository documentation.
```

- [ ] **Step 2: Create the CI workflow**

Create `.github/workflows/quality.yml` with this structure:

```yaml
name: quality

on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read

jobs:
  quality:
    name: quality
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
      - name: Install dependencies
        run: python -m pip install --disable-pip-version-check -r requirements.txt
      - name: Compile Python
        run: python -m compileall -q ais_decoder ais_tanker_pipeline run_pipeline.py scripts tests
      - name: Validate JSON configs
        shell: python
        run: |
          import json
          from pathlib import Path
          for path in sorted(Path("configs").glob("*.json")):
              json.loads(path.read_text(encoding="utf-8"))
              print(path)
      - name: Run unit and synthetic integration tests
        run: python -m unittest discover -s tests -v
      - name: Check public repository safety
        run: python scripts/check_repository_safety.py --repo .
```

Pin only official `actions/checkout` and `actions/setup-python` major versions in this first pass. Do not add third-party actions.

- [ ] **Step 3: Run every CI command locally**

```powershell
$Repo = (git rev-parse --show-toplevel)
$Python = "$Repo\.venv\Scripts\python.exe"
Push-Location $Repo
try {
  & $Python -m pip install -r requirements.txt
  & $Python -m compileall -q ais_decoder ais_tanker_pipeline run_pipeline.py scripts tests
  Get-ChildItem configs -Filter *.json | ForEach-Object {
    Get-Content -Raw $_.FullName | ConvertFrom-Json | Out-Null
  }
  & $Python -m unittest discover -s tests -v
  & $Python scripts/check_repository_safety.py --repo .
  git diff --check
} finally { Pop-Location }
```

Expected: every command exits `0`; the end-to-end test uses a temporary directory and bundled synthetic files only.

- [ ] **Step 4: Commit the GitHub integration**

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo add -- .github/pull_request_template.md .github/workflows/quality.yml
git -C $Repo commit -m "ci: add pull request quality gate"
```

---

### Task 7: Push the Governance Branch and Merge the First PR

**Files:**
- No new files; validates all governance-branch changes.

**Interfaces:**
- Consumes: completed Tasks 1-6 and initial `main` Ruleset.
- Produces: public governance PR with successful `quality` status and a squash commit on `main`.

- [ ] **Step 1: Run the complete pre-push verification**

```powershell
$Repo = (git rev-parse --show-toplevel)
$Python = "$Repo\.venv\Scripts\python.exe"
Push-Location $Repo
try {
  git status --short --branch
  git diff main...HEAD --check
  & $Python -m unittest discover -s tests -v
  & $Python scripts/check_repository_safety.py --repo .
  git fsck --full
} finally { Pop-Location }
```

Expected: clean worktree; tests, safety scan, and object check pass.

- [ ] **Step 2: Push only the task branch**

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo push -u origin docs/public-repository-governance
```

Expected: branch push succeeds while `main` remains unchanged.

- [ ] **Step 3: Create the PR in GitHub**

Open:

```text
https://github.com/1cwyc/oil-crude-demo-/compare/main...docs/public-repository-governance?expand=1
```

Title:

```text
Establish public repository governance and handoff workflow
```

Complete every PR-template section with the exact verification commands and results from Step 1. Do not mark the receiving-host checkbox until the current host has stopped editing.

- [ ] **Step 4: Wait for and inspect `quality`**

Expected: the single status job named `quality` passes. If it fails, inspect logs, fix on the same task branch, rerun local checks, commit, and push. Do not merge a red check.

- [ ] **Step 5: Squash merge and synchronize local `main`**

After the PR is green, use **Squash and merge** in GitHub, then run:

```powershell
$Repo = (git rev-parse --show-toplevel)
git -C $Repo switch main
git -C $Repo fetch --prune origin
git -C $Repo pull --ff-only origin main
git -C $Repo branch -d docs/public-repository-governance
```

Expected: local `main` matches `origin/main`; the local task branch deletes without force.

---

### Task 8: Require `quality` and Complete Cross-Host Acceptance

**Files:**
- No repository files unless acceptance reveals a documentation defect; any defect uses a new `fix/` or `docs/` PR.

**Interfaces:**
- Consumes: merged governance PR and at least one successful `quality` check.
- Produces: final protected workflow and verified independent clone on Host B.

- [ ] **Step 1: Add the successful status check to the Ruleset**

In **Settings → Rules → Rulesets → protected-main**, enable **Require status checks to pass** and select the exact status context:

```text
quality
```

Keep all existing rules and the empty bypass list. Save with enforcement `Active`.

- [ ] **Step 2: Verify the final Ruleset through the public API**

```powershell
$Rules = Invoke-RestMethod -Headers @{ 'User-Agent' = 'Codex' } `
  -Uri 'https://api.github.com/repos/1cwyc/oil-crude-demo-/rulesets'
$Rules | ConvertTo-Json -Depth 10
```

Expected: `protected-main` is active. Open its returned API URL if the summary does not include rule details, and verify PR, deletion, force-push, linear-history, and `quality` requirements.

- [ ] **Step 3: Validate the final repository from Host A**

```powershell
$Repo = (git rev-parse --show-toplevel)
$Python = "$Repo\.venv\Scripts\python.exe"
git -C $Repo remote -v
git -C $Repo status --short --branch
git -C $Repo rev-parse main
git -C $Repo rev-parse origin/main
git -C $Repo fsck --full
& $Python "$Repo\scripts\check_repository_safety.py" --repo $Repo
```

Expected: only the new `origin`; clean `main`; equal hashes; all checks pass.

- [ ] **Step 4: Clone independently on Host B**

In a new empty directory on Host B:

```powershell
git clone git@github.com:1cwyc/oil-crude-demo-.git
Set-Location .\oil-crude-demo-
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe scripts\check_repository_safety.py --repo .
```

Expected: clone and all synthetic tests pass without copying Host A's `.git`, virtual environment, outputs, or real AIS data.

- [ ] **Step 5: Perform one PR relay smoke test**

On Host B:

```powershell
git switch main
git pull --ff-only origin main
git switch -c docs/handoff-smoke-test
```

Make one factual clarification to `docs/HANDOFF.md`, run the full local checks, commit, push, and open a PR using the template. Host A must not edit the branch. Merge only after `quality` passes, then pull `main` on Host A.

Expected: the second-host PR completes the documented sequential relay without direct `main` updates.

- [ ] **Step 6: Record completion evidence**

In the handoff smoke-test PR, record:

- Host A and Host B clone paths without usernames or secrets.
- Local and remote commit hashes.
- Test counts and final success.
- Ruleset name and required check name.
- Confirmation that no real AIS data was read or uploaded.

The governance migration is complete only after this PR is merged and both hosts have a clean, synchronized `main`.

---

## Execution Checkpoints

Stop for user action at these points:

1. Task 2 Step 2: user saves the initial GitHub Ruleset in the web UI.
2. Task 2 Step 5: user removes the smoke-test branch target from the Ruleset.
3. Task 7 Step 3: user creates or confirms the governance PR if no authenticated GitHub CLI is available.
4. Task 7 Step 5: user confirms squash merge.
5. Task 8 Step 1: user adds `quality` as a required check.
6. Task 8 Step 4: commands must run on Host B; Host A cannot substitute for this acceptance test.

Do not request a GitHub personal access token. SSH is sufficient for Git transport; repository settings are handled in the authenticated GitHub web UI.

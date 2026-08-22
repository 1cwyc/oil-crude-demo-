# Changelog

## 1.0.1（发布复核补充）

- 跳过已完成阶段前增加派生输出大小和修改时间校验，防止截断或误改文件被静默复用。
- 明确单日 STA 的油轮覆盖边界、UTC 00:00 相邻日窗口和年度登记表冻结要求。
- 发布包清除自测输出和 Python 缓存，避免换电脑后发生路径签名冲突。
- 2025-07-15 单日试验配置按用户要求将船型码扩展为 80–89；本次先完成油轮位置筛选，不执行时间采样。

## 1.0.1 — 2026-08-06

- First self-contained Windows release folder.
- Bundles the previously validated DuckDB AIS decoder instead of referencing a user-specific desktop path.
- Defaults to the 2025-07-15 pilot files under `F:\AIS`.
- Adds environment setup, doctor/plan, guarded production run, and self-test launchers.
- Supports annual STA registry reuse, daily POS filtering, three-hour nearest sampling, CSV export, and density heatmap output.

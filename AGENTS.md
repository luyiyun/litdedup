# AGENTS.md

## Project Snapshot

`litdedup` 当前是一个独立维护的 Python 命令行工具，不再默认依赖原始纵向项目的目录结构。它的目标是服务系统综述 / 文献检索清理流程，负责检索结果导入、清理、去重、人工复核回流、结果导出与报告生成。

## Tech Stack

- Python `3.12`
- CLI: `Typer`
- Config schema: `Pydantic`
- Storage: `SQLite`
- Fuzzy matching: `RapidFuzz`
- Progress display: `tqdm`
- Dependency / task runner: `uv`

## Repository Layout

- `main.py`: 简单入口，转发到 `litdedup.cli:main`
- `src/litdedup/cli.py`: 命令行子命令定义
- `src/litdedup/config.py`: 默认配置、运行目录与 profile 定义
- `src/litdedup/db.py`: SQLite 连接、表结构与持久化操作
- `src/litdedup/parsers.py`: `NBIB/RIS` 解码、解析、标准化
- `src/litdedup/dedup.py`: 精确去重、模糊去重、人工复核导出 / 回流
- `src/litdedup/export.py`: `CSV/RIS` 导出
- `src/litdedup/report.py`: Markdown / JSON 报告生成
- `tests/test_litdedup.py`: 当前主要集成测试

## Runtime Conventions

- 默认运行目录是当前工作目录下的 `./dedup/`
- 如果用户传入 `--runtime-dir`，则以显式路径为准
- 默认运行目录中会生成：
  - `dedup.sqlite`
  - `config.json`
  - `manual_review_queue.csv`
  - `deduplicated_records.ris`
  - `deduplicated_records.csv`
  - `dedup_report.md`
  - `dedup_report.json`

## Current Profiles

- `pubmed_nbib`
- `embase_ris`
- `wos_ris`

已知实现约定：

- Embase 摘要优先 `N2`
- WoS RIS 支持 BOM 去除
- 不做自动编码探测；导入顺序是 `CLI --encoding -> profile encoding -> default utf-8`
- 输出文件默认使用 `utf-8`，可由各命令显式覆盖
- `review-export` 默认不覆盖已有人工复核 CSV，需显式 `--force`

## Common Commands

安装依赖：

```bash
uv sync --extra dev
```

查看帮助：

```bash
uv run litdedup --help
```

运行测试：

```bash
uv run python -m pytest -q
```

初始化默认运行目录：

```bash
uv run litdedup init
```

## Maintenance Notes

- `README.md` 是用户文档的事实来源；如果默认路径、命令示例或工作流变化，需要同步更新
- `src/litdedup.egg-info/` 属于生成产物，不应作为需求或实现意图的事实来源
- 当前工作区里没有 `.git` 元数据；如果后续恢复 Git 管理，建议把仓库根目录约定重新写入本文件
- 新增解析 profile、导出文件名或默认行为时，优先补 `tests/test_litdedup.py`
- 这个工具现在按“独立仓库”思路维护，避免重新引入对旧上游项目固定目录结构的隐式依赖

# `litdedup`

`litdedup` 是一个独立维护的本地命令行工具，用于系统综述检索结果的导入、清理、去重、人工复核回流与结果导出。

当前默认支持：

- `PubMed` 的 `NBIB`
- `Embase` 的 `RIS`
- `Web of Science` 的 `RIS`

核心能力包括：

- 渐进式导入与本地 `SQLite` 持久化
- 精确去重与保守模糊去重
- 人工复核队列导出 / 回流
- 去重后 `RIS/CSV` 导出
- Markdown / JSON 报告生成

## 1. 仓库结构与默认运行目录

代码位于：

```text
src/litdedup/
```

测试位于：

```text
tests/
```

默认运行产物位于当前工作目录下的：

```text
./dedup/
```

也就是说，你在什么目录执行 `uv run litdedup ...`，默认就会在那个目录下创建 `dedup/`。如果你想把运行产物放到别处，可以显式传 `--runtime-dir`。

默认会生成这些文件：

- `dedup/dedup.sqlite`
- `dedup/config.json`
- `dedup/manual_review_queue.csv`
- `dedup/deduplicated_records.ris`
- `dedup/deduplicated_records.csv`
- `dedup/dedup_report.md`
- `dedup/dedup_report.json`

## 2. 安装

在仓库根目录执行：

```bash
uv sync --extra dev
```

查看帮助：

```bash
uv run litdedup --help
```

如果你的环境里 `uv` 缓存目录有权限问题，可以这样运行：

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run litdedup --help
```

## 3. 编码策略

当前版本采用“配置优先、样本探测兜底”的混合模式：

1. 先看 `config.json` 中 profile 的 `encoding`
2. 如果没有显式指定，再用文件前一小段样本做探测
3. 如果命令行传了 `--encoding`，则命令行优先级最高

默认内置策略：

- `pubmed_nbib`: `utf-8-sig`
- `embase_ris`: 不固定编码，允许采样探测
- `wos_ris`: `utf-8-sig`

如果你知道某一批文件的编码，可以直接在导入时指定：

```bash
uv run litdedup import data/wos/special.ris --profile wos_ris --encoding mac_roman
```

## 4. 常用完整流程

### 4.1 初始化运行目录

```bash
uv run litdedup init
```

如果你想用默认模板覆盖已有的 `config.json`：

```bash
uv run litdedup init --force
```

### 4.2 导入检索结果

PubMed：

```bash
uv run litdedup import data/pubmed/*.nbib
```

Embase：

```bash
uv run litdedup import data/embase/*.ris --profile embase_ris
```

WoS：

```bash
uv run litdedup import data/wos/*.ris --profile wos_ris
```

如果要重导某一批已经导入过的文件：

```bash
uv run litdedup import data/embase/*.ris --profile embase_ris --force
```

### 4.3 查看当前导入状态

```bash
uv run litdedup stats
```

这会输出 JSON，其中包括：

- 原始导入记录数
- 各数据库记录数
- 关键字段缺失率
- 编码使用情况
- 去重阶段统计

### 4.4 精确去重

```bash
uv run litdedup dedup-exact
```

精确去重优先使用这些强标识符：

- `PMID`
- `DOI`
- `PMCID`

### 4.5 模糊去重

```bash
uv run litdedup dedup-fuzzy
```

这一步会：

- 自动合并高置信重复
- 将边界案例留给人工复核

### 4.6 导出人工复核队列

```bash
uv run litdedup review-export
```

默认输出：

```text
dedup/manual_review_queue.csv
```

注意：

- 如果目标路径已经存在，`review-export` 会直接报错，避免不小心覆盖已经编辑过的 CSV
- 如果确认要覆盖，必须显式加 `--force`

例如：

```bash
uv run litdedup review-export --force
```

也可以导出到别的位置：

```bash
uv run litdedup review-export --output dedup/manual_review_queue_round2.csv
```

### 4.7 人工复核后回流

在 `manual_review_queue.csv` 里主要填写这三列：

- `decision`
- `preferred_keeper`
- `notes`

`decision` 只支持：

- `merge`
- `separate`
- `skip`

回流命令：

```bash
uv run litdedup review-import dedup/manual_review_queue.csv
```

如果 `decision` 列全空，命令会直接报错，提示还没有真正保存人工决定。

### 4.8 导出去重后的结果

```bash
uv run litdedup export
```

默认会生成：

- `deduplicated_records.ris`
- `deduplicated_records.csv`

如果还有未处理的人工复核对，`export` 会拒绝继续。
如果只是想先导出一个临时版本，可以：

```bash
uv run litdedup export --allow-pending
```

也可以自定义输出路径：

```bash
uv run litdedup export \
  --ris-output dedup/my_deduped.ris \
  --csv-output dedup/my_deduped.csv
```

### 4.9 生成报告

```bash
uv run litdedup report
```

默认会生成：

- `dedup_report.md`
- `dedup_report.json`

## 5. 子命令说明

### `init`

初始化运行目录、数据库和配置文件。

常用参数：

- `--runtime-dir`
- `--force`

### `import`

导入一个或多个 `NBIB/RIS` 文件。

常用参数：

- `--profile`
- `--encoding`
- `--runtime-dir`
- `--force`

### `stats`

查看当前数据库状态和字段完整性。

### `dedup-exact`

执行强标识符精确去重。

### `dedup-fuzzy`

执行保守模糊去重，输出人工复核候选到数据库。

### `review-export`

导出人工复核 CSV。

常用参数：

- `--output`
- `--runtime-dir`
- `--force`

### `review-import`

将人工填写后的 CSV 决策回流到数据库。

### `export`

导出去重后的最终记录。

常用参数：

- `--allow-pending`
- `--csv-output`
- `--ris-output`
- `--runtime-dir`

### `report`

生成 Markdown / JSON 报告。

## 6. `config.json` 说明

每个 profile 至少包括这些关键字段：

- `format`
- `source_name`
- `record_start_tag`
- `record_end_tag`
- `encoding`
- `encoding_candidates`
- `field_map`
- `identifier_aliases`

当前几个重要约定：

- Embase 摘要优先读取 `N2`，其次 `AB`
- WoS 会先去除 BOM 再解析 RIS
- PubMed 会从 `AID/LID` 中提取 DOI

## 7. 常见问题

### 7.1 `review-import` 没有效果

先检查 `manual_review_queue.csv` 里的 `decision` 列是不是已经真正保存了：

- `merge`
- `separate`
- `skip`

如果全空，`review-import` 会直接报错。

### 7.2 `review-export` 提示文件已存在

这是当前的保护行为，避免覆盖已经人工编辑过的 CSV。
如果确认要覆盖，请显式加：

```bash
uv run litdedup review-export --force
```

### 7.3 导入前卡很久才出现进度条

当前版本已经把整文件重型编码探测改成“配置优先 + 样本探测”，通常会比旧版快很多。
如果知道文件编码，直接加 `--encoding` 会更快。

### 7.4 CSV 在 Excel 或 WPS 中乱码

当前导出的 `manual_review_queue.csv` 和 `deduplicated_records.csv` 都采用兼容表格软件的 UTF-8 输出。
如果仍有个别字符异常，优先检查源文件本身编码，必要时在导入时显式指定 `--encoding`。

## 8. 开发与测试

运行测试：

```bash
uv run python -m pytest -q
```

当前测试覆盖的重点包括：

- `Embase N2` 摘要解析
- `WoS BOM` 处理
- `cp1252 / mac_roman` 编码处理
- 人工复核回流
- `review-export` 的覆盖保护
- 默认运行目录基于当前工作目录

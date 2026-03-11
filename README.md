# Anime Metadata Pipeline

动画元数据增量处理与跨源匹配 Pipeline —— 从 MAL（MyAnimeList）获取动画元数据，经过质量检查后与 Wikidata 实体进行跨源匹配，支持断点恢复和增量处理。

## 项目架构

```
pipeline/
├── run.py                  # CLI 入口
├── requirements.txt        # Python 依赖
├── data/
│   ├── kaggle/             # Kaggle CSV 数据集（MAL 动画数据）
│   ├── pipeline.db         # SQLite 数据库（运行时生成）
│   └── export.jsonl        # 导出文件（运行时生成）
└── pipeline/
    ├── __init__.py
    ├── runner.py            # 主流程编排：fetch → quality → match → export → report
    ├── fetcher.py           # 数据获取：Jikan API / Kaggle CSV 两种模式
    ├── quality.py           # 数据质量检查：4 条规则
    ├── matcher.py           # 跨源匹配：Wikidata 精确 ID + 模糊标题匹配
    ├── database.py          # SQLite 数据库：schema、连接管理、CRUD
    ├── exporter.py          # 数据导出：JSON Lines 格式
    └── report.py            # 处理报告：本轮 + 累计统计
```

### 数据流

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────┐     ┌──────────┐
│  Stage 1     │     │  Stage 2     │     │  Stage 3     │     │ Stage 4  │     │ Stage 5  │
│  Fetch       │────▶│  Quality     │────▶│  Match       │────▶│ Export   │────▶│ Report   │
│  (API/CSV)   │     │  Check       │     │  (Wikidata)  │     │ (JSONL)  │     │          │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────┘     └──────────┘
       │                    │                    │
       ▼                    ▼                    ▼
   checkpoint           quality_issues       match_result
   (页码/行号)           (不合格记录)         (匹配结果)
```

## 基本功能

### 1. 数据获取（Fetch）

- **API 模式**：通过 Jikan API 在线获取 MAL 动画元数据，自动处理分页和 429 限流退避重试
- **CSV 模式**：从本地 Kaggle CSV 数据集批量加载，自动检测分隔符和列名映射
- **增量加载**：通过 checkpoint 记录已获取的页码（API）或文件名+行号（CSV），下次运行从断点继续

### 2. 质量检查（Quality Check）

对入库的动画数据执行 4 条质量规则检查：

| 规则 | 说明 |
|------|------|
| `required_fields` | `mal_id` 和 `title` 必须存在且非空 |
| `score_range` | `score` 若存在须在 [0, 10] 范围内 |
| `year_validity` | `year` 若存在须在 [1900, 2030] 范围内 |
| `episodes_non_negative` | `episodes` 若存在须 ≥ 0 |

不合格数据记录到 `quality_issues` 表，不阻塞整体流程。

### 3. 跨源匹配（Match）

将通过质量检查的 MAL 条目与 Wikidata 实体进行匹配：

- **精确 ID 匹配**：通过 Wikidata P4086 属性（MAL anime ID）精确匹配，置信度 = 1.0
- **模糊标题匹配**：对精确未匹配到的，使用 rapidfuzz `token_sort_ratio` 比较英文/日文标题，阈值 ≥ 0.85

匹配结果记录匹配方式（`exact_id` / `fuzzy_title`）和置信度（0.0 ~ 1.0）。

### 4. 结果存储

使用 SQLite 数据库，schema 区分三种状态：

| 状态 | 含义 |
|------|------|
| `matched` | 置信度 ≥ 0.85，已确认匹配 |
| `review` | 置信度 < 0.85，低置信度待人工审查 |
| `unmatched` | 未找到对应 Wikidata 实体 |

支持增量写入，使用 `ON CONFLICT` 策略避免重复插入。

### 5. 断点恢复

Pipeline 中途中断后（kill、崩溃、网络断开），重启后自动从断点继续：

| 阶段 | 恢复机制 |
|------|---------|
| Fetch | checkpoint 记录页码/CSV行号，从上次停止处继续加载 |
| Quality | 幂等操作，重跑无副作用 |
| Match | `get_unmatched_passed_ids()` 自动排除已匹配 ID；`match_stage` checkpoint 记录 exact/fuzzy 子阶段 |

### 6. 数据导出

导出为 JSON Lines 格式，每行包含：

```json
{
  "mal_metadata": { "mal_id": 1, "title": "...", "score": 8.78, ... },
  "wikidata_metadata": { "qid": "Q215380", "label_en": "...", ... },
  "match": { "method": "exact_id", "confidence": 1.0, "status": "matched", ... }
}
```

支持按状态过滤导出（`matched` / `unmatched` / `review`）。

## 快速开始

### 环境准备

```bash
# Python 3.10+
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 基本用法

```bash
# 从 Kaggle CSV 加载 1000 条数据，完整运行 pipeline
python run.py --source csv --csv-path data/kaggle --max-items 1000

# 从 Jikan API 获取 3 页数据（每页 25 条）
python run.py --source api --max-pages 3

# 跳过获取阶段，只对已有数据执行质量检查 + 匹配
python run.py --skip-fetch

# 只获取数据和质量检查，跳过匹配
python run.py --source csv --csv-path data/kaggle --max-items 500 --skip-match --no-export

# 仅导出已有数据
python run.py --export-only
python run.py --export-only --export-status matched        # 仅导出已匹配
python run.py --export-only --export-path output.jsonl     # 自定义导出路径

# 仅查看报告
python run.py --report-only

# 显示详细日志
python run.py --source csv --csv-path data/kaggle --max-items 500 -v
```

### CLI 参数一览

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--source` | 数据源类型：`api` 或 `csv` | `api` |
| `--csv-path` | CSV 文件路径或目录 | 无 |
| `--max-items` | CSV 模式每轮最多加载条数 | 全部 |
| `--max-pages` | API 模式每轮最多获取页数 | 2 |
| `--skip-fetch` | 跳过数据获取阶段 | 否 |
| `--skip-match` | 跳过跨源匹配阶段 | 否 |
| `--no-export` | 跳过 JSONL 导出 | 否 |
| `--export-path` | 自定义导出路径 | `data/export.jsonl` |
| `--export-only` | 仅导出，不运行 pipeline | 否 |
| `--export-status` | 按状态过滤导出 | 全部 |
| `--report-only` | 仅生成报告 | 否 |
| `-v, --verbose` | 显示详细日志 | 否 |

## 断点恢复演示

以下演示如何验证 pipeline 的断点恢复能力：

### 步骤 1：清理环境，加载数据

```bash
# 清理旧数据
rm -f data/pipeline.db data/export.jsonl

# 加载 1000 条数据，跳过匹配阶段
python run.py --source csv --csv-path data/kaggle --max-items 1000 --skip-match --no-export -v
```

### 步骤 2：启动匹配阶段，中途 kill

```bash
# 后台启动 pipeline，等待数秒后 kill
python run.py --skip-fetch --no-export -v &
PID=$!
sleep 8
kill $PID
```

### 步骤 3：重启 pipeline，验证断点恢复

```bash
# 重启 pipeline，观察日志
python run.py --skip-fetch --no-export -v
```

预期行为：
- fetch 阶段不会重新加载数据（已通过 `--skip-fetch` 跳过）
- quality 阶段会对未匹配的记录做幂等检查
- match 阶段只处理尚未匹配的记录，已匹配的不会重复处理

### 步骤 4：验证增量加载

```bash
# 再加载 1000 条新数据（从第 1001 行继续）
python run.py --source csv --csv-path data/kaggle --max-items 1000 --skip-match --no-export -v
```

预期行为：数据库中应有 2000 条记录，CSV 从断点继续加载。

### 查看数据库内容

```bash
# 查看各表数据量
sqlite3 data/pipeline.db -header -column "
SELECT 'mal_anime' as 表名, COUNT(*) as 行数 FROM mal_anime
UNION ALL SELECT 'quality_issues', COUNT(*) FROM quality_issues
UNION ALL SELECT 'match_result', COUNT(*) FROM match_result
UNION ALL SELECT 'checkpoint', COUNT(*) FROM checkpoint;
"

# 查看 checkpoint 状态
sqlite3 data/pipeline.db -header -column "SELECT * FROM checkpoint;"

# 查看匹配结果
sqlite3 data/pipeline.db -header -column "SELECT mal_id, wikidata_qid, match_method, confidence, status FROM match_result LIMIT 10;"

# 查看前 10 条动画数据
sqlite3 data/pipeline.db -header -column "SELECT mal_id, title, type, score, year FROM mal_anime LIMIT 10;"
```

## 数据库 Schema

```sql
-- MAL 动画元数据
mal_anime (mal_id PK, title, title_english, title_japanese, type, episodes,
           status, score, scored_by, rank, popularity, synopsis, year,
           studios JSON, genres JSON, raw_json, fetched_at)

-- 质量检查问题记录
quality_issues (id PK, mal_id FK, rule_name, detail, created_at)

-- Wikidata 实体缓存
wikidata_anime (qid PK, label_en, label_ja, mal_id_claim, description_en, fetched_at)

-- 匹配结果
match_result (id PK, mal_id FK, wikidata_qid, match_method, confidence, status, matched_at)

-- 断点恢复 checkpoint
checkpoint (stage PK, last_value, updated_at)
```

## 依赖

- Python 3.10+
- `requests` — HTTP 请求（Jikan API / Wikidata SPARQL）
- `aiohttp` — 异步 HTTP（预留）
- `rapidfuzz` — 模糊字符串匹配
- `sqlite3` — 内置，无需额外安装

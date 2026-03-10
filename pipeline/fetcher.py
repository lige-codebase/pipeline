"""数据获取模块：支持 Jikan API 在线获取和 Kaggle CSV 本地加载两种模式。"""

import csv
import json
import time
import logging
import os
import requests
from typing import Generator, Optional

from .database import get_connection, upsert_mal_anime, get_checkpoint, set_checkpoint

logger = logging.getLogger(__name__)

# ── Jikan API 配置 ──
JIKAN_BASE = "https://api.jikan.moe/v4"
PAGE_SIZE = 25
REQUEST_INTERVAL = 1.2
MAX_RETRIES = 5


# ═══════════════════════════════════════════
#  Jikan API 模式（保留原有逻辑）
# ═══════════════════════════════════════════

def _request_with_retry(url: str, params: dict | None = None) -> dict:
    """带重试和退避的 GET 请求。"""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning("Rate limited (429), waiting %ds…", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = 2 ** attempt
            logger.warning("Request failed (%s), retry in %ds…", e, wait)
            time.sleep(wait)
    raise RuntimeError("Max retries exceeded")


def fetch_anime_page(page: int) -> dict:
    """获取某一页的动画列表。"""
    time.sleep(REQUEST_INTERVAL)
    return _request_with_retry(f"{JIKAN_BASE}/anime", {"page": page, "limit": PAGE_SIZE})


def fetch_incremental_api(max_pages: int | None = None) -> Generator[dict, None, None]:
    """
    从 Jikan API 增量获取动画数据。
    利用 checkpoint 记录上次获取到的页码，下次从该页继续。
    """
    with get_connection() as conn:
        cp = get_checkpoint(conn, "fetch")
        start_page = int(cp) + 1 if cp else 1

    logger.info("API fetch starting from page %d", start_page)
    page = start_page
    pages_fetched = 0

    while True:
        if max_pages is not None and pages_fetched >= max_pages:
            logger.info("Reached max_pages limit (%d), stopping.", max_pages)
            break

        try:
            result = fetch_anime_page(page)
        except Exception as e:
            logger.error("Failed to fetch page %d: %s. Stopping fetch.", page, e)
            break

        data_list = result.get("data", [])
        pagination = result.get("pagination", {})

        if not data_list:
            logger.info("No data on page %d, fetch complete.", page)
            break

        with get_connection() as conn:
            for item in data_list:
                upsert_mal_anime(conn, item)
                yield item
            set_checkpoint(conn, "fetch", str(page))

        pages_fetched += 1
        logger.info("Page %d fetched: %d items (total pages fetched: %d)",
                     page, len(data_list), pages_fetched)

        if not pagination.get("has_next_page", False):
            logger.info("No more pages, fetch complete.")
            break

        page += 1

    logger.info("API fetch finished. Total pages this round: %d", pages_fetched)


# ═══════════════════════════════════════════
#  Kaggle CSV 模式
# ═══════════════════════════════════════════

# 常见 Kaggle MAL 数据集的字段映射
# Kaggle CSV 常见列名 → 我们 upsert_mal_anime 所需的 dict key
CSV_COLUMN_MAP = {
    # mal_id 映射（不同数据集可能用不同列名）
    "mal_id": "mal_id",
    "anime_id": "mal_id",
    "MAL_ID": "mal_id",
    "uid": "mal_id",
    # 标题
    "title": "title",
    "Name": "title",
    "name": "title",
    "title_english": "title_english",
    "English name": "title_english",
    "title_japanese": "title_japanese",
    "Japanese name": "title_japanese",
    "Other name": "title_japanese",
    # 类型
    "type": "type",
    "Type": "type",
    # 集数
    "episodes": "episodes",
    "Episodes": "episodes",
    # 状态
    "status": "status",
    "Status": "status",
    # 评分
    "score": "score",
    "Score": "score",
    "rating": "score",
    "Rating": "score",
    # 评分人数
    "scored_by": "scored_by",
    "Scored By": "scored_by",
    "Members": "scored_by",
    # 排名
    "rank": "rank",
    "Rank": "rank",
    # 人气
    "popularity": "popularity",
    "Popularity": "popularity",
    # 简介
    "synopsis": "synopsis",
    "Synopsis": "synopsis",
    "sypnopsis": "synopsis",
    # 年份
    "year": "year",
    "Start aance": "year",
    "aired_from_year": "year",
    "Premiered": "year",
    # 制作公司
    "studios": "studios",
    "Studios": "studios",
    # 类型标签
    "genres": "genres",
    "Genres": "genres",
    "genre": "genres",
    "Genre": "genres",
}


def _detect_csv_delimiter(file_path: str) -> str:
    """自动检测 CSV 分隔符。"""
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        sample = f.read(4096)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except csv.Error:
        return ","


def _normalize_csv_row(row: dict) -> dict:
    """将 CSV 行的各种列名映射为统一的 dict 格式（兼容 upsert_mal_anime）。"""
    normalized = {}

    # 先映射列名
    for csv_col, our_key in CSV_COLUMN_MAP.items():
        if csv_col in row and row[csv_col]:
            val = row[csv_col].strip() if isinstance(row[csv_col], str) else row[csv_col]
            if val == "" or val == "Unknown" or val == "UNKNOWN":
                continue
            # 避免覆盖已有更高优先级的值
            if our_key not in normalized:
                normalized[our_key] = val

    # 处理未映射的列：直接保留原列名（如果与我们字段同名）
    known_keys = {"mal_id", "title", "title_english", "title_japanese", "type",
                  "episodes", "status", "score", "scored_by", "rank", "popularity",
                  "synopsis", "year", "studios", "genres"}
    for col, val in row.items():
        if col in known_keys and col not in normalized and val:
            normalized[col] = val

    # 类型转换
    if "mal_id" in normalized:
        try:
            normalized["mal_id"] = int(float(str(normalized["mal_id"])))
        except (ValueError, TypeError):
            return {}  # mal_id 无效则跳过

    for int_field in ("episodes", "scored_by", "rank", "popularity", "year"):
        if int_field in normalized:
            try:
                normalized[int_field] = int(float(str(normalized[int_field])))
            except (ValueError, TypeError):
                normalized[int_field] = None

    if "score" in normalized:
        try:
            normalized["score"] = float(str(normalized["score"]))
        except (ValueError, TypeError):
            normalized["score"] = None

    # 处理 studios 和 genres：CSV 中可能是逗号分隔的字符串
    for list_field in ("studios", "genres"):
        if list_field in normalized and isinstance(normalized[list_field], str):
            # 如果已经是 JSON 数组格式，保持原样；否则拆分为列表
            val = normalized[list_field]
            if not val.startswith("["):
                names = [s.strip() for s in val.split(",") if s.strip()]
                # 转换为 upsert_mal_anime 期望的 [{name: ...}] 格式
                normalized[list_field] = [{"name": n} for n in names]
            else:
                try:
                    parsed = json.loads(val)
                    if isinstance(parsed, list):
                        # 如果是 [{name: ...}] 格式则保持，否则包装
                        if parsed and isinstance(parsed[0], str):
                            normalized[list_field] = [{"name": n} for n in parsed]
                        else:
                            normalized[list_field] = parsed
                except json.JSONDecodeError:
                    normalized[list_field] = []

    # 处理 year 从 Premiered 字段提取（如 "Spring 2006" → 2006）
    if "year" in normalized and isinstance(normalized["year"], str):
        import re
        match = re.search(r"(\d{4})", str(normalized["year"]))
        if match:
            normalized["year"] = int(match.group(1))
        else:
            normalized["year"] = None

    return normalized


def _find_csv_files(data_dir: str) -> list[str]:
    """在目录中查找所有 CSV 文件。"""
    csv_files = []
    for f in sorted(os.listdir(data_dir)):
        if f.lower().endswith(".csv"):
            csv_files.append(os.path.join(data_dir, f))
    return csv_files


def fetch_incremental_csv(
    csv_path: str,
    batch_size: int = 500,
    max_items: int | None = None,
) -> Generator[dict, None, None]:
    """
    从本地 CSV 文件增量加载 MAL 数据。

    增量策略：使用 checkpoint 记录已处理到的行偏移量（文件名:行号），
    下次运行从断点继续。

    Args:
        csv_path: CSV 文件路径（单个文件或包含 CSV 的目录）
        batch_size: 每批写入数据库的条目数
        max_items: 本轮最多加载的条目数（None = 全部）

    Yields:
        每条动画的 dict
    """
    # 确定要加载的文件列表
    if os.path.isdir(csv_path):
        csv_files = _find_csv_files(csv_path)
        if not csv_files:
            logger.error("No CSV files found in %s", csv_path)
            return
        logger.info("Found %d CSV files in %s", len(csv_files), csv_path)
    else:
        csv_files = [csv_path]

    # 读取 checkpoint：格式 "filename:line_number"
    with get_connection() as conn:
        cp = get_checkpoint(conn, "fetch_csv")
    last_file = ""
    last_line = 0
    if cp:
        parts = cp.rsplit(":", 1)
        if len(parts) == 2:
            last_file = parts[0]
            last_line = int(parts[1])
        logger.info("Resuming CSV load from %s line %d", last_file, last_line)

    total_loaded = 0
    skipped = 0

    for csv_file in csv_files:
        filename = os.path.basename(csv_file)

        # 跳过已完全处理的文件
        if last_file and filename < last_file:
            logger.debug("Skipping already processed file: %s", filename)
            continue

        delimiter = _detect_csv_delimiter(csv_file)
        logger.info("Loading %s (delimiter=%r)", filename, delimiter)

        start_line = last_line if filename == last_file else 0
        batch = []
        line_num = 0

        with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter=delimiter)

            for line_num_raw, row in enumerate(reader, start=1):
                if filename == last_file and line_num_raw <= start_line:
                    continue

                line_num = line_num_raw
                normalized = _normalize_csv_row(row)

                if not normalized or "mal_id" not in normalized:
                    skipped += 1
                    continue

                batch.append((normalized, line_num))

                if len(batch) >= batch_size:
                    # 批量写入
                    with get_connection() as conn:
                        for item, ln in batch:
                            upsert_mal_anime(conn, item)
                            yield item
                            total_loaded += 1
                            if max_items and total_loaded >= max_items:
                                set_checkpoint(conn, "fetch_csv", f"{filename}:{ln}")
                                logger.info("Reached max_items limit (%d), stopping.", max_items)
                                return
                        set_checkpoint(conn, "fetch_csv", f"{filename}:{ln}")
                    batch = []

        # 写入最后一批
        if batch:
            with get_connection() as conn:
                for item, ln in batch:
                    upsert_mal_anime(conn, item)
                    yield item
                    total_loaded += 1
                    if max_items and total_loaded >= max_items:
                        set_checkpoint(conn, "fetch_csv", f"{filename}:{ln}")
                        logger.info("Reached max_items limit (%d), stopping.", max_items)
                        return
                set_checkpoint(conn, "fetch_csv", f"{filename}:{ln}")

        logger.info("Finished loading %s: %d items loaded", filename, line_num)

        # 重置 last_file/last_line，后续文件从头开始
        last_file = ""
        last_line = 0

    logger.info("CSV fetch finished. Total loaded: %d, skipped: %d", total_loaded, skipped)


# ═══════════════════════════════════════════
#  统一入口
# ═══════════════════════════════════════════

def fetch_incremental(
    max_pages: int | None = None,
    source: str = "api",
    csv_path: Optional[str] = None,
    max_items: int | None = None,
) -> Generator[dict, None, None]:
    """
    统一的增量获取入口。

    Args:
        max_pages: API 模式下最多获取页数
        source: 数据源类型 "api" 或 "csv"
        csv_path: CSV 模式下的文件/目录路径
        max_items: CSV 模式下最多加载条目数
    """
    if source == "csv":
        if not csv_path:
            raise ValueError("csv_path is required when source='csv'")
        yield from fetch_incremental_csv(csv_path, max_items=max_items)
    else:
        yield from fetch_incremental_api(max_pages=max_pages)

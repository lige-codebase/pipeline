"""Wikidata 跨源匹配模块：精确 ID 匹配 + 模糊标题匹配。"""

import time
import logging
import requests
from rapidfuzz import fuzz
from typing import Optional

from .database import (
    get_connection, set_checkpoint, get_checkpoint,
    get_unmatched_passed_ids,
)

logger = logging.getLogger(__name__)

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
REQUEST_INTERVAL = 2.0  # Wikidata SPARQL 端点也有限流


def _sparql_query(query: str) -> list[dict]:
    """执行 SPARQL 查询并返回 bindings。"""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "AnimePipeline/1.0 (data-engineering-project)",
    }
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            time.sleep(REQUEST_INTERVAL)
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={"query": query},
                headers=headers,
                timeout=90,
            )
            if resp.status_code in (429, 403):
                wait = 15 * (attempt + 1)
                logger.warning("Wikidata %d, waiting %ds (attempt %d/%d)",
                               resp.status_code, wait, attempt + 1, max_attempts)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()["results"]["bindings"]
        except requests.exceptions.HTTPError:
            raise
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            wait = 10 * (attempt + 1)
            logger.warning("SPARQL query failed (%s), retry in %ds…", e, wait)
            time.sleep(wait)
    logger.warning("SPARQL query exhausted all retries, returning empty.")
    return []


def fetch_wikidata_mal_mappings(mal_ids: list[int]) -> dict[int, dict]:
    """
    批量查询 Wikidata 中有 P4086 (MAL anime ID) 的实体。

    按批次查询以避免 SPARQL 查询超时。

    Returns:
        {mal_id: {"qid": "Q...", "label_en": "...", "label_ja": "...", "description_en": "..."}}
    """
    result = {}
    batch_size = 50  # SPARQL VALUES 子句每批

    for i in range(0, len(mal_ids), batch_size):
        batch = mal_ids[i:i + batch_size]
        values_str = " ".join(f'"{mid}"' for mid in batch)

        query = f"""
        SELECT ?item ?itemLabel ?malId ?jaLabel ?description WHERE {{
          VALUES ?malId {{ {values_str} }}
          ?item wdt:P4086 ?malId .
          OPTIONAL {{ ?item rdfs:label ?jaLabel . FILTER(LANG(?jaLabel) = "ja") }}
          OPTIONAL {{ ?item schema:description ?description . FILTER(LANG(?description) = "en") }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        """
        try:
            bindings = _sparql_query(query)
            for b in bindings:
                mal_id = int(b["malId"]["value"])
                qid = b["item"]["value"].rsplit("/", 1)[-1]
                result[mal_id] = {
                    "qid": qid,
                    "label_en": b.get("itemLabel", {}).get("value", ""),
                    "label_ja": b.get("jaLabel", {}).get("value", ""),
                    "description_en": b.get("description", {}).get("value", ""),
                }
            logger.info("Wikidata batch %d-%d: found %d mappings",
                        i, i + len(batch), len(bindings))
        except Exception as e:
            logger.error("Wikidata batch %d-%d failed: %s", i, i + len(batch), e)

    return result


def fetch_wikidata_anime_labels(limit: int = 1000) -> list[dict]:
    """
    获取 Wikidata 中有 P4086 但无匹配的动画实体标签（用于模糊匹配回退）。
    使用更轻量的查询：只取 anime television series (Q63952888) 实体。
    分页获取以避免超时。
    """
    all_results = []
    offset = 0
    page_size = 500

    while offset < limit:
        batch_limit = min(page_size, limit - offset)
        query = f"""
        SELECT ?item ?itemLabel ?jaLabel WHERE {{
          ?item wdt:P31 wd:Q63952888 .
          FILTER NOT EXISTS {{ ?item wdt:P4086 ?malId }}
          OPTIONAL {{ ?item rdfs:label ?jaLabel . FILTER(LANG(?jaLabel) = "ja") }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        LIMIT {batch_limit} OFFSET {offset}
        """
        try:
            bindings = _sparql_query(query)
        except Exception as e:
            logger.warning("Failed to fetch Wikidata labels at offset %d: %s", offset, e)
            break

        if not bindings:
            break

        for b in bindings:
            qid = b["item"]["value"].rsplit("/", 1)[-1]
            all_results.append({
                "qid": qid,
                "label_en": b.get("itemLabel", {}).get("value", ""),
                "label_ja": b.get("jaLabel", {}).get("value", ""),
                "mal_id_claim": None,
            })

        offset += len(bindings)
        if len(bindings) < batch_limit:
            break

    logger.info("Fetched %d Wikidata anime labels for fuzzy matching", len(all_results))
    return all_results


def match_exact_id(mal_ids: list[int]) -> dict[int, dict]:
    """
    高置信度匹配：通过 Wikidata P4086 属性精确匹配。
    
    Returns:
        {mal_id: {"qid": ..., "method": "exact_id", "confidence": 1.0, ...}}
    """
    mappings = fetch_wikidata_mal_mappings(mal_ids)
    result = {}
    for mal_id, info in mappings.items():
        result[mal_id] = {
            **info,
            "method": "exact_id",
            "confidence": 1.0,
        }
    return result


def match_fuzzy_title(
    unmatched_mal_ids: list[int],
    wikidata_entities: list[dict],
    threshold: float = 0.85,
) -> dict[int, dict]:
    """
    模糊匹配：通过标题相似度匹配。

    同时比较英文标题和日文标题，取最高分。

    Returns:
        {mal_id: {"qid": ..., "method": "fuzzy_title", "confidence": float, ...}}
    """
    if not wikidata_entities:
        return {}

    result = {}

    with get_connection() as conn:
        for mal_id in unmatched_mal_ids:
            row = conn.execute(
                "SELECT title, title_english, title_japanese FROM mal_anime WHERE mal_id = ?",
                (mal_id,),
            ).fetchone()
            if not row:
                continue

            mal_titles = [
                t for t in [row["title"], row["title_english"], row["title_japanese"]]
                if t and t.strip()
            ]
            if not mal_titles:
                continue

            best_score = 0.0
            best_entity = None

            for entity in wikidata_entities:
                # 跳过已有 MAL ID 声明的（它们应在 exact_id 阶段匹配）
                if entity.get("mal_id_claim") is not None:
                    continue

                wiki_titles = [
                    t for t in [entity.get("label_en", ""), entity.get("label_ja", "")]
                    if t and t.strip()
                ]
                if not wiki_titles:
                    continue

                for mt in mal_titles:
                    for wt in wiki_titles:
                        score = fuzz.token_sort_ratio(mt.lower(), wt.lower()) / 100.0
                        if score > best_score:
                            best_score = score
                            best_entity = entity

            if best_score >= threshold and best_entity:
                result[mal_id] = {
                    "qid": best_entity["qid"],
                    "label_en": best_entity.get("label_en", ""),
                    "label_ja": best_entity.get("label_ja", ""),
                    "description_en": "",
                    "method": "fuzzy_title",
                    "confidence": round(best_score, 4),
                }

    logger.info("Fuzzy match: %d matched out of %d candidates", len(result), len(unmatched_mal_ids))
    return result


def save_match_results(matches: dict[int, dict]):
    """将匹配结果写入数据库。"""
    with get_connection() as conn:
        for mal_id, info in matches.items():
            confidence = info["confidence"]
            status = "matched" if confidence >= 0.85 else "review"
            conn.execute("""
                INSERT INTO match_result (mal_id, wikidata_qid, match_method, confidence, status)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, (mal_id, info["qid"], info["method"], confidence, status))

            # 缓存 Wikidata 实体信息
            conn.execute("""
                INSERT INTO wikidata_anime (qid, label_en, label_ja, mal_id_claim, description_en)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(qid) DO UPDATE SET
                    label_en=excluded.label_en, label_ja=excluded.label_ja,
                    mal_id_claim=excluded.mal_id_claim, description_en=excluded.description_en
            """, (
                info["qid"], info.get("label_en", ""), info.get("label_ja", ""),
                mal_id if info["method"] == "exact_id" else None,
                info.get("description_en", ""),
            ))


def save_unmatched(mal_ids: list[int]):
    """将未匹配的记录写入 match_result 表，标记为 unmatched。"""
    with get_connection() as conn:
        for mal_id in mal_ids:
            conn.execute("""
                INSERT INTO match_result (mal_id, wikidata_qid, match_method, confidence, status)
                VALUES (?, NULL, 'none', 0.0, 'unmatched')
                ON CONFLICT DO NOTHING
            """, (mal_id,))


def run_matching(mal_ids: list[int] | None = None):
    """
    对给定的 MAL ID 执行跨源匹配流程，支持被 kill 后断点恢复。

    断点恢复策略：
    - 精确匹配每批 SPARQL 完成后立即保存结果
    - 重启时通过 get_unmatched_passed_ids() 自动排除已匹配的 ID
    - checkpoint 记录当前匹配子阶段（exact / fuzzy / done）

    流程:
    1. 精确 ID 匹配 (P4086)
    2. 模糊标题匹配（对精确未匹配到的）
    3. 剩余标记为 unmatched
    """
    with get_connection() as conn:
        if mal_ids is None:
            mal_ids = get_unmatched_passed_ids(conn)
        match_cp = get_checkpoint(conn, "match_stage")

    if not mal_ids:
        logger.info("No IDs to match.")
        return {"exact": {}, "fuzzy": {}, "unmatched": []}

    logger.info("Matching %d MAL IDs…", len(mal_ids))

    exact_matches = {}
    fuzzy_matches = {}

    # Step 1: 精确 ID 匹配（跳过已在 fuzzy 阶段的情况）
    if match_cp != "fuzzy":
        logger.info("Step 1: Exact ID matching…")
        exact_matches = match_exact_id(mal_ids)
        save_match_results(exact_matches)
        logger.info("Exact ID matched: %d", len(exact_matches))
        with get_connection() as conn:
            set_checkpoint(conn, "match_stage", "fuzzy")

    # 重新获取未匹配的 ID（精确匹配后可能已减少）
    with get_connection() as conn:
        remaining = get_unmatched_passed_ids(conn)

    # Step 2: 模糊标题匹配
    if remaining:
        logger.info("Step 2: Fuzzy title matching for %d remaining…", len(remaining))
        wikidata_entities = fetch_wikidata_anime_labels(limit=1000)
        fuzzy_matches = match_fuzzy_title(remaining, wikidata_entities)
        save_match_results(fuzzy_matches)
        logger.info("Fuzzy title matched: %d", len(fuzzy_matches))

    # Step 3: 未匹配
    with get_connection() as conn:
        still_unmatched = get_unmatched_passed_ids(conn)
    save_unmatched(still_unmatched)

    # 标记匹配阶段完成，清除子阶段 checkpoint
    with get_connection() as conn:
        set_checkpoint(conn, "match", str(max(mal_ids)))
        set_checkpoint(conn, "match_stage", "done")

    return {"exact": exact_matches, "fuzzy": fuzzy_matches, "unmatched": still_unmatched}

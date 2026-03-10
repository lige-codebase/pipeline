"""数据导出模块：将匹配结果导出为 JSON Lines 格式。"""

import json
import logging
import os
from typing import Optional

from .database import get_connection

logger = logging.getLogger(__name__)


def export_jsonl(
    output_path: Optional[str] = None,
    status_filter: Optional[str] = None,
) -> str:
    """
    导出匹配结果为 JSON Lines 文件。

    Args:
        output_path: 输出文件路径，默认在 data/ 目录下。
        status_filter: 按 status 过滤 ('matched'/'unmatched'/'review')，None 表示全部。

    Returns:
        输出文件路径。
    """
    if output_path is None:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
        os.makedirs(data_dir, exist_ok=True)
        output_path = os.path.join(data_dir, "export.jsonl")

    count = 0
    with get_connection() as conn:
        query = """
            SELECT
                m.mal_id, m.title, m.title_english, m.title_japanese,
                m.type, m.episodes, m.status AS anime_status, m.score,
                m.year, m.studios, m.genres,
                r.wikidata_qid, r.match_method, r.confidence,
                r.status AS match_status, r.matched_at,
                w.label_en AS wiki_label_en, w.label_ja AS wiki_label_ja,
                w.description_en AS wiki_description
            FROM match_result r
            JOIN mal_anime m ON m.mal_id = r.mal_id
            LEFT JOIN wikidata_anime w ON w.qid = r.wikidata_qid
        """
        params = []
        if status_filter:
            query += " WHERE r.status = ?"
            params.append(status_filter)
        query += " ORDER BY m.mal_id"

        rows = conn.execute(query, params).fetchall()

        with open(output_path, "w", encoding="utf-8") as f:
            for row in rows:
                d = dict(row)
                # 解析 JSON 字段
                for field in ("studios", "genres"):
                    if d.get(field):
                        try:
                            d[field] = json.loads(d[field])
                        except (json.JSONDecodeError, TypeError):
                            pass

                record = {
                    "mal_metadata": {
                        "mal_id": d["mal_id"],
                        "title": d["title"],
                        "title_english": d["title_english"],
                        "title_japanese": d["title_japanese"],
                        "type": d["type"],
                        "episodes": d["episodes"],
                        "status": d["anime_status"],
                        "score": d["score"],
                        "year": d["year"],
                        "studios": d["studios"],
                        "genres": d["genres"],
                    },
                    "wikidata_metadata": {
                        "qid": d["wikidata_qid"],
                        "label_en": d["wiki_label_en"],
                        "label_ja": d["wiki_label_ja"],
                        "description": d["wiki_description"],
                    } if d["wikidata_qid"] else None,
                    "match": {
                        "method": d["match_method"],
                        "confidence": d["confidence"],
                        "status": d["match_status"],
                        "matched_at": d["matched_at"],
                    },
                }
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                count += 1

    logger.info("Exported %d records to %s", count, output_path)
    return output_path

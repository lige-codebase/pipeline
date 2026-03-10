"""数据库模块：SQLite schema 定义、连接管理、CRUD 操作。"""

import sqlite3
import json
import os
from contextlib import contextmanager
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "pipeline.db")


def _ensure_dir():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


@contextmanager
def get_connection(db_path: Optional[str] = None):
    path = db_path or DB_PATH
    _ensure_dir()
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Optional[str] = None):
    """创建所有表。"""
    with get_connection(db_path) as conn:
        conn.executescript("""
        -- MAL 原始元数据
        CREATE TABLE IF NOT EXISTS mal_anime (
            mal_id          INTEGER PRIMARY KEY,
            title           TEXT,
            title_english   TEXT,
            title_japanese  TEXT,
            type            TEXT,
            episodes        INTEGER,
            status          TEXT,
            score           REAL,
            scored_by       INTEGER,
            rank            INTEGER,
            popularity      INTEGER,
            synopsis        TEXT,
            year            INTEGER,
            studios         TEXT,       -- JSON array
            genres          TEXT,       -- JSON array
            raw_json        TEXT,       -- 完整原始 JSON
            fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 质量检查结果
        CREATE TABLE IF NOT EXISTS quality_issues (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            mal_id          INTEGER NOT NULL,
            rule_name       TEXT NOT NULL,
            detail          TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (mal_id) REFERENCES mal_anime(mal_id)
        );

        -- Wikidata 实体缓存
        CREATE TABLE IF NOT EXISTS wikidata_anime (
            qid             TEXT PRIMARY KEY,   -- e.g. Q215380
            label_en        TEXT,
            label_ja        TEXT,
            mal_id_claim    INTEGER,            -- P4086 值
            description_en  TEXT,
            fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 匹配结果
        CREATE TABLE IF NOT EXISTS match_result (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            mal_id          INTEGER NOT NULL,
            wikidata_qid    TEXT,
            match_method    TEXT NOT NULL,       -- 'exact_id' / 'fuzzy_title'
            confidence      REAL NOT NULL,       -- 0.0 ~ 1.0
            status          TEXT NOT NULL DEFAULT 'matched',
                            -- 'matched' / 'unmatched' / 'review'
            matched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (mal_id) REFERENCES mal_anime(mal_id)
        );
        CREATE INDEX IF NOT EXISTS idx_match_mal ON match_result(mal_id);
        CREATE INDEX IF NOT EXISTS idx_match_status ON match_result(status);

        -- Pipeline 检查点（断点恢复）
        CREATE TABLE IF NOT EXISTS checkpoint (
            stage           TEXT PRIMARY KEY,    -- 'fetch' / 'quality' / 'match'
            last_value      TEXT NOT NULL,       -- 阶段相关的进度值
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)


# ── checkpoint helpers ──

def get_checkpoint(conn: sqlite3.Connection, stage: str) -> Optional[str]:
    row = conn.execute(
        "SELECT last_value FROM checkpoint WHERE stage = ?", (stage,)
    ).fetchone()
    return row["last_value"] if row else None


def set_checkpoint(conn: sqlite3.Connection, stage: str, value: str):
    conn.execute(
        "INSERT INTO checkpoint(stage, last_value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
        "ON CONFLICT(stage) DO UPDATE SET last_value=excluded.last_value, updated_at=excluded.updated_at",
        (stage, value),
    )


# ── mal_anime helpers ──

def upsert_mal_anime(conn: sqlite3.Connection, item: dict):
    """插入或更新一条 MAL 动画记录。"""
    studios = json.dumps([s.get("name", "") for s in (item.get("studios") or [])], ensure_ascii=False)
    genres = json.dumps([g.get("name", "") for g in (item.get("genres") or [])], ensure_ascii=False)
    conn.execute("""
        INSERT INTO mal_anime (mal_id, title, title_english, title_japanese,
            type, episodes, status, score, scored_by, rank, popularity,
            synopsis, year, studios, genres, raw_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(mal_id) DO UPDATE SET
            title=excluded.title, title_english=excluded.title_english,
            title_japanese=excluded.title_japanese, type=excluded.type,
            episodes=excluded.episodes, status=excluded.status,
            score=excluded.score, scored_by=excluded.scored_by,
            rank=excluded.rank, popularity=excluded.popularity,
            synopsis=excluded.synopsis, year=excluded.year,
            studios=excluded.studios, genres=excluded.genres,
            raw_json=excluded.raw_json, fetched_at=CURRENT_TIMESTAMP
    """, (
        item["mal_id"], item.get("title"), item.get("title_english"),
        item.get("title_japanese"), item.get("type"), item.get("episodes"),
        item.get("status"), item.get("score"), item.get("scored_by"),
        item.get("rank"), item.get("popularity"), item.get("synopsis"),
        item.get("year"),
        studios, genres,
        json.dumps(item, ensure_ascii=False, default=str),
    ))


def get_mal_ids_without_match(conn: sqlite3.Connection) -> list[int]:
    """获取已入库但尚未匹配的 MAL ID。"""
    rows = conn.execute("""
        SELECT m.mal_id FROM mal_anime m
        LEFT JOIN match_result r ON m.mal_id = r.mal_id
        WHERE r.id IS NULL
        ORDER BY m.mal_id
    """).fetchall()
    return [r["mal_id"] for r in rows]


def get_mal_ids_passed_quality(conn: sqlite3.Connection) -> list[int]:
    """获取通过质量检查（无 blocking 问题）的 MAL ID。"""
    rows = conn.execute("""
        SELECT m.mal_id FROM mal_anime m
        LEFT JOIN quality_issues q ON m.mal_id = q.mal_id
        WHERE q.id IS NULL
        ORDER BY m.mal_id
    """).fetchall()
    return [r["mal_id"] for r in rows]


def get_unmatched_passed_ids(conn: sqlite3.Connection) -> list[int]:
    """获取通过质量检查且尚未匹配的 MAL ID。"""
    rows = conn.execute("""
        SELECT m.mal_id FROM mal_anime m
        LEFT JOIN quality_issues q ON m.mal_id = q.mal_id
        LEFT JOIN match_result r ON m.mal_id = r.mal_id
        WHERE q.id IS NULL AND r.id IS NULL
        ORDER BY m.mal_id
    """).fetchall()
    return [r["mal_id"] for r in rows]

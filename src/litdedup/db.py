from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

from litdedup.parsers import file_sha256


def utc_now() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="seconds")


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def initialize_database(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS source_files (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE,
            format TEXT NOT NULL,
            profile_name TEXT NOT NULL,
            file_hash TEXT NOT NULL,
            imported_at TEXT NOT NULL,
            encoding_used TEXT,
            encoding_source TEXT,
            record_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY,
            source_file_id INTEGER NOT NULL REFERENCES source_files(id) ON DELETE CASCADE,
            source_db TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            record_type TEXT,
            record_type_category TEXT,
            title TEXT,
            abstract TEXT,
            authors_json TEXT NOT NULL DEFAULT '[]',
            author_surnames_json TEXT NOT NULL DEFAULT '[]',
            journal TEXT,
            year INTEGER,
            volume TEXT,
            issue TEXT,
            start_page TEXT,
            end_page TEXT,
            doi TEXT,
            pmid TEXT,
            pmcid TEXT,
            keywords_json TEXT NOT NULL DEFAULT '[]',
            language TEXT,
            url TEXT,
            title_norm TEXT,
            journal_norm TEXT,
            first_author_norm TEXT,
            page_key TEXT,
            completeness_score REAL NOT NULL DEFAULT 0,
            warnings_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            UNIQUE(source_file_id, source_record_id)
        );

        CREATE TABLE IF NOT EXISTS record_fields_raw (
            id INTEGER PRIMARY KEY,
            record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
            tag TEXT NOT NULL,
            value TEXT NOT NULL,
            position INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS clusters (
            id INTEGER PRIMARY KEY,
            canonical_record_id INTEGER REFERENCES records(id) ON DELETE SET NULL,
            stage TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cluster_members (
            cluster_id INTEGER NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
            record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
            merged_stage TEXT NOT NULL,
            PRIMARY KEY(cluster_id, record_id),
            UNIQUE(record_id)
        );

        CREATE TABLE IF NOT EXISTS candidate_pairs (
            id INTEGER PRIMARY KEY,
            pair_key TEXT NOT NULL UNIQUE,
            left_record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
            right_record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
            left_cluster_id INTEGER NOT NULL,
            right_cluster_id INTEGER NOT NULL,
            title_score REAL NOT NULL,
            author_score REAL NOT NULL,
            author_overlap REAL NOT NULL,
            first_author_match INTEGER NOT NULL,
            journal_match INTEGER NOT NULL,
            year_diff INTEGER NOT NULL,
            page_match INTEGER NOT NULL,
            doi_conflict INTEGER NOT NULL,
            pmid_conflict INTEGER NOT NULL,
            type_conflict INTEGER NOT NULL,
            composite_score REAL NOT NULL,
            recommendation TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            resolved_at TEXT
        );

        CREATE TABLE IF NOT EXISTS manual_reviews (
            id INTEGER PRIMARY KEY,
            pair_id INTEGER NOT NULL UNIQUE REFERENCES candidate_pairs(id) ON DELETE CASCADE,
            pair_key TEXT NOT NULL,
            left_record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
            right_record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
            decision TEXT NOT NULL,
            preferred_keeper_record_id INTEGER REFERENCES records(id) ON DELETE SET NULL,
            notes TEXT,
            decided_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS stage_metrics (
            stage TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_records_doi ON records(doi);
        CREATE INDEX IF NOT EXISTS idx_records_pmid ON records(pmid);
        CREATE INDEX IF NOT EXISTS idx_records_pmcid ON records(pmcid);
        CREATE INDEX IF NOT EXISTS idx_records_year ON records(year);
        CREATE INDEX IF NOT EXISTS idx_records_title_norm ON records(title_norm);
        CREATE INDEX IF NOT EXISTS idx_records_first_author_norm ON records(first_author_norm);
        CREATE INDEX IF NOT EXISTS idx_records_journal_norm ON records(journal_norm);
        CREATE INDEX IF NOT EXISTS idx_source_files_hash ON source_files(file_hash);
        """
    )
    ensure_source_file_columns(conn)
    conn.commit()


def ensure_source_file_columns(conn: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(source_files)").fetchall()
    }
    if "encoding_used" not in columns:
        conn.execute("ALTER TABLE source_files ADD COLUMN encoding_used TEXT")
    if "encoding_source" not in columns:
        conn.execute("ALTER TABLE source_files ADD COLUMN encoding_source TEXT")


def upsert_stage_metrics(conn: sqlite3.Connection, stage: str, payload: dict) -> None:
    conn.execute(
        """
        INSERT INTO stage_metrics(stage, payload_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(stage) DO UPDATE SET payload_json = excluded.payload_json, updated_at = excluded.updated_at
        """,
        (stage, json.dumps(payload, ensure_ascii=False, sort_keys=True), utc_now()),
    )
    conn.commit()


def get_stage_metrics(conn: sqlite3.Connection, stage: str) -> dict:
    row = conn.execute("SELECT payload_json FROM stage_metrics WHERE stage = ?", (stage,)).fetchone()
    if not row:
        return {}
    return json.loads(row["payload_json"])


def clear_dedup_state(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM manual_reviews")
    conn.execute("DELETE FROM candidate_pairs")
    conn.execute("DELETE FROM cluster_members")
    conn.execute("DELETE FROM clusters")
    conn.execute("DELETE FROM stage_metrics")
    conn.commit()


def existing_source_by_hash(conn: sqlite3.Connection, file_hash: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM source_files WHERE file_hash = ? ORDER BY id LIMIT 1",
        (file_hash,),
    ).fetchone()


def existing_source_by_path(conn: sqlite3.Connection, path: Path) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM source_files WHERE path = ?",
        (str(path.resolve()),),
    ).fetchone()


def delete_source_file(conn: sqlite3.Connection, path: Path) -> None:
    conn.execute("DELETE FROM source_files WHERE path = ?", (str(path.resolve()),))
    conn.commit()


def register_source_file(
    conn: sqlite3.Connection,
    path: Path,
    profile_name: str,
    fmt: str,
    *,
    encoding_used: str | None = None,
    encoding_source: str | None = None,
) -> tuple[int, str]:
    file_hash = file_sha256(path)
    conn.execute(
        """
        INSERT INTO source_files(path, format, profile_name, file_hash, imported_at, encoding_used, encoding_source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (str(path.resolve()), fmt, profile_name, file_hash, utc_now(), encoding_used, encoding_source),
    )
    source_file_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.commit()
    return source_file_id, file_hash


def finalize_source_file(
    conn: sqlite3.Connection,
    source_file_id: int,
    record_count: int,
    *,
    encoding_used: str | None = None,
    encoding_source: str | None = None,
) -> None:
    conn.execute(
        "UPDATE source_files SET record_count = ?, encoding_used = COALESCE(?, encoding_used), encoding_source = COALESCE(?, encoding_source) WHERE id = ?",
        (record_count, encoding_used, encoding_source, source_file_id),
    )
    conn.commit()


def insert_record(
    conn: sqlite3.Connection,
    source_file_id: int,
    normalized_record: dict[str, object],
    raw_entries: Iterable[tuple[int, str, str]],
) -> int:
    conn.execute(
        """
        INSERT INTO records (
            source_file_id, source_db, source_file, source_record_id,
            record_type, record_type_category, title, abstract,
            authors_json, author_surnames_json, journal, year, volume, issue,
            start_page, end_page, doi, pmid, pmcid, keywords_json, language, url,
            title_norm, journal_norm, first_author_norm, page_key, completeness_score,
            warnings_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_file_id,
            normalized_record["source_db"],
            normalized_record["source_file"],
            normalized_record["source_record_id"],
            normalized_record["record_type"],
            normalized_record["record_type_category"],
            normalized_record["title"],
            normalized_record["abstract"],
            json.dumps(normalized_record["authors"], ensure_ascii=False),
            json.dumps(normalized_record["author_surnames"], ensure_ascii=False),
            normalized_record["journal"],
            normalized_record["year"],
            normalized_record["volume"],
            normalized_record["issue"],
            normalized_record["start_page"],
            normalized_record["end_page"],
            normalized_record["doi"],
            normalized_record["pmid"],
            normalized_record["pmcid"],
            json.dumps(normalized_record["keywords"], ensure_ascii=False),
            normalized_record["language"],
            normalized_record["url"],
            normalized_record["title_norm"],
            normalized_record["journal_norm"],
            normalized_record["first_author_norm"],
            normalized_record["page_key"],
            normalized_record["completeness_score"],
            json.dumps(normalized_record["warnings"], ensure_ascii=False),
            utc_now(),
        ),
    )
    record_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.executemany(
        """
        INSERT INTO record_fields_raw(record_id, tag, value, position)
        VALUES (?, ?, ?, ?)
        """,
        [(record_id, tag, value, position) for position, tag, value in raw_entries],
    )
    return record_id


def fetch_all_records(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM records ORDER BY id").fetchall()


def fetch_record_map(conn: sqlite3.Connection, record_ids: Iterable[int] | None = None) -> dict[int, sqlite3.Row]:
    if record_ids is None:
        rows = fetch_all_records(conn)
    else:
        ids = list(record_ids)
        if not ids:
            return {}
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"SELECT * FROM records WHERE id IN ({placeholders})",
            ids,
        ).fetchall()
    return {int(row["id"]): row for row in rows}


def fetch_cluster_members(conn: sqlite3.Connection) -> dict[int, list[int]]:
    rows = conn.execute(
        "SELECT cluster_id, record_id FROM cluster_members ORDER BY cluster_id, record_id"
    ).fetchall()
    clusters: dict[int, list[int]] = {}
    for row in rows:
        clusters.setdefault(int(row["cluster_id"]), []).append(int(row["record_id"]))
    return clusters


def fetch_cluster_representatives(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT clusters.id AS cluster_id, records.*
        FROM clusters
        JOIN records ON records.id = clusters.canonical_record_id
        WHERE clusters.status = 'active'
        ORDER BY clusters.id
        """
    ).fetchall()


def rebuild_clusters(
    conn: sqlite3.Connection,
    record_groups: list[list[int]],
    canonical_picker: callable,
    stage: str,
) -> dict[int, int]:
    conn.execute("DELETE FROM cluster_members")
    conn.execute("DELETE FROM clusters")
    record_to_cluster: dict[int, int] = {}
    for group in sorted((sorted(set(group)) for group in record_groups), key=lambda g: g[0]):
        canonical_record_id = canonical_picker(group)
        conn.execute(
            "INSERT INTO clusters(canonical_record_id, stage, status, created_at) VALUES (?, ?, 'active', ?)",
            (canonical_record_id, stage, utc_now()),
        )
        cluster_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.executemany(
            "INSERT INTO cluster_members(cluster_id, record_id, merged_stage) VALUES (?, ?, ?)",
            [(cluster_id, record_id, stage) for record_id in group],
        )
        for record_id in group:
            record_to_cluster[record_id] = cluster_id
    conn.commit()
    return record_to_cluster


def pending_candidate_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM candidate_pairs cp
        JOIN cluster_members left_cm ON left_cm.record_id = cp.left_record_id
        JOIN cluster_members right_cm ON right_cm.record_id = cp.right_record_id
        WHERE cp.status = 'pending'
          AND left_cm.cluster_id != right_cm.cluster_id
        """
    ).fetchone()
    return int(row["n"])


def write_candidate_pairs(conn: sqlite3.Connection, rows: list[dict]) -> None:
    conn.execute("DELETE FROM candidate_pairs")
    conn.execute("DELETE FROM manual_reviews")
    conn.executemany(
        """
        INSERT INTO candidate_pairs(
            pair_key, left_record_id, right_record_id, left_cluster_id, right_cluster_id,
            title_score, author_score, author_overlap, first_author_match, journal_match,
            year_diff, page_match, doi_conflict, pmid_conflict, type_conflict,
            composite_score, recommendation, status, created_at, resolved_at
        ) VALUES (
            :pair_key, :left_record_id, :right_record_id, :left_cluster_id, :right_cluster_id,
            :title_score, :author_score, :author_overlap, :first_author_match, :journal_match,
            :year_diff, :page_match, :doi_conflict, :pmid_conflict, :type_conflict,
            :composite_score, :recommendation, :status, :created_at, :resolved_at
        )
        """,
        rows,
    )
    conn.commit()


@contextmanager
def transactional(conn: sqlite3.Connection):
    try:
        yield
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from litdedup.config import AppConfig, config_summary
from litdedup.db import get_stage_metrics


KEY_FIELDS = {
    "title": "title",
    "authors": "authors_json",
    "year": "year",
    "abstract": "abstract",
    "doi": "doi",
    "pmid": "pmid",
    "journal": "journal",
    "pages": "start_page",
}


def build_report_payload(conn: sqlite3.Connection, config: AppConfig) -> dict[str, Any]:
    total_records = scalar(conn, "SELECT COUNT(*) FROM records")
    total_files = scalar(conn, "SELECT COUNT(*) FROM source_files")
    rows = conn.execute(
        """
        SELECT title, authors_json, year, abstract, doi, pmid, journal, start_page
        FROM records
        """
    ).fetchall()
    source_breakdown = {
        row["source_db"]: row["n"]
        for row in conn.execute(
            "SELECT source_db, COUNT(*) AS n FROM records GROUP BY source_db ORDER BY source_db"
        ).fetchall()
    }
    missing = {}
    for label, column in KEY_FIELDS.items():
        if label == "authors":
            missing_count = sum(1 for row in rows if not json.loads(row["authors_json"] or "[]"))
        else:
            missing_count = sum(1 for row in rows if not row[column])
        missing[label] = {"missing": missing_count, "total": total_records}
    record_types = {
        row["record_type_category"]: row["n"]
        for row in conn.execute(
            """
            SELECT record_type_category, COUNT(*) AS n
            FROM records
            GROUP BY record_type_category
            ORDER BY n DESC, record_type_category ASC
            """
        ).fetchall()
    }
    parser_profile_summary = config_summary(config)
    parser_profile_summary["observed_raw_tags"] = {
        row["tag"]: row["n"]
        for row in conn.execute(
            """
            SELECT tag, COUNT(*) AS n
            FROM record_fields_raw
            WHERE tag IN ('AB', 'N2', 'PMID', 'DO', 'AID', 'LID', 'T1', 'TI', 'A1', 'AU', 'Y1', 'PY')
            GROUP BY tag
            ORDER BY tag
            """
        ).fetchall()
    }
    encoding_by_file = [
        {
            "path": row["path"],
            "profile_name": row["profile_name"],
            "encoding_used": row["encoding_used"] or "",
            "encoding_source": row["encoding_source"] or "",
            "record_count": row["record_count"],
        }
        for row in conn.execute(
            """
            SELECT path, profile_name, encoding_used, encoding_source, record_count
            FROM source_files
            ORDER BY id
            """
        ).fetchall()
    ]
    encoding_summary = {
        f"{(row['encoding_used'] or 'unknown')}|{(row['encoding_source'] or 'unknown')}": row["n"]
        for row in conn.execute(
            """
            SELECT COALESCE(encoding_used, 'unknown') AS encoding_used,
                   COALESCE(encoding_source, 'unknown') AS encoding_source,
                   COUNT(*) AS n
            FROM source_files
            GROUP BY COALESCE(encoding_used, 'unknown'), COALESCE(encoding_source, 'unknown')
            ORDER BY n DESC, encoding_used ASC, encoding_source ASC
            """
        ).fetchall()
    }
    payload = {
        "summary": {
            "total_source_files": total_files,
            "raw_imported_records": total_records,
            "exact_deduplicated_clusters": get_stage_metrics(conn, "exact").get("exact_clusters"),
            "fuzzy_auto_merged_clusters": get_stage_metrics(conn, "fuzzy").get("post_fuzzy_clusters"),
            "manual_review_pending": scalar(
                conn, "SELECT COUNT(*) FROM candidate_pairs WHERE status = 'pending'"
            ),
            "final_unique_clusters": scalar(conn, "SELECT COUNT(*) FROM clusters"),
        },
        "source_breakdown": source_breakdown,
        "missing_key_fields": missing,
        "record_type_distribution": record_types,
        "exact_metrics": get_stage_metrics(conn, "exact"),
        "fuzzy_metrics": get_stage_metrics(conn, "fuzzy"),
        "review_metrics": get_stage_metrics(conn, "review"),
        "encoding_summary": encoding_summary,
        "encoding_by_file": encoding_by_file,
        "parser_profile_summary": parser_profile_summary,
    }
    return payload


def write_report(
    payload: dict[str, Any],
    markdown_path: Path,
    json_path: Path,
    *,
    markdown_encoding: str = "utf-8",
    json_encoding: str = "utf-8",
) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding=json_encoding)
    markdown_path.write_text(markdown_report(payload), encoding=markdown_encoding)


def markdown_report(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# Deduplication Report",
        "",
        "## Summary",
        "",
        f"- Total source files: `{summary['total_source_files']}`",
        f"- Raw imported records: `{summary['raw_imported_records']}`",
        f"- Exact deduplicated clusters: `{summary['exact_deduplicated_clusters']}`",
        f"- Fuzzy auto-merged clusters: `{summary['fuzzy_auto_merged_clusters']}`",
        f"- Manual review pending: `{summary['manual_review_pending']}`",
        f"- Final unique clusters: `{summary['final_unique_clusters']}`",
        "",
        "## Source Breakdown",
        "",
    ]
    for source_db, count in payload["source_breakdown"].items():
        lines.append(f"- {source_db}: `{count}`")
    lines.extend(["", "## Missing Key Fields", ""])
    for field, info in payload["missing_key_fields"].items():
        total = info["total"] or 1
        pct = info["missing"] / total * 100
        lines.append(f"- {field}: `{info['missing']}/{total}` missing ({pct:.1f}%)")
    lines.extend(["", "## Record Types", ""])
    for category, count in payload["record_type_distribution"].items():
        lines.append(f"- {category}: `{count}`")
    lines.extend(["", "## Encoding Summary", ""])
    for key, count in payload["encoding_summary"].items():
        encoding_used, encoding_source = key.split("|", 1)
        lines.append(f"- {encoding_used} via {encoding_source}: `{count}` file(s)")
    lines.extend(
        [
            "",
            "## Parser/Profile Notes",
            "",
            "- PubMed profile: `NBIB`",
            "- Embase profile: abstract priority `N2 -> AB`",
            "- WoS profile: BOM stripping enabled before RIS parsing",
            "- Encoding priority: `CLI --encoding -> profile encoding -> default utf-8`",
            "",
            "## Stage Metrics",
            "",
            f"- Exact: `{json.dumps(payload['exact_metrics'], ensure_ascii=False)}`",
            f"- Fuzzy: `{json.dumps(payload['fuzzy_metrics'], ensure_ascii=False)}`",
            f"- Review: `{json.dumps(payload['review_metrics'], ensure_ascii=False)}`",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def scalar(conn: sqlite3.Connection, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return int(row[0]) if row and row[0] is not None else 0

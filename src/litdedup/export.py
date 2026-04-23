from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path


def json_list(value: str | None) -> list[str]:
    if not value:
        return []
    return json.loads(value)


def cluster_payloads(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT c.id AS cluster_id, c.canonical_record_id, r.*
        FROM clusters c
        JOIN records r ON r.id = c.canonical_record_id
        WHERE c.status = 'active'
        ORDER BY c.id
        """
    ).fetchall()
    payloads: list[dict] = []
    for row in rows:
        members = conn.execute(
            """
            SELECT r.*
            FROM cluster_members cm
            JOIN records r ON r.id = cm.record_id
            WHERE cm.cluster_id = ?
            ORDER BY r.id
            """,
            (row["cluster_id"],),
        ).fetchall()
        payloads.append(consolidate_cluster(dict(row), [dict(member) for member in members]))
    return payloads


def consolidate_cluster(canonical: dict, members: list[dict]) -> dict:
    payload = dict(canonical)
    for field in ("abstract", "journal", "volume", "issue", "start_page", "end_page", "url", "language", "doi", "pmid", "pmcid"):
        if payload.get(field):
            continue
        for member in members:
            if member.get(field):
                payload[field] = member[field]
                break
    if not payload.get("authors_json"):
        for member in members:
            if member.get("authors_json"):
                payload["authors_json"] = member["authors_json"]
                break
    if not payload.get("keywords_json"):
        all_keywords: list[str] = []
        seen: set[str] = set()
        for member in members:
            for keyword in json_list(member.get("keywords_json")):
                normalized = keyword.strip().lower()
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    all_keywords.append(keyword)
        payload["keywords_json"] = json.dumps(all_keywords, ensure_ascii=False)
    payload["member_ids"] = [member["id"] for member in members]
    payload["source_members"] = [
        f"{member['source_db']}:{member['source_record_id']}"
        for member in members
    ]
    payload["member_count"] = len(members)
    return payload


def export_deduplicated_csv(conn: sqlite3.Connection, output_path: Path, *, encoding: str = "utf-8") -> int:
    payloads = cluster_payloads(conn)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding=encoding) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "cluster_id",
                "canonical_record_id",
                "source_db",
                "record_type",
                "title",
                "year",
                "authors",
                "journal",
                "volume",
                "issue",
                "start_page",
                "end_page",
                "doi",
                "pmid",
                "pmcid",
                "abstract",
                "keywords",
                "language",
                "url",
                "member_count",
                "source_members",
            ],
        )
        writer.writeheader()
        for payload in payloads:
            writer.writerow(
                {
                    "cluster_id": payload["cluster_id"],
                    "canonical_record_id": payload["canonical_record_id"],
                    "source_db": payload["source_db"],
                    "record_type": payload["record_type"],
                    "title": payload["title"] or "",
                    "year": payload["year"] or "",
                    "authors": "; ".join(json_list(payload.get("authors_json"))),
                    "journal": payload["journal"] or "",
                    "volume": payload["volume"] or "",
                    "issue": payload["issue"] or "",
                    "start_page": payload["start_page"] or "",
                    "end_page": payload["end_page"] or "",
                    "doi": payload["doi"] or "",
                    "pmid": payload["pmid"] or "",
                    "pmcid": payload["pmcid"] or "",
                    "abstract": payload["abstract"] or "",
                    "keywords": "; ".join(json_list(payload.get("keywords_json"))),
                    "language": payload["language"] or "",
                    "url": payload["url"] or "",
                    "member_count": payload["member_count"],
                    "source_members": "; ".join(payload["source_members"]),
                }
            )
    return len(payloads)


def export_deduplicated_ris(conn: sqlite3.Connection, output_path: Path, *, encoding: str = "utf-8") -> int:
    payloads = cluster_payloads(conn)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding=encoding) as handle:
        for payload in payloads:
            handle.write(f"TY  - {ris_type(payload.get('record_type') or '')}\n")
            for author in json_list(payload.get("authors_json")):
                handle.write(f"AU  - {author}\n")
            if payload.get("title"):
                handle.write(f"TI  - {payload['title']}\n")
            if payload.get("journal"):
                handle.write(f"T2  - {payload['journal']}\n")
            if payload.get("abstract"):
                handle.write(f"AB  - {payload['abstract']}\n")
            if payload.get("year"):
                handle.write(f"PY  - {payload['year']}\n")
            if payload.get("volume"):
                handle.write(f"VL  - {payload['volume']}\n")
            if payload.get("issue"):
                handle.write(f"IS  - {payload['issue']}\n")
            if payload.get("start_page"):
                handle.write(f"SP  - {payload['start_page']}\n")
            if payload.get("end_page"):
                handle.write(f"EP  - {payload['end_page']}\n")
            if payload.get("doi"):
                handle.write(f"DO  - {payload['doi']}\n")
            if payload.get("url"):
                handle.write(f"UR  - {payload['url']}\n")
            for keyword in json_list(payload.get("keywords_json")):
                handle.write(f"KW  - {keyword}\n")
            if payload.get("language"):
                handle.write(f"LA  - {payload['language']}\n")
            handle.write(f"N1  - cluster_id={payload['cluster_id']}\n")
            handle.write(f"N1  - source_members={'; '.join(payload['source_members'])}\n")
            handle.write("ER  - \n\n")
    return len(payloads)


def ris_type(record_type: str) -> str:
    lowered = (record_type or "").lower()
    if "conference" in lowered or "cpaper" in lowered:
        return "CPAPER"
    if "chapter" in lowered:
        return "CHAP"
    return "JOUR"

from __future__ import annotations

import csv
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from rapidfuzz import fuzz
from tqdm import tqdm

from litdedup.db import (
    fetch_all_records,
    fetch_cluster_members,
    fetch_cluster_representatives,
    fetch_record_map,
    pending_candidate_count,
    rebuild_clusters,
    upsert_stage_metrics,
    utc_now,
    write_candidate_pairs,
)


class UnionFind:
    def __init__(self, items: Iterable[int]):
        self.parent = {item: item for item in items}

    def find(self, item: int) -> int:
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            if left_root < right_root:
                self.parent[right_root] = left_root
            else:
                self.parent[left_root] = right_root

    def groups(self) -> dict[int, list[int]]:
        buckets: dict[int, list[int]] = defaultdict(list)
        for item in self.parent:
            buckets[self.find(item)].append(item)
        return buckets


@dataclass
class CandidateEvaluation:
    left_record_id: int
    right_record_id: int
    left_cluster_id: int
    right_cluster_id: int
    title_score: float
    author_score: float
    author_overlap: float
    first_author_match: bool
    journal_match: bool
    year_diff: int
    page_match: bool
    doi_conflict: bool
    pmid_conflict: bool
    type_conflict: bool
    composite_score: float
    recommendation: str | None
    type_pair: str

    @property
    def pair_key(self) -> str:
        left, right = sorted((self.left_record_id, self.right_record_id))
        return f"{left}:{right}"


def json_list(value: str | None) -> list[str]:
    if not value:
        return []
    return json.loads(value)


def choose_canonical_record(
    record_rows: dict[int, sqlite3.Row],
    record_ids: list[int],
    source_priority: dict[str, int],
    preferred_keepers: set[int] | None = None,
) -> int:
    preferred_keepers = preferred_keepers or set()

    def key(record_id: int) -> tuple:
        row = record_rows[record_id]
        preferred = 0 if record_id in preferred_keepers else 1
        source_rank = source_priority.get(row["source_db"], len(source_priority))
        return (
            preferred,
            -float(row["completeness_score"] or 0),
            source_rank,
            record_id,
        )

    return sorted(record_ids, key=key)[0]


def build_exact_clusters(conn: sqlite3.Connection, source_priority: dict[str, int]) -> dict[str, int]:
    rows = fetch_all_records(conn)
    if not rows:
        return {
            "raw_records": 0,
            "exact_clusters": 0,
            "pmid_groups": 0,
            "doi_groups": 0,
            "pmcid_groups": 0,
            "merged_records": 0,
        }
    union_find = UnionFind(int(row["id"]) for row in rows)
    identifier_stats = {"pmid_groups": 0, "doi_groups": 0, "pmcid_groups": 0}
    record_map = {int(row["id"]): row for row in rows}

    for column, metric_key in (("pmid", "pmid_groups"), ("doi", "doi_groups"), ("pmcid", "pmcid_groups")):
        buckets: dict[str, list[int]] = defaultdict(list)
        for row in rows:
            value = (row[column] or "").strip().lower()
            if value:
                buckets[value].append(int(row["id"]))
        for ids in buckets.values():
            if len(ids) <= 1:
                continue
            identifier_stats[metric_key] += 1
            for idx, left_id in enumerate(ids):
                for right_id in ids[idx + 1 :]:
                    if should_union_strong_id(record_map=record_map, left_id=left_id, right_id=right_id):
                        union_find.union(left_id, right_id)

    groups = [sorted(ids) for ids in union_find.groups().values()]
    rebuild_clusters(
        conn,
        groups,
        canonical_picker=lambda group: choose_canonical_record(record_map, group, source_priority),
        stage="exact",
    )
    metrics = {
        "raw_records": len(rows),
        "exact_clusters": len(groups),
        "merged_records": len(rows) - len(groups),
        **identifier_stats,
    }
    upsert_stage_metrics(conn, "exact", metrics)
    return metrics


def build_fuzzy_candidates(conn: sqlite3.Connection) -> tuple[list[CandidateEvaluation], list[tuple[int, int]]]:
    representatives = fetch_cluster_representatives(conn)
    rows = [dict(row) for row in representatives]
    row_map = {int(row["id"]): row for row in rows}

    block_map: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        title_norm = row["title_norm"] or ""
        if title_norm:
            year_key = str(row["year"] or "")
            block_map[("title_year", f"{year_key}:{title_norm[:40]}")].append(row)
            if row["first_author_norm"]:
                block_map[("author_title", f"{row['first_author_norm']}:{title_norm[:28]}")].append(row)
        if row["journal_norm"] and row["volume"] and row["start_page"]:
            block_map[("journal_vol_page", f"{row['journal_norm']}:{row['volume']}:{row['start_page']}")].append(row)

    candidate_ids: set[tuple[int, int]] = set()
    for group in block_map.values():
        if len(group) < 2:
            continue
        for idx, left in enumerate(group):
            for right in group[idx + 1 :]:
                left_id, right_id = sorted((int(left["id"]), int(right["id"])))
                if left_id == right_id:
                    continue
                candidate_ids.add((left_id, right_id))

    evaluations: list[CandidateEvaluation] = []
    auto_merge_edges: list[tuple[int, int]] = []

    for left_id, right_id in tqdm(
        sorted(candidate_ids),
        desc="Scoring fuzzy candidates",
        unit="pair",
    ):
        left = row_map[left_id]
        right = row_map[right_id]
        evaluation = evaluate_pair(left, right)
        if evaluation.recommendation is None:
            continue
        evaluations.append(evaluation)
        if evaluation.recommendation == "auto_merge":
            auto_merge_edges.append((evaluation.left_cluster_id, evaluation.right_cluster_id))

    return evaluations, auto_merge_edges


def evaluate_pair(left: dict, right: dict) -> CandidateEvaluation:
    left_authors = json_list(left["author_surnames_json"])
    right_authors = json_list(right["author_surnames_json"])
    left_set = set(left_authors)
    right_set = set(right_authors)

    if left_set or right_set:
        overlap = len(left_set & right_set) / max(len(left_set | right_set), 1)
    else:
        overlap = 0.0
    author_score = overlap * 100
    title_score = float(fuzz.token_set_ratio(left["title_norm"] or "", right["title_norm"] or ""))
    first_author_match = bool(left["first_author_norm"] and left["first_author_norm"] == right["first_author_norm"])
    journal_match = bool(left["journal_norm"] and left["journal_norm"] == right["journal_norm"])
    page_match = bool(left["page_key"] and left["page_key"] == right["page_key"])
    year_diff = abs((left["year"] or 0) - (right["year"] or 0)) if left["year"] and right["year"] else 99
    doi_conflict = bool(left["doi"] and right["doi"] and left["doi"] != right["doi"])
    pmid_conflict = bool(left["pmid"] and right["pmid"] and left["pmid"] != right["pmid"])
    left_type = left["record_type_category"] or "other"
    right_type = right["record_type_category"] or "other"
    type_conflict = left_type != right_type
    type_pair = "|".join(sorted((left_type, right_type)))

    year_score = max(0.0, 100.0 - min(year_diff, 5) * 20)
    composite_score = round(
        (title_score * 0.6)
        + (author_score * 0.15)
        + (100.0 if journal_match else 0.0) * 0.1
        + (100.0 if first_author_match else 0.0) * 0.05
        + (100.0 if page_match else 0.0) * 0.05
        + (year_score * 0.05),
        2,
    )

    recommendation: str | None = None
    if doi_conflict or pmid_conflict:
        recommendation = None
    elif {"correction", "chapter"} & {left_type, right_type}:
        recommendation = None
    elif {left_type, right_type} == {"conference", "journal"}:
        if title_score >= 92 and year_diff <= 1 and (first_author_match or overlap >= 0.4):
            recommendation = "review"
    elif title_score >= 98 and year_diff <= 1 and (first_author_match or overlap >= 0.8) and (
        journal_match or page_match or overlap >= 0.9
    ):
        recommendation = "auto_merge"
    elif (
        title_score >= 95
        and first_author_match
        and year_diff <= 1
        and overlap >= 0.5
        and (journal_match or page_match)
    ):
        recommendation = "auto_merge"
    elif title_score >= 88 and year_diff <= 1 and (first_author_match or overlap >= 0.4 or journal_match or page_match):
        recommendation = "review"
    elif page_match and journal_match and year_diff <= 1 and title_score >= 80:
        recommendation = "review"

    return CandidateEvaluation(
        left_record_id=int(left["id"]),
        right_record_id=int(right["id"]),
        left_cluster_id=int(left["cluster_id"]),
        right_cluster_id=int(right["cluster_id"]),
        title_score=round(title_score, 2),
        author_score=round(author_score, 2),
        author_overlap=round(overlap, 4),
        first_author_match=first_author_match,
        journal_match=journal_match,
        year_diff=year_diff,
        page_match=page_match,
        doi_conflict=doi_conflict,
        pmid_conflict=pmid_conflict,
        type_conflict=type_conflict,
        composite_score=composite_score,
        recommendation=recommendation,
        type_pair=type_pair,
    )


def should_union_strong_id(record_map: dict[int, sqlite3.Row], left_id: int, right_id: int) -> bool:
    left = record_map[left_id]
    right = record_map[right_id]
    left_type = left["record_type_category"] or "other"
    right_type = right["record_type_category"] or "other"
    if {left_type, right_type} == {"conference", "journal"}:
        return False
    if {"correction", "chapter"} & {left_type, right_type}:
        return False
    return True


def apply_cluster_merges(
    conn: sqlite3.Connection,
    source_priority: dict[str, int],
    edges: list[tuple[int, int]],
    stage: str,
    preferred_keeper_ids: set[int] | None = None,
) -> dict[int, int]:
    current_clusters = fetch_cluster_members(conn)
    current_cluster_ids = sorted(current_clusters)
    if not current_cluster_ids:
        return {}
    union_find = UnionFind(current_cluster_ids)
    for left_cluster_id, right_cluster_id in edges:
        if left_cluster_id in union_find.parent and right_cluster_id in union_find.parent:
            union_find.union(left_cluster_id, right_cluster_id)
    cluster_groups = union_find.groups().values()
    record_groups: list[list[int]] = []
    for cluster_id_group in cluster_groups:
        record_ids: list[int] = []
        for cluster_id in cluster_id_group:
            record_ids.extend(current_clusters.get(cluster_id, []))
        if record_ids:
            record_groups.append(sorted(record_ids))
    record_map = fetch_record_map(conn)
    preferred_keeper_ids = preferred_keeper_ids or set()
    return rebuild_clusters(
        conn,
        record_groups,
        canonical_picker=lambda group: choose_canonical_record(record_map, group, source_priority, preferred_keeper_ids),
        stage=stage,
    )


def run_fuzzy_dedup(conn: sqlite3.Connection, source_priority: dict[str, int]) -> dict[str, int]:
    current_clusters = fetch_cluster_members(conn)
    if not current_clusters:
        raise RuntimeError("No clusters found. Run dedup-exact first.")
    pre_cluster_count = len(current_clusters)
    evaluations, auto_merge_edges = build_fuzzy_candidates(conn)
    record_to_cluster = apply_cluster_merges(conn, source_priority, auto_merge_edges, stage="fuzzy_auto")
    pending_rows: list[dict] = []
    auto_rows: list[dict] = []

    for evaluation in evaluations:
        left_cluster_id = record_to_cluster.get(evaluation.left_record_id, evaluation.left_cluster_id)
        right_cluster_id = record_to_cluster.get(evaluation.right_record_id, evaluation.right_cluster_id)
        if left_cluster_id == right_cluster_id:
            status = "auto_merged"
            auto_rows.append(
                candidate_row_payload(evaluation, left_cluster_id, right_cluster_id, status=status, recommendation="auto_merge")
            )
            continue
        pending_rows.append(
            candidate_row_payload(evaluation, left_cluster_id, right_cluster_id, status="pending", recommendation="review")
        )

    write_candidate_pairs(conn, auto_rows + pending_rows)
    metrics = {
        "pre_fuzzy_clusters": pre_cluster_count,
        "auto_merge_pairs": len(auto_rows),
        "manual_review_pending": len(pending_rows),
        "post_fuzzy_clusters": len(fetch_cluster_members(conn)),
    }
    upsert_stage_metrics(conn, "fuzzy", metrics)
    return metrics


def candidate_row_payload(
    evaluation: CandidateEvaluation,
    left_cluster_id: int,
    right_cluster_id: int,
    *,
    status: str,
    recommendation: str,
) -> dict:
    return {
        "pair_key": evaluation.pair_key,
        "left_record_id": evaluation.left_record_id,
        "right_record_id": evaluation.right_record_id,
        "left_cluster_id": left_cluster_id,
        "right_cluster_id": right_cluster_id,
        "title_score": evaluation.title_score,
        "author_score": evaluation.author_score,
        "author_overlap": evaluation.author_overlap,
        "first_author_match": int(evaluation.first_author_match),
        "journal_match": int(evaluation.journal_match),
        "year_diff": evaluation.year_diff,
        "page_match": int(evaluation.page_match),
        "doi_conflict": int(evaluation.doi_conflict),
        "pmid_conflict": int(evaluation.pmid_conflict),
        "type_conflict": int(evaluation.type_conflict),
        "composite_score": evaluation.composite_score,
        "recommendation": recommendation,
        "status": status,
        "created_at": utc_now(),
        "resolved_at": utc_now() if status != "pending" else None,
    }


def export_review_queue(conn: sqlite3.Connection, output_path: Path, *, encoding: str = "utf-8") -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT cp.id AS pair_id, cp.pair_key, cp.left_record_id, cp.right_record_id,
               cp.left_cluster_id, cp.right_cluster_id, cp.title_score, cp.author_score,
               cp.author_overlap, cp.composite_score, cp.recommendation,
               left_record.title AS left_title, left_record.year AS left_year,
               left_record.authors_json AS left_authors_json, left_record.journal AS left_journal,
               left_record.doi AS left_doi, left_record.pmid AS left_pmid, left_record.source_db AS left_source_db,
               right_record.title AS right_title, right_record.year AS right_year,
               right_record.authors_json AS right_authors_json, right_record.journal AS right_journal,
               right_record.doi AS right_doi, right_record.pmid AS right_pmid, right_record.source_db AS right_source_db
        FROM candidate_pairs cp
        JOIN records AS left_record ON left_record.id = cp.left_record_id
        JOIN records AS right_record ON right_record.id = cp.right_record_id
        JOIN cluster_members left_cm ON left_cm.record_id = cp.left_record_id
        JOIN cluster_members right_cm ON right_cm.record_id = cp.right_record_id
        WHERE cp.status = 'pending'
          AND left_cm.cluster_id != right_cm.cluster_id
        ORDER BY cp.composite_score DESC, cp.id ASC
        """
    ).fetchall()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding=encoding) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "pair_id",
                "pair_key",
                "left_record_id",
                "right_record_id",
                "left_cluster_id",
                "right_cluster_id",
                "left_title",
                "right_title",
                "left_year",
                "right_year",
                "left_authors",
                "right_authors",
                "left_journal",
                "right_journal",
                "left_doi",
                "right_doi",
                "left_pmid",
                "right_pmid",
                "left_source_db",
                "right_source_db",
                "title_score",
                "author_score",
                "author_overlap",
                "composite_score",
                "recommendation",
                "decision",
                "preferred_keeper",
                "notes",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "pair_id": row["pair_id"],
                    "pair_key": row["pair_key"],
                    "left_record_id": row["left_record_id"],
                    "right_record_id": row["right_record_id"],
                    "left_cluster_id": row["left_cluster_id"],
                    "right_cluster_id": row["right_cluster_id"],
                    "left_title": row["left_title"] or "",
                    "right_title": row["right_title"] or "",
                    "left_year": row["left_year"] or "",
                    "right_year": row["right_year"] or "",
                    "left_authors": "; ".join(json_list(row["left_authors_json"])),
                    "right_authors": "; ".join(json_list(row["right_authors_json"])),
                    "left_journal": row["left_journal"] or "",
                    "right_journal": row["right_journal"] or "",
                    "left_doi": row["left_doi"] or "",
                    "right_doi": row["right_doi"] or "",
                    "left_pmid": row["left_pmid"] or "",
                    "right_pmid": row["right_pmid"] or "",
                    "left_source_db": row["left_source_db"] or "",
                    "right_source_db": row["right_source_db"] or "",
                    "title_score": row["title_score"],
                    "author_score": row["author_score"],
                    "author_overlap": row["author_overlap"],
                    "composite_score": row["composite_score"],
                    "recommendation": row["recommendation"],
                    "decision": "",
                    "preferred_keeper": "",
                    "notes": "",
                }
            )
    return {"pending_rows": len(rows)}


def import_review_decisions(
    conn: sqlite3.Connection,
    csv_path: Path,
    source_priority: dict[str, int],
    *,
    encoding: str = "utf-8",
) -> dict[str, int]:
    decisions: list[dict] = []
    with csv_path.open("r", newline="", encoding=encoding) as handle:
        for row in csv.DictReader(handle):
            decision = (row.get("decision") or "").strip().lower()
            if not decision:
                continue
            if decision not in {"merge", "separate", "skip"}:
                raise ValueError(f"Unsupported decision '{decision}' for pair_id={row.get('pair_id')}")
            decisions.append(row)

    if not decisions:
        raise ValueError(
            "No manual review decisions were found in the CSV. "
            "Please save your edits back to the `decision` column using one of: merge, separate, skip."
        )

    merge_edges: list[tuple[int, int]] = []
    preferred_keepers: set[int] = set()
    merge_count = 0
    separate_count = 0
    skip_count = 0
    for row in tqdm(decisions, desc="Applying manual review", unit="pair"):
        pair_id = int(row["pair_id"])
        pair = conn.execute(
            "SELECT * FROM candidate_pairs WHERE id = ?",
            (pair_id,),
        ).fetchone()
        if not pair:
            raise ValueError(f"Unknown pair_id={pair_id}")
        if row["decision"] == "merge":
            current_left_cluster = current_cluster_for_record(conn, int(pair["left_record_id"]))
            current_right_cluster = current_cluster_for_record(conn, int(pair["right_record_id"]))
            if current_left_cluster != current_right_cluster:
                merge_edges.append((current_left_cluster, current_right_cluster))
            preferred_keeper = (row.get("preferred_keeper") or "").strip()
            if preferred_keeper:
                preferred_id = int(preferred_keeper)
                if preferred_id not in {int(pair["left_record_id"]), int(pair["right_record_id"])}:
                    raise ValueError(
                        f"preferred_keeper must be one of the pair record ids for pair_id={pair_id}"
                    )
                preferred_keepers.add(preferred_id)
            merge_count += 1
            status = "merged_by_review"
        elif row["decision"] == "separate":
            separate_count += 1
            status = "separate_by_review"
        else:
            skip_count += 1
            status = "skipped_by_review"
        conn.execute(
            "UPDATE candidate_pairs SET status = ?, resolved_at = ? WHERE id = ?",
            (status, utc_now(), pair_id),
        )
        conn.execute(
            """
            INSERT INTO manual_reviews(pair_id, pair_key, left_record_id, right_record_id, decision, preferred_keeper_record_id, notes, decided_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pair_id) DO UPDATE SET
                decision = excluded.decision,
                preferred_keeper_record_id = excluded.preferred_keeper_record_id,
                notes = excluded.notes,
                decided_at = excluded.decided_at
            """,
            (
                pair_id,
                pair["pair_key"],
                pair["left_record_id"],
                pair["right_record_id"],
                row["decision"],
                int(row["preferred_keeper"]) if (row.get("preferred_keeper") or "").strip() else None,
                (row.get("notes") or "").strip(),
                utc_now(),
            ),
        )
    conn.commit()

    if merge_edges:
        apply_cluster_merges(conn, source_priority, merge_edges, stage="manual_review", preferred_keeper_ids=preferred_keepers)

    metrics = {
        "reviewed_pairs": len(decisions),
        "merged_by_review": merge_count,
        "separate_by_review": separate_count,
        "skipped_by_review": skip_count,
        "pending_after_review": pending_candidate_count(conn),
        "final_clusters_after_review": len(fetch_cluster_members(conn)),
    }
    upsert_stage_metrics(conn, "review", metrics)
    return metrics


def current_cluster_for_record(conn: sqlite3.Connection, record_id: int) -> int:
    row = conn.execute(
        "SELECT cluster_id FROM cluster_members WHERE record_id = ?",
        (record_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Record {record_id} is not assigned to a cluster")
    return int(row["cluster_id"])

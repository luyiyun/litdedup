from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class ProfileConfig(BaseModel):
    format: str
    source_name: str
    record_start_tag: str
    record_end_tag: str | None = None
    strip_bom: bool = False
    encoding: str = ""
    encoding_candidates: list[str] = Field(default_factory=list)
    continuation_rule: str = "space"
    field_map: dict[str, list[str]]
    multi_value_fields: list[str] = Field(default_factory=list)
    identifier_aliases: dict[str, list[str]] = Field(default_factory=dict)
    source_record_id_tags: list[str] = Field(default_factory=list)


class AppConfig(BaseModel):
    source_priority: list[str]
    profiles: dict[str, ProfileConfig]


def default_runtime_dir() -> Path:
    return Path.cwd().resolve() / "dedup"


def default_config() -> AppConfig:
    return AppConfig(
        source_priority=["PubMed", "Embase", "WoS"],
        profiles={
            "pubmed_nbib": ProfileConfig(
                format="nbib",
                source_name="PubMed",
                record_start_tag="PMID",
                encoding="utf-8-sig",
                encoding_candidates=["utf-8-sig", "utf-8", "latin-1", "cp1252", "mac_roman"],
                continuation_rule="nbib_indent",
                field_map={
                    "record_type": ["PT"],
                    "title": ["TI"],
                    "abstract": ["AB"],
                    "authors": ["AU", "FAU"],
                    "journal": ["JT", "TA"],
                    "year": ["DP"],
                    "volume": ["VI"],
                    "issue": ["IP"],
                    "pages": ["PG"],
                    "keywords": ["OT", "MH"],
                    "language": ["LA"],
                    "url": [],
                },
                multi_value_fields=[
                    "AU",
                    "FAU",
                    "OT",
                    "MH",
                    "AD",
                    "IS",
                    "AID",
                    "LID",
                    "PT",
                    "PHST",
                ],
                identifier_aliases={
                    "doi": ["AID", "LID"],
                    "pmid": ["PMID"],
                    "pmcid": ["PMC", "PMCID"],
                },
                source_record_id_tags=["PMID"],
            ),
            "embase_ris": ProfileConfig(
                format="ris",
                source_name="Embase",
                record_start_tag="TY",
                record_end_tag="ER",
                encoding="",
                encoding_candidates=["utf-8-sig", "utf-8", "cp1252", "mac_roman", "latin-1"],
                continuation_rule="space",
                field_map={
                    "record_type": ["M3", "TY"],
                    "title": ["T1", "TI"],
                    "abstract": ["N2", "AB"],
                    "authors": ["A1", "AU"],
                    "journal": ["JF", "JO", "T2"],
                    "year": ["Y1", "PY"],
                    "volume": ["VL"],
                    "issue": ["IS"],
                    "start_page": ["SP"],
                    "end_page": ["EP"],
                    "keywords": ["KW"],
                    "language": ["LA"],
                    "url": ["UR", "L2"],
                },
                multi_value_fields=[
                    "A1",
                    "AU",
                    "KW",
                    "SN",
                    "AD",
                    "DB",
                    "M1",
                    "N2",
                ],
                identifier_aliases={
                    "doi": ["DO", "L2"],
                    "pmid": ["C5"],
                    "pmcid": [],
                },
                source_record_id_tags=["U2", "C5", "DO"],
            ),
            "wos_ris": ProfileConfig(
                format="ris",
                source_name="WoS",
                record_start_tag="TY",
                record_end_tag="ER",
                strip_bom=True,
                encoding="utf-8-sig",
                encoding_candidates=["utf-8-sig", "utf-8", "cp1252", "mac_roman", "latin-1"],
                continuation_rule="space",
                field_map={
                    "record_type": ["TY"],
                    "title": ["TI", "T1"],
                    "abstract": ["AB", "N2"],
                    "authors": ["AU", "A1"],
                    "journal": ["T2", "JO", "JF"],
                    "year": ["PY", "Y1", "C6"],
                    "volume": ["VL"],
                    "issue": ["IS"],
                    "start_page": ["SP", "C7"],
                    "end_page": ["EP"],
                    "keywords": ["KW", "DE", "ID"],
                    "language": ["LA"],
                    "url": ["UR"],
                },
                multi_value_fields=[
                    "AU",
                    "A1",
                    "SN",
                    "AB",
                    "DE",
                    "ID",
                ],
                identifier_aliases={
                    "doi": ["DO"],
                    "pmid": ["PM", "PMID"],
                    "pmcid": [],
                },
                source_record_id_tags=["AN", "UT", "DO"],
            ),
        },
    )


def save_config(config: AppConfig, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(config.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def load_config(path: Path) -> AppConfig:
    return AppConfig.model_validate_json(path.read_text(encoding="utf-8"))


def ensure_runtime_dir(path: Path | None = None) -> Path:
    runtime_dir = path or default_runtime_dir()
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return runtime_dir


def ensure_config(runtime_dir: Path) -> Path:
    config_path = runtime_dir / "config.json"
    if not config_path.exists():
        save_config(default_config(), config_path)
    return config_path


def runtime_paths(runtime_dir: Path | None = None) -> dict[str, Path]:
    root = ensure_runtime_dir(runtime_dir)
    return {
        "runtime_dir": root,
        "config": root / "config.json",
        "db": root / "dedup.sqlite",
        "manual_review_csv": root / "manual_review_queue.csv",
        "dedup_ris": root / "deduplicated_records.ris",
        "dedup_csv": root / "deduplicated_records.csv",
        "report_md": root / "dedup_report.md",
        "report_json": root / "dedup_report.json",
    }


def infer_profile_from_path(path: Path, config: AppConfig) -> str:
    suffix = path.suffix.lower()
    parts = {part.lower() for part in path.parts}
    if suffix == ".nbib":
        return "pubmed_nbib"
    if suffix == ".ris" and "embase" in parts:
        return "embase_ris"
    if suffix == ".ris" and "wos" in parts:
        return "wos_ris"
    if suffix == ".ris":
        candidates = [name for name, profile in config.profiles.items() if profile.format == "ris"]
        if len(candidates) == 1:
            return candidates[0]
    raise ValueError(f"Unable to infer profile for {path}. Please pass --profile explicitly.")


def source_priority_map(config: AppConfig) -> dict[str, int]:
    return {name: index for index, name in enumerate(config.source_priority)}


def config_summary(config: AppConfig) -> dict[str, Any]:
    return {
        "source_priority": config.source_priority,
        "profiles": {
            name: {
                "format": profile.format,
                "source_name": profile.source_name,
                "encoding": profile.encoding,
                "encoding_candidates": profile.encoding_candidates,
                "abstract_tags": profile.field_map.get("abstract", []),
                "title_tags": profile.field_map.get("title", []),
                "author_tags": profile.field_map.get("authors", []),
                "year_tags": profile.field_map.get("year", []),
                "strip_bom": profile.strip_bom,
            }
            for name, profile in config.profiles.items()
        },
    }

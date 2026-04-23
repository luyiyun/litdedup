from __future__ import annotations

import hashlib
import html
import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from charset_normalizer import from_bytes

from litdedup.config import ProfileConfig


RIS_TAG_RE = re.compile(r"^([A-Z0-9]{2})  - ?(.*)$")
NBIB_TAG_RE = re.compile(r"^([A-Z0-9]{2,4})\s*-\s?(.*)$")
DOI_RE = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.IGNORECASE)
YEAR_RE = re.compile(r"(19|20)\d{2}")
DEFAULT_ENCODING_CANDIDATES = [
    "utf-8-sig",
    "utf-8",
    "cp1252",
    "mac_roman",
    "latin-1",
]
MOJIBAKE_CHAR_RE = re.compile(r"[‡†•™œŒ…‰‹›ŠŽŸ¢¤¬¨´¸ˆ˜¯ˇ]")
IN_WORD_SYMBOL_RE = re.compile(r"(?i)[a-z\u00c0-\u024f][—–‡†•™œŒ…‰‹›ŠŽŸ¢¤¬¨´¸ˆ˜¯ˇ][a-z\u00c0-\u024f]")
CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
SAMPLE_BYTES = 128 * 1024
SAMPLE_LINES = 2000


@dataclass
class ParsedRecord:
    source_record_id: str
    fields: dict[str, list[str]]
    raw_entries: list[tuple[int, str, str]]
    warnings: list[str]
    raw_text: str


@dataclass
class DecodedSource:
    text: str
    lines: list[str]
    encoding_used: str
    detection_method: str


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def decode_source(path: Path, profile: ProfileConfig, *, override_encoding: str | None = None) -> DecodedSource:
    raw = path.read_bytes()
    explicit_encoding = (override_encoding or "").strip()
    if not raw:
        encoding_used = explicit_encoding or profile.encoding or "utf-8"
        detection_method = "cli" if explicit_encoding else ("config" if profile.encoding else "empty")
        return DecodedSource(text="", lines=[], encoding_used=encoding_used, detection_method=detection_method)

    chosen_encoding = explicit_encoding
    detection_method = "cli"
    if not chosen_encoding:
        chosen_encoding = (profile.encoding or "").strip()
        detection_method = "config" if chosen_encoding else ""

    if not chosen_encoding:
        chosen_encoding, detection_method = detect_encoding_from_sample(raw, profile)

    text = decode_with_encoding(raw, chosen_encoding)
    lines = text.splitlines()
    return DecodedSource(
        text=text,
        lines=lines,
        encoding_used=chosen_encoding,
        detection_method=detection_method,
    )


def detect_encoding_from_sample(raw: bytes, profile: ProfileConfig) -> tuple[str, str]:
    sample = raw[:SAMPLE_BYTES]
    detector_candidates = [match.encoding for match in from_bytes(sample) if match.encoding]
    candidates = dedup_encodings(profile.encoding_candidates + detector_candidates + DEFAULT_ENCODING_CANDIDATES)

    best_encoding = ""
    best_score: int | None = None
    for encoding in candidates:
        try:
            sample_text = decode_with_encoding(sample, encoding)
        except UnicodeDecodeError:
            continue
        limited_text = "\n".join(sample_text.splitlines()[:SAMPLE_LINES])
        score = decoded_text_score(limited_text)
        if best_score is None or score > best_score:
            best_score = score
            best_encoding = encoding

    if best_encoding:
        return best_encoding, "sample_detected"
    return DEFAULT_ENCODING_CANDIDATES[0], "fallback"


def decode_with_encoding(raw: bytes, encoding: str) -> str:
    text = raw.decode(encoding, errors="strict")
    return normalize_decoded_text(text)


def dedup_encodings(encodings: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for encoding in encodings:
        if not encoding:
            continue
        key = encoding.lower().replace("_", "-")
        if key in seen:
            continue
        seen.add(key)
        ordered.append(encoding)
    return ordered


def normalize_decoded_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return unicodedata.normalize("NFC", text)


def decoded_text_score(text: str) -> int:
    score = 1000
    score -= text.count("\ufffd") * 200
    score -= len(MOJIBAKE_CHAR_RE.findall(text)) * 40
    score -= len(IN_WORD_SYMBOL_RE.findall(text)) * 120
    score -= len(CONTROL_CHAR_RE.findall(text)) * 200
    return score


def count_records(decoded: DecodedSource, profile: ProfileConfig) -> int:
    count = 0
    for raw_line in decoded.lines:
        line = raw_line.rstrip("\n\r")
        if profile.strip_bom:
            line = line.lstrip("\ufeff")
        if profile.format == "ris" and line.startswith(f"{profile.record_start_tag}  -"):
            count += 1
        elif profile.format == "nbib" and line.startswith(f"{profile.record_start_tag}-"):
            count += 1
    return count


def parse_file(decoded: DecodedSource, profile: ProfileConfig) -> list[ParsedRecord]:
    if profile.format == "ris":
        return parse_ris(decoded, profile)
    if profile.format == "nbib":
        return parse_nbib(decoded, profile)
    raise ValueError(f"Unsupported format: {profile.format}")


def parse_ris(decoded: DecodedSource, profile: ProfileConfig) -> list[ParsedRecord]:
    records: list[ParsedRecord] = []
    current: dict[str, list[str]] | None = None
    raw_entries: list[tuple[int, str, str]] = []
    warnings: list[str] = []
    raw_lines: list[str] = []
    current_tag: str | None = None
    position = 0

    for raw_line in decoded.lines:
        line = raw_line
        if profile.strip_bom:
            line = line.lstrip("\ufeff")
        raw_lines.append(line)
        match = RIS_TAG_RE.match(line)
        if match:
            tag, value = match.groups()
            if tag == profile.record_start_tag:
                if current is not None and raw_entries:
                    records.append(finalize_record(profile, current, raw_entries, warnings, raw_lines[:-1]))
                    raw_lines = [line]
                current = defaultdict(list)
                raw_entries = []
                warnings = []
            if current is None:
                continue
            current[tag].append(value.strip())
            position += 1
            raw_entries.append((position, tag, value.strip()))
            current_tag = tag
            if tag == profile.record_end_tag:
                records.append(finalize_record(profile, current, raw_entries, warnings, raw_lines))
                current = None
                raw_entries = []
                warnings = []
                raw_lines = []
                current_tag = None
            continue

        if current is not None and current_tag:
            continuation = line.strip()
            if continuation:
                append_continuation(current, raw_entries, current_tag, continuation)
    if current is not None and raw_entries:
        warnings.append("Missing explicit ER end tag; record finalized at EOF.")
        records.append(finalize_record(profile, current, raw_entries, warnings, raw_lines))
    return records


def parse_nbib(decoded: DecodedSource, profile: ProfileConfig) -> list[ParsedRecord]:
    records: list[ParsedRecord] = []
    current: dict[str, list[str]] | None = None
    raw_entries: list[tuple[int, str, str]] = []
    warnings: list[str] = []
    raw_lines: list[str] = []
    current_tag: str | None = None
    position = 0

    for raw_line in decoded.lines:
        line = raw_line.rstrip("\n\r")
        raw_lines.append(line)
        if not line.strip():
            if current is not None and raw_entries:
                records.append(finalize_record(profile, current, raw_entries, warnings, raw_lines))
                current = None
                raw_entries = []
                warnings = []
                raw_lines = []
                current_tag = None
            continue
        match = NBIB_TAG_RE.match(line)
        if match:
            tag, value = match.groups()
            if tag == profile.record_start_tag and current is not None and raw_entries:
                records.append(finalize_record(profile, current, raw_entries, warnings, raw_lines[:-1]))
                raw_lines = [line]
                raw_entries = []
                warnings = []
            if current is None:
                current = defaultdict(list)
            current[tag].append(value.strip())
            position += 1
            raw_entries.append((position, tag, value.strip()))
            current_tag = tag
            continue
        if current is not None and current_tag:
            continuation = line.strip()
            if continuation:
                append_continuation(current, raw_entries, current_tag, continuation)
    if current is not None and raw_entries:
        records.append(finalize_record(profile, current, raw_entries, warnings, raw_lines))
    return records


def append_continuation(
    current: dict[str, list[str]],
    raw_entries: list[tuple[int, str, str]],
    current_tag: str,
    continuation: str,
) -> None:
    current[current_tag][-1] = f"{current[current_tag][-1]} {continuation}".strip()
    last_position, _, _ = raw_entries[-1]
    raw_entries[-1] = (last_position, current_tag, current[current_tag][-1])


def finalize_record(
    profile: ProfileConfig,
    fields: dict[str, list[str]],
    raw_entries: list[tuple[int, str, str]],
    warnings: list[str],
    raw_lines: Iterable[str],
) -> ParsedRecord:
    source_record_id = derive_source_record_id(fields, profile)
    return ParsedRecord(
        source_record_id=source_record_id,
        fields={tag: values[:] for tag, values in fields.items()},
        raw_entries=raw_entries[:],
        warnings=warnings[:],
        raw_text="\n".join(raw_lines),
    )


def derive_source_record_id(fields: dict[str, list[str]], profile: ProfileConfig) -> str:
    for tag in profile.source_record_id_tags:
        values = fields.get(tag, [])
        if values:
            return values[0]
    digest = hashlib.sha1(json.dumps(fields, sort_keys=True).encode("utf-8")).hexdigest()
    return f"generated:{digest}"


def first_non_empty(fields: dict[str, list[str]], tags: list[str]) -> str:
    for tag in tags:
        for value in fields.get(tag, []):
            if value.strip():
                return value.strip()
    return ""


def all_values(fields: dict[str, list[str]], tags: list[str]) -> list[str]:
    results: list[str] = []
    for tag in tags:
        for value in fields.get(tag, []):
            value = value.strip()
            if value:
                results.append(value)
    return results


def normalize_doi(value: str) -> str:
    if not value:
        return ""
    match = DOI_RE.search(value)
    if not match:
        return ""
    doi = match.group(1).rstrip(".;,)")
    return doi.lower()


def normalize_text(value: str) -> str:
    if not value:
        return ""
    value = html.unescape(value)
    value = unicodedata.normalize("NFKC", value)
    value = value.replace("\ufeff", " ")
    value = value.lower()
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"[^0-9a-z]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def parse_year(value: str) -> int | None:
    if not value:
        return None
    match = YEAR_RE.search(value)
    if not match:
        return None
    return int(match.group(0))


def parse_pages(value: str) -> tuple[str, str]:
    if not value:
        return "", ""
    value = value.strip()
    if "-" in value:
        start, end = value.split("-", 1)
        return start.strip(), end.strip()
    return value, ""


def extract_identifier(fields: dict[str, list[str]], tags: list[str], normalizer: callable | None = None) -> str:
    for tag in tags:
        for value in fields.get(tag, []):
            cleaned = normalizer(value) if normalizer else value.strip()
            if cleaned:
                return cleaned
    return ""


def author_surnames(authors: list[str]) -> list[str]:
    surnames: list[str] = []
    for author in authors:
        author = author.strip()
        if not author:
            continue
        if "," in author:
            surname = author.split(",", 1)[0].strip()
        else:
            parts = author.split()
            if len(parts) >= 2 and len(parts[-1].strip(".").strip()) <= 2:
                surname = parts[0].strip()
            else:
                surname = parts[-1].strip()
        surname = normalize_text(surname)
        if surname:
            surnames.append(surname)
    return surnames


def record_type_category(record_type: str) -> str:
    text = normalize_text(record_type)
    if any(token in text for token in ("erratum", "correction")):
        return "correction"
    if any(token in text for token in ("chapter", "chap")):
        return "chapter"
    if any(token in text for token in ("conference", "cpaper", "abstract")):
        return "conference"
    if any(token in text for token in ("journal", "article", "jour")):
        return "journal"
    if any(token in text for token in ("generic", "gen")):
        return "generic"
    return "other"


def completeness_score(record: dict[str, object]) -> float:
    score = 0.0
    if record.get("doi"):
        score += 10
    if record.get("pmid"):
        score += 10
    if record.get("pmcid"):
        score += 8
    if record.get("title"):
        score += 6
    if record.get("year") is not None:
        score += 5
    authors = record.get("authors") or []
    if authors:
        score += 5
    if record.get("abstract"):
        score += 4
    if record.get("journal"):
        score += 3
    if record.get("start_page") or record.get("end_page"):
        score += 2
    if record.get("url"):
        score += 1
    return score


def normalize_record(
    parsed: ParsedRecord,
    profile: ProfileConfig,
    source_file: Path,
) -> dict[str, object]:
    fields = parsed.fields
    field_map = profile.field_map

    title = first_non_empty(fields, field_map.get("title", []))
    abstract = first_non_empty(fields, field_map.get("abstract", []))
    authors = all_values(fields, field_map.get("authors", []))
    journal = first_non_empty(fields, field_map.get("journal", []))
    year = parse_year(first_non_empty(fields, field_map.get("year", [])))
    volume = first_non_empty(fields, field_map.get("volume", []))
    issue = first_non_empty(fields, field_map.get("issue", []))
    start_page = first_non_empty(fields, field_map.get("start_page", []))
    end_page = first_non_empty(fields, field_map.get("end_page", []))
    pages = first_non_empty(fields, field_map.get("pages", []))
    if pages and not start_page:
        start_page, end_page = parse_pages(pages)
    keywords = all_values(fields, field_map.get("keywords", []))
    language = first_non_empty(fields, field_map.get("language", []))
    url = first_non_empty(fields, field_map.get("url", []))
    record_type = first_non_empty(fields, field_map.get("record_type", []))

    doi = extract_identifier(fields, profile.identifier_aliases.get("doi", []), normalize_doi)
    pmid = extract_identifier(fields, profile.identifier_aliases.get("pmid", []), lambda x: re.sub(r"\D", "", x))
    pmcid = extract_identifier(fields, profile.identifier_aliases.get("pmcid", []))

    author_names = [author for author in authors if author]
    surnames = author_surnames(author_names)

    normalized = {
        "source_db": profile.source_name,
        "source_file": str(source_file),
        "source_record_id": parsed.source_record_id,
        "record_type": record_type,
        "record_type_category": record_type_category(record_type),
        "title": html.unescape(title).strip(),
        "abstract": html.unescape(abstract).strip(),
        "authors": author_names,
        "author_surnames": surnames,
        "journal": html.unescape(journal).strip(),
        "year": year,
        "volume": volume.strip(),
        "issue": issue.strip(),
        "start_page": start_page.strip(),
        "end_page": end_page.strip(),
        "doi": doi,
        "pmid": pmid,
        "pmcid": pmcid,
        "keywords": [html.unescape(keyword).strip() for keyword in keywords if keyword.strip()],
        "language": language.strip(),
        "url": url.strip(),
        "title_norm": normalize_text(title),
        "journal_norm": normalize_text(journal),
        "first_author_norm": surnames[0] if surnames else "",
        "page_key": normalize_text(start_page),
        "warnings": parsed.warnings,
    }
    normalized["completeness_score"] = completeness_score(normalized)
    return normalized

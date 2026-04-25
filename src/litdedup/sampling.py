from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path

from litdedup.config import ProfileConfig
from litdedup.parsers import decode_source, parse_file


@dataclass(frozen=True)
class SampleMetrics:
    total_records: int
    sampled_records: int
    encoding_used: str
    encoding_source: str


def sample_records_to_file(
    input_path: Path,
    output_path: Path,
    profile: ProfileConfig,
    *,
    count: int,
    seed: int | None = None,
    encoding: str | None = None,
    output_encoding: str = "utf-8",
) -> SampleMetrics:
    decoded = decode_source(input_path, profile, override_encoding=encoding)
    records = parse_file(decoded, profile)

    if count < 1:
        raise ValueError("--count must be greater than 0.")
    if count > len(records):
        raise ValueError(f"Cannot sample {count} records from {len(records)} available records.")

    sampled_records = random.Random(seed).sample(records, count)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n\n".join(record.raw_text.strip() for record in sampled_records)
    output_path.write_text(content + "\n", encoding=output_encoding)

    return SampleMetrics(
        total_records=len(records),
        sampled_records=len(sampled_records),
        encoding_used=decoded.encoding_used,
        encoding_source=decoded.detection_method,
    )

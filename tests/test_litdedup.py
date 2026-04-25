from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from litdedup.cli import app
from litdedup.config import default_config, default_runtime_dir


runner = CliRunner()


def write_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def write_bytes(path: Path, content: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_default_runtime_dir_uses_current_working_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    assert default_runtime_dir() == tmp_path.resolve() / "dedup"


def test_embase_n2_and_wos_bom_parsing(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    embase = write_text(
        tmp_path / "embase" / "sample.ris",
        """TY  - JOUR
M3  - Article
Y1  - 2026
T1  - Sample CRC model from Embase
N2  - Abstract from N2 field.
A1  - Zhang, San
JF  - Journal of Testing
DO  - 10.1000/xyz123
ER  - 
""",
    )
    wos = write_text(
        tmp_path / "wos" / "sample.ris",
        "\ufeffTY  - JOUR\nTI  - First WOS record survives BOM\nAU  - Li, Si\nPY  - 2024\nT2  - Testing Journal\nER  - \n"
        "TY  - JOUR\nTI  - Second WOS record\nAU  - Wang, Wu\nPY  - 2024\nT2  - Testing Journal\nER  - \n",
    )
    runner.invoke(app, ["init", "--runtime-dir", str(runtime_dir)])
    result1 = runner.invoke(app, ["import", str(embase), "--profile", "embase_ris", "--runtime-dir", str(runtime_dir)])
    assert result1.exit_code == 0, result1.output
    result2 = runner.invoke(app, ["import", str(wos), "--profile", "wos_ris", "--runtime-dir", str(runtime_dir)])
    assert result2.exit_code == 0, result2.output

    stats = runner.invoke(app, ["stats", "--runtime-dir", str(runtime_dir)])
    assert stats.exit_code == 0, stats.output
    payload = json.loads(stats.stdout)
    assert payload["summary"]["raw_imported_records"] == 3
    assert payload["missing_key_fields"]["abstract"]["missing"] == 2
    assert "utf-8|default" in payload["encoding_summary"]


def test_import_requires_explicit_non_utf8_encoding_and_uses_cli_value(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    runner.invoke(app, ["init", "--runtime-dir", str(runtime_dir)])

    cp1252_ris = (
        "TY  - JOUR\n"
        "M3  - Article\n"
        "Y1  - 2025\n"
        "T1  - Étude pronostique du côlon\n"
        "N2  - Résumé avec CEA élevé.\n"
        "A1  - Dupont, Éric\n"
        "JF  - Revue Clinique\n"
        "ER  - \n"
    ).encode("cp1252")
    mac_ris = (
        "TY  - JOUR\n"
        "TI  - Prognóstico de cáncer colorrectal\n"
        "AB  - Señales clínicas y supervivencia.\n"
        "AU  - Peña, Ana\n"
        "PY  - 2024\n"
        "T2  - Revista Oncológica\n"
        "ER  - \n"
    ).encode("mac_roman")

    cp_path = write_bytes(tmp_path / "embase" / "cp1252.ris", cp1252_ris)
    mac_path = write_bytes(tmp_path / "wos" / "mac.ris", mac_ris)

    cp_default = runner.invoke(app, ["import", str(cp_path), "--profile", "embase_ris", "--runtime-dir", str(runtime_dir)])
    assert cp_default.exit_code != 0
    assert cp_default.exception is not None

    for args in (
        ["import", str(cp_path), "--profile", "embase_ris", "--runtime-dir", str(runtime_dir), "--encoding", "cp1252"],
        ["import", str(mac_path), "--profile", "wos_ris", "--runtime-dir", str(runtime_dir), "--encoding", "mac_roman"],
    ):
        result = runner.invoke(app, args)
        assert result.exit_code == 0, result.output

    conn = sqlite3.connect(runtime_dir / "dedup.sqlite")
    rows = conn.execute("SELECT title, abstract FROM records ORDER BY id").fetchall()
    source_rows = conn.execute(
        "SELECT profile_name, encoding_used, encoding_source FROM source_files ORDER BY id"
    ).fetchall()
    conn.close()
    titles = {row[0] for row in rows}
    abstracts = {row[1] for row in rows}
    assert "Étude pronostique du côlon" in titles
    assert "Prognóstico de cáncer colorrectal" in titles
    assert "Résumé avec CEA élevé." in abstracts
    assert "Señales clínicas y supervivencia." in abstracts
    assert (source_rows[0][0], source_rows[0][1], source_rows[0][2]) == ("embase_ris", "cp1252", "cli")
    assert (source_rows[1][0], source_rows[1][1], source_rows[1][2]) == ("wos_ris", "mac_roman", "cli")


def test_incremental_import_exact_and_review_roundtrip(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    runner.invoke(app, ["init", "--runtime-dir", str(runtime_dir)])

    pubmed = write_text(
        tmp_path / "pubmed" / "records.nbib",
        """PMID- 12345678
DP  - 2024
TI  - A colorectal prediction model
AB  - Abstract one.
AU  - Zhang S
JT  - Test Journal
PG  - 10-20
LID - 10.1111/test.1 [doi]
PT  - Journal Article

PMID- 87654321
DP  - 2024
TI  - Conference abstract model
AB  - Abstract two.
AU  - Li W
JT  - Test Journal
PG  - 21-22
PT  - Conference Abstract
""",
    )
    embase = write_text(
        tmp_path / "embase" / "records.ris",
        """TY  - JOUR
M3  - Article
Y1  - 2024
T1  - A colorectal prediction model
N2  - Abstract from embase.
A1  - Zhang, San
JF  - Test Journal
DO  - 10.1111/test.1
ER  - 
TY  - JOUR
M3  - Article
Y1  - 2024
T1  - Conference abstract model
N2  - Full article version.
A1  - Li, Wei
JF  - Test Journal
DO  - 10.2222/test.2
ER  - 
""",
    )

    for args in (
        ["import", str(pubmed), "--profile", "pubmed_nbib", "--runtime-dir", str(runtime_dir)],
        ["import", str(embase), "--profile", "embase_ris", "--runtime-dir", str(runtime_dir)],
    ):
        result = runner.invoke(app, args)
        assert result.exit_code == 0, result.output

    exact = runner.invoke(app, ["dedup-exact", "--runtime-dir", str(runtime_dir)])
    assert exact.exit_code == 0, exact.output
    exact_payload = json.loads(exact.stdout)
    assert exact_payload["raw_records"] == 4
    assert exact_payload["exact_clusters"] == 3

    fuzzy = runner.invoke(app, ["dedup-fuzzy", "--runtime-dir", str(runtime_dir)])
    assert fuzzy.exit_code == 0, fuzzy.output
    review_export = runner.invoke(app, ["review-export", "--runtime-dir", str(runtime_dir)])
    assert review_export.exit_code == 0, review_export.output

    queue_path = runtime_dir / "manual_review_queue.csv"
    queue_bytes = queue_path.read_bytes()
    assert not queue_bytes.startswith(b"\xef\xbb\xbf")
    rows = list(csv.DictReader(queue_path.open("r", encoding="utf-8")))
    assert rows, "Expected at least one pending review row"
    rows[0]["decision"] = "separate"
    rows[0]["notes"] = "Conference abstract should remain separate."
    with queue_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    review_import = runner.invoke(
        app,
        ["review-import", str(queue_path), "--runtime-dir", str(runtime_dir)],
    )
    assert review_import.exit_code == 0, review_import.output

    export_result = runner.invoke(app, ["export", "--runtime-dir", str(runtime_dir)])
    assert export_result.exit_code == 0, export_result.output
    sample_path = runtime_dir / "deduplicated_sample.ris"
    sample_result = runner.invoke(
        app,
        ["sample", str(runtime_dir / "deduplicated_records.ris"), str(sample_path), "--count", "1", "--seed", "5"],
    )
    assert sample_result.exit_code == 0, sample_result.output
    assert "profile standard_ris" in sample_result.output
    assert sample_path.read_text(encoding="utf-8").startswith("TY  - ")
    report_result = runner.invoke(app, ["report", "--runtime-dir", str(runtime_dir)])
    assert report_result.exit_code == 0, report_result.output
    assert (runtime_dir / "deduplicated_records.ris").exists()
    assert (runtime_dir / "deduplicated_records.csv").exists()
    assert (runtime_dir / "dedup_report.md").exists()
    assert not (runtime_dir / "deduplicated_records.csv").read_bytes().startswith(b"\xef\xbb\xbf")
    report_payload = json.loads((runtime_dir / "dedup_report.json").read_text(encoding="utf-8"))
    assert report_payload["encoding_by_file"]


def test_export_and_review_support_explicit_output_encodings(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    runner.invoke(app, ["init", "--runtime-dir", str(runtime_dir)])

    pubmed = write_text(
        tmp_path / "pubmed" / "records.nbib",
        """PMID- 12345678
DP  - 2024
TI  - UTF8 export sample
AB  - Abstract one.
AU  - Zhang S
JT  - Test Journal
PG  - 10-20
PT  - Journal Article
""",
    )
    result = runner.invoke(app, ["import", str(pubmed), "--profile", "pubmed_nbib", "--runtime-dir", str(runtime_dir)])
    assert result.exit_code == 0, result.output
    result = runner.invoke(app, ["dedup-exact", "--runtime-dir", str(runtime_dir)])
    assert result.exit_code == 0, result.output

    review_csv = runtime_dir / "manual_review_utf8sig.csv"
    result = runner.invoke(
        app,
        ["review-export", "--runtime-dir", str(runtime_dir), "--output", str(review_csv), "--encoding", "utf-8-sig"],
    )
    assert result.exit_code == 0, result.output
    assert review_csv.read_bytes().startswith(b"\xef\xbb\xbf")

    export_csv = runtime_dir / "dedup_utf8sig.csv"
    export_ris = runtime_dir / "dedup_utf8sig.ris"
    result = runner.invoke(
        app,
        [
            "export",
            "--runtime-dir",
            str(runtime_dir),
            "--csv-output",
            str(export_csv),
            "--ris-output",
            str(export_ris),
            "--csv-encoding",
            "utf-8-sig",
            "--ris-encoding",
            "utf-8",
        ],
    )
    assert result.exit_code == 0, result.output
    assert export_csv.read_bytes().startswith(b"\xef\xbb\xbf")


def test_sample_ris_records_with_seed_is_deterministic(tmp_path: Path) -> None:
    source = write_text(
        tmp_path / "embase" / "records.ris",
        """TY  - JOUR
TI  - First RIS sample record
AU  - Alpha, Ann
ER  - 
TY  - JOUR
TI  - Second RIS sample record
AU  - Beta, Bob
ER  - 
TY  - JOUR
TI  - Third RIS sample record
AU  - Gamma, Gia
ER  - 
""",
    )
    output = tmp_path / "sampled.ris"

    result = runner.invoke(
        app,
        ["sample", str(source), str(output), "--count", "2", "--seed", "11"],
    )
    assert result.exit_code == 0, result.output
    assert "profile embase_ris" in result.output
    first_output = output.read_text(encoding="utf-8")
    assert first_output.count("TY  - JOUR") == 2
    assert "First RIS sample record" not in first_output
    assert first_output.index("Second RIS sample record") < first_output.index("Third RIS sample record")
    assert first_output.endswith("\n")
    assert "\n\nTY  - JOUR" in first_output

    result = runner.invoke(
        app,
        ["sample", str(source), str(output), "--count", "2", "--profile", "embase_ris", "--seed", "11"],
    )
    assert result.exit_code != 0
    assert "already exists" in result.output

    result = runner.invoke(
        app,
        ["sample", str(source), str(output), "--count", "2", "--profile", "embase_ris", "--seed", "11", "--force"],
    )
    assert result.exit_code == 0, result.output
    assert output.read_text(encoding="utf-8") == first_output


def test_sample_nbib_records_with_seed_preserves_nbib_blocks(tmp_path: Path) -> None:
    source = write_text(
        tmp_path / "pubmed" / "records.nbib",
        """PMID- 111
TI  - First NBIB sample record
AU  - Alpha A

PMID- 222
TI  - Second NBIB sample record
AU  - Beta B

PMID- 333
TI  - Third NBIB sample record
AU  - Gamma G
""",
    )
    output = tmp_path / "sampled.nbib"

    result = runner.invoke(app, ["sample", str(source), str(output), "--count", "2", "--seed", "7"])
    assert result.exit_code == 0, result.output
    sampled = output.read_text(encoding="utf-8")
    assert sampled.count("PMID-") == 2
    assert "Third NBIB sample record" not in sampled
    assert sampled.index("Second NBIB sample record") < sampled.index("First NBIB sample record")
    assert "\n\nPMID- " in sampled
    assert sampled.endswith("\n")


def test_sample_rejects_oversized_count(tmp_path: Path) -> None:
    source = write_text(
        tmp_path / "pubmed" / "records.nbib",
        """PMID- 111
TI  - Only NBIB sample record
""",
    )
    output = tmp_path / "sampled.nbib"

    result = runner.invoke(app, ["sample", str(source), str(output), "--count", "2"])
    assert result.exit_code != 0
    assert "Cannot sample 2 records from 1 available records" in result.output
    assert not output.exists()


def test_sample_supports_explicit_input_encoding(tmp_path: Path) -> None:
    source = write_bytes(
        tmp_path / "embase" / "records.ris",
        (
            "TY  - JOUR\n"
            "TI  - Étude pronostique du côlon\n"
            "AU  - Dupont, Éric\n"
            "ER  - \n"
        ).encode("cp1252"),
    )
    output = tmp_path / "sampled.ris"

    result = runner.invoke(
        app,
        ["sample", str(source), str(output), "--count", "1", "--profile", "embase_ris", "--encoding", "cp1252"],
    )
    assert result.exit_code == 0, result.output
    assert "Étude pronostique du côlon" in output.read_text(encoding="utf-8")


def test_sample_generic_ris_defaults_to_standard_ris(tmp_path: Path) -> None:
    source = write_text(
        tmp_path / "records.ris",
        """TY  - JOUR
TI  - Generic RIS sample record
AU  - Alpha, Ann
AB  - Generic abstract.
PY  - 2026
ER  - 
""",
    )
    output = tmp_path / "sampled.ris"

    result = runner.invoke(app, ["sample", str(source), str(output), "--count", "1"])
    assert result.exit_code == 0, result.output
    assert "profile standard_ris" in result.output
    assert "Generic RIS sample record" in output.read_text(encoding="utf-8")


def test_sample_wos_path_still_uses_wos_profile(tmp_path: Path) -> None:
    source = write_text(
        tmp_path / "wos" / "records.ris",
        "\ufeffTY  - JOUR\nTI  - WOS hinted sample record\nAU  - Alpha, Ann\nER  - \n",
    )
    output = tmp_path / "sampled.ris"

    result = runner.invoke(app, ["sample", str(source), str(output), "--count", "1"])
    assert result.exit_code == 0, result.output
    assert "profile wos_ris" in result.output
    assert "WOS hinted sample record" in output.read_text(encoding="utf-8")


def test_sample_runtime_config_missing_standard_ris_uses_builtin_fallback(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    old_config = default_config().model_dump(mode="json")
    old_config["profiles"].pop("standard_ris")
    (runtime_dir / "config.json").write_text(json.dumps(old_config, ensure_ascii=False, indent=2), encoding="utf-8")
    source = write_text(
        tmp_path / "records.ris",
        """TY  - JOUR
TI  - Runtime fallback RIS record
AU  - Alpha, Ann
ER  - 
""",
    )
    output = tmp_path / "sampled.ris"

    result = runner.invoke(
        app,
        ["sample", str(source), str(output), "--count", "1", "--runtime-dir", str(runtime_dir)],
    )
    assert result.exit_code == 0, result.output
    assert "profile standard_ris" in result.output
    assert "Runtime fallback RIS record" in output.read_text(encoding="utf-8")
    saved_config = json.loads((runtime_dir / "config.json").read_text(encoding="utf-8"))
    assert "standard_ris" not in saved_config["profiles"]


def test_profiles_command_outputs_text_and_json(tmp_path: Path) -> None:
    text_result = runner.invoke(app, ["profiles"])
    assert text_result.exit_code == 0, text_result.output
    assert "standard_ris" in text_result.output
    assert "format=ris" in text_result.output
    assert "pubmed_nbib" in text_result.output

    runtime_dir = tmp_path / "runtime"
    runner.invoke(app, ["init", "--runtime-dir", str(runtime_dir)])
    json_result = runner.invoke(app, ["profiles", "--json", "--runtime-dir", str(runtime_dir)])
    assert json_result.exit_code == 0, json_result.output
    payload = json.loads(json_result.stdout)
    assert payload["profiles"]["standard_ris"]["format"] == "ris"
    assert payload["profiles"]["standard_ris"]["title_tags"] == ["TI", "T1"]
    assert "RIS" in payload["source_priority"]


def test_review_import_requires_nonempty_decisions(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    runner.invoke(app, ["init", "--runtime-dir", str(runtime_dir)])
    queue_path = write_text(
        runtime_dir / "manual_review_queue.csv",
        "pair_id,decision,preferred_keeper,notes\n1,,,,\n",
    )
    result = runner.invoke(app, ["review-import", str(queue_path), "--runtime-dir", str(runtime_dir)])
    assert result.exit_code != 0
    assert result.exception is not None
    assert "No manual review decisions were found" in str(result.exception)


def test_review_export_requires_force_when_file_exists(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    runner.invoke(app, ["init", "--runtime-dir", str(runtime_dir)])
    queue_path = write_text(runtime_dir / "manual_review_queue.csv", "already here\n")

    result = runner.invoke(app, ["review-export", "--runtime-dir", str(runtime_dir)])
    assert result.exit_code != 0
    assert "already exists" in result.output
    assert "manual review queue" in result.output
    assert "--force" in result.output

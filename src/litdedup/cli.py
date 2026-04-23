from __future__ import annotations

import json
from pathlib import Path

import typer
from tqdm import tqdm

from litdedup.config import (
    ensure_config,
    ensure_runtime_dir,
    infer_profile_from_path,
    load_config,
    runtime_paths,
    save_config,
    source_priority_map,
    default_config,
)
from litdedup.db import (
    clear_dedup_state,
    connect,
    delete_source_file,
    existing_source_by_hash,
    existing_source_by_path,
    finalize_source_file,
    initialize_database,
    insert_record,
    pending_candidate_count,
    register_source_file,
)
from litdedup.dedup import build_exact_clusters, export_review_queue, import_review_decisions, run_fuzzy_dedup
from litdedup.export import export_deduplicated_csv, export_deduplicated_ris
from litdedup.parsers import count_records, decode_source, file_sha256, normalize_record, parse_file
from litdedup.report import build_report_payload, write_report


app = typer.Typer(add_completion=False, no_args_is_help=True)


def resolve_runtime_paths(runtime_dir: Path | None) -> dict[str, Path]:
    paths = runtime_paths(runtime_dir)
    ensure_runtime_dir(paths["runtime_dir"])
    ensure_config(paths["runtime_dir"])
    conn = connect(paths["db"])
    initialize_database(conn)
    conn.close()
    return paths


def runtime_option(path: Path | None) -> dict[str, Path]:
    return resolve_runtime_paths(path)


@app.command()
def init(
    runtime_dir: Path | None = typer.Option(
        None,
        "--runtime-dir",
        help="Custom runtime directory. Defaults to ./dedup/ under the current working directory.",
    ),
    force: bool = typer.Option(False, "--force", help="Overwrite config.json with defaults."),
) -> None:
    paths = runtime_option(runtime_dir)
    if force or not paths["config"].exists():
        save_config(default_config(), paths["config"])
    typer.echo(f"Runtime initialized at {paths['runtime_dir']}")
    typer.echo(f"Config: {paths['config']}")
    typer.echo(f"Database: {paths['db']}")


@app.command(name="import")
def import_records_cmd(
    files: list[Path] = typer.Argument(..., exists=True, readable=True),
    profile: str | None = typer.Option(None, "--profile", help="Explicit parser profile."),
    encoding: str | None = typer.Option(None, "--encoding", help="Force file decoding encoding for this import run."),
    runtime_dir: Path | None = typer.Option(None, "--runtime-dir", help="Custom runtime directory."),
    force: bool = typer.Option(False, "--force", help="Re-import an already seen file path/hash."),
) -> None:
    paths = runtime_option(runtime_dir)
    conn = connect(paths["db"])
    config = load_config(paths["config"])

    imported_files = 0
    imported_records = 0
    for file_path in files:
        chosen_profile_name = profile or infer_profile_from_path(file_path, config)
        if chosen_profile_name not in config.profiles:
            raise typer.BadParameter(f"Unknown profile: {chosen_profile_name}")
        profile_config = config.profiles[chosen_profile_name]
        file_hash = file_sha256(file_path)
        existing_by_hash = existing_source_by_hash(conn, file_hash)
        existing_by_path = existing_source_by_path(conn, file_path)
        if (existing_by_hash or existing_by_path) and not force:
            source = existing_by_hash or existing_by_path
            typer.echo(
                f"Skipping {file_path}: already imported as {source['path']} with hash {source['file_hash'][:12]}..."
            )
            continue
        if force and existing_by_path:
            clear_dedup_state(conn)
            delete_source_file(conn, file_path)

        decoded = decode_source(file_path, profile_config, override_encoding=encoding)
        total_records = count_records(decoded, profile_config)
        source_file_id, _ = register_source_file(
            conn,
            file_path,
            chosen_profile_name,
            profile_config.format,
            encoding_used=decoded.encoding_used,
            encoding_source=decoded.detection_method,
        )
        clear_dedup_state(conn)
        parsed_records = parse_file(decoded, profile_config)
        for parsed in tqdm(parsed_records, desc=f"Importing {file_path.name}", unit="record", total=total_records or None):
            normalized = normalize_record(parsed, profile_config, file_path)
            insert_record(conn, source_file_id, normalized, parsed.raw_entries)
            imported_records += 1
        finalize_source_file(
            conn,
            source_file_id,
            len(parsed_records),
            encoding_used=decoded.encoding_used,
            encoding_source=decoded.detection_method,
        )
        imported_files += 1
        conn.commit()
        typer.echo(
            f"Imported {len(parsed_records)} records from {file_path} using profile {chosen_profile_name}"
            f" (encoding={decoded.encoding_used}, source={decoded.detection_method})"
        )

    typer.echo(f"Imported files: {imported_files}")
    typer.echo(f"Imported records: {imported_records}")
    conn.close()


@app.command()
def stats(
    runtime_dir: Path | None = typer.Option(None, "--runtime-dir", help="Custom runtime directory."),
) -> None:
    paths = runtime_option(runtime_dir)
    conn = connect(paths["db"])
    config = load_config(paths["config"])
    payload = build_report_payload(conn, config)
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
    conn.close()


@app.command(name="dedup-exact")
def dedup_exact(
    runtime_dir: Path | None = typer.Option(None, "--runtime-dir", help="Custom runtime directory."),
) -> None:
    paths = runtime_option(runtime_dir)
    conn = connect(paths["db"])
    config = load_config(paths["config"])
    metrics = build_exact_clusters(conn, source_priority_map(config))
    typer.echo(json.dumps(metrics, ensure_ascii=False, indent=2))
    conn.close()


@app.command(name="dedup-fuzzy")
def dedup_fuzzy(
    runtime_dir: Path | None = typer.Option(None, "--runtime-dir", help="Custom runtime directory."),
) -> None:
    paths = runtime_option(runtime_dir)
    conn = connect(paths["db"])
    config = load_config(paths["config"])
    metrics = run_fuzzy_dedup(conn, source_priority_map(config))
    typer.echo(json.dumps(metrics, ensure_ascii=False, indent=2))
    conn.close()


@app.command(name="review-export")
def review_export(
    runtime_dir: Path | None = typer.Option(None, "--runtime-dir", help="Custom runtime directory."),
    output: Path | None = typer.Option(None, "--output", help="Custom CSV output path."),
    force: bool = typer.Option(False, "--force", help="Overwrite an existing manual review CSV."),
) -> None:
    paths = runtime_option(runtime_dir)
    conn = connect(paths["db"])
    output_path = output or paths["manual_review_csv"]
    if output_path.exists() and not force:
        raise typer.BadParameter(
            f"{output_path} already exists. Pass --force to overwrite the existing manual review queue."
        )
    metrics = export_review_queue(conn, output_path)
    typer.echo(f"Wrote {metrics['pending_rows']} pending review rows to {output_path}")
    conn.close()


@app.command(name="review-import")
def review_import(
    csv_path: Path = typer.Argument(..., exists=True, readable=True),
    runtime_dir: Path | None = typer.Option(None, "--runtime-dir", help="Custom runtime directory."),
) -> None:
    paths = runtime_option(runtime_dir)
    conn = connect(paths["db"])
    config = load_config(paths["config"])
    metrics = import_review_decisions(conn, csv_path, source_priority_map(config))
    typer.echo(json.dumps(metrics, ensure_ascii=False, indent=2))
    conn.close()


@app.command()
def export(
    runtime_dir: Path | None = typer.Option(None, "--runtime-dir", help="Custom runtime directory."),
    allow_pending: bool = typer.Option(
        False,
        "--allow-pending",
        help="Allow export even if manual review pairs are still pending.",
    ),
    csv_output: Path | None = typer.Option(None, "--csv-output", help="Custom CSV output path."),
    ris_output: Path | None = typer.Option(None, "--ris-output", help="Custom RIS output path."),
) -> None:
    paths = runtime_option(runtime_dir)
    conn = connect(paths["db"])
    pending = pending_candidate_count(conn)
    if pending and not allow_pending:
        raise typer.BadParameter(
            f"There are still {pending} pending review pairs. Resolve them or pass --allow-pending."
        )
    csv_path = csv_output or paths["dedup_csv"]
    ris_path = ris_output or paths["dedup_ris"]
    csv_count = export_deduplicated_csv(conn, csv_path)
    ris_count = export_deduplicated_ris(conn, ris_path)
    typer.echo(f"Exported {csv_count} clusters to {csv_path}")
    typer.echo(f"Exported {ris_count} clusters to {ris_path}")
    conn.close()


@app.command()
def report(
    runtime_dir: Path | None = typer.Option(None, "--runtime-dir", help="Custom runtime directory."),
    markdown_output: Path | None = typer.Option(None, "--markdown-output", help="Custom Markdown report path."),
    json_output: Path | None = typer.Option(None, "--json-output", help="Custom JSON report path."),
) -> None:
    paths = runtime_option(runtime_dir)
    conn = connect(paths["db"])
    config = load_config(paths["config"])
    payload = build_report_payload(conn, config)
    write_report(payload, markdown_output or paths["report_md"], json_output or paths["report_json"])
    typer.echo(f"Wrote report to {markdown_output or paths['report_md']}")
    typer.echo(f"Wrote report JSON to {json_output or paths['report_json']}")
    conn.close()


def main() -> None:
    app()

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


COMMON_SCHEMA = "court-common-parquet/v1"
COLLECTION = "ndcs"
JURISDICTION = "nd"
SOURCE_SYSTEM = "ndcri"
SOURCE_PROFILE = "ndcs_ndcri"
IDENTIFIER_KIND = "case_number"
IDENTIFIER_LABEL = "Case No."
OMIT_NDJSON_TABLES = {
    "docket_entries": "omitted because the current sidecar exceeds GitHub's normal per-file limit; use Parquet",
}


TABLE_COLUMNS: dict[str, list[str]] = {
    "cases": [
        "case_uid",
        "native_case_key",
        "collection",
        "jurisdiction",
        "source_profile",
        "source_system_primary",
        "court_id",
        "court_name",
        "court_location",
        "identifier_kind",
        "identifier_label",
        "identifier_display",
        "identifier_normalized",
        "caption",
        "case_type",
        "status",
        "filed_date",
        "judicial_officer",
        "capture_status",
        "document_count",
        "docket_entry_count",
        "raw_ref_count",
        "source_specific_json",
    ],
    "case_identifiers": [
        "case_uid",
        "collection",
        "source_profile",
        "identifier_kind",
        "identifier_label",
        "identifier_display",
        "identifier_normalized",
        "is_primary",
        "source_specific_json",
    ],
    "courts": [
        "court_id",
        "collection",
        "jurisdiction",
        "source_profile",
        "court_name",
        "court_location",
        "court_type",
        "native_court_label",
        "source_specific_json",
    ],
    "documents": [
        "document_uid",
        "case_uid",
        "collection",
        "source_system",
        "document_number",
        "motion_sequence",
        "title",
        "document_type",
        "filed_date",
        "availability",
        "source_url",
        "sha256",
        "bytes_len",
        "content_type",
        "storage_backend",
        "release_tag",
        "asset_name",
        "download_url",
        "raw_ref_uid",
        "source_specific_json",
    ],
    "parties": [
        "party_uid",
        "case_uid",
        "collection",
        "source_system",
        "name",
        "role",
        "display_role",
        "source_specific_json",
    ],
    "attorneys": [
        "attorney_uid",
        "case_uid",
        "collection",
        "source_system",
        "name",
        "firm",
        "role",
        "source_specific_json",
    ],
    "representation": [
        "representation_uid",
        "case_uid",
        "collection",
        "party_uid",
        "attorney_uid",
        "firm",
        "role",
        "source_specific_json",
    ],
    "docket_entries": [
        "entry_uid",
        "case_uid",
        "collection",
        "source_system",
        "source_profile",
        "sequence",
        "source_index",
        "entry_date",
        "title",
        "text",
        "event_type",
        "motion_sequence",
        "document_uid",
        "has_document",
        "raw_ref_uid",
        "source_specific_json",
    ],
    "calendar": [
        "calendar_uid",
        "case_uid",
        "collection",
        "source_system",
        "appearance_date",
        "part",
        "purpose",
        "judicial_officer",
        "raw_ref_uid",
        "source_specific_json",
    ],
    "motions": [
        "motion_uid",
        "case_uid",
        "collection",
        "source_system",
        "motion_sequence",
        "filed_date",
        "filed_by",
        "relief",
        "status",
        "decision_text",
        "document_uid",
        "raw_ref_uid",
        "source_specific_json",
    ],
    "raw_refs": [
        "raw_ref_uid",
        "case_uid",
        "collection",
        "source_system",
        "source_profile",
        "kind",
        "nested_kind",
        "capture_id",
        "raw_path",
        "html_sha256",
        "sha256",
        "source_url",
        "page_number",
        "table_row_number",
        "source_specific_json",
    ],
    "capture_runs": [
        "capture_run_uid",
        "collection",
        "source_system",
        "source_profile",
        "run_id",
        "observed_at",
        "job_id",
        "status",
        "query_type",
        "query_value",
        "rows_fetched",
        "pages_fetched",
        "source_specific_json",
    ],
    "search_frontier": [
        "frontier_uid",
        "collection",
        "source_system",
        "source_profile",
        "frontier_key",
        "observation_id",
        "observed_at",
        "run_id",
        "job_id",
        "query_type",
        "query_value",
        "status",
        "case_count",
        "lower_bound_count",
        "pages_fetched",
        "rows_fetched",
        "next_action",
        "source_specific_json",
    ],
    "barriers": [
        "barrier_uid",
        "collection",
        "source_system",
        "source_profile",
        "barrier_type",
        "url",
        "method",
        "title",
        "captured_at",
        "resolved_at",
        "raw_ref_uid",
        "source_specific_json",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export NDCS promoted raw runs into data/common tables.")
    parser.add_argument("--data-repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--common-dir", default="")
    parser.add_argument("--no-parquet", action="store_true")
    return parser.parse_args()


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u00a0", " ")).strip()


def json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True, separators=(",", ":"))


def stable_hash(*parts: Any, length: int = 24) -> str:
    payload = "\u001f".join(clean_text(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def rel(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def case_uid(case_number: str) -> str:
    return f"ndcs:ndcri:{clean_text(case_number)}"


def slug(value: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", clean_text(value).lower())).strip("_") or "unknown"


def parse_date(value: Any) -> str:
    text = clean_text(value)
    match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if not match:
        return text
    month, day, year = (int(part) for part in match.groups())
    return f"{year:04d}-{month:02d}-{day:02d}"


def parse_capture_timestamp(value: str) -> str:
    match = re.search(r"(20\d{6}T\d{6})Z?", value)
    if not match:
        return ""
    stamp = match.group(1)
    return f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}T{stamp[9:11]}:{stamp[11:13]}:{stamp[13:15]}Z"


def parse_search_dir(path: Path) -> dict[str, str]:
    name = path.parent.name
    match = re.match(r"(?P<seq>\d+)-(?P<product>\d+)-(?P<from>\d{8})-(?P<to>\d{8})", name)
    if not match:
        return {"job_id": name, "product": "", "date_from": "", "date_to": ""}
    data = match.groupdict()
    return {
        "job_id": name,
        "product": data["product"],
        "date_from": data["from"],
        "date_to": data["to"],
    }


def normalize_case_type_status(case_type: Any, status: Any) -> tuple[str, str]:
    type_text = clean_text(case_type)
    status_text = clean_text(status)
    if status_text:
        return type_text, status_text
    for suffix in ["Closed Dismissed", "Closed as Misdemeanor", "Closed", "Open", "Inactive", "Dismissed"]:
        if type_text.endswith(f" {suffix}"):
            return type_text[: -len(suffix)].strip(), suffix
    return type_text, status_text


def parse_court_location(value: Any) -> tuple[str, str, str, str]:
    text = clean_text(value).removeprefix("--").strip()
    match = re.match(r"(.+?\b(?:County|Municipal|District Court|Supreme Court|Juvenile Court))\b(.*)$", text)
    if match:
        court_name = clean_text(match.group(1))
        officer = clean_text(match.group(2))
    else:
        court_name = text
        officer = ""
    court_type = "municipal" if "Municipal" in court_name else "district"
    return slug(court_name), court_name, text, officer


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def rg_files(root: Path, glob: str) -> list[Path]:
    try:
        proc = subprocess.run(
            ["rg", "--files", str(root), "-g", glob],
            check=True,
            capture_output=True,
            text=True,
        )
        return [Path(line) for line in proc.stdout.splitlines() if line.strip()]
    except Exception:
        return sorted(root.glob(f"**/{glob}"))


def add_raw_ref(raw_refs: dict[str, dict[str, Any]], data_repo: Path, case_id: str, path: Path, **extra: Any) -> str:
    raw_path = rel(data_repo, path)
    uid = f"ndcs:ndcri:raw:{stable_hash(case_id, raw_path, extra.get('kind'), extra.get('table_row_number'))}"
    raw_refs.setdefault(
        uid,
        {
            "raw_ref_uid": uid,
            "case_uid": case_id,
            "collection": COLLECTION,
            "source_system": SOURCE_SYSTEM,
            "source_profile": SOURCE_PROFILE,
            "kind": clean_text(extra.get("kind")),
            "nested_kind": clean_text(extra.get("nested_kind")),
            "capture_id": clean_text(extra.get("capture_id")),
            "raw_path": raw_path,
            "html_sha256": clean_text(extra.get("html_sha256")),
            "sha256": clean_text(extra.get("sha256")),
            "source_url": clean_text(extra.get("source_url")),
            "page_number": extra.get("page_number"),
            "table_row_number": extra.get("table_row_number"),
            "source_specific_json": json_dumps(extra),
        },
    )
    return uid


def write_json_if_changed(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return
    path.write_text(text, encoding="utf-8")


def write_ndjson_if_changed(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    if path.exists() and path.read_bytes() == tmp_path.read_bytes():
        tmp_path.unlink()
        return
    tmp_path.replace(path)


def main() -> int:
    args = parse_args()
    data_repo = Path(args.data_repo).resolve()
    common_dir = Path(args.common_dir).resolve() if args.common_dir else data_repo / "data" / "common"
    common_dir.mkdir(parents=True, exist_ok=True)

    case_state: dict[str, dict[str, Any]] = {}
    raw_refs: dict[str, dict[str, Any]] = {}
    docket_entries: dict[str, dict[str, Any]] = {}
    documents: dict[str, dict[str, Any]] = {}
    capture_runs: dict[str, dict[str, Any]] = {}
    search_frontier: dict[str, dict[str, Any]] = {}
    courts_seen: dict[str, dict[str, Any]] = {}

    index_files = sorted(data_repo.glob("raw/runs/**/product-*-date_filed-*.index.json"))
    for index_path in index_files:
        try:
            obj = read_json(index_path)
        except Exception:
            continue
        rows = obj.get("rows") if isinstance(obj.get("rows"), list) else []
        summary = obj.get("summary") if isinstance(obj.get("summary"), dict) else {}
        search = parse_search_dir(index_path)
        run_id = rel(data_repo / "raw" / "runs", index_path).split("/")[0]
        observed_at = parse_capture_timestamp(index_path.name) or parse_capture_timestamp(str(index_path))
        query_value = f"product={search['product']} date_filed={search['date_from']}..{search['date_to']}"
        run_uid = f"ndcs:ndcri:capture-run:{stable_hash(run_id, search['job_id'], index_path.name)}"
        capture_runs[run_uid] = {
            "capture_run_uid": run_uid,
            "collection": COLLECTION,
            "source_system": SOURCE_SYSTEM,
            "source_profile": SOURCE_PROFILE,
            "run_id": run_id,
            "observed_at": observed_at,
            "job_id": search["job_id"],
            "status": "capped" if summary.get("capped") else "complete",
            "query_type": "date_filed",
            "query_value": query_value,
            "rows_fetched": int(summary.get("row_count") or len(rows)),
            "pages_fetched": None,
            "source_specific_json": json_dumps({"summary": summary, "path": rel(data_repo, index_path)}),
        }
        frontier_uid = f"ndcs:ndcri:frontier:{stable_hash(search['job_id'], index_path.name)}"
        search_frontier[frontier_uid] = {
            "frontier_uid": frontier_uid,
            "collection": COLLECTION,
            "source_system": SOURCE_SYSTEM,
            "source_profile": SOURCE_PROFILE,
            "frontier_key": search["job_id"],
            "observation_id": index_path.stem,
            "observed_at": observed_at,
            "run_id": run_id,
            "job_id": search["job_id"],
            "query_type": "date_filed",
            "query_value": query_value,
            "status": "capped" if summary.get("capped") else "complete",
            "case_count": int(summary.get("row_count") or len(rows)),
            "lower_bound_count": int(summary.get("cap_limit") or 0) if summary.get("capped") else None,
            "pages_fetched": None,
            "rows_fetched": int(summary.get("row_count") or len(rows)),
            "next_action": "",
            "source_specific_json": json_dumps({"summary": summary, "path": rel(data_repo, index_path)}),
        }
        html_path = index_path.with_suffix(".html")
        for idx, row in enumerate(rows, start=1):
            number = clean_text(row.get("case_number"))
            if not number:
                continue
            uid = case_uid(number)
            type_text, status_text = normalize_case_type_status(row.get("case_type"), row.get("case_status"))
            court_id, court_name, court_location, officer = parse_court_location(row.get("court_location"))
            courts_seen.setdefault(
                court_id,
                {
                    "court_id": court_id,
                    "collection": COLLECTION,
                    "jurisdiction": JURISDICTION,
                    "source_profile": SOURCE_PROFILE,
                    "court_name": court_name,
                    "court_location": court_location,
                    "court_type": "municipal" if "Municipal" in court_name else "district",
                    "native_court_label": court_location,
                    "source_specific_json": json_dumps({}),
                },
            )
            state = case_state.setdefault(
                number,
                {
                    "case_number": number,
                    "caption": "",
                    "case_type": "",
                    "status": "",
                    "filed_date": "",
                    "court_id": "",
                    "court_name": "",
                    "court_location": "",
                    "judicial_officer": "",
                    "detail_url": "",
                    "index_refs": [],
                    "detail_refs": [],
                    "has_detail": False,
                    "docket_entry_count": 0,
                    "document_count": 0,
                },
            )
            for target, value in [
                ("caption", row.get("style")),
                ("case_type", type_text),
                ("status", status_text),
                ("filed_date", parse_date(row.get("filing_date"))),
                ("court_id", court_id),
                ("court_name", court_name),
                ("court_location", court_location),
                ("judicial_officer", officer),
                ("detail_url", row.get("detail_url")),
            ]:
                if clean_text(value) and not clean_text(state.get(target)):
                    state[target] = clean_text(value)
            ref_uid = add_raw_ref(
                raw_refs,
                data_repo,
                uid,
                index_path,
                kind="index",
                source_url=row.get("detail_url"),
                table_row_number=idx,
                run_id=run_id,
                job_id=search["job_id"],
            )
            state["index_refs"].append(ref_uid)
            if html_path.exists():
                state["index_refs"].append(add_raw_ref(raw_refs, data_repo, uid, html_path, kind="index_html", table_row_number=idx))

    detail_files = sorted(rg_files(data_repo / "raw" / "runs", "detail-summary.json"))
    for summary_path in detail_files:
        try:
            summary = read_json(summary_path)
        except Exception:
            continue
        number = clean_text(summary.get("case_number") or summary_path.parent.name)
        if not number:
            continue
        uid = case_uid(number)
        state = case_state.setdefault(
            number,
            {
                "case_number": number,
                "caption": "",
                "case_type": "",
                "status": "",
                "filed_date": "",
                "court_id": "",
                "court_name": "",
                "court_location": "",
                "judicial_officer": "",
                "detail_url": "",
                "index_refs": [],
                "detail_refs": [],
                "has_detail": False,
                "docket_entry_count": 0,
                "document_count": 0,
            },
        )
        state["has_detail"] = True
        ref_uid = add_raw_ref(raw_refs, data_repo, uid, summary_path, kind="detail_summary")
        state["detail_refs"].append(ref_uid)
        html_path = summary_path.parent / "detail.html"
        if html_path.exists():
            state["detail_refs"].append(add_raw_ref(raw_refs, data_repo, uid, html_path, kind="detail_html"))
        entries = summary.get("indexed_entries") if isinstance(summary.get("indexed_entries"), list) else []
        for source_index, entry in enumerate(entries, start=1):
            source_no = clean_text(entry.get("index_number") or source_index)
            entry_uid = f"ndcs:ndcri:docket:{stable_hash(uid, source_no, entry.get('date'), entry.get('title'), entry.get('description'))}"
            docket_entries.setdefault(
                entry_uid,
                {
                    "entry_uid": entry_uid,
                    "case_uid": uid,
                    "collection": COLLECTION,
                    "source_system": SOURCE_SYSTEM,
                    "source_profile": SOURCE_PROFILE,
                    "sequence": int(source_no) if source_no.isdigit() else source_index,
                    "source_index": source_no,
                    "entry_date": parse_date(entry.get("date")),
                    "title": clean_text(entry.get("title")),
                    "text": clean_text(entry.get("description")),
                    "event_type": clean_text(entry.get("title")),
                    "motion_sequence": "",
                    "document_uid": "",
                    "has_document": False,
                    "raw_ref_uid": ref_uid,
                    "source_specific_json": json_dumps({"summary_path": rel(data_repo, summary_path), **entry}),
                },
            )
        links = summary.get("document_links") if isinstance(summary.get("document_links"), list) else []
        for link_index, link in enumerate(links, start=1):
            href = clean_text(link.get("href") or link.get("url") or link.get("document_url"))
            doc_uid = f"ndcs:ndcri:document:{stable_hash(uid, link_index, href, link.get('text'), link.get('title'))}"
            documents.setdefault(
                doc_uid,
                {
                    "document_uid": doc_uid,
                    "case_uid": uid,
                    "collection": COLLECTION,
                    "source_system": SOURCE_SYSTEM,
                    "document_number": clean_text(link.get("index_number") or link_index),
                    "motion_sequence": "",
                    "title": clean_text(link.get("text") or link.get("title")),
                    "document_type": "",
                    "filed_date": parse_date(link.get("date")),
                    "availability": "link_only",
                    "source_url": href,
                    "sha256": "",
                    "bytes_len": None,
                    "content_type": "",
                    "storage_backend": "",
                    "release_tag": "",
                    "asset_name": "",
                    "download_url": "",
                    "raw_ref_uid": ref_uid,
                    "source_specific_json": json_dumps({"summary_path": rel(data_repo, summary_path), **link}),
                },
            )
        state["docket_entry_count"] = max(state["docket_entry_count"], len(entries))
        state["document_count"] = max(state["document_count"], len(links))

    tables = {name: [] for name in TABLE_COLUMNS}
    for number, state in sorted(case_state.items()):
        uid = case_uid(number)
        raw_ref_count = len(set(state["index_refs"] + state["detail_refs"]))
        capture_status = "detail_with_roa" if state["has_detail"] and state["docket_entry_count"] else "detail_no_roa" if state["has_detail"] else "index_only"
        source_specific = {
            "index_raw_ref_uids": sorted(set(state["index_refs"])),
            "detail_raw_ref_uids": sorted(set(state["detail_refs"])),
        }
        tables["cases"].append(
            {
                "case_uid": uid,
                "native_case_key": number,
                "collection": COLLECTION,
                "jurisdiction": JURISDICTION,
                "source_profile": SOURCE_PROFILE,
                "source_system_primary": SOURCE_SYSTEM,
                "court_id": clean_text(state["court_id"]) or "unknown_court",
                "court_name": clean_text(state["court_name"]),
                "court_location": clean_text(state["court_location"]),
                "identifier_kind": IDENTIFIER_KIND,
                "identifier_label": IDENTIFIER_LABEL,
                "identifier_display": number,
                "identifier_normalized": number,
                "caption": clean_text(state["caption"]),
                "case_type": clean_text(state["case_type"]),
                "status": clean_text(state["status"]),
                "filed_date": clean_text(state["filed_date"]),
                "judicial_officer": clean_text(state["judicial_officer"]),
                "capture_status": capture_status,
                "document_count": state["document_count"],
                "docket_entry_count": state["docket_entry_count"],
                "raw_ref_count": raw_ref_count,
                "source_specific_json": json_dumps(source_specific),
            }
        )
        tables["case_identifiers"].append(
            {
                "case_uid": uid,
                "collection": COLLECTION,
                "source_profile": SOURCE_PROFILE,
                "identifier_kind": IDENTIFIER_KIND,
                "identifier_label": IDENTIFIER_LABEL,
                "identifier_display": number,
                "identifier_normalized": number,
                "is_primary": True,
                "source_specific_json": json_dumps({}),
            }
        )
    tables["courts"] = sorted(courts_seen.values(), key=lambda row: row["court_id"])
    tables["documents"] = sorted(documents.values(), key=lambda row: (row["case_uid"], row["document_uid"]))
    tables["docket_entries"] = sorted(docket_entries.values(), key=lambda row: (row["case_uid"], row["sequence"], row["entry_uid"]))
    tables["raw_refs"] = sorted(raw_refs.values(), key=lambda row: (row["case_uid"], row["raw_ref_uid"]))
    tables["capture_runs"] = sorted(capture_runs.values(), key=lambda row: row["capture_run_uid"])
    tables["search_frontier"] = sorted(search_frontier.values(), key=lambda row: row["frontier_uid"])

    write_parquet = not args.no_parquet
    table_manifest = {}
    for name, columns in TABLE_COLUMNS.items():
        rows = [{column: row.get(column) for column in columns} for row in tables.get(name, [])]
        tables[name] = rows
        ndjson_path = common_dir / f"{name}.ndjson"
        parquet_path = common_dir / f"{name}.parquet"
        table_manifest[name] = {
            "rows": len(rows),
            "columns": columns,
        }
        omitted_reason = OMIT_NDJSON_TABLES.get(name)
        if omitted_reason:
            if ndjson_path.exists():
                ndjson_path.unlink()
            table_manifest[name]["ndjson_omitted_reason"] = omitted_reason
        else:
            write_ndjson_if_changed(ndjson_path, rows)
            table_manifest[name]["ndjson_path"] = ndjson_path.relative_to(data_repo).as_posix()
        if write_parquet:
            pd.DataFrame(rows, columns=columns).to_parquet(parquet_path, index=False)
            table_manifest[name]["parquet_path"] = parquet_path.relative_to(data_repo).as_posix()

    capture_status_counts = Counter(row.get("capture_status") for row in tables["cases"])
    manifest = {
        "schema": COMMON_SCHEMA,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "collection": COLLECTION,
        "jurisdiction": JURISDICTION,
        "source_system": SOURCE_SYSTEM,
        "source_profile": SOURCE_PROFILE,
        "source_profiles": {
            SOURCE_PROFILE: {
                "identifier_label": IDENTIFIER_LABEL,
                "identifier_kind": IDENTIFIER_KIND,
                "primary_facets": ["court_id", "case_type", "status", "capture_status"],
                "case_route": "/ndcs/case/:identifier",
            }
        },
        "inputs": {
            "index_files": len(index_files),
            "detail_summary_files": len(detail_files),
        },
        "tables": table_manifest,
        "limitations": [
            "Party and attorney tables are empty until NDCS detail pages are parsed beyond indexed register entries.",
            "Document rows are emitted only when detail summaries expose document links; promoted summaries currently expose no document links.",
            "Court location strings are normalized from search result rows and may include judicial-officer text when the source display omits a delimiter.",
        ],
        "summary": {
            "cases": len(tables["cases"]),
            "courts": len(tables["courts"]),
            "documents": len(tables["documents"]),
            "docket_entries": len(tables["docket_entries"]),
            "raw_refs": len(tables["raw_refs"]),
            "capture_runs": len(tables["capture_runs"]),
            "search_frontier_rows": len(tables["search_frontier"]),
            "capture_status_counts": dict(capture_status_counts),
        },
    }
    write_json_if_changed(common_dir / "manifest.json", manifest)
    print(json.dumps(manifest["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

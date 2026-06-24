# NDCS Data

Public companion data repository for the North Dakota Court Search archive tooling.

The product/tooling repo stays small. This repo receives promoted public-record capture output: search indexes, parsed docket/detail manifests, run summaries, and derived coverage/index files.

## Layout

```text
raw/runs/<run-id>/          public-safe promoted run artifacts
archive/cases/             canonical per-case JSON, once normalized
archive/cases-index.ndjson append-oriented case provenance index
data/manifest.json         repository-level manifest
data/common/               common court-viewer derived tables
coverage/                  coverage summaries
promotions/                promotion manifests
```

## Common Export

`scripts/export_common_ndcs_data.py` derives `data/common/*` from promoted
`raw/runs/**` search indexes and detail summaries.

As of 2026-06-24, the common export contains 38,771 cases, 69 courts, 307,672
docket entries, 117,304 raw refs, 510 capture runs, and 510 search-frontier
rows. Promoted detail summaries currently expose no document links, so
`data/common/documents.*` is schema-valid but empty.

`docket_entries` is Parquet-only because its NDJSON sidecar exceeds GitHub's
normal per-file limit. Smaller tables keep NDJSON sidecars for inspection.

## Promotion Rules

The promoter excludes operational CAPTCHA artifacts, bridge tokens, local secrets, browser profiles, logs, and raw HTML by default. Worker/result JSON is sanitized before commit to remove CAPTCHA answers, suggestions, validation hashes, image paths, and embedded HTML.

Document bytes, if later captured, should use a separate content-addressed storage path or release-asset flow rather than large direct git commits unless explicitly enabled.

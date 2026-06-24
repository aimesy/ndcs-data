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

As of 2026-06-24, the common export contains 38,771 cases, 69 courts, 308,741
docket entries, 118,673 raw refs, 517 capture runs, and 517 search-frontier
rows. Promoted detail summaries currently expose no document links, so
`data/common/documents.*` is schema-valid but empty.

Case-scoped high-volume NDJSON sidecars are sharded by case-number county,
year, and type under `data/common/shards/<table>/`, with per-table shard
manifests. Smaller tables keep top-level NDJSON sidecars for inspection.

## Promotion Rules

The promoter excludes operational CAPTCHA artifacts, bridge tokens, local secrets, browser profiles, logs, and raw HTML by default. Worker/result JSON is sanitized before commit to remove CAPTCHA answers, suggestions, validation hashes, image paths, and embedded HTML.

Document bytes, if later captured, should use a separate content-addressed storage path or release-asset flow rather than large direct git commits unless explicitly enabled.

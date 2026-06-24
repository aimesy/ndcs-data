# NDCS Data

Public companion data repository for the North Dakota Court Search archive tooling.

The product/tooling repo stays small. This repo receives promoted public-record capture output: search indexes, parsed docket/detail manifests, run summaries, and derived coverage/index files.

## Layout

```text
raw/runs/<run-id>/          public-safe promoted run artifacts
archive/cases/             canonical per-case JSON, once normalized
archive/cases-index.ndjson append-oriented case provenance index
data/manifest.json         repository-level manifest
coverage/                  coverage summaries
promotions/                promotion manifests
```

## Promotion Rules

The promoter excludes operational CAPTCHA artifacts, bridge tokens, local secrets, browser profiles, logs, and raw HTML by default. Worker/result JSON is sanitized before commit to remove CAPTCHA answers, suggestions, validation hashes, image paths, and embedded HTML.

Document bytes, if later captured, should use a separate content-addressed storage path or release-asset flow rather than large direct git commits unless explicitly enabled.


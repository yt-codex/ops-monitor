# Ops Monitor

Ops Monitor consolidates run health across independent GitHub repos by reading a standardized `ops/probe.json` from each repo and publishing a plain, fast dashboard on GitHub Pages.

## What this repo contains

- `templates/probe-emitter/emit_probe.py`: drop-in script for source repos to write `ops/probe.json`.
- `templates/probe-emitter/workflow.yml`: scheduled workflow template that runs your workload, emits probe, and commits `ops/probe.json`.
- `scripts/collect_probes.py`: central collector that fetches probes, computes severities, stores small history baselines, and renders docs.
- `scripts/send_telegram_alerts.py`: optional WARN/FAIL alert sender.
- `.github/workflows/monitor.yml`: scheduled monitor workflow + Pages deployment.
- `schemas/probe.schema.json`: probe contract reference.

## Probe contract (`ops/probe.json`)

Expected shape:

- `status`: `OK | WARN | FAIL`
- `last_run_time`: ISO-8601 UTC
- `duration_seconds`
- `freshness.max_date` and/or `freshness.lag_seconds`
- `row_counts` (object)
- `schema_hash` (string)
- `key_checks[]` (`name`, `status`, optional `detail`, `metric`)
- `warnings[]`
- `artifact_links[]` (`label`, `url`)

## 1) Add per-repo probe emitter

In each monitored repo:

1. Copy [emit_probe.py](templates/probe-emitter/emit_probe.py) to `ops/emit_probe.py`.
2. Add a workflow based on [workflow.yml](templates/probe-emitter/workflow.yml).
3. Replace the placeholder workload block with your actual scheduled task.
4. Keep the emitter step `if: always()` and `continue-on-error: true` so emission stays non-blocking.

## 2) Configure this monitor repo

Edit [repos.json](config/repos.json):

- Replace placeholder entries with real `owner/repo`.
- Optionally override per-repo `branch`, `probe_path`, `freshness_warn_hours`, `freshness_fail_hours`, and `duration_warn_multiplier`.

## 3) GitHub settings/secrets

Required for cross-repo reads (especially private repos):

- `PROBE_READ_TOKEN`: fine-grained PAT with read access to monitored repos (`contents:read`).

Optional Telegram alerts:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Optional GitHub variable:

- `DASHBOARD_BASE_URL` (defaults to `https://<owner>.github.io/<repo>/`)

## 4) Publish to GitHub Pages

This repo already includes [monitor.yml](.github/workflows/monitor.yml), which:

1. Runs on schedule/manual dispatch.
2. Fetches probes and renders `docs/index.html` + `docs/<repo>.html`.
3. Writes `data/history/*.json` for baseline/severity computation.
4. Commits generated outputs.
5. Deploys `docs/` via GitHub Pages Actions.

Enable Pages in repo settings with source set to `GitHub Actions`.

## Local run

```bash
python scripts/collect_probes.py --config config/repos.json
```

Outputs:

- `data/latest.json`
- `data/history/<repo>.json`
- `data/details/<repo>.json`
- `docs/index.html`
- `docs/<repo>.html` and `docs/<repo>.json`

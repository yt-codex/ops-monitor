#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

ALLOWED_STATUSES = {"OK", "WARN", "FAIL"}
STATUS_SORT = {"FAIL": 0, "WARN": 1, "OK": 2}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
            parsed = date.fromisoformat(text)
            return datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc)
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return fallback


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def slugify_repo(repo: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", repo.strip().lower()).strip("-")


def format_seconds(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    value = max(0.0, float(seconds))
    if 0 < value < 1:
        return "<1s"
    whole = int(value)
    days, rem = divmod(whole, 86_400)
    hours, rem = divmod(rem, 3_600)
    minutes, sec = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def format_timestamp(value: str | None) -> str:
    parsed = parse_dt(value)
    if parsed is None:
        return "n/a"
    return parsed.strftime("%Y-%m-%d %H:%M UTC")


def fetch_probe(
    owner: str,
    repo: str,
    branch: str,
    probe_path: str,
    token: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    encoded_path = "/".join(urllib.parse.quote(seg, safe="") for seg in probe_path.split("/"))
    api_url = (
        f"https://api.github.com/repos/{owner}/{repo}/contents/{encoded_path}"
        f"?ref={urllib.parse.quote(branch, safe='')}"
    )
    headers = {
        "Accept": "application/vnd.github.raw",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "ops-monitor",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(api_url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload), None
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None, f"probe not found ({probe_path} on {branch})"
        return None, f"http {exc.code}"
    except urllib.error.URLError as exc:
        return None, f"network error: {exc.reason}"
    except json.JSONDecodeError:
        return None, "invalid JSON in probe"


def ensure_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def normalize_artifacts(items: Any) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for item in ensure_list(items):
        if isinstance(item, dict):
            label = str(item.get("label", "artifact")).strip() or "artifact"
            url = str(item.get("url", "")).strip()
            if url:
                normalized.append({"label": label, "url": url})
        elif isinstance(item, str) and item.strip():
            normalized.append({"label": "artifact", "url": item.strip()})
    return normalized


def normalize_checks(items: Any) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in ensure_list(items):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "check")).strip() or "check"
        status = str(item.get("status", "WARN")).upper().strip()
        if status not in ALLOWED_STATUSES:
            status = "WARN"
        detail = str(item.get("detail", "")).strip()
        entry = {"name": name, "status": status, "detail": detail}
        if "metric" in item:
            entry["metric"] = item["metric"]
        normalized.append(entry)
    return normalized


def normalize_row_counts(items: Any) -> dict[str, int | float]:
    if not isinstance(items, dict):
        return {}
    normalized: dict[str, int | float] = {}
    for key, value in items.items():
        number = to_int(value)
        if number is None:
            float_number = to_float(value)
            if float_number is None:
                continue
            normalized[str(key)] = round(float_number, 4)
            continue
        normalized[str(key)] = number
    return normalized


def median_duration(history: list[dict[str, Any]]) -> float | None:
    values: list[float] = []
    for row in history:
        if str(row.get("status", "")).upper() != "OK":
            continue
        duration = to_float(row.get("duration_seconds"))
        if duration is not None and duration > 0:
            values.append(duration)
    if not values:
        return None
    return statistics.median(values)


def severity_for(
    base_status: str,
    lag_seconds: float | None,
    warn_lag_seconds: int | None,
    fail_lag_seconds: int | None,
    duration_seconds: float | None,
    baseline_duration_seconds: float | None,
    duration_warn_multiplier: float | None,
) -> tuple[str, list[str]]:
    severity = base_status
    reasons: list[str] = []

    if (
        lag_seconds is not None
        and fail_lag_seconds is not None
        and lag_seconds > fail_lag_seconds
    ):
        severity = "FAIL"
        reasons.append(f"freshness lag {format_seconds(lag_seconds)} exceeds fail threshold")
    elif (
        lag_seconds is not None
        and warn_lag_seconds is not None
        and lag_seconds > warn_lag_seconds
        and severity == "OK"
    ):
        severity = "WARN"
        reasons.append(f"freshness lag {format_seconds(lag_seconds)} exceeds warn threshold")

    if (
        severity == "OK"
        and duration_seconds is not None
        and baseline_duration_seconds is not None
        and duration_warn_multiplier
        and baseline_duration_seconds > 0
        and duration_seconds > (baseline_duration_seconds * duration_warn_multiplier)
    ):
        severity = "WARN"
        reasons.append(
            f"duration {duration_seconds:.1f}s exceeded baseline {baseline_duration_seconds:.1f}s"
        )
    return severity, reasons


def pick_top_warning(
    warnings: list[str],
    checks: list[dict[str, Any]],
    reasons: list[str],
    fetch_error: str | None,
) -> str:
    for warning in warnings:
        clean = warning.strip()
        if clean:
            return clean
    for check in checks:
        if check.get("status") in {"WARN", "FAIL"}:
            detail = str(check.get("detail", "")).strip()
            if detail:
                return f"{check.get('name')}: {detail}"
            return f"{check.get('name')} is {check.get('status')}"
    if reasons:
        return reasons[0]
    if fetch_error:
        return fetch_error
    return "None"


def probe_url(owner: str, repo: str, branch: str, probe_path: str) -> str:
    return f"https://github.com/{owner}/{repo}/blob/{branch}/{probe_path}"


def run_url(owner: str, repo: str, run_id: Any) -> str | None:
    if not run_id:
        return None
    return f"https://github.com/{owner}/{repo}/actions/runs/{run_id}"


def html_template(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    :root {{
      --bg: #f8fafc;
      --card: #ffffff;
      --text: #0f172a;
      --muted: #475569;
      --ok: #166534;
      --ok-bg: #dcfce7;
      --warn: #854d0e;
      --warn-bg: #fef3c7;
      --fail: #991b1b;
      --fail-bg: #fee2e2;
      --line: #e2e8f0;
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Helvetica Neue", sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.4;
    }}
    main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 24px 16px 40px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 1.5rem;
    }}
    p {{
      color: var(--muted);
      margin: 0 0 16px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 10px;
      overflow: hidden;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 10px 12px;
      text-align: left;
      vertical-align: top;
      font-size: 0.95rem;
    }}
    th {{
      background: #f1f5f9;
      font-weight: 600;
    }}
    tr:last-child td {{
      border-bottom: none;
    }}
    .badge {{
      display: inline-block;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.02em;
    }}
    .OK {{
      color: var(--ok);
      background: var(--ok-bg);
    }}
    .WARN {{
      color: var(--warn);
      background: var(--warn-bg);
    }}
    .FAIL {{
      color: var(--fail);
      background: var(--fail-bg);
    }}
    code {{
      background: #e2e8f0;
      border-radius: 4px;
      padding: 1px 4px;
      font-size: 0.9em;
      display: inline-block;
      max-width: 100%;
      overflow-wrap: anywhere;
      word-break: break-all;
      white-space: normal;
    }}
    .meta-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }}
    .meta-card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 12px;
      overflow-wrap: anywhere;
    }}
    .meta-title {{
      font-size: 0.76rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--muted);
      margin-bottom: 4px;
    }}
    .help {{
      cursor: help;
      border-bottom: 1px dotted #94a3b8;
    }}
    a {{
      color: #0f766e;
      text-decoration: none;
    }}
    a:hover {{
      text-decoration: underline;
    }}
    ul {{
      margin: 0;
      padding-left: 20px;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .mono {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      font-size: 0.85rem;
      word-break: break-word;
    }}
  </style>
</head>
<body>
  <main>
    {body}
  </main>
</body>
</html>
"""


def render_index(summary: dict[str, Any]) -> str:
    root_url = str(summary.get("dashboard_base_url") or "").strip() or "../"
    generated = escape(format_timestamp(summary.get("generated_at")))
    body = (
        f"<h1>Ops Monitor</h1>"
        f"<p>Generated {generated}. This path now redirects to the root overview dashboard.</p>"
        f"<p><a href=\"{escape(root_url, quote=True)}\">Go to dashboard</a></p>"
        f"<script>window.location.replace({json.dumps(root_url)});</script>"
        f"<noscript><p class=\"subtle\">JavaScript is disabled. Use the link above.</p></noscript>"
    )
    return html_template("Ops Monitor", body)


def format_html_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return str(value)
    return json.dumps(value, sort_keys=True)


def render_detail(detail: dict[str, Any]) -> str:
    dashboard_home_url = str(detail.get("dashboard_home_url") or "../")
    last_run_help = "UTC timestamp of the latest emitted probe run."
    freshness_help = (
        "How stale data is: now minus freshness.max_date (or last_run_time when max_date is missing)."
    )
    duration_help = "Runtime reported by the probe for the latest run."
    schema_hash_help = (
        "Fingerprint of source schema shape. Changes often indicate schema drift."
    )

    warnings_html = (
        "<ul>" + "".join(f"<li>{escape(item)}</li>" for item in detail.get("warnings", [])) + "</ul>"
        if detail.get("warnings")
        else "<p class=\"subtle\">No warnings.</p>"
    )
    artifacts = detail.get("artifact_links", [])
    artifacts_html = (
        "<ul>"
        + "".join(
            f"<li><a href=\"{escape(item['url'])}\">{escape(item['label'])}</a></li>"
            for item in artifacts
        )
        + "</ul>"
        if artifacts
        else "<p class=\"subtle\">No artifacts.</p>"
    )

    checks_rows = []
    for check in detail.get("key_checks", []):
        metric = format_html_value(check.get("metric")) if "metric" in check else ""
        checks_rows.append(
            "<tr>"
            f"<td>{escape(str(check.get('name', 'check')))}</td>"
            f"<td><span class=\"badge {escape(str(check.get('status', 'WARN')))}\">"
            f"{escape(str(check.get('status', 'WARN')))}</span></td>"
            f"<td>{escape(str(check.get('detail', '')))}</td>"
            f"<td class=\"mono\">{escape(metric)}</td>"
            "</tr>"
        )
    checks_html = (
        "<div class=\"card\"><table><thead><tr><th>Check</th><th>Status</th><th>Detail</th><th>Metric</th></tr></thead>"
        f"<tbody>{''.join(checks_rows)}</tbody></table></div>"
        if checks_rows
        else "<p class=\"subtle\">No key checks recorded.</p>"
    )

    row_count_rows = []
    for name, value in detail.get("row_counts", {}).items():
        row_count_rows.append(
            "<tr>"
            f"<td>{escape(str(name))}</td>"
            f"<td>{escape(str(value))}</td>"
            "</tr>"
        )
    row_counts_html = (
        "<div class=\"card\"><table><thead><tr><th>Dataset</th><th>Rows</th></tr></thead>"
        f"<tbody>{''.join(row_count_rows)}</tbody></table></div>"
        if row_count_rows
        else "<p class=\"subtle\">No row counts provided.</p>"
    )

    meta = detail.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    meta_rows = []
    for key, value in sorted(meta.items()):
        meta_rows.append(
            "<tr>"
            f"<td>{escape(str(key))}</td>"
            f"<td class=\"mono\">{escape(format_html_value(value))}</td>"
            "</tr>"
        )
    meta_html = (
        "<div class=\"card\"><table><thead><tr><th>Field</th><th>Value</th></tr></thead>"
        f"<tbody>{''.join(meta_rows)}</tbody></table></div>"
        if meta_rows
        else "<p class=\"subtle\">No meta fields provided.</p>"
    )

    body = (
        f"<p><a href=\"{escape(dashboard_home_url, quote=True)}\">Back to dashboard</a> | "
        f"<a href=\"{escape(detail['slug'])}.json\">Raw JSON</a></p>"
        f"<h1>{escape(detail['full_name'])}</h1>"
        "<div class=\"meta-grid\">"
        f"<div class=\"meta-card\"><div class=\"meta-title\">Severity</div>"
        f"<span class=\"badge {escape(detail['severity'])}\">{escape(detail['severity'])}</span></div>"
        f"<div class=\"meta-card\"><div class=\"meta-title\">Probe Status</div>"
        f"<span class=\"badge {escape(detail['status'])}\">{escape(detail['status'])}</span></div>"
        f"<div class=\"meta-card\"><div class=\"meta-title\"><span class=\"help\" title=\"{escape(last_run_help, quote=True)}\">Last Run</span></div>{escape(format_timestamp(detail.get('last_run_time')))}</div>"
        f"<div class=\"meta-card\"><div class=\"meta-title\"><span class=\"help\" title=\"{escape(freshness_help, quote=True)}\">Freshness Lag</span></div>{escape(detail.get('freshness_lag_human', 'n/a'))}</div>"
        f"<div class=\"meta-card\"><div class=\"meta-title\"><span class=\"help\" title=\"{escape(duration_help, quote=True)}\">Duration</span></div>{escape(detail.get('duration_human', 'n/a'))}</div>"
        f"<div class=\"meta-card\"><div class=\"meta-title\">Schema Version</div><code>{escape(detail.get('schema_version') or 'n/a')}</code></div>"
        f"<div class=\"meta-card\"><div class=\"meta-title\"><span class=\"help\" title=\"{escape(schema_hash_help, quote=True)}\">Schema Hash</span></div><code>{escape(detail.get('schema_hash') or 'n/a')}</code></div>"
        f"<div class=\"meta-card\"><div class=\"meta-title\">Probe</div>"
        f"<a href=\"{escape(detail['probe_source_url'])}\">{escape(detail['probe_source_url'])}</a></div>"
        "</div>"
        "<h2>Warnings</h2>"
        f"{warnings_html}"
        "<h2>Probe Meta</h2>"
        f"{meta_html}"
        "<h2>Key Checks</h2>"
        f"{checks_html}"
        "<h2>Row Counts</h2>"
        f"{row_counts_html}"
        "<h2>Artifacts</h2>"
        f"{artifacts_html}"
    )
    return html_template(f"Ops Monitor - {detail['full_name']}", body)


def parse_repo_entry(entry: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    repo_ref = str(entry.get("repo") or "").strip()
    owner = str(entry.get("owner") or "").strip()
    name = str(entry.get("name") or "").strip()
    if repo_ref and "/" in repo_ref:
        owner, name = repo_ref.split("/", 1)
    if not owner or not name:
        raise ValueError("repo config must include repo='owner/name' or owner+name")

    full_name = f"{owner}/{name}"
    branch = str(entry.get("branch") or defaults.get("branch") or "main")
    probe_path = str(entry.get("probe_path") or defaults.get("probe_path") or "ops/probe.json")
    warn_hours = to_int(entry.get("freshness_warn_hours"))
    fail_hours = to_int(entry.get("freshness_fail_hours"))
    duration_multiplier = to_float(entry.get("duration_warn_multiplier"))
    if warn_hours is None:
        warn_hours = to_int(defaults.get("freshness_warn_hours"))
    if fail_hours is None:
        fail_hours = to_int(defaults.get("freshness_fail_hours"))
    if duration_multiplier is None:
        duration_multiplier = to_float(defaults.get("duration_warn_multiplier"))

    return {
        "owner": owner,
        "name": name,
        "full_name": full_name,
        "slug": slugify_repo(entry.get("slug") or full_name),
        "branch": branch,
        "probe_path": probe_path,
        "warn_lag_seconds": warn_hours * 3600 if warn_hours is not None else None,
        "fail_lag_seconds": fail_hours * 3600 if fail_hours is not None else None,
        "duration_warn_multiplier": duration_multiplier,
    }


def sanitize_warnings(values: Any) -> list[str]:
    cleaned: list[str] = []
    for item in ensure_list(values):
        text = str(item).strip()
        if text:
            cleaned.append(text)
    return cleaned


def repo_detail(
    repo_cfg: dict[str, Any],
    token: str | None,
    history_dir: Path,
    details_dir: Path,
    retention: int,
    dashboard_base_url: str,
    generated_at: datetime,
) -> dict[str, Any]:
    owner = repo_cfg["owner"]
    repo = repo_cfg["name"]
    branch = repo_cfg["branch"]
    probe_path = repo_cfg["probe_path"]

    probe, fetch_error = fetch_probe(owner, repo, branch, probe_path, token)
    probe_payload = probe if isinstance(probe, dict) else {}

    status = str(probe_payload.get("status", "FAIL")).upper().strip()
    if status not in ALLOWED_STATUSES:
        status = "WARN"

    warnings = sanitize_warnings(probe_payload.get("warnings"))
    if fetch_error:
        status = "FAIL"
        warnings.append(f"Probe fetch failed: {fetch_error}")

    checks = normalize_checks(probe_payload.get("key_checks"))
    artifact_links = normalize_artifacts(probe_payload.get("artifact_links"))
    row_counts = normalize_row_counts(probe_payload.get("row_counts"))

    last_run = parse_dt(probe_payload.get("last_run_time"))
    duration_seconds = to_float(
        probe_payload.get("duration_seconds", probe_payload.get("duration"))
    )
    schema_hash = str(probe_payload.get("schema_hash") or "").strip() or None
    schema_version = str(probe_payload.get("schema_version") or "").strip() or None

    freshness = probe_payload.get("freshness")
    if not isinstance(freshness, dict):
        freshness = {}
    max_date_value = freshness.get("max_date") or probe_payload.get("max_date")
    lag_seconds = to_float(freshness.get("lag_seconds"))
    if lag_seconds is None:
        max_date_dt = parse_dt(max_date_value)
        if max_date_dt is not None:
            lag_seconds = max(0.0, (generated_at - max_date_dt).total_seconds())
        elif last_run is not None:
            lag_seconds = max(0.0, (generated_at - last_run).total_seconds())

    history_path = history_dir / f"{repo_cfg['slug']}.json"
    history = read_json(history_path, [])
    if not isinstance(history, list):
        history = []

    baseline = median_duration(history)
    severity, severity_reasons = severity_for(
        base_status=status,
        lag_seconds=lag_seconds,
        warn_lag_seconds=repo_cfg["warn_lag_seconds"],
        fail_lag_seconds=repo_cfg["fail_lag_seconds"],
        duration_seconds=duration_seconds,
        baseline_duration_seconds=baseline,
        duration_warn_multiplier=repo_cfg["duration_warn_multiplier"],
    )
    if severity_reasons:
        warnings.extend(severity_reasons)

    meta = probe_payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    run_id = meta.get("run_id")
    run_link = meta.get("run_url") or run_url(owner, repo, run_id)
    if run_link and not any(item.get("url") == run_link for item in artifact_links):
        artifact_links.append({"label": "workflow_run", "url": run_link})

    top_warning = pick_top_warning(warnings, checks, severity_reasons, fetch_error)
    detail_filename = f"{repo_cfg['slug']}.html"
    detail_url = f"{dashboard_base_url}{detail_filename}" if dashboard_base_url else None
    dashboard_home_url = dashboard_base_url or "../"

    detail = {
        "slug": repo_cfg["slug"],
        "owner": owner,
        "repo": repo,
        "full_name": repo_cfg["full_name"],
        "branch": branch,
        "probe_path": probe_path,
        "probe_source_url": probe_url(owner, repo, branch, probe_path),
        "status": status,
        "severity": severity,
        "last_run_time": iso_utc(last_run),
        "duration_seconds": duration_seconds,
        "duration_human": format_seconds(duration_seconds),
        "freshness_lag_seconds": lag_seconds,
        "freshness_lag_human": format_seconds(lag_seconds),
        "max_date": str(max_date_value) if max_date_value is not None else None,
        "schema_version": schema_version,
        "schema_hash": schema_hash,
        "row_counts": row_counts,
        "key_checks": checks,
        "warnings": warnings,
        "top_warning": top_warning,
        "artifact_links": artifact_links,
        "meta": meta,
        "baseline_duration_seconds": baseline,
        "detail_page": detail_filename,
        "detail_url": detail_url,
        "dashboard_home_url": dashboard_home_url,
        "generated_at": iso_utc(generated_at),
    }

    snapshot = {
        "captured_at": iso_utc(generated_at),
        "last_run_time": detail["last_run_time"],
        "status": status,
        "severity": severity,
        "duration_seconds": duration_seconds,
        "freshness_lag_seconds": lag_seconds,
        "schema_hash": schema_hash,
        "top_warning": top_warning,
    }
    if not history or history[-1] != snapshot:
        history.append(snapshot)
    history = history[-max(1, retention) :]

    write_json(history_path, history)
    write_json(details_dir / f"{repo_cfg['slug']}.json", detail)
    return detail


def dashboard_base_url(value: str | None) -> str:
    if not value:
        return ""
    text = value.strip()
    if not text:
        return ""
    if not text.endswith("/"):
        text += "/"
    return text


def default_dashboard_url_from_env() -> str | None:
    repo_ref = str(os.environ.get("GITHUB_REPOSITORY") or "").strip()
    if "/" not in repo_ref:
        return None
    owner, name = repo_ref.split("/", 1)
    if not owner or not name:
        return None
    return f"https://{owner}.github.io/{name}/"


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect repo probes and build docs dashboard.")
    parser.add_argument("--config", default="config/repos.json")
    parser.add_argument("--output", default="data/latest.json")
    parser.add_argument("--history-dir", default="data/history")
    parser.add_argument("--details-dir", default="data/details")
    parser.add_argument("--docs-dir", default="docs")
    parser.add_argument("--retention", type=int, default=None)
    parser.add_argument("--dashboard-base-url", default=None)
    args = parser.parse_args()

    config_path = Path(args.config)
    config = read_json(config_path, {})
    if not isinstance(config, dict):
        raise SystemExit(f"Invalid config JSON: {config_path}")

    defaults = config.get("defaults")
    if not isinstance(defaults, dict):
        defaults = {}
    repos_cfg = config.get("repos")
    if not isinstance(repos_cfg, list):
        repos_cfg = []

    retention = args.retention or to_int(config.get("history_retention")) or 40
    token = str((os.environ.get("GH_TOKEN") or "")).strip() or None
    raw_base_url = args.dashboard_base_url or os.environ.get("DASHBOARD_BASE_URL")
    if not raw_base_url:
        raw_base_url = default_dashboard_url_from_env()
    base_url = dashboard_base_url(raw_base_url)

    generated = now_utc()
    parsed_repos: list[dict[str, Any]] = []
    for row in repos_cfg:
        if not isinstance(row, dict):
            continue
        try:
            parsed_repos.append(parse_repo_entry(row, defaults))
        except ValueError as exc:
            print(f"Skipping repo entry: {exc}")

    history_dir = Path(args.history_dir)
    details_dir = Path(args.details_dir)
    docs_dir = Path(args.docs_dir)
    docs_dir.mkdir(parents=True, exist_ok=True)

    details: list[dict[str, Any]] = []
    for repo_cfg in parsed_repos:
        details.append(
            repo_detail(
                repo_cfg=repo_cfg,
                token=token,
                history_dir=history_dir,
                details_dir=details_dir,
                retention=retention,
                dashboard_base_url=base_url,
                generated_at=generated,
            )
        )

    details.sort(key=lambda d: (STATUS_SORT.get(d["severity"], 9), d["full_name"].lower()))
    counts = {"OK": 0, "WARN": 0, "FAIL": 0}
    for row in details:
        counts[row["severity"]] = counts.get(row["severity"], 0) + 1

    dashboard_url = base_url or None
    summary = {
        "generated_at": iso_utc(generated),
        "dashboard_base_url": base_url or None,
        "dashboard_url": dashboard_url,
        "repo_count": len(details),
        "counts": counts,
        "repos": details,
    }

    output_path = Path(args.output)
    write_json(output_path, summary)
    write_json(docs_dir / "latest.json", summary)

    (docs_dir / ".nojekyll").write_text("", encoding="utf-8")
    docs_index_path = docs_dir / "index.html"
    if docs_index_path.exists():
        docs_index_path.unlink()
    for detail in details:
        (docs_dir / f"{detail['slug']}.html").write_text(
            render_detail(detail),
            encoding="utf-8",
        )
        write_json(docs_dir / f"{detail['slug']}.json", detail)

    print(f"Generated dashboard for {len(details)} repos in {docs_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

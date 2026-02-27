#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def read_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def build_message(summary: dict[str, Any]) -> str:
    repos = summary.get("repos")
    if not isinstance(repos, list):
        repos = []
    flagged = [r for r in repos if str(r.get("severity")) in {"WARN", "FAIL"}]
    if not flagged:
        return ""

    generated = summary.get("generated_at") or "n/a"
    lines = [
        f"Ops Monitor Alerts ({generated})",
        f"WARN/FAIL repos: {len(flagged)}",
    ]
    for repo in flagged[:15]:
        full_name = str(repo.get("full_name", "unknown"))
        severity = str(repo.get("severity", "WARN"))
        lag = str(repo.get("freshness_lag_human", "n/a"))
        top = truncate(str(repo.get("top_warning", "no warning")), 120)
        lines.append(f"- {full_name}: {severity}, lag {lag}")
        lines.append(f"  {top}")
        detail_url = str(repo.get("detail_url") or "").strip()
        if detail_url:
            lines.append(f"  {detail_url}")

    dashboard_url = str(summary.get("dashboard_url") or "").strip()
    if dashboard_url:
        lines.append(f"Dashboard: {dashboard_url}")
    message = "\n".join(lines)
    return truncate(message, 3900)


def send_telegram(token: str, chat_id: str, text: str) -> None:
    api_url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    request = urllib.request.Request(api_url, data=payload, method="POST")
    with urllib.request.urlopen(request, timeout=20) as response:
        response.read()


def main() -> int:
    parser = argparse.ArgumentParser(description="Send WARN/FAIL alerts to Telegram.")
    parser.add_argument("--summary", default="data/latest.json")
    args = parser.parse_args()

    token = str(os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = str(os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        print("Telegram env vars missing, skipping alerts.")
        return 0

    summary = read_summary(Path(args.summary))
    message = build_message(summary)
    if not message:
        print("No WARN/FAIL statuses detected, no Telegram message sent.")
        return 0

    try:
        send_telegram(token, chat_id, message)
    except Exception as exc:  # non-blocking on alert failures
        print(f"Telegram send failed: {exc}")
        return 0

    print("Telegram alert sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

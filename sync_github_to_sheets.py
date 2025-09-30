#!/usr/bin/env python3

import os
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from dateutil import parser as dateparser
from google.oauth2.service_account import Credentials
import gspread
from dotenv import load_dotenv
import subprocess
import shutil
import json

GITHUB_API_URL = "https://api.github.com"
DEFAULT_SHEET_NAME = "Sheet1"
DEFAULT_TRACKER_SHEET_NAME = "Tracker"


def load_configuration() -> Dict[str, str]:
    load_dotenv()
    # Prefer explicit env var, then file, then gh CLI
    token = os.getenv("GH_TOKEN", "").strip()
    if not token:
        token_file = os.getenv("GH_TOKEN_FILE", "").strip()
        if token_file:
            try:
                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read().strip()
            except FileNotFoundError:
                raise SystemExit(f"GH_TOKEN_FILE not found: {token_file}")
    if not token and shutil.which("gh"):
        try:
            res = subprocess.run(
                ["gh", "auth", "token"],
                check=False,
                capture_output=True,
                text=True,
            )
            if res.returncode == 0 and res.stdout.strip():
                token = res.stdout.strip()
        except Exception:
            pass

    mode = (os.getenv("MODE", "rows") or "rows").strip().lower()
    track_by = (os.getenv("TRACK_BY", "creator") or "creator").strip().lower()

    config = {
        "GH_TOKEN": token,
        "GH_OWNER": os.getenv("GH_OWNER", ""),
        "GH_REPO": os.getenv("GH_REPO", ""),
        "GOOGLE_SHEETS_ID": os.getenv("GOOGLE_SHEETS_ID", ""),
        "GOOGLE_SERVICE_ACCOUNT_FILE": os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json"),
        "SHEET_NAME": os.getenv("SHEET_NAME", DEFAULT_SHEET_NAME),
        "MODE": mode,
        "TRACK_BY": track_by,
        "TRACKER_SHEET_NAME": os.getenv("TRACKER_SHEET_NAME", DEFAULT_TRACKER_SHEET_NAME),
    }
    required_keys = [
        "GH_OWNER",
        "GH_REPO",
        "GOOGLE_SHEETS_ID",
        "GOOGLE_SERVICE_ACCOUNT_FILE",
    ]
    missing = [k for k in required_keys if not config[k]]
    if not config["GH_TOKEN"]:
        missing.append("GH_TOKEN (or GH_TOKEN_FILE or gh auth login)")
    if missing:
        raise SystemExit(f"Missing configuration: {', '.join(missing)}")
    if config["MODE"] not in {"rows", "tracker"}:
        raise SystemExit("MODE must be either 'rows' or 'tracker'")
    if config["TRACK_BY"] not in {"creator", "requester"}:
        raise SystemExit("TRACK_BY must be either 'creator' or 'requester'")
    return config


def build_github_session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    return session


def paginate(session: requests.Session, url: str, params: Optional[Dict[str, str]] = None) -> Iterable[Dict]:
    """Yield JSON items across GitHub paginated responses using Link headers."""
    params = dict(params or {})
    params.setdefault("per_page", 100)
    while url:
        resp = session.get(url, params=params)
        if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
            retry_at = int(resp.headers.get("X-RateLimit-Reset", "0"))
            wait_seconds = max(1, retry_at - int(time.time()))
            print(f"GitHub rate limit reached. Sleeping {wait_seconds}s...", file=sys.stderr)
            time.sleep(wait_seconds)
            continue
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            for item in data:
                yield item
        else:
            # Some endpoints return an object with 'items'
            for item in data.get("items", []):
                yield item
        link = resp.headers.get("Link", "")
        next_url = None
        if link:
            parts = [p.strip() for p in link.split(",")]
            for part in parts:
                if 'rel="next"' in part:
                    # <https://...>; rel="next"
                    start = part.find("<") + 1
                    end = part.find(">", start)
                    next_url = part[start:end]
                    break
        url = next_url
        params = None  # params only for first call


def get_pull_requests(session: requests.Session, owner: str, repo: str, state: str = "all") -> List[Dict]:
    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/pulls"
    params = {"state": state, "sort": "created", "direction": "desc"}
    return list(paginate(session, url, params=params))


def get_issue_events(session: requests.Session, owner: str, repo: str, issue_number: int) -> List[Dict]:
    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/issues/{issue_number}/events"
    return list(paginate(session, url))


def get_pr_commits(session: requests.Session, owner: str, repo: str, pr_number: int) -> List[Dict]:
    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/pulls/{pr_number}/commits"
    return list(paginate(session, url))


def parse_iso8601(value: Optional[str]):
    if not value:
        return None
    return dateparser.isoparse(value)


def summarize_pr(session: requests.Session, owner: str, repo: str, pr: Dict) -> Tuple[List[str], Dict[str, str]]:
    """Return a row list and a dict of key fields for a PR."""
    number = pr.get("number")
    title = pr.get("title")
    creator = (pr.get("user") or {}).get("login")
    created_at = pr.get("created_at")
    pr_url = pr.get("html_url")

    # Review requested events
    events = get_issue_events(session, owner, repo, int(number))
    review_events = [e for e in events if e.get("event") == "review_requested"]
    marked_done_at: Optional[str] = review_events[0].get("created_at") if review_events else None
    # Collect all requested reviewers (users and/or teams)
    reviewers: List[str] = []
    for e in review_events:
        if e.get("requested_reviewer"):
            rev = (e["requested_reviewer"] or {}).get("login")
            if rev:
                reviewers.append(rev)
        elif e.get("requested_team"):
            team_slug = (e["requested_team"] or {}).get("slug")
            if team_slug:
                reviewers.append(f"team:{team_slug}")
    # De-duplicate while preserving order
    seen = set()
    reviewers_unique = [r for r in reviewers if not (r in seen or seen.add(r))]

    # Commits after marked_done_at
    updates_after_done = 0
    if marked_done_at:
        md_dt = parse_iso8601(marked_done_at)
        commits = get_pr_commits(session, owner, repo, int(number))
        for c in commits:
            commit_dt_str = ((c.get("commit") or {}).get("author") or {}).get("date")
            commit_dt = parse_iso8601(commit_dt_str)
            if commit_dt and md_dt and commit_dt > md_dt:
                updates_after_done += 1

    # Finalization
    merged_at = pr.get("merged_at")
    finalized_by = (pr.get("merged_by") or {}).get("login") if merged_at else None
    finalized_at = merged_at or (pr.get("closed_at") if pr.get("state") == "closed" else None)
    if not finalized_by and merged_at:
        # In some cases merged_by is missing; best-effort fallback using timeline (not used to avoid extra calls)
        finalized_by = None

    row = [
        number,
        title,
        creator,
        created_at,
        marked_done_at,
        ", ".join(reviewers_unique) if reviewers_unique else None,
        updates_after_done,
        finalized_by,
        finalized_at,
        pr_url,
    ]

    meta = {
        "number": str(number),
        "url": pr_url or "",
    }
    return row, meta


def connect_sheet(google_sheets_id: str, service_account_file: str, sheet_name: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if json_str:
        try:
            info = json.loads(json_str)
            creds = Credentials.from_service_account_info(info, scopes=scopes)
        except Exception as exc:
            raise SystemExit(f"Invalid GOOGLE_SERVICE_ACCOUNT_JSON: {exc}")
    else:
        creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(google_sheets_id)
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=100, cols=12)
    return ws


def ensure_headers(ws) -> None:
    headers = [
        "PR #",
        "Task Title",
        "Creator",
        "Created At",
        "Marked Done (Review Requested)",
        "Reviewers",
        "Updates After Done",
        "Finalized By",
        "Finalized At",
        "PR URL",
    ]
    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(headers, value_input_option="USER_ENTERED")
    elif first_row != headers:
        # Keep user customizations; do not override silently
        pass


def ensure_tracker_headers(ws) -> None:
    headers = ["Account", "Count"]
    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(headers, value_input_option="USER_ENTERED")


def append_rows(ws, rows: List[List]) -> None:
    if not rows:
        return
    ws.append_rows(rows, value_input_option="USER_ENTERED")


def compute_tracker_counts(session: requests.Session, owner: str, repo: str, prs: List[Dict], track_by: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for pr in prs:
        number = pr.get("number")
        events = get_issue_events(session, owner, repo, int(number))
        review_events = [e for e in events if e.get("event") == "review_requested"]
        if not review_events:
            continue
        # First review requested
        first_event = review_events[0]
        if track_by == "creator":
            account = (pr.get("user") or {}).get("login")
        else:  # requester
            account = ((first_event.get("actor") or {}).get("login"))
        if not account:
            continue
        counts[account] = counts.get(account, 0) + 1
    return counts


def main() -> None:
    config = load_configuration()
    session = build_github_session(config["GH_TOKEN"])

    owner = config["GH_OWNER"]
    repo = config["GH_REPO"]
    prs = get_pull_requests(session, owner, repo, state="all")
    print(f"Fetched {len(prs)} PRs from {owner}/{repo}")
    print(f"Mode: {config['MODE']}, Track by: {config['TRACK_BY']}")

    if config["MODE"] == "tracker":
        print(f"Connecting to Google Sheets ID: {config['GOOGLE_SHEETS_ID'][:10]}...")
        try:
            tracker_ws = connect_sheet(
                config["GOOGLE_SHEETS_ID"],
                config["GOOGLE_SERVICE_ACCOUNT_FILE"],
                config["TRACKER_SHEET_NAME"],
            )
            print("Connected successfully")
        except Exception as e:
            print(f"Failed to connect to Google Sheets: {e}")
            raise
        ensure_tracker_headers(tracker_ws)
        print("Computing tracker counts...")
        counts = compute_tracker_counts(session, owner, repo, prs, config["TRACK_BY"])
        print(f"Found {len(counts)} accounts with review requests")
        # Clear existing (keep sheet), write fresh counts
        tracker_ws.clear()
        ensure_tracker_headers(tracker_ws)
        rows = [[account, count] for account, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))]
        append_rows(tracker_ws, rows)
        print(f"Updated tracker sheet '{config['TRACKER_SHEET_NAME']}' with {len(rows)} accounts")
        return

    # Default 'rows' mode: append one row per PR
    ws = connect_sheet(
        config["GOOGLE_SHEETS_ID"],
        config["GOOGLE_SERVICE_ACCOUNT_FILE"],
        config["SHEET_NAME"],
    )
    ensure_headers(ws)

    rows: List[List] = []
    for pr in prs:
        row, _ = summarize_pr(session, owner, repo, pr)
        rows.append(row)

    append_rows(ws, rows)
    print(f"Appended {len(rows)} rows to Google Sheet '{config['SHEET_NAME']}'")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


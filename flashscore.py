"""
Flashscore schedule resolution + live commentary for the World Cup poller.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

from flashscore_lookup import (
    FS_FEED_BASE,
    _HEADERS,
    _candidates,
    _normalize,
    fetch_live_commentary as _fetch_raw_commentary,
)

logger = logging.getLogger("worldcup_poller.flashscore")

WC_SEASON_ID = "6kKoWOjD"
WC_STAGE_ID = "zeSHfCx3"
WC_TOURNAMENT_ID = "lvUBR5F8"

_REQUEST_TIMEOUT = 10
_MAX_PAGES = 5


def fs_get(path: str) -> Optional[str]:
    url = f"{FS_FEED_BASE}{path}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.RequestException as e:
        logger.warning("fs_get(%s) failed: %s", path, e)
        return None


def _parse_rows(raw: str) -> Iterator[Dict[str, str]]:
    if not raw:
        return
    for chunk in raw.split("¬~"):
        if not chunk:
            continue
        fields: Dict[str, str] = {}
        for pair in chunk.split("¬"):
            if "÷" not in pair:
                continue
            key, _, value = pair.partition("÷")
            if key:
                fields[key] = value
        if fields:
            yield fields


def _clean(value: Optional[str]) -> str:
    if not value:
        return ""
    return value.replace("&amp;", "&").strip()


def _parse_match_rows(raw: str) -> Iterator[Dict[str, str]]:
    for row in _parse_rows(raw):
        if row.get("AA"):
            yield row


def _row_to_fixture(row: Dict[str, str]) -> Optional[Tuple[str, str, str]]:
    match_id = (row.get("AA") or "").strip()
    home = _clean(row.get("CX") or row.get("FH"))
    away = _clean(row.get("AF") or row.get("FK"))

    if not match_id or not home or not away:
        return None

    if home.strip().lower() == away.strip().lower():
        logger.warning(
            "home_team == away_team ('%s') for match_id=%s -- skipping",
            home, match_id
        )
        return None

    return match_id, home, away


def build_schedule_map() -> Dict[Tuple[str, str], str]:
    schedule: Dict[Tuple[str, str], str] = {}

    def _collect(path_fmt: str) -> int:
        added = 0
        for page in range(1, _MAX_PAGES + 1):
            raw = fs_get(path_fmt.format(page=page))
            if not raw:
                break
            rows = list(_parse_match_rows(raw))
            if not rows:
                break
            for row in rows:
                parsed = _row_to_fixture(row)
                if not parsed:
                    continue
                match_id, home, away = parsed
                schedule[(_normalize(home), _normalize(away))] = match_id
                added += 1
        return added

    full_path = f"to_{WC_STAGE_ID}_{WC_SEASON_ID}_{{page}}"
    added = _collect(full_path)
    if added:
        logger.info("build_schedule_map: %d fixtures from full schedule feed", added)
        return schedule

    logger.info("Full schedule feed empty -- falling back to today's feed")
    today_path = f"t_1_8_{WC_TOURNAMENT_ID}_3_en_{{page}}"
    added = _collect(today_path)
    logger.info("build_schedule_map: %d fixtures total", added)
    return schedule


def resolve_from_map(
    schedule_map: Dict[Tuple[str, str], str],
    home_team: str,
    away_team: str,
) -> Optional[str]:
    if not schedule_map or not home_team or not away_team:
        return None

    home_candidates = _candidates(home_team)
    away_candidates = _candidates(away_team)

    for (h, a), match_id in schedule_map.items():
        direct = h in home_candidates and a in away_candidates
        swapped = h in away_candidates and a in home_candidates
        if direct or swapped:
            return match_id

    return None


def fetch_live_commentary_by_id(flashscore_id: str) -> List[Dict[str, Any]]:
    """
    Fetch + parse Flashscore's live text commentary.
    Returns entries matching Rust CommentaryEntry shape:
    minute, text, type, team, player, createdAt
    """
    if not flashscore_id:
        return []

    raw_entries = _fetch_raw_commentary(flashscore_id)
    out: List[Dict[str, Any]] = []
    for entry in raw_entries:
        time_str = entry.get("time", "") or ""
        minute_match = re.search(r"\d+", time_str)
        minute = int(minute_match.group()) if minute_match else 0
        
        # ✅ FIXED: Match Rust CommentaryEntry EXACTLY
        out.append({
            "minute": minute,
            "text": entry.get("text", ""),
            "type": "commentary",  # ✅ "type" NOT "event_type"
            "team": None,
            "player": None,
            "createdAt": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),  # ✅ "createdAt" with timezone
        })
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    smap = build_schedule_map()
    print(f"Schedule map has {len(smap)} fixtures")
    for (h, a), mid in list(smap.items())[:10]:
        print(f"  {h} vs {a} -> {mid}")
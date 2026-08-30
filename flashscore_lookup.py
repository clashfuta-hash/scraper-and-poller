"""
Bridge between 365Scores fixtures and Flashscore commentary.

Your existing worldcup_poller_flashscore.py scraper already resolves every
World Cup fixture's home_team / away_team / flashscore_id (== match_id) and
writes it into MongoDB (clashdb.fixtures). You do NOT need a separate
Flashscore "search by team name" call — that endpoint doesn't exist as a
plain GET (confirmed by the repeated 404s). The schedule feed parser
(parse_schedule_feed / parse_today_feed) is the resolver, and it already ran.

This module just does the lookup + commentary fetch using IDs that are
already sitting in Mongo.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any, Dict, List, Optional

import requests
from pymongo.collection import Collection

logger = logging.getLogger("worldcup_poller.flashscore_lookup")

FS_NINJA_HOST = "global.flashscore.ninja"
FS_FEED_BASE = f"https://{FS_NINJA_HOST}/2/x/feed/"
X_FSIGN_TOKEN = "SW9D1eZo"

_HEADERS = {
    "Accept": "text/plain, */*; q=0.01",
    "Referer": "https://www.flashscore.com/",
    "Origin": "https://www.flashscore.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "X-Fsign": X_FSIGN_TOKEN,
}


def _normalize(name: str) -> str:
    """Lowercase, strip accents/punctuation, drop common suffixes so that
    365Scores' naming and Flashscore's naming converge on the same key.
    e.g. 'Korea Republic' vs 'South Korea', 'USA' vs 'United States'."""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().strip()
    name = re.sub(r"\b(national team|nt|fc|the)\b", "", name)
    name = re.sub(r"[^a-z0-9 ]", "", name)
    return re.sub(r"\s+", " ", name).strip()


# Add pairs here as you find 365Scores/Flashscore naming mismatches.
# key = normalized 365Scores name -> value = normalized Flashscore name
_ALIASES = {
    "south korea": "korea republic",
    "usa": "united states",
    "ivory coast": "cote divoire",
}


def _candidates(name: str) -> set[str]:
    n = _normalize(name)
    out = {n}
    if n in _ALIASES:
        out.add(_ALIASES[n])
    # reverse lookup too
    for k, v in _ALIASES.items():
        if v == n:
            out.add(k)
    return out


def find_flashscore_id_from_db(
    fixtures_col: Collection,
    home_team: str,
    away_team: str,
) -> Optional[Dict[str, Any]]:
    """
    Look up the Flashscore match_id for a 365Scores fixture using the
    fixtures already scraped into MongoDB by worldcup_poller_flashscore.py.

    Returns the fixture document (containing flashscore_id) or None.
    """
    home_candidates = _candidates(home_team)
    away_candidates = _candidates(away_team)

    # Pull World Cup fixtures once and match in Python — the collection is
    # small (one tournament's worth of matches), so this avoids needing a
    # fragile exact-string Mongo query against inconsistent name casing.
    cursor = fixtures_col.find(
        {"league": "World Cup 2026"},
        {"match_id": 1, "flashscore_id": 1, "home_team": 1, "away_team": 1,
         "status": 1, "date_iso": 1, "time": 1},
    )

    for doc in cursor:
        doc_home = _normalize(doc.get("home_team", ""))
        doc_away = _normalize(doc.get("away_team", ""))

        direct_match = doc_home in home_candidates and doc_away in away_candidates
        swapped_match = doc_home in away_candidates and doc_away in home_candidates

        if direct_match or swapped_match:
            logger.info(
                "Matched %s vs %s -> flashscore_id=%s (db: %s vs %s)",
                home_team, away_team, doc.get("flashscore_id"),
                doc.get("home_team"), doc.get("away_team"),
            )
            return doc

    logger.warning(
        "No Flashscore match found in DB for %s vs %s "
        "(scraper may not have run yet, or team names need an alias — "
        "see _ALIASES)",
        home_team, away_team,
    )
    return None


def fetch_live_commentary(flashscore_id: str) -> List[Dict[str, Any]]:
    """
    Fetch live text commentary for a known Flashscore match_id.

    NOTE: confirm the exact commentary feed name against your captured
    network traffic — df_lcpo_1_{id} is the pattern from your earlier
    fetcher and is consistent with the dc_/d_hb_/li_/od_ family already
    verified working in worldcup_poller_flashscore.py, but it has not been
    independently re-verified in *this* file. If it 404s, recapture it the
    same way you got dc_/d_hb_/li_/od_: DevTools Network tab on a live match
    page, filter by flashscore.ninja, look for the commentary/text-feed call.
    """
    url = f"{FS_FEED_BASE}df_lcpo_1_{flashscore_id}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        return _parse_commentary(resp.text)
    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch commentary for %s: %s", flashscore_id, e)
        return []


def _parse_commentary(raw: str) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for part in raw.split("¬~MB÷"):
        if not part.strip():
            continue
        time_match = re.search(r"¬MK÷([^¬]+)", part)
        text_match = re.search(r"¬MD÷([^¬]+)", part)
        text = (text_match.group(1) if text_match else "").replace("¬", "").strip()
        if text:
            entries.append({
                "time": (time_match.group(1) if time_match else "").strip(),
                "text": text,
                "source": "flashscore",
            })
    return entries


def get_commentary_for_matchup(
    fixtures_col: Collection,
    home_team: str,
    away_team: str,
) -> List[Dict[str, Any]]:
    """One-call convenience: 365Scores team names in -> commentary entries out."""
    fixture = find_flashscore_id_from_db(fixtures_col, home_team, away_team)
    if not fixture:
        return []
    fs_id = fixture.get("flashscore_id") or fixture.get("match_id")
    if not fs_id:
        return []
    return fetch_live_commentary(fs_id)


if __name__ == "__main__":
    import os
    from pymongo import MongoClient

    logging.basicConfig(level=logging.INFO)

    client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    col = client["clashdb"]["fixtures"]

    # Example using names from your earlier test cases
    for h, a in [("Colombia", "Portugal"), ("Croatia", "Ghana"), ("Panama", "England")]:
        fixture = find_flashscore_id_from_db(col, h, a)
        if fixture:
            print(f"{h} vs {a} -> flashscore_id={fixture.get('flashscore_id')}")
        else:
            print(f"{h} vs {a} -> NOT FOUND in DB yet")
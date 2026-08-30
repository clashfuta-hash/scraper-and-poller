"""
Central configuration for the league live poller.

SCOPE: leagues (config.LEAGUES) only -- no World Cup, no other
internationals, no Club Friendlies. The old standalone World Cup
scraper.py has been removed entirely. Club Friendlies support (the
EPL/Serie A club-name lists, the 6000+-club global competitionId, and
the date-window scraping it required) has moved to the standalone
`friendly_funtassy` service, which keeps its own config and writes into
this same shared `games` collection.

ARCHITECTURE:
365Scores is the sole live data source — fixtures discovery, score, status,
and structured events (goal/card/sub) all come from it.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "")
MONGO_DB = os.environ.get("MONGO_DB", "clashdb")
# NOTE: renamed from "fixtures" -> "games". Both the World Cup poller and
# the new multi-league scraper (leagues_scraper.py) now write into the
# same "games" collection. Override with MONGO_COLLECTION env var if you
# need to point at a different collection name.
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "games")

# Rust API
FANCLASH_API = os.environ.get("FANCLASH_API", "https://clash-api-m5mr.onrender.com/api")

# 365Scores
THREESIXTYFIVE_BASE_URL = "https://webws.365scores.com"
THREESIXTYFIVE_APP_TYPE_ID = 5
THREESIXTYFIVE_LANG_ID = 1
THREESIXTYFIVE_USER_COUNTRY_ID = 413
THREESIXTYFIVE_TIMEZONE = "Africa/Nairobi"

# Polling
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
SCRAPE_DAYS_AHEAD = 20

# ============================================================
# SCRAPE WINDOW (anchored on today)
# ============================================================
# Window size, in days, used by scrape_all_leagues_window() in
# leagues_scraper.py -- anchors strictly on "today"
# (datetime.now(UTC).date()), no reference-date creep/high-water-mark
# heuristics.
SCRAPE_WINDOW_DAYS = 15

# ============================================================
# LEAGUE-BASED FIXTURES (leagues_scraper.py)
# ============================================================
# 365Scores competitionId for each league/cup, derived from each
# competition's canonical 365scores.com URL slug (the trailing number
# in e.g. .../league/premier-league-7 is the competitionId). These are
# stable in practice but 365Scores has been known to reshuffle IDs
# across seasons -- if a league starts returning 0 games, re-derive the
# id from https://www.365scores.com/football/league/<slug>-<id> and
# update this dict.
#
# `prefix` is used to build each document's matchId, e.g. "epl_4627864",
# mirroring the existing wc26_<gameId> convention used for the World Cup.
LEAGUES = {
    "epl": {
        "competition_id": 7,
        "name": "Premier League",
        "prefix": "epl",
    },
    "seriea": {
        "competition_id": 17,
        "name": "Serie A",
        "prefix": "seriea",
    },
    "ucl": {
        "competition_id": 572,
        "name": "UEFA Champions League",
        "prefix": "ucl",
    },
    "europa": {
        "competition_id": 573,
        "name": "UEFA Europa League",
        "prefix": "europa",
    },
    "facup": {
        "competition_id": 8,
        "name": "FA Cup",
        "prefix": "facup",
    },
    "community_shield": {
        "competition_id": 10,
        "name": "Community Shield",
        "prefix": "community_shield",
    },
}

# ============================================================
# PRIORITY-LEAGUE ROLLING WINDOW (legacy -- see SCRAPE_WINDOW_DAYS)
# ============================================================
# Instead of anchoring the scrape window on "now" (a dead zone until the
# priority leagues' seasons actually start), this used to anchor on a
# reference date that starts here and creeps forward by one day per real
# calendar day (see FixtureStore.advance_reference_date_if_needed).
#
# NO LONGER USED by scrape_all_leagues_window() -- leagues now anchor on
# today directly, per SCRAPE_WINDOW_DAYS above. Left
# in place only because mongo_store.FixtureStore.get_reference_date() /
# advance_reference_date_if_needed() still reference these constants;
# harmless dead weight now that nothing calls those methods from the
# scrape path.
REFERENCE_DATE_DEFAULT = "2026-08-13"
REFERENCE_WINDOW_DAYS = 15 

# Order matters here -- this is also the priority order used when
# building the "top of feed" response (EPL first, down to Community
# Shield). Qualifying rounds are excluded regardless of league.
PRIORITY_LEAGUE_ORDER = ["epl", "ucl", "europa", "facup", "community_shield"]

# Club Friendlies configuration (competitionId, EPL/Serie A club-name
# lists, date windows) has been removed from this repo entirely -- it
# now lives in the standalone `friendly_funtassy` service, which keeps
# its own full copy of this file and writes into the same shared
# `games` collection via a different `source` tag ("friendly_hardcoded"
# vs this repo's "365scores").

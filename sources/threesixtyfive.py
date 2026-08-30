"""
365Scores API client for league and World Cup/international data.
Fetches: fixtures, live scores, events, lineups, statistics, and commentary.

NOTE: This is the league-scraper's copy of the module and had drifted from
the World Cup poller's copy -- it was missing the shared _fetch_play_by_play_raw
helper and, worse, fetch_commentary() hardcoded "team": None instead of
resolving CompetitorNum the way fetch_match_events() already did. Both are
fixed below by mirroring the World Cup version's structure exactly.

Club Friendlies support (date-range fetching + client-side club-name
filtering) has been removed from this file -- friendlies are now handled
entirely by the standalone `friendly_funtassy` service, which keeps its
own full copy of this module. Nothing in this repo calls friendlies logic
anymore. NOTE: friendly_funtassy's copy of fetch_lineups likely has the
same "empty placeholder" bug fixed below -- apply the same fix there too.

LINEUPS FIX (see fetch_lineups below):
365Scores can return a non-null lineups object on homeCompetitor/
awayCompetitor BEFORE the official squad is actually published -- an
object with an empty "members" list (and often "formation": ""). The old
check here was:

    if not home_lineups and not away_lineups:
        return None

which only catches the case where the key is missing/None entirely. A
`{"formation": "", "members": []}` shell is truthy, so it sailed straight
through, got joined against an empty roster, and was returned as a
"successful" empty-squad result. Confirmed against a real stored
document: lineups.homeLineup.coach.name == "Unknown" (meaning no
"Management" member existed at all) with formation == "" and empty
players/bench arrays on both sides -- i.e. `members` was genuinely [].

That empty result got forwarded and stored downstream with
lineupsFetched=true and no way to retry. Fixed by requiring at least one
side to actually have non-empty `members` before returning a result;
otherwise return None so poller.py's existing retry-until-success logic
(_fetch_and_forward_lineups / should_fetch_lineups in poller.py) tries
again on a later poll cycle instead of locking in an empty stub.

STATISTICS BUG (see extract_statistics_from_game below):
extract_statistics_from_game() was reading top-level `game` keys
(homePossession, homeShots, homeCorners, ...) that DO NOT EXIST on the
/web/game/ response. Checked against the repo's own committed sample,
game_4627864.json: the full set of top-level keys on `game` is
actualPlayTime, awayCompetitor, chartEvents, competitionDisplayName,
hasStats, homeCompetitor, id, statusGroup, statusId, statusText,
topPerformers, venue, widgets, etc. -- none of the guessed
home*/away* stat fields are present at the top level OR nested inside
homeCompetitor/awayCompetitor. The sample has hasStats: True, so
365Scores does have stats for this game -- they're served through one
of the `widgets` entries (e.g. a SportRadar-hosted LMT/Momentum widget
URL), the same pattern commentary uses (playByPlay.feedURL) instead of
living directly on /web/game/.

Because nobody has captured a raw response from one of those widget
URLs yet, there is no verified field shape to parse, and guessing again
would repeat the exact mistake that caused the original bug (nulls
silently written to storage as if they were real zero-value stats).
Instead, extract_statistics_from_game() now fails LOUDLY: it logs an
error the first time it's called and returns None instead of a
dict full of Nones, so callers can no longer mistake "we never
implemented this" for "the match legitimately has no stats yet". See
the function docstring for what still needs to happen before this can
return real data.
"""

from __future__ import annotations

import logging
import re
import requests
from typing import List, Dict, Any, Optional

logger = logging.getLogger("worldcup_poller.sources.threesixtyfive")

# Base URL for 365Scores API
BASE_URL = "https://webws.365scores.com"

# Default headers (mimicking browser request)
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.365scores.com/",
    "Origin": "https://www.365scores.com",
}


def is_game_finished(game: Dict[str, Any]) -> bool:
    """
    Determine if a game has finished.

    365Scores uses "Ended" as the primary status text for completed games.
    Other possible values: "Finished", "FT", "Full Time", "AET", "Pen"

    Signals checked, in order:
      1. game.chartEvents.statuses[0].isFinished -- explicit bool
      2. game.justEnded -- fires the moment a match ends
      3. game.statusText -- confirmed 365Scores value is "Ended"
      4. game.gameTime >= 90 with no extra time
    """
    # Check 1: Explicit isFinished flag
    try:
        statuses = (game.get("chartEvents") or {}).get("statuses") or []
        if statuses and "isFinished" in statuses[0]:
            return bool(statuses[0]["isFinished"])
    except (AttributeError, IndexError, TypeError):
        pass

    # Check 2: justEnded flag
    if game.get("justEnded"):
        return True

    # Check 3: statusText - 365Scores uses "Ended"
    status_text = (game.get("statusText") or "").strip().lower()
    finished_keywords = [
        "ended",
        "finished",
        "ft",
        "full-time",
        "aet",
        "pen",
        "penalties",
    ]
    if status_text in finished_keywords:
        return True

    # Check 4: Time-based fallback - if gameTime >= 90 and not extra time
    time_elapsed = game.get("gameTime", 0)
    if time_elapsed >= 90:
        # Don't mark if it's half time or extra time
        if "half" not in status_text and "extra" not in status_text:
            # Also check if we have a winner (both scores set)
            home_comp = game.get("homeCompetitor", {})
            away_comp = game.get("awayCompetitor", {})
            if (
                home_comp.get("score") is not None
                and away_comp.get("score") is not None
            ):
                return True

    # Check 5: If game has ended but statusText contains "ended" in any form
    if "ended" in status_text:
        return True

    return False


def fetch_games_by_competition(
    competition_ids: List[int],
    timezone_name: str = "Africa/Nairobi",
    user_country_id: int = 413,
    show_odds: bool = True,
) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch games for given competition IDs using the /web/games/fixtures/ endpoint.
    """
    params = {
        "appTypeId": 5,
        "langId": 1,
        "timezoneName": timezone_name,
        "userCountryId": user_country_id,
        "competitions": ",".join(str(cid) for cid in competition_ids),
        "showOdds": str(show_odds).lower(),
        "includeTopBettingOpportunity": "1",
        "topBookmaker": "14",
    }

    url = f"{BASE_URL}/web/games/fixtures/"

    try:
        logger.debug(f"Fetching from {url} with params {params}")
        response = requests.get(url, headers=DEFAULT_HEADERS, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        games = data.get("games", [])
        logger.info(
            f"fetch_games_by_competition({competition_ids}): {len(games)} games returned"
        )

        return games

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch games from 365Scores: {e}")
        return None
    except ValueError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        return None


def fetch_game_details(
    game_id: str,
    away_id: int,
    home_id: int,
    competition_id: int,
    lang_id: int = 1,
    user_country_id: int = 413,
) -> Optional[Dict[str, Any]]:
    """
    Fetch full game details including lineups using the /web/game/ endpoint.

    Args:
        game_id: 365Scores game ID (e.g., "4627864")
        away_id: Away team competitor ID
        home_id: Home team competitor ID
        competition_id: Competition ID (e.g., 5930)
        lang_id: Language ID (1 = English)
        user_country_id: Country ID (413 = Kenya)

    Returns:
        Full game data including lineups, statistics, events, commentary
    """
    matchup_id = f"{away_id}-{home_id}-{competition_id}"

    params = {
        "appTypeId": 5,
        "langId": lang_id,
        "timezoneName": "Africa/Nairobi",
        "userCountryId": user_country_id,
        "gameId": game_id,
        "matchupId": matchup_id,
    }

    url = f"{BASE_URL}/web/game/"

    try:
        logger.debug(f"Fetching game details from {url} with params {params}")
        response = requests.get(url, headers=DEFAULT_HEADERS, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        logger.info(f"fetch_game_details({game_id}): Success")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch game details for {game_id}: {e}")
        return None
    except ValueError as e:
        logger.error(f"Failed to parse JSON response for {game_id}: {e}")
        return None


def _lineup_has_real_squad(lineup: Optional[Dict[str, Any]]) -> bool:
    """
    True only if a lineup object actually has player entries in it.

    365Scores can return a non-null lineups object on a competitor BEFORE
    the official squad is published -- e.g. {"formation": "", "members": []}.
    That object is truthy (`if not lineup` sees it as present), but carries
    zero real squad data. Treat that as "not ready" so callers never
    mistake a shell object for a real, storable lineup.
    """
    if not lineup:
        return False
    members = lineup.get("members") or []
    return len(members) > 0


def fetch_lineups(
    game_id: str, away_id: int, home_id: int, competition_id: int
) -> Optional[Dict[str, Any]]:
    """
    Fetch only lineups from the game details endpoint.

    Returns None if lineups aren't genuinely published yet -- either the
    key is missing entirely, OR 365Scores has only returned an empty
    pre-publish placeholder (non-null object, but zero members) on BOTH
    sides. Callers must treat None as "try again later, nothing to
    store" -- this function will never return a result where both sides
    have zero members, since that shape gets permanently persisted
    downstream with no retry mechanism once forwarded.

    Returns (on success -- at least one side has a real squad):
        {
            "fixture_id": "wc26_<game_id>",
            "home": {
                "formation": "4-3-3",
                "members": [...]
            },
            "away": {
                "formation": "4-2-3-1",
                "members": [...]
            }
        }
    """
    data = fetch_game_details(game_id, away_id, home_id, competition_id)

    if not data or "game" not in data:
        logger.warning(f"No game data found for {game_id}")
        return None

    game = data.get("game", {})

    home_competitor = game.get("homeCompetitor", {})
    away_competitor = game.get("awayCompetitor", {})

    home_lineups = home_competitor.get("lineups")
    away_lineups = away_competitor.get("lineups")

    # FIX: check for actual squad content, not just object presence. A
    # pre-publish placeholder dict is truthy but has zero real players --
    # the old `if not home_lineups and not away_lineups` check let that
    # straight through.
    if not _lineup_has_real_squad(home_lineups) and not _lineup_has_real_squad(
        away_lineups
    ):
        home_member_count = len((home_lineups or {}).get("members") or [])
        away_member_count = len((away_lineups or {}).get("members") or [])
        logger.debug(
            f"Lineups not yet published for {game_id} "
            f"(home_members={home_member_count}, away_members={away_member_count}) "
            f"-- will retry on next poll cycle"
        )
        return None

    # Player names live in a separate top-level "members" array on the
    # game object, keyed by the same "id" used inside lineups.members[].
    # The lineup entries themselves never include a name field, so we
    # have to join them here.
    roster = {m["id"]: m for m in game.get("members", []) if "id" in m}

    def _attach_names(lineup: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not lineup:
            return {}
        for player in lineup.get("members", []):
            info = roster.get(player.get("id"))
            if info:
                player["name"] = info.get("name")
                player["shortName"] = info.get("shortName")
                player["athleteId"] = info.get("athleteId")
                player["jerseyNumber"] = info.get("jerseyNumber")
        return lineup

    home_lineups = _attach_names(home_lineups)
    away_lineups = _attach_names(away_lineups)

    result = {
        "fixture_id": f"wc26_{game_id}",
        "home": home_lineups or {},
        "away": away_lineups or {},
    }

    logger.info(f"fetch_lineups({game_id}): Found lineups")
    return result


# All keywords are matched with regex word boundaries (\b), never plain
# substring containment -- naive "in" checks cause false positives like
# "pen" matching inside "suspended", or "ended" matching inside
# "suspended" too. \b works fine for multi-word phrases like "half time"
# since spaces are already non-word characters.
HALFTIME_STATUS_KEYWORDS = ("ht", "half time", "halftime")
STOPPED_STATUS_KEYWORDS = (
    "stopped",
    "suspended",
    "interrupted",
    "delayed",
    "abandoned",
)
FULLTIME_STATUS_KEYWORDS = (
    "ft",
    "aet",
    "pen",
    "ended",
    "finished",
    "full-time",
    "full time",
    "penalties",
)


def _matches(text: str, keywords: tuple) -> bool:
    return any(re.search(r"\b" + re.escape(kw) + r"\b", text) for kw in keywords)


def classify_match_phase(status_text: Optional[str]) -> Optional[str]:
    """
    Classify a 365Scores statusText into one of the three moments we care
    about for statistics snapshots: "halftime", "stopped", "fulltime".
    Returns None if the match is in open play (or status is unknown).
    """
    text = (status_text or "").strip().lower()
    if not text:
        return None
    if _matches(text, FULLTIME_STATUS_KEYWORDS):
        return "fulltime"
    if _matches(text, HALFTIME_STATUS_KEYWORDS):
        return "halftime"
    if _matches(text, STOPPED_STATUS_KEYWORDS):
        return "stopped"
    return None


# Set to True the first time extract_statistics_from_game() logs its
# "not implemented" error, so the poller's logs get one loud warning per
# process instead of one per snapshot (every halftime/stopped/fulltime
# transition, for every live match). The underlying problem never
# changes call to call, so repeating the same error every ~15s adds
# noise without adding information.
_STATS_UNIMPLEMENTED_WARNED = False


def extract_statistics_from_game(game: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Build the statistics payload from an already-fetched `game` object.

    STATUS: NOT IMPLEMENTED. The home*/away* fields this used to read
    (homePossession, homeShots, homeCorners, homeFouls, homeYellowCards,
    homeRedCards, homeOffsides, homePasses, homePassAccuracy, and the
    away* equivalents) do not exist anywhere on the `game` object --
    verified against the repo's committed sample response,
    game_4627864.json. Real statistics for a match with hasStats: True
    are served through one of the URLs in game["widgets"] (SportRadar-
    hosted, e.g. an LMT/Momentum widget), the same way commentary lives
    behind playByPlay.feedURL instead of directly on /web/game/. Nobody
    has captured a raw response from one of those widget URLs yet, so
    there is no confirmed field shape to parse.

    Returns None unconditionally right now, on purpose -- NOT a dict of
    Nones. Returning a fully-populated-looking dict where every value
    happens to be None is exactly how the original bug went undetected:
    it stores and forwards indistinguishably from "this match really has
    no stats", and both poller.py and downstream storage treat it as a
    successful snapshot. Callers (see fetch_statistics() and
    fetch_complete_match_data() below) must treat this None the same way
    fetch_lineups() treats an unpublished lineup: "nothing to store".

    To implement this for real:
      1. Capture one raw response body from a widgetUrl in
         game["widgets"] for a live match (e.g. the SportRadarLMT_V3
         entry) and inspect its actual field names.
      2. Replace this function's body with a parser for that confirmed
         shape.
      3. Remove the _STATS_UNIMPLEMENTED_WARNED short-circuit below.
    """
    global _STATS_UNIMPLEMENTED_WARNED
    if not _STATS_UNIMPLEMENTED_WARNED:
        logger.error(
            "extract_statistics_from_game() is not implemented: the "
            "home*/away* stat fields it used to read do not exist on the "
            "/web/game/ response (confirmed against game_4627864.json). "
            "Real stats live behind one of game['widgets'] (SportRadar-"
            "hosted), which has not been captured/parsed yet. Returning "
            "None instead of a dict of Nones so this stops being stored "
            "as a fake zero-value statistics snapshot. See this "
            "function's docstring for what's needed to implement it."
        )
        _STATS_UNIMPLEMENTED_WARNED = True
    else:
        logger.debug(
            "extract_statistics_from_game() called again -- still not "
            "implemented, returning None (see earlier error log)."
        )
    return None


def fetch_statistics(
    game_id: str, away_id: int, home_id: int, competition_id: int
) -> Optional[Dict[str, Any]]:
    """
    Fetch statistics from the game details endpoint.

    NOTE: this makes its own fetch_game_details() call. If you already
    have a `game` object on hand, prefer extract_statistics_from_game(game)
    to avoid a redundant network request.

    Returns None -- see extract_statistics_from_game()'s docstring.
    Statistics extraction is not implemented; the guessed home*/away*
    fields it used to read don't exist on this endpoint's response.
    """
    data = fetch_game_details(game_id, away_id, home_id, competition_id)

    if not data or "game" not in data:
        return None

    game = data.get("game", {})
    return extract_statistics_from_game(game)


# ============================================================================
# PLAY-BY-PLAY FEED (shared by fetch_commentary and fetch_match_events)
# ----------------------------------------------------------------------------
# The /web/game/ endpoint does NOT embed commentary or a discrete events
# list directly -- game.commentary and game.events are not real fields.
# It only returns game.playByPlay.feedURL, a pointer to a separate feed
# (pbpgenerator.365scores.com). That feed is the ONLY place 365Scores
# exposes per-event type/minute/side data; the cumulative counters used
# by extract_statistics_from_game() (homeCorners, homeYellowCards, ...)
# have no per-event breakdown.
#
# THIS SHARED HELPER WAS MISSING from the league version of this file --
# fetch_commentary() and fetch_match_events() each duplicated their own
# raw fetch instead of sharing one. Restored to match the World Cup
# version so both functions stay consistent and only need one network
# call's worth of logic to maintain.
#
# Two things the raw feed gets wrong that we correct here:
#   1. The feedURL 365Scores returns comes pre-built with lang=37
#      (Dutch), not English -- every other endpoint in this file uses
#      langId=1, so we force lang=1 here too before fetching.
#   2. Entry field names are PascalCase (.NET-style): Comment, Timeline,
#      Type, TypeName, Period, Title, IsMajor, Players, CompetitorNum --
#      not the lowercase minute/text/type/team/player shape used
#      elsewhere in this codebase.
# ============================================================================


def _fetch_play_by_play_raw(
    game_id: str, away_id: int, home_id: int, competition_id: int
) -> List[Dict[str, Any]]:
    """Shared fetch + unwrap for the play-by-play feed. Both
    fetch_commentary() and fetch_match_events() parse this same raw list
    into different shapes -- this only does the network call + finding
    the entry list in the response, not any field-level interpretation.
    """
    data = fetch_game_details(game_id, away_id, home_id, competition_id)

    if not data or "game" not in data:
        return []

    game = data.get("game", {})
    pbp = game.get("playByPlay") or {}
    feed_url = pbp.get("feedURL")

    if not feed_url:
        logger.debug(f"No playByPlay feedURL for {game_id}")
        return []

    # Force English -- 365Scores returns lang=37 (Dutch) by default here.
    feed_url = re.sub(r"lang=\d+", "lang=1", feed_url)

    try:
        response = requests.get(feed_url, headers=DEFAULT_HEADERS, timeout=30)
        response.raise_for_status()
        raw = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch play-by-play feed for {game_id}: {e}")
        return []
    except ValueError as e:
        logger.error(f"Failed to parse play-by-play JSON for {game_id}: {e}")
        return []

    # We don't rely on knowing the exact wrapper key name -- find the
    # first top-level list of dicts in the response instead.
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for value in raw.values():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return value

    return []


def _competitor_num_to_side(competitor_num: Any) -> Optional[str]:
    """365Scores' play-by-play feed marks each entry with CompetitorNum
    (1 or 2), not a competitor id -- it can't be joined against
    homeCompetitor/awayCompetitor.id the way fetch_lineups() joins
    roster members.

    UNCONFIRMED against a live payload: assuming NUM 1 = home,
    NUM 2 = away, matching the ordering 365Scores uses everywhere else
    in this file (homeCompetitor first, awayCompetitor second). Verify
    against one real play-by-play response before trusting this for
    actual sub-fixture settlement -- if it's backwards, every
    first_goal/first_card/first_corner market will settle to the wrong
    team.
    """
    if competitor_num == 1:
        return "home"
    if competitor_num == 2:
        return "away"
    return None


def fetch_commentary(
    game_id: str, away_id: int, home_id: int, competition_id: int
) -> List[Dict[str, Any]]:
    """
    Fetch commentary via 365Scores' separate play-by-play feed. See the
    module-level comment above _fetch_play_by_play_raw for why this
    can't come from the /web/game/ response directly.

    Returns:
        List of commentary entries with:
        {
            "minute": int,
            "text": str,
            "type": str,
            "team": Optional[str],   # "home" | "away" | None
            "player": Optional[str],
        }
        Note: createdAt is added by the poller when forwarding.
    """
    raw_commentary = _fetch_play_by_play_raw(game_id, away_id, home_id, competition_id)
    if not raw_commentary:
        return []

    commentary_list = []
    for entry in raw_commentary:
        minute_raw = entry.get("Timeline")
        try:
            minute = int(minute_raw) if minute_raw is not None else 0
        except (ValueError, TypeError):
            minute = 0

        text = entry.get("Comment") or entry.get("Title") or ""

        players = entry.get("Players") or []
        player = players[0].get("PlayerName") if players else None

        commentary_list.append(
            {
                "minute": minute,
                "text": text,
                "type": entry.get("TypeName", "commentary"),
                # FIX: this was hardcoded to None in the league version --
                # restored to actually resolve the side via CompetitorNum,
                # same as fetch_match_events() already did correctly.
                "team": _competitor_num_to_side(entry.get("CompetitorNum")),
                "player": player,
            }
        )

    logger.info(f"fetch_commentary({game_id}): Found {len(commentary_list)} entries")
    return commentary_list


# TypeName markers for classifying play-by-play entries into the three
# sub-fixture event buckets. Matched as substrings against the lowercased
# TypeName -- these are guesses at 365Scores' actual English TypeName
# strings ("Goal", "Yellow Card", "Red Card", "Corner", etc.) based on
# common convention across similar feeds; confirm against a real payload
# and adjust if 365Scores uses different wording.
_CARD_TYPENAME_MARKERS = ("yellow card", "red card", "second yellow")
_CORNER_TYPENAME_MARKERS = ("corner",)
_GOAL_TYPENAME_MARKERS = (
    "goal",
)  # checked last: "goal" is a substring of nothing above, but keep order defensive


def _classify_event_typename(type_name: Optional[str]) -> Optional[str]:
    t = (type_name or "").strip().lower()
    if any(m in t for m in _CARD_TYPENAME_MARKERS):
        return "card"
    if any(m in t for m in _CORNER_TYPENAME_MARKERS):
        return "corner"
    if any(m in t for m in _GOAL_TYPENAME_MARKERS):
        return "goal"
    return None


def fetch_match_events(
    game_id: str, away_id: int, home_id: int, competition_id: int
) -> List[Dict[str, Any]]:
    """
    Discrete goal/card/corner events, derived from the SAME play-by-play
    feed fetch_commentary() reads. This is what feeds the first_goal /
    first_card / first_corner sub-fixture markets -- there is no other
    per-event data source in this API client.

    Returns [{event_type, minute, team, player}, ...] for entries
    classified as goal/card/corner, sorted by minute. Anything else in
    the feed (kickoff markers, half-end markers, general commentary) is
    skipped here -- fetch_commentary() still returns those separately
    for the chat/commentary feed; this makes its own network call rather
    than sharing a single fetch with fetch_commentary(), consistent with
    how fetch_statistics()/fetch_lineups()/fetch_commentary() each
    already make independent fetch_game_details() calls in this file.

    KNOWN GAPS, both flagged inline where they matter:
      - Team attribution (_competitor_num_to_side) assumes
        CompetitorNum 1=home, 2=away -- unconfirmed against a live
        payload.
      - Own goals are not special-cased. A TypeName containing "goal"
        for an own goal will attribute the event to whichever side
        CompetitorNum points at (likely the scoring player's own team),
        which is backwards for a first_goal market -- an own goal by
        the away team should count as a home team's "first goal" for
        settlement purposes. Needs a real payload to know whether
        365Scores' TypeName distinguishes "Own Goal" from "Goal" so
        this can be corrected.
    """
    raw_commentary = _fetch_play_by_play_raw(game_id, away_id, home_id, competition_id)
    if not raw_commentary:
        return []

    events: List[Dict[str, Any]] = []
    for entry in raw_commentary:
        event_type = _classify_event_typename(entry.get("TypeName"))
        if event_type is None:
            continue

        team = _competitor_num_to_side(entry.get("CompetitorNum"))
        if team is None:
            logger.debug(
                f"{game_id}: skipping {event_type} event with unresolvable team "
                f"(CompetitorNum={entry.get('CompetitorNum')!r})"
            )
            continue

        minute_raw = entry.get("Timeline")
        try:
            minute = int(minute_raw) if minute_raw is not None else 0
        except (ValueError, TypeError):
            minute = 0

        players = entry.get("Players") or []
        player = players[0].get("PlayerName") if players else None

        events.append(
            {
                "event_type": event_type,
                "minute": minute,
                "team": team,
                "player": player,
            }
        )

    events.sort(key=lambda e: e["minute"])
    logger.info(
        f"fetch_match_events({game_id}): Found {len(events)} goal/card/corner events"
    )
    return events


def fetch_complete_match_data(
    game_id: str, away_id: int, home_id: int, competition_id: int
) -> Optional[Dict[str, Any]]:
    """
    Fetch all match data: details, lineups, statistics, and commentary in one go.

    NOTE: the "lineups" key here uses the RAW homeCompetitor/awayCompetitor
    .lineups objects directly -- unlike fetch_lineups(), this does NOT
    filter out empty pre-publish placeholders. If you're using this
    function's lineups output for anything that gets persisted (as
    opposed to transient display), check _lineup_has_real_squad() on
    each side yourself before storing, or use fetch_lineups() instead,
    which already guards this.

    NOTE: "statistics" now comes from extract_statistics_from_game(),
    same as fetch_statistics() -- it used to duplicate the (broken)
    field-guessing inline here separately, which meant a second copy of
    the same bug to fix. It will be None until statistics extraction is
    actually implemented; see extract_statistics_from_game()'s
    docstring.
    """
    data = fetch_game_details(game_id, away_id, home_id, competition_id)

    if not data or "game" not in data:
        return None

    game = data.get("game", {})

    return {
        "game_id": game_id,
        "details": game,
        "lineups": {
            "home": game.get("homeCompetitor", {}).get("lineups", {}),
            "away": game.get("awayCompetitor", {}).get("lineups", {}),
        },
        "statistics": extract_statistics_from_game(game),
        "commentary": game.get("commentary", []),
        "score": {
            "home": game.get("homeCompetitor", {}).get("score", 0),
            "away": game.get("awayCompetitor", {}).get("score", 0),
        },
        "status": game.get("statusText"),
        "time_elapsed": int(game.get("gameTime", 0) or 0),
        "is_finished": is_game_finished(game),
    }

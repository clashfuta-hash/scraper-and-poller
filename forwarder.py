"""
Forwards updates from poller to Rust backend API.
Handles: fixtures, live updates, events, commentary, lineups, statistics,
finalization, notifications, and sub-fixture markets.
"""

from __future__ import annotations

import logging
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("worldcup_poller.forwarder")


class Forwarder:
    def __init__(self, api_url: str, timeout: int = 30, max_retries: int = 3):
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout

        # Create session with retry logic
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "WorldCupPoller/1.0",
            }
        )

        # Retry strategy for transient failures
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "PUT", "GET", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _post(self, endpoint: str, data: Dict[str, Any]) -> bool:
        """Generic POST request with error handling."""
        url = f"{self.api_url}{endpoint}"
        try:
            response = self.session.post(url, json=data, timeout=self.timeout)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to POST to {endpoint}: {e}")
            if hasattr(e, "response") and e.response:
                logger.error(f"Payload: {data}")
                logger.error(f"Response: {e.response.text[:500]}")
            return False

    def _put(self, endpoint: str, data: Dict[str, Any]) -> bool:
        """Generic PUT request with error handling."""
        url = f"{self.api_url}{endpoint}"
        try:
            response = self.session.put(url, json=data, timeout=self.timeout)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to PUT to {endpoint}: {e}")
            return False

    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Generic GET request with error handling."""
        url = f"{self.api_url}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to GET from {endpoint}: {e}")
            return None

    # ============================================================
    # FIXTURE MANAGEMENT
    # ============================================================

    def forward_fixture(self, fixture: Dict[str, Any]) -> bool:
        """
        Forward a single fixture to the Rust API.
        """
        return self._post("/games", fixture)

    def forward_fixtures_bulk(self, fixtures: List[Dict[str, Any]]) -> bool:
        """
        Forward multiple fixtures in bulk.
        """
        return self._post("/games/bulk", {"fixtures": fixtures})

    # ============================================================
    # LIVE UPDATES
    # ============================================================

    def forward_live_update(self, update: Dict[str, Any]) -> bool:
        """
        Forward a live match update to the Rust API.

        Matches LiveGameUpdate struct in Rust:
        {
            "fixtureId": "wc26_123",
            "eventType": "score_update",
            "homeScore": 1,
            "awayScore": 0,
            "minute": 67,
            "minuteDisplay": "67'",
            "status": "live",
            "isLive": true,
            "availableForVoting": false,
            "scorer": "Player Name",
            "player": "Player Name",
            "assist": "Assist Name",
            "team": "home",
            "timestamp": "2026-07-22T15:05:18Z",
            "minutesPlayed": 67
        }
        """
        # Build payload with camelCase keys matching Rust struct
        payload = {}

        # Required fields - must always be present
        payload["fixtureId"] = update.get("fixture_id") or update.get("fixtureId")
        if not payload["fixtureId"]:
            logger.error("Missing fixture_id in live update")
            return False

        payload["eventType"] = (
            update.get("event_type") or update.get("eventType") or "score_update"
        )
        payload["homeScore"] = int(
            update.get("home_score") or update.get("homeScore") or 0
        )
        payload["awayScore"] = int(
            update.get("away_score") or update.get("awayScore") or 0
        )
        payload["minute"] = int(update.get("minute") or update.get("gameTime") or 0)

        # Optional fields - only include if present
        if "minute_display" in update or "minuteDisplay" in update:
            payload["minuteDisplay"] = update.get("minute_display") or update.get(
                "minuteDisplay"
            )
        if "status" in update:
            payload["status"] = update.get("status")
        if "is_live" in update:
            payload["isLive"] = update.get("is_live")
        if "available_for_voting" in update:
            payload["availableForVoting"] = update.get("available_for_voting")
        if "scorer" in update:
            payload["scorer"] = update.get("scorer")
        if "player" in update:
            payload["player"] = update.get("player")
        if "assist" in update:
            payload["assist"] = update.get("assist")
        if "team" in update:
            payload["team"] = update.get("team")
        if "timestamp" in update:
            payload["timestamp"] = update.get("timestamp")
        elif "timestamp" not in payload:
            payload["timestamp"] = datetime.now(timezone.utc).isoformat()
        if "minutes_played" in update or "minutesPlayed" in update:
            payload["minutesPlayed"] = update.get("minutes_played") or update.get(
                "minutesPlayed"
            )

        return self._post("/games/live-update", payload)

    def forward_score_update(
        self, match_id: str, home_score: int, away_score: int, minute: int
    ) -> bool:
        """
        Forward a score update.
        """
        payload = {
            "fixtureId": match_id,
            "eventType": "score",
            "homeScore": home_score,
            "awayScore": away_score,
            "minute": minute,
        }
        return self._post("/games/score", payload)

    def forward_status_update(
        self, match_id: str, status: str, is_live: bool, available_for_voting: bool
    ) -> bool:
        """
        Forward a status update.
        """
        payload = {
            "fixtureId": match_id,
            "status": status,
            "isLive": is_live,
            "availableForVoting": available_for_voting,
        }
        return self._post("/games/status", payload)

    # ============================================================
    # EVENTS (Goals, Cards, Substitutions)
    # ============================================================

    def forward_event(self, event: Dict[str, Any]) -> bool:
        """
        Forward a single event to the Rust API.

        Matches EventRequest struct:
        {
            "fixtureId": "wc26_123",
            "eventType": "goal",
            "minute": 23,
            "team": "home",
            "player": "Player Name",
            "assist": "Assist Name",
            "homeScore": 1,
            "awayScore": 0,
        }
        """
        # Build payload with camelCase keys
        payload = {}
        payload["fixtureId"] = event.get("fixture_id") or event.get("fixtureId")
        if not payload["fixtureId"]:
            logger.error("Missing fixture_id in event")
            return False

        payload["eventType"] = event.get("event_type") or event.get("eventType")
        if not payload["eventType"]:
            logger.error("Missing event_type in event")
            return False

        payload["minute"] = int(event.get("minute") or 0)
        payload["team"] = event.get("team")
        if not payload["team"]:
            logger.error("Missing team in event")
            return False

        payload["player"] = event.get("player")
        if not payload["player"]:
            logger.error("Missing player in event")
            return False

        payload["homeScore"] = int(
            event.get("home_score") or event.get("homeScore") or 0
        )
        payload["awayScore"] = int(
            event.get("away_score") or event.get("awayScore") or 0
        )

        if "assist" in event:
            payload["assist"] = event.get("assist")

        return self._post("/games/events", payload)

    def forward_bulk_events(self, bulk: Dict[str, Any]) -> bool:
        """
        Forward multiple events at once.

        Expected payload:
        {
            "fixtureId": "wc26_123",
            "events": [
                {
                    "eventType": "goal",
                    "minute": 23,
                    "team": "home",
                    "player": "Player Name",
                    ...
                }
            ]
        }
        """
        return self._post("/games/events/bulk", bulk)

    def forward_event_batch(
        self, fixture_id: str, events: List[Dict[str, Any]]
    ) -> bool:
        """
        Forward a batch of events for a fixture.
        """
        return self._post(f"/games/{fixture_id}/events/batch", {"events": events})

    # ============================================================
    # COMMENTARY
    # ============================================================

    def _format_timestamp(self, ts) -> str:
        """Plain ISO-8601 string with millisecond precision and a Z
        suffix, e.g. "2026-07-19T19:18:33.823Z". Used as the value
        wrapped by _format_bson_date() below -- never sent bare for
        commentary, since CommentaryEntry.created_at needs the Extended
        JSON shape (see _format_bson_date's docstring)."""
        if ts is None:
            return (
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            )
        if isinstance(ts, datetime):
            return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        if isinstance(ts, str):
            ts = ts.replace("+00:00", "Z")
            if "." not in ts:
                ts = ts.replace("Z", "").replace("+00:00", "")
                ts = ts + ".000Z"
            if not ts.endswith("Z") and "+" not in ts:
                ts = ts + "Z"
            return ts
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def _format_bson_date(self, ts) -> Dict[str, str]:
        """
        MongoDB Extended JSON date wrapper, e.g. {"$date": "2026-07-19T19:18:33.823Z"}.

        CommentaryEntry.created_at (models/game.rs) is typed
        `#[serde(rename = "createdAt")] pub created_at: BsonDateTime` --
        mongodb::bson::DateTime, NOT chrono::DateTime and NOT a plain
        String. When Axum's Json extractor deserializes a request body
        into that field, it's going through serde_json (the body is
        plain JSON, not real BSON), and bson::DateTime's Deserialize impl
        for self-describing formats only accepts the relaxed/canonical
        Extended JSON date shape -- {"$date": "<ISO-8601>"} -- not a bare
        string. Sending a bare string here 422s the whole request before
        the handler code (which overwrites created_at server-side anyway)
        ever runs.

        Contrast with forward_live_update() above, which sends
        "timestamp" as a bare ISO string and works fine -- that's because
        LiveGameUpdate.timestamp is `Option<chrono::DateTime<Utc>>`,
        which DOES accept a bare RFC3339 string. Don't wrap that one the
        same way, or it'll break instead.
        """
        return {"$date": self._format_timestamp(ts)}

    def _normalize_commentary_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build a commentary entry matching CommentaryEntry exactly:
        { minute: i32, text: String, type: String, team: Option<String>,
          player: Option<String>, createdAt: BsonDateTime }.
        """
        try:
            minute = int(entry.get("minute", 0))
        except (ValueError, TypeError):
            minute = 0

        text = str(entry.get("text", ""))

        event_type = entry.get("type") or entry.get("event_type") or "commentary"
        event_type = str(event_type)

        created_at = entry.get("createdAt") or entry.get("created_at")
        created_at = self._format_bson_date(created_at)

        return {
            "minute": minute,
            "text": text,
            "type": event_type,
            "team": entry.get("team"),
            "player": entry.get("player"),
            "createdAt": created_at,
        }

    def forward_commentary(self, commentary: Dict[str, Any]) -> bool:
        """
        Forward commentary to the Rust API.

        Expected payload:
        {
            "match_id": "wc26_123",
            "entry": {
                "minute": 23,
                "text": "Great goal by Player!",
                "type": "goal|chance|card|substitution|general",
                "team": "home|away",
                "player": "Player Name",
                "created_at": "2026-06-27T15:00:00Z"
            }
        }
        """
        payload = {
            "match_id": commentary.get("match_id"),
            "entry": self._normalize_commentary_entry(commentary.get("entry", {})),
        }
        return self._post("/games/commentary", payload)

    def forward_commentary_bulk(
        self, fixture_id: str, entries: List[Dict[str, Any]]
    ) -> bool:
        """
        Forward multiple commentary entries for a fixture.
        """
        normalized = [self._normalize_commentary_entry(e) for e in entries]
        payload = {"match_id": fixture_id, "entries": normalized}
        return self._post("/games/commentary/bulk", payload)

    # ============================================================
    # LINEUPS
    # ============================================================

    def forward_lineups(self, lineups: Dict[str, Any]) -> bool:
        """
        Forward lineups to the Rust API.

        IMPORTANT: keys MUST be camelCase to match Rust's LineupsUpdate
        struct (#[serde(rename = "fixtureId")] etc, all fields required,
        no Option/default) -- a snake_case payload here fails serde
        deserialization and the Rust API returns 422 Unprocessable Entity
        with no useful detail on which field was the problem. Confirmed
        by decoding the actual 422 response body during a live run.

        Expected payload:
        {
            "fixtureId": "wc26_123",
            "homeTeam": "Team A",
            "awayTeam": "Team B",
            "lineups": {
                "home": {
                    "formation": "4-3-3",
                    "coach": {"name": "Coach Name"},
                    "players": [
                        {
                            "name": "Player",
                            "position": "GK",
                            "jerseyNumber": 1,
                            "captain": false,
                            "lineup": "starting",
                            "playerId": "123"
                        }
                    ],
                    "bench": [...]
                },
                "away": {...}
            }
        }
        """
        return self._post("/games/lineups", lineups)

    def forward_lineups_simplified(
        self, fixture_id: str, home_players: List[Dict], away_players: List[Dict]
    ) -> bool:
        """
        Forward simplified lineups (just starting XI).
        """
        payload = {
            "fixture_id": fixture_id,
            "home": home_players,
            "away": away_players,
        }
        return self._post("/games/lineups/simplified", payload)

    # ============================================================
    # STATISTICS
    # ============================================================

    def forward_statistics(self, statistics: Dict[str, Any]) -> bool:
        """
        Forward match statistics to the Rust API.

        Expected payload:
        {
            "fixture_id": "wc26_123",
            "statistics": {
                "home": {
                    "possession": 55,
                    "shots": 12,
                    "shots_on_target": 5,
                    "shots_off_target": 4,
                    "corners": 6,
                    "fouls": 10,
                    "yellow_cards": 2,
                    "red_cards": 0,
                    "offsides": 1,
                    "passes": 450,
                    "pass_accuracy": 78,
                },
                "away": {
                    "possession": 45,
                    "shots": 8,
                    "shots_on_target": 3,
                    ...
                }
            },
            "minute": 67
        }
        """
        return self._post("/games/statistics", statistics)

    def forward_statistics_bulk(self, stats_bulk: Dict[str, Any]) -> bool:
        """
        Forward multiple statistics snapshots at once.

        Expected payload:
        {
            "fixture_id": "wc26_123",
            "snapshots": [
                {
                    "minute": 15,
                    "statistics": {...}
                },
                {
                    "minute": 30,
                    "statistics": {...}
                }
            ]
        }
        """
        return self._post("/games/statistics/bulk", stats_bulk)

    def forward_statistics_snapshot(
        self, fixture_id: str, minute: int, stats: Dict[str, Any]
    ) -> bool:
        """
        Forward a single statistics snapshot.
        """
        payload = {
            "fixture_id": fixture_id,
            "minute": minute,
            "statistics": stats,
        }
        return self._post("/games/statistics/snapshot", payload)

    # ============================================================
    # MATCH FINALIZATION
    # ============================================================

    def finalize_match(self, finalize_data: Dict[str, Any]) -> bool:
        """
        Finalize match result.

        Expected payload:
        {
            "fixture_id": "wc26_123",
            "result": "home|away|draw",
            "home_score": 2,
            "away_score": 1,
            "winner": "home_team|away_team|none",
            "status": "completed",
        }
        """
        return self._post("/games/finalize", finalize_data)

    def forward_match_result(
        self, fixture_id: str, result: str, home_score: int, away_score: int
    ) -> bool:
        """
        Forward just the match result.
        """
        payload = {
            "fixture_id": fixture_id,
            "result": result,
            "home_score": home_score,
            "away_score": away_score,
        }
        return self._post("/games/result", payload)

    def move_to_history(self, fixture_id: str) -> bool:
        """
        Move a completed match to history.
        """
        return self._post(f"/games/{fixture_id}/move-to-history", {})

    def settle_bets(self, fixture_id: str, result: str) -> bool:
        """
        Settle all straight-up (moneyline) bets for a completed match.

        THIS METHOD WAS MISSING -- poller.py's _settle_and_complete_match()
        and the max-retries fallback in _fetch_live_updates() both call
        self.forwarder.settle_bets(match_id, result) unconditionally the
        moment a match reaches full-time. With this method absent, that
        call raised AttributeError on EVERY completed match -- not a
        caught/logged failure, an unhandled exception that aborted
        _process_match() before update_status(..., "completed") or
        move_to_history() ever ran. poll_once()'s per-match try/except
        swallowed it and logged an error, so the poller kept running and
        looked "fine" in the logs, but no match ever finished archiving.

        Args:
            fixture_id: The match id (e.g. "epl_4627864" or
                "seriea_4627864").
            result: "home", "away", or "draw".
        """
        if result not in ("home", "away", "draw"):
            logger.error(f"Invalid result for settlement: {result!r}")
            return False

        payload = {
            "fixture_id": fixture_id,
            "result": result,
        }
        logger.info(f"💰 Settling bets for {fixture_id} with result: {result}")
        return self._post("/actions/bet/settle", payload)

    # ============================================================
    # SUB-FIXTURE MARKETS
    # ============================================================

    def create_sub_fixture_market(
        self,
        match_id: str,
        market_type: str,
        options: List[str],
        line: Optional[float] = None,
        lock_at: Optional[str] = None,
    ) -> bool:
        """
        Create a single sub-fixture market for a fixture.
        """
        payload = {
            "match_id": match_id,
            "market_type": market_type,
            "options": options,
            "line": line,
            "lock_at": lock_at,
        }
        return self._post("/sub_fixtures/sub-fixture/market/create", payload)

    def create_sub_fixture_markets(self, match_id: str) -> bool:
        """
        Create the standard set of sub-fixture markets for a newly
        created fixture: first_goal, first_card, first_corner, and
        over_under_2_5.
        """
        markets = [
            {"market_type": "first_goal", "options": ["home", "away"]},
            {"market_type": "first_card", "options": ["home", "away"]},
            {"market_type": "first_corner", "options": ["home", "away"]},
            {
                "market_type": "over_under_2_5",
                "options": ["over", "under"],
                "line": 2.5,
            },
        ]

        all_ok = True
        for m in markets:
            ok = self.create_sub_fixture_market(
                match_id=match_id,
                market_type=m["market_type"],
                options=m["options"],
                line=m.get("line"),
            )
            if not ok:
                logger.error(
                    f"Failed to create sub-fixture market '{m['market_type']}' "
                    f"for {match_id}"
                )
                all_ok = False

        return all_ok

    def settle_sub_fixture_market(
        self, match_id: str, market_id: str, result: str
    ) -> bool:
        """
        Settle one sub-fixture market (first_goal / first_card /
        first_corner / over_under_2_5) with its outcome.

        THIS METHOD WAS ALSO MISSING, same class of bug as settle_bets
        above. poller.py calls this from TWO places, both unguarded:
          - _check_first_event_markets(), fired on EVERY live poll the
            moment a first goal/card/corner appears in the play-by-play
            feed. Without this method, the very first qualifying event
            in ANY live match raised AttributeError, which aborted
            _process_match() for that fixture BEFORE it ever reached
            _fetch_commentary() later in the same method (fetch_commentary
            is called after fetch_live_updates in poller.py's
            _process_match). Since the crash reproduces identically on
            every subsequent poll cycle for that same fixture, commentary
            (and, for matches where the crash happens before halftime,
            statistics too) silently stopped forever for that match, not
            just for one cycle.
          - _settle_over_under_market(), called unconditionally as part
            of match completion -- so even a 0-0 match with no
            goal/card/corner event at all still hit this same missing
            method at full-time, blocking settle_bets/move_to_history
            from ever running for it.

        CONFIRMED against sub_fixture_routes() in the Rust router: the
        settle route is "/sub-fixture/settle" -- flat, no "/market"
        segment -- unlike create's "/sub-fixture/market/create". The
        earlier guess that it mirrored create's path 404'd because of
        that extra "/market" segment; this is the corrected path.

        Args:
            match_id: The match id.
            market_id: One of MARKET_ID_FIRST_GOAL, MARKET_ID_FIRST_CARD,
                MARKET_ID_FIRST_CORNER, MARKET_ID_OVER_UNDER_2_5 (see
                poller.py's module-level constants).
            result: The settlement outcome -- "home"/"away" for the
                first_* markets, "over"/"under" for over_under_2_5.
        """
        payload = {
            "match_id": match_id,
            "market_id": market_id,
            "result": result,
        }
        return self._post("/sub_fixtures/sub-fixture/settle", payload)

    # ============================================================
    # NOTIFICATIONS
    # ============================================================

    def forward_notification(self, notification: Dict[str, Any]) -> bool:
        """
        Forward a notification to the Rust API.
        """
        return self._post("/games/notify", notification)

    def forward_lineups_available_notification(
        self, fixture_id: str, home_team: str, away_team: str
    ) -> bool:
        """
        Send notification that lineups are available.
        """
        payload = {
            "fixture_id": fixture_id,
            "event_type": "lineups_available",
            "title": f"📋 Lineups are out! {home_team} vs {away_team}",
            "body": f"Check the starting XI for {home_team} vs {away_team}.",
            "data": {
                "home_team": home_team,
                "away_team": away_team,
                "type": "lineups_available",
            },
        }
        return self._post("/games/notify", payload)

    def forward_match_live_notification(
        self, fixture_id: str, home_team: str, away_team: str
    ) -> bool:
        """
        Send notification that match is live.
        """
        payload = {
            "fixture_id": fixture_id,
            "event_type": "match_live",
            "title": f"⚽ {home_team} vs {away_team} is LIVE!",
            "body": f"The match has kicked off! Follow the action.",
            "data": {
                "home_team": home_team,
                "away_team": away_team,
                "type": "match_live",
            },
        }
        return self._post("/games/notify", payload)

    def forward_goal_notification(
        self,
        fixture_id: str,
        scorer: str,
        minute: int,
        home_score: int,
        away_score: int,
    ) -> bool:
        """
        Send notification that a goal was scored.
        """
        payload = {
            "fixture_id": fixture_id,
            "event_type": "goal_scored",
            "title": f"⚽ GOAL! {scorer} scores!",
            "body": f"{scorer} scores at {minute}'! Score: {home_score}-{away_score}",
            "data": {
                "scorer": scorer,
                "minute": minute,
                "home_score": home_score,
                "away_score": away_score,
                "type": "goal_scored",
            },
        }
        return self._post("/games/notify", payload)

    def forward_match_ended_notification(
        self, fixture_id: str, home_team: str, away_team: str, result: str
    ) -> bool:
        """
        Send notification that match has ended.
        """
        payload = {
            "fixture_id": fixture_id,
            "event_type": "match_ended",
            "title": f"🏁 Full Time: {home_team} vs {away_team}",
            "body": f"Match ended. Result: {result}",
            "data": {
                "home_team": home_team,
                "away_team": away_team,
                "result": result,
                "type": "match_ended",
            },
        }
        return self._post("/games/notify", payload)

    # ============================================================
    # GAME MANAGEMENT
    # ============================================================

    def get_game(self, match_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a game by match_id from the Rust API.
        """
        return self._get(f"/games/match/{match_id}")

    def get_live_games(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get all live games from the Rust API.
        """
        return self._get("/games/live")

    def get_upcoming_games(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get all upcoming games from the Rust API.
        """
        return self._get("/games/upcoming")

    def get_history_games(
        self, limit: int = 50, skip: int = 0
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get history games from the Rust API.
        """
        return self._get("/games/history", {"limit": limit, "skip": skip})

    # ============================================================
    # BULK SYNC
    # ============================================================

    def sync_fixtures(self, fixtures: List[Dict[str, Any]]) -> bool:
        """
        Sync all fixtures at once (full update).
        """
        return self._post("/games/sync", {"fixtures": fixtures})

    def sync_live_data(self, live_data: Dict[str, Any]) -> bool:
        """
        Sync live data for multiple matches at once.
        """
        return self._post("/games/sync/live", live_data)

    # ============================================================
    # HEALTH CHECK
    # ============================================================

    def health_check(self) -> bool:
        """
        Check if the Rust API is healthy.
        """
        result = self._get("/health")
        return result is not None and result.get("status") == "healthy"

    def ping(self) -> bool:
        """
        Simple ping to check API availability.
        """
        try:
            response = self.session.get(f"{self.api_url}/ping", timeout=5)
            return response.status_code == 200
        except:
            return False


# ============================================================
# FACTORY FUNCTION
# ============================================================


def create_forwarder(api_url: str = None, **kwargs) -> Forwarder:
    """
    Create a Forwarder instance with optional configuration.
    """
    import os

    if api_url is None:
        api_url = os.environ.get(
            "FANCLASH_API", "https://clash-api-m5mr.onrender.com/api"
        )
    return Forwarder(api_url, **kwargs)

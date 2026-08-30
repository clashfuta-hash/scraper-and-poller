# League-Based Fixtures — What Changed

## 1. New leagues, one collection: `games` (was `fixtures`)

`config.py` now defines `LEAGUES`, a dict of the six competitions requested:

| key                | Competition            | 365Scores competitionId |
|--------------------|-------------------------|--------------------------|
| `epl`              | Premier League          | 7                        |
| `seriea`           | Serie A                 | 17                       |
| `ucl`              | UEFA Champions League   | 572                      |
| `europa`           | UEFA Europa League      | 573                      |
| `facup`            | FA Cup                  | 8                        |
| `community_shield` | Community Shield        | 10                       |

These IDs were derived from each competition's canonical 365scores.com URL
slug (e.g. `.../league/premier-league-7` → `7`). **I could not live-verify
them from this sandbox** — the container's network egress only allows a
fixed domain allowlist (github, pypi, npm, etc.), not `365scores.com` —
so before you rely on this in production, run:

```
python leagues_scraper.py --league all
```

against a real MONGO_URI once and check the logs / collection for each
league actually returning games. If any league logs `0 games returned`,
re-derive its id from `https://www.365scores.com/football/league/<slug>-<id>`
and update `config.LEAGUES`.

`config.MONGO_COLLECTION` now defaults to **`"games"`** instead of
`"fixtures"`, and `.env` was updated to match. Both the original World Cup
`scraper.py` and the new `leagues_scraper.py` write into whatever
`config.MONGO_COLLECTION` points at, so they now share the same
`games` collection automatically — no per-file collection name to update.

## 2. New file: `leagues_scraper.py`

```bash
# Scrape every configured league (full fixture list each):
python leagues_scraper.py --league all

# Scrape just one league:
python leagues_scraper.py --league epl
python leagues_scraper.py --league facup

# Fetch ONLY one round of EPL fixtures (auto-detects the next/current
# round — if the season hasn't started yet, that's Round 1):
python leagues_scraper.py --league epl --round-only

# Pin a specific round instead of auto-detecting:
python leagues_scraper.py --league epl --round-only --round-num 1
```

Each upserted document gets `matchId = "<prefix>_<365scores gameId>"`,
e.g. `epl_4627864`, `facup_...`, `ucl_...` — mirroring the existing
`wc26_<gameId>` convention so nothing else has to special-case IDs.

New fields added to every document (all `None` on old World Cup docs,
populated on new league docs): `leagueKey`, `roundNum`, `roundName`,
`groupNum`, `groupName`. A new compound index `(leagueKey, roundNum)`
was added so you can query e.g. "give me EPL round 1" directly:

```python
store.get_fixtures_by_league_round("epl", 1)
```

## 3. New Render endpoints (`server.py`)

- `GET /scrape/leagues?league=epl` — trigger one league (or `league=all`)
- `GET /scrape/epl-round` — trigger the EPL-round-only scrape
  (`?round=1` to pin a round; omit for auto-detect)

The existing `/scrape` (World Cup only) is untouched.

## 4. ⚠️ Cross-repo action required: `fanclash-api`

`patch.md` in this repo shows the Rust API's `games.rs` handlers doing:

```rust
let collection: Collection<Game> = state.db.collection("fixtures");
```

That collection name is **hardcoded in the Rust API**, which is a
*different* repo (`fanclash-api`) that wasn't provided here. Since this
scraper now writes to `games`, the Rust API needs the same rename
(`"fixtures"` → `"games"`) in every handler that does
`state.db.collection("fixtures")`, or the API will keep reading an
increasingly stale/empty `fixtures` collection while the scraper fills up
`games`. I can do that pass too if you share/paste `fanclash-api`.

## 5. Also noted, not changed

`.env` is committed to the repo with no `.gitignore`. The `MONGO_URI` value
in it is a placeholder (`username:password@...`), not a real credential, so
nothing sensitive was actually exposed here — but the same pattern is what
already burned you with the signing keystore. Worth adding a `.gitignore`
for `.env` and rotating anything in it if a real URI was ever pasted in
during testing.

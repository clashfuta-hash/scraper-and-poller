"""
Viewer tracking for live matches.
Tracks which users are viewing which matches for engagement metrics.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Set, Optional
from collections import defaultdict

logger = logging.getLogger("worldcup_poller.viewer_gate")


class ViewerGate:
    """Tracks active viewers for live matches."""
    
    def __init__(self, stale_threshold_seconds: int = 60):
        self.stale_threshold_seconds = stale_threshold_seconds
        self.viewers: Dict[str, Set[str]] = defaultdict(set)  # match_id -> set of user_ids
        self.last_heartbeat: Dict[str, datetime] = {}  # user_id -> last heartbeat

    def heartbeat(self, user_id: str, match_id: str) -> int:
        """Record a user heartbeat for a match."""
        self.viewers[match_id].add(user_id)
        self.last_heartbeat[user_id] = datetime.now(timezone.utc)
        self._cleanup_stale()
        return len(self.viewers[match_id])

    def get_viewer_count(self, match_id: str) -> int:
        """Get active viewer count for a match."""
        self._cleanup_stale()
        return len(self.viewers.get(match_id, set()))

    def get_all_viewer_counts(self) -> Dict[str, int]:
        """Get viewer counts for all matches."""
        self._cleanup_stale()
        return {match_id: len(users) for match_id, users in self.viewers.items()}

    def remove_viewer(self, user_id: str, match_id: str) -> int:
        """Remove a viewer from a match."""
        if match_id in self.viewers:
            self.viewers[match_id].discard(user_id)
        self.last_heartbeat.pop(user_id, None)
        return len(self.viewers.get(match_id, set()))

    def _cleanup_stale(self):
        """Remove stale viewers who haven't sent a heartbeat."""
        now = datetime.now(timezone.utc)
        stale_users = [
            user_id for user_id, last in self.last_heartbeat.items()
            if (now - last).total_seconds() > self.stale_threshold_seconds
        ]
        
        for user_id in stale_users:
            self.last_heartbeat.pop(user_id, None)
            for match_id in list(self.viewers.keys()):
                self.viewers[match_id].discard(user_id)

        # Remove empty match entries
        empty_matches = [mid for mid, users in self.viewers.items() if not users]
        for mid in empty_matches:
            del self.viewers[mid]


# Global viewer gate instance
viewer_gate = ViewerGate()
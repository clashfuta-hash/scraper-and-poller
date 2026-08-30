"""
Circuit breaker for external API calls to prevent cascading failures.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("worldcup_poller.breaker")


class CircuitBreaker:
    """Simple circuit breaker for external API calls."""
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_max_attempts: int = 3,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_attempts = half_open_max_attempts
        
        self.failure_count = 0
        self.state = "closed"  # closed, open, half-open
        self.last_failure_time: Optional[datetime] = None
        self.half_open_attempts = 0

    def call(self, func, *args, **kwargs):
        """Execute a function with circuit breaker protection."""
        if not self._allow_request():
            logger.warning(f"Circuit breaker '{self.name}' is OPEN, request blocked")
            raise CircuitBreakerOpenError(f"Circuit breaker '{self.name}' is open")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _allow_request(self) -> bool:
        """Check if request should be allowed."""
        if self.state == "closed":
            return True
        
        if self.state == "open":
            # Check if recovery timeout has elapsed
            if self.last_failure_time:
                elapsed = (datetime.now() - self.last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout:
                    self.state = "half-open"
                    self.half_open_attempts = 0
                    logger.info(f"Circuit breaker '{self.name}' moved to half-open")
                    return True
            return False
        
        # half-open: allow limited requests
        if self.half_open_attempts < self.half_open_max_attempts:
            self.half_open_attempts += 1
            return True
        
        return False

    def _on_success(self):
        """Called when request succeeds."""
        if self.state == "half-open":
            self.state = "closed"
            self.failure_count = 0
            self.half_open_attempts = 0
            logger.info(f"Circuit breaker '{self.name}' recovered to closed")
        elif self.state == "closed":
            self.failure_count = 0

    def _on_failure(self):
        """Called when request fails."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()

        if self.state == "closed" and self.failure_count >= self.failure_threshold:
            self.state = "open"
            logger.warning(f"Circuit breaker '{self.name}' opened after {self.failure_count} failures")
        elif self.state == "half-open":
            self.state = "open"
            logger.warning(f"Circuit breaker '{self.name}' re-opened after half-open failure")


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


# Global breakers
_breakers = {}


def get_breaker(name: str) -> CircuitBreaker:
    """Get or create a circuit breaker by name."""
    if name not in _breakers:
        _breakers[name] = CircuitBreaker(name)
    return _breakers[name]
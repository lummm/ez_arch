import os
from typing import NamedTuple


DEFAULT_POLL_INTERVAL_MS = 3000

class _ENV(NamedTuple):
    BROADCAST_PORT: int = int(os.environ.get("BROADCAST_PORT", -1))
    DEALER_PORT: int = int(os.environ.get("DEALER_PORT", -1))
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO")
    POLL_INTERVAL_MS: int = int(os.environ.get(
        "POLL_INTERVAL_MS", DEFAULT_POLL_INTERVAL_MS
    ))
    IN_PORT: int = int(os.environ["IN_PORT"])
ENV = _ENV()

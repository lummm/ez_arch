import os
from typing import NamedTuple


DEFAULT_POLL_INTERVAL_MS = 3000

class _ENV(NamedTuple):
    BROADCAST_PORT: int = int(os.environ.get("BROADCAST_PORT", -1))
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO")
    POLL_INTERVAL_MS: int = int(os.environ.get(
        "POLL_INTERVAL_MS", DEFAULT_POLL_INTERVAL_MS
    ))
    PORT_A: int = int(os.environ["PORT_A"])
    PORT_B: int = int(os.environ["PORT_B"])
ENV = _ENV()

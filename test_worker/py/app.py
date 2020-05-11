from typing import List
from typing import NamedTuple

import zmq.asyncio

DEFAULT_HEARTBEAT_S = 3

DEFAULT_POLL_INTERVAL_MS = 3000


Frames = List[bytes]


class App(NamedTuple):
    c: zmq.asyncio.Context = zmq.asyncio.Context()
    dealer: zmq.asyncio.Socket = None
    hb_interval_s: float = DEFAULT_HEARTBEAT_S
    con_s: str = ""
    poller: zmq.asyncio.Poller = zmq.asyncio.Poller()
    poll_interval_ms: int = DEFAULT_POLL_INTERVAL_MS
    service_name: bytes = b""

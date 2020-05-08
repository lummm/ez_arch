from typing import List
from typing import NamedTuple

import zmq

DEFAULT_HEARTBEAT_S = 3

DEFAULT_POLL_INTERVAL_MS = 3000


Frames = List[bytes]


class App(NamedTuple):
    c: zmq.Context = zmq.Context()
    dealer: zmq.Socket = None
    hb_interval_s: float = DEFAULT_HEARTBEAT_S
    in_con_s: str = ""
    out_con_s: str = ""
    poller: zmq.Poller = zmq.Poller()
    poll_interval_ms: int = DEFAULT_POLL_INTERVAL_MS
    router: zmq.Socket = None
    service_name: bytes = b""

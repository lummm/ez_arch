from typing import List
from typing import NamedTuple

import zmq

DEFAULT_HEARTBEAT_MS = 3000


Frames = List[bytes]


class App(NamedTuple):
    c: zmq.Context = zmq.Context()
    dealer: zmq.Socket = None
    hb_interval_ms: int = DEFAULT_HEARTBEAT_MS
    in_con_s: str = ""
    out_con_s: str = ""
    poller: zmq.Poller = zmq.Poller()
    router: zmq.Socket = None
    service_name: bytes = b""

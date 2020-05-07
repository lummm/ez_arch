from typing import List
from typing import NamedTuple

import zmq

from env import ENV


Frames = List[bytes]


class App(NamedTuple):
    broker_dealer: zmq.Socket = None
    broker_sub: zmq.Socket = None
    c: zmq.Context = zmq.Context()
    in_router: zmq.Socket = None
    poller: zmq.Poller = zmq.Poller()
    poll_interval_ms: int = ENV.POLL_INTERVAL_MS
    worker_dealer: zmq.Socket = None

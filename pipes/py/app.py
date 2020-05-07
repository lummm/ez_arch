from typing import NamedTuple

import zmq

from env import ENV


class App(NamedTuple):
    c: zmq.Context = zmq.Context()
    dealer: zmq.Socket = None
    poller: zmq.Poller = zmq.Poller()
    poll_interval_ms: int = ENV.POLL_INTERVAL_MS
    pub: zmq.Socket = None
    router: zmq.Socket = None

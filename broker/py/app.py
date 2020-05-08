from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Set
from typing import Tuple

import zmq

from env import ENV


Frames = List[bytes]


class App(NamedTuple):
    broker_dealer: zmq.Socket = None
    broker_sub: zmq.Socket = None
    c: zmq.Context = zmq.Context()
    in_router: zmq.Socket = None
    poller: zmq.Poller = zmq.Poller()
    service_addrs: Dict[bytes, Set[bytes]] = {} # service_id -> avail worker addrs
    worker_dealer: zmq.Socket = None
    worker_expiry: Dict[bytes, int] = {} # worker_addr -> expiry ts

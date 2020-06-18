import asyncio
from typing import Dict
from typing import NamedTuple
from typing import Set

import zmq

from apptypes import Frames


class State(NamedTuple):
    broker_dealer: zmq.Socket = None
    broker_sub: zmq.Socket = None
    c: zmq.Context = zmq.Context()
    in_pipe_addr: bytes = b""
    in_router: zmq.Socket = None
    poller: zmq.Poller = zmq.Poller()
    service_addrs: Dict[bytes, Set[bytes]] = {} # service_id -> avail worker addrs
    worker_pipe_addr: bytes = b""
    worker_router: zmq.Socket = None
    worker_tasks: Dict[bytes, int] = {}        # worker_addr -> # tasks assigned
    worker_expiry: Dict[bytes, float] = {} # worker_addr -> expiry ts


_state = State()

work_qs: asyncio.Queue = asyncio.Queue()


def state() -> State:
    return _state


def replace(new_state: State) -> None:
    global _state
    _state = new_state
    return


def update(**kwargs) -> None:
    replace(
        _state._replace(**kwargs)
    )
    return


def q_work(frames: Frames) -> None:
    work_qs.put_nowait(frames)
    return

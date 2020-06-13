import logging
import time

import app
from apptypes import Frames
from env import ENV
import state
import protoc


def send_to_worker(
        worker_addr: bytes,
        return_addr: bytes,
        request_id: bytes,
        req_body: Frames
) -> None:
    frames = [app.state().worker_pipe_addr, worker_addr, b"",
              return_addr, request_id] + req_body
    app.state().worker_router.send_multipart(frames)
    return


def process_heartbeat(
        worker_addr: bytes,
        frames: Frames          # LEVEL 2 WORKER HEARTBEAT
) -> None:
    service_name = frames[0]
    state.broadcast_heartbeat(worker_addr, service_name)
    return


def process_reply(
        worker_addr: bytes,
        frames: Frames          # LEVEL 2 WORKER REPLY
) -> None:
    response = frames
    app.state().in_router.send_multipart([app.state().in_pipe_addr] + response)
    state.broadcast_worker_unengaged(worker_addr)
    return


def remove_worker(
        worker_addr: bytes
) -> None:
    logging.info("removing worker at addr: %s", worker_addr)
    app.state().worker_expiry.pop(worker_addr, None)
    app.state().worker_tasks.pop(worker_addr, None)
    for service_name in app.state().service_addrs:
        app.state().service_addrs[service_name] = set(filter(
            lambda addr: addr != worker_addr,
            app.state().service_addrs[service_name]
        ))
    return


def purge_dead_workers() -> None:
    now = time.time()
    items = list(app.state().worker_expiry.items())
    for w, expiry_ts in items:
        if expiry_ts < now:
            remove_worker(w)
    return

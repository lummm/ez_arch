import logging
import time

from app import App
from app import Frames
from env import ENV
import state
import protoc


def send_to_worker(
        app: App,
        worker_addr: bytes,
        return_addr: bytes,
        request_id: bytes,
        req_body: Frames
) -> App:
    frames = [app.worker_pipe_addr, worker_addr, b"",
              return_addr, request_id] + req_body
    app.worker_router.send_multipart(frames)
    return app


def process_heartbeat(
        app: App,
        worker_addr: bytes,
        frames: Frames          # LEVEL 2 WORKER HEARTBEAT
) -> App:
    service_name = frames[0]
    state.broadcast_heartbeat(app, worker_addr, service_name)
    return app


def process_reply(
        app: App,
        worker_addr: bytes,
        frames: Frames          # LEVEL 2 WORKER REPLY
) -> App:
    response = frames
    app.in_router.send_multipart([app.in_pipe_addr] + response)
    state.broadcast_worker_unengaged(app, worker_addr)
    return app


def remove_worker(
        app: App,
        worker_addr: bytes
) -> App:
    logging.info("removing worker at addr: %s", worker_addr)
    app.worker_expiry.pop(worker_addr, None)
    app.worker_tasks.pop(worker_addr, None)
    for service_name in app.service_addrs:
        app.service_addrs[service_name] = set(filter(
            lambda addr: addr != worker_addr,
            app.service_addrs[service_name]
        ))
    return app


def purge_dead_workers(app: App) -> App:
    now = time.time()
    items = list(app.worker_expiry.items())
    for w, expiry_ts in items:
        if expiry_ts < now:
            app = remove_worker(app, w)
    return app

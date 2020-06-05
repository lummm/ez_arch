import logging

from app import App
from app import Frames
import protoc
import state
import worker


def client_msg_handle(
        app: App,
        return_addr: bytes,
        frames: Frames          # CLIENT LEVEL 1
) -> App:
    service_name = frames[0]
    body = frames[1:]
    if service_name not in app.service_addrs:
        logging.error("no available workers for %s", service_name)
        return app
    avail_workers = app.service_addrs[service_name]
    if not avail_workers:
        logging.error("no available workers for %s", service_name)
        return app
    selected: bytes
    min_task_count = None
    for w in avail_workers:
        task_count = app.worker_tasks.get(w, 0)
        if min_task_count is None:
            selected = w
            min_task_count = task_count
        if task_count < min_task_count:
            selected = w
            min_task_count = task_count
    logging.debug("sending work to %s for service %s.  %s Tasks pending",
                  selected, service_name, min_task_count)
    app = worker.send_to_worker(app, selected, return_addr, body)
    state.broadcast_worker_engaged(app, selected)
    return app


def worker_msg_handle(
        app: App,
        worker_addr: bytes,
        frames: Frames          # LEVEL 1 WORKER
) -> App:
    msg_type = frames[0]
    body = frames[1:]
    if msg_type == protoc.HEARTBEAT:
        return worker.process_heartbeat(app, worker_addr, body)
    if msg_type == protoc.REPLY:
        logging.debug("reply from worker addr: %s", worker_addr)
        return worker.process_reply(app, worker_addr, body)
    logging.error("unknown worker msg type: %s", msg_type)
    return app


def state_msg_handle(
        app: App,
        frames: Frames          # B_STATE FLAT
) -> App:
    return_addr = frames[0]
    msg_type = frames[1]
    rest = frames[2:]
    if msg_type == protoc.HEARTBEAT:
        return state.handle_heartbeat(app, rest)
    if msg_type == protoc.WORKER_ENGAGED:
        return state.handle_worker_engaged(app, rest)
    if msg_type == protoc.WORKER_UNENGAGED:
        return state.handle_worker_unengaged(app, rest)
    logging.error("no such broker state message type: %s", msg_type)
    return app

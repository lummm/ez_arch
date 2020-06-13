import logging

import app
from apptypes import Frames
import protoc
import state
import worker


def client_msg_handle(
        return_addr: bytes,
        frames: Frames          # CLIENT LEVEL 1
) -> None:
    request_id = frames[0]
    service_name = frames[1]
    body = frames[2:]
    if service_name not in app.state().service_addrs:
        logging.error("no available workers for %s", service_name)
        return
    avail_workers = app.state().service_addrs[service_name]
    if not avail_workers:
        logging.error("no available workers for %s", service_name)
        return
    selected: bytes
    min_task_count = None
    for w in avail_workers:
        task_count = app.state().worker_tasks.get(w, 0)
        if min_task_count is None:
            selected = w
            min_task_count = task_count
        if task_count < min_task_count:
            selected = w
            min_task_count = task_count
    logging.debug("sending work to %s for service %s.  %s Tasks pending",
                  selected, service_name, min_task_count)
    worker.send_to_worker(selected, return_addr, request_id, body)
    state.broadcast_worker_engaged(selected)
    return


def worker_msg_handle(
        worker_addr: bytes,
        frames: Frames          # LEVEL 1 WORKER
) -> None:
    msg_type = frames[0]
    body = frames[1:]
    if msg_type == protoc.HEARTBEAT:
        return worker.process_heartbeat(worker_addr, body)
    if msg_type == protoc.REPLY:
        logging.debug("reply from worker addr: %s", worker_addr)
        return worker.process_reply(worker_addr, body)
    logging.error("unknown worker msg type: %s", msg_type)
    return


def state_msg_handle(
        frames: Frames          # B_STATE FLAT
) -> None:
    return_addr = frames[0]
    msg_type = frames[1]
    rest = frames[2:]
    if msg_type == protoc.HEARTBEAT:
        return state.handle_heartbeat(rest)
    if msg_type == protoc.WORKER_ENGAGED:
        return state.handle_worker_engaged(rest)
    if msg_type == protoc.WORKER_UNENGAGED:
        return state.handle_worker_unengaged(rest)
    logging.error("no such broker state message type: %s", msg_type)
    return

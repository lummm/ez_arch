import logging

from app import App
from app import Frames
import state
import worker


def handle(
        app: App,
        return_addr: bytes,
        frames: Frames          # CLIENT LEVEL 1
)-> App:
    service_name = frames[0]
    body = frames[1:]
    # consult service lookup table for the address to deal this to
    if not service_name in app.service_addrs:
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

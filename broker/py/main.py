#!/usr/bin/env python3

import logging

import zmq

from app import App
from app import Frames
import handlers
import conn
from env import ENV
import protoc
import worker


def setup_logging() -> None:
    logging.basicConfig(
        level=ENV.LOG_LEVEL,
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


def handle_broker_broadcast(
        app: App,
        frames: Frames          # B_STATE FLAT + ROUTING
) -> App:
    return handlers.state_msg_handle(app, frames)


def handle_req_frames(
        app: App,
        frames: Frames          # INPUT FLAT + ROUTING
) -> App:
    in_pipe_addr = frames[0]
    return_addr = frames[1]
    assert b"" == frames[2]
    msg_type = frames[3]
    assert msg_type == protoc.CLIENT
    body = frames[4:]
    app = app._replace(in_pipe_addr=in_pipe_addr)
    app = handlers.client_msg_handle(app, return_addr, body)
    return app


def handle_worker_frames(
        app: App,
        frames: Frames          # INPUT FLAT + ROUTING
) -> App:
    worker_pipe_addr = frames[0]
    return_addr = frames[1]
    assert b"" == frames[2]
    msg_type = frames[3]
    assert msg_type == protoc.WORKER
    body = frames[4:]
    app = app._replace(worker_pipe_addr=worker_pipe_addr)
    app = handlers.worker_msg_handle(app, return_addr, body)
    return app


def loop_body(app: App) -> App:
    items = app.poller.poll(ENV.POLL_INTERVAL_MS)
    items_dict = dict(items)

    def work_on_socket(app, socket, handler):
        if socket in items_dict:
            frames = socket.recv_multipart()
            app = handler(app, frames)
        return app
    app = work_on_socket(app, app.broker_sub, handle_broker_broadcast)
    app = work_on_socket(app, app.worker_router, handle_worker_frames)
    app = work_on_socket(app, app.in_router, handle_req_frames)
    app = worker.purge_dead_workers(app)
    return app


def loop(app: App) -> None:
    while True:
        app = loop_body(app)
    return


def main():
    setup_logging()
    app = App()
    app = conn.connect(app)
    loop(app)
    return


if __name__ == "__main__":
    main()

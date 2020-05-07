#!/usr/bin/env python3

import logging

import zmq

from app import App
from app import Frames
import conn
from env import ENV


def setup_logging()-> None:
    logging.basicConfig(
        level=ENV.LOG_LEVEL,
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


def handle_broker_broadcast(
        app: App,
        frames: Frames
)-> App:
    logging.info("broker broadcast: %s", frames)
    return app


def handle_input_frames(
        app: App,
        frames: Frames
)-> App:
    logging.info("input: %s", frames)
    return app


def loop_body(app: App)-> App:
    items = app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        frames = socket.recv_multipart()
        logging.info("got frames: %s", frames)
        if socket == app.in_router:
            app = handle_input_frames(app, frames)
        if socket == app.broker_sub:
            app = handle_broker_broadcast(app, frames)
    return app


def loop(app: App)-> None:
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

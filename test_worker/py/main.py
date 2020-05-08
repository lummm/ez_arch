#!/usr/bin/env python3

import logging
import os

from app import App
from app import Frames
import msg


def setup_logging()-> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


def loop_body(app: App)-> App:
    items = app.poller.poll(app.hb_interval_ms)
    msg.send_heartbeat(app)
    for socket, _event in items:
        frames = socket.recv_multipart()
        logging.debug("recvd frames: %s", frames)
    return app


def run_loop(app)-> None:
    while True:
        app = loop_body(app)
    return


def main():
    app = App(
        in_con_s = "tcp://127.0.0.1:9005",
        out_con_s = "tcp://127.0.0.1:9000",
        service_name = b"TEST_SERVICE",
    )
    app = msg.connect(app)
    run_loop(app)
    return


if __name__ == "__main__":
    main()

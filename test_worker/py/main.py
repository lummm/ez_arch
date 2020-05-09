#!/usr/bin/env python3

import logging
from threading import Thread
import time
from typing import NamedTuple
import os

from app import App
from app import Frames
import msg


class _ENV(NamedTuple):
    IN_PORT: int = int(os.environ["IN_PORT"])
    RES_PORT: int = int(os.environ["RES_PORT"])
ENV = _ENV()


def setup_logging()-> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


def loop_body(app: App)-> App:
    items = app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        frames = socket.recv_multipart()
        logging.info("recvd frames: %s", frames)
        pipe_addr = frames[0]
        router_addr = frames[1]
        assert b"" == frames[2]
        my_addr = frames[3]
        assert b"" == frames[4]
        client_return_addr = frames[5:7]
        assert b"" == frames[7]
        req_body = frames[8:]
        # broker_addr = frames[0]
        # return_addr = frames[1]
        # assert b"" == frames[2]
        # req_body = frames[3:]
        reply = [b"ECHO"] + req_body
        msg.send_response(app, client_return_addr, reply)
    return app


def run_work_loop()-> None:
    app = App(
        in_con_s = "tcp://127.0.0.1:{}".format(ENV.IN_PORT),
        out_con_s = "tcp://127.0.0.1:{}".format(ENV.RES_PORT),
        service_name = b"TEST_SERVICE",
    )
    app = msg.connect(app)
    while True:
        app = loop_body(app)
    return


def run_hb_loop()-> None:
    app = App(
        out_con_s = "tcp://127.0.0.1:{}".format(ENV.RES_PORT),
        service_name = b"TEST_SERVICE",
    )
    app = msg.connect(app)
    while True:
        msg.send_heartbeat(app)
        time.sleep(app.hb_interval_s)
    return


def main():
    setup_logging()
    t = Thread(target = run_hb_loop, args = ())
    t.start()
    run_work_loop()
    return


if __name__ == "__main__":
    main()

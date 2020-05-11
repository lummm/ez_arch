#!/usr/bin/env python3

import asyncio
import logging
# from threading import Thread
import time
from typing import NamedTuple
import os
import sys

from app import App
from app import Frames
import msg


class _ENV(NamedTuple):
    PORT: int = int(os.environ["PORT"])
ENV = _ENV()


def setup_logging()-> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


async def mock_handler(
        app: App,
        req_body: Frames
)-> Frames:
    await asyncio.sleep(2)
    return [b"ECHO"] + req_body


# async def run_hb_loop(app: App)-> None:
#     app = await msg.connect(app)
#     while True:
#         msg.send_heartbeat(app)
#         await asyncio.sleep(app.hb_interval_s)
#     return


async def loop_body(app: App)-> App:
    msg.send_heartbeat(app)
    items = await app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        frames = await socket.recv_multipart()
        logging.info("recvd frames: %s", frames)
        assert b"" == frames[0]
        client_return_addr = frames[1]
        req_body = frames[2:]
        reply = await mock_handler(app, req_body)
        msg.send_response(app, client_return_addr, reply)
    return app


async def run_loop(app: App)-> None:
    app = await msg.connect(app)
    while True:
        app = await loop_body(app)
    return


def main():
    setup_logging()
    app = App(
        con_s = "tcp://127.0.0.1:{}".format(ENV.PORT),
        service_name = b"TEST_SERVICE",
    )
    # t = Thread(target = run_hb_loop, args = (app,))
    # t.start()
    asyncio.run(run_loop(app))
    # try:
    #     run_work_loop(app)
    # except Exception as e:
    #     logging.exception("worker died: %s", e)
    #     os._exit(1)
    return


if __name__ == "__main__":
    main()

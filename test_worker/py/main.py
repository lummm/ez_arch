#!/usr/bin/env python3

import asyncio
import logging
import os
from typing import NamedTuple

import ez_arch_worker.api as ez_worker

# from app import App
# from app import Frames
# import msg

Frames = ez_worker.Frames


class _ENV(NamedTuple):
    PORT: int = int(os.environ["PORT"])
ENV = _ENV()

class State(NamedTuple):
    res_count: int = 0


def setup_logging()-> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


async def mock_handler(
        state: State,
        req_body: Frames
)-> Frames:
    new_state = state._replace(res_count = state.res_count + 1)
    res = [b"ECHO"] + req_body
    logging.info("new state: %s", new_state)
    await asyncio.sleep(2)
    return (new_state, res)


# async def loop_body(app: App)-> App:
#     msg.send_heartbeat(app)
#     items = await app.poller.poll(app.poll_interval_ms)
#     for socket, _event in items:
#         frames = await socket.recv_multipart()
#         logging.info("recvd frames: %s", frames)
#         assert b"" == frames[0]
#         client_return_addr = frames[1]
#         req_body = frames[2:]
#         reply = await mock_handler(app, req_body)
#         msg.send_response(app, client_return_addr, reply)
#     return app


# async def run_loop(app: App)-> None:
#     app = await msg.connect(app)
#     while True:
#         app = await loop_body(app)
#     return

async def run_loop():
    await ez_worker.run_worker(
        service_name = b"TEST_SERVICE",
        handler = mock_handler,
        initial_state = State(),
        listen_host = "localhost",
        listen_port = ENV.PORT,
    )


def main():
    setup_logging()
    asyncio.run(run_loop())
    return


if __name__ == "__main__":
    main()

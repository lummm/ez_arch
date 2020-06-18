#!/usr/bin/env python3

import asyncio
import logging
import os
from typing import NamedTuple

import ez_arch_worker.api as ez_worker
from ez_arch_worker.api import Frames


class _ENV(NamedTuple):
    PORT: int = int(os.environ["PORT"])


ENV = _ENV()


class State(NamedTuple):
    res_count: int = 0


def setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


state = State()


def update_state(**kwargs) -> None:
    global state
    state = state._replace(**kwargs)
    return


async def mock_handler(req_body: Frames) -> Frames:
    update_state(res_count=state.res_count + 1)
    handle_map = {
        b"/echo": lambda x: x,
        b"/req_count": lambda _: b"%d" % state.res_count,
    }
    res: Frames
    try:
        url, data = req_body
        if url in handle_map:
            res = [b"OK", handle_map[url](data)]
        else:
            res = [b"ERR",
                   "{} not found".format(url.decode("utf-8")).encode("utf-8")]
    except Exception as e:
        logging.exception("worker died: %s", e)
        res = [b"ERR", b"worker exception"]
    await asyncio.sleep(1)
    return res


async def run_loop():
    await ez_worker.run_worker(
        service_name=b"TEST_SERVICE",
        handler=mock_handler,
        listen_host="localhost",
        port=ENV.PORT,
    )


def main():
    setup_logging()
    asyncio.run(run_loop())
    return


if __name__ == "__main__":
    main()

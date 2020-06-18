#!/usr/bin/env python3

import asyncio
import json
import logging
import os

from ez_arch_worker.api import EzClient
from ez_arch_worker.api import Frames
from ez_arch_worker.api import new_client


EZ_INPUT_PORT = int(os.environ["IN_PIPE_PORT_A"])


async def req_res_cycle(ez_client: EzClient, req: Frames) -> None:
    logging.info("SENDING REQUEST %s", req)
    res = await ez_client(req)
    logging.info("got res: %s\n", res)
    return


async def do_req() -> None:
    ez_client = await new_client("localhost", EZ_INPUT_PORT)
    data = json.dumps({"test": "data"}).encode("utf-8")
    await req_res_cycle(ez_client, [b"TEST_SERVICE", b"/echo", data])
    await req_res_cycle(ez_client, [b"TEST_SERVICE", b"/req_count", b""])
    await req_res_cycle(ez_client, [b"TEST_SERVICE", b"/nope", b""])
    await req_res_cycle(ez_client, [b"TEST_SERVICE", b"bad"])
    return


def main() -> None:
    logging.basicConfig(level="INFO")
    asyncio.run(do_req())
    return


if __name__ == '__main__':
    main()

#!/bin/bash

docker run -it --rm --net=host \
       -e WORKER_PORT=9004 \
       -e EZ_PORT=9000 \
       tengelisconsulting/ez

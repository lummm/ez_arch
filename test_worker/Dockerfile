FROM tengelisconsulting/mypy
RUN python3 -m pip install \
        ez_arch_worker==0.2.1
WORKDIR /app
COPY ./py ./py
RUN mypy --ignore-missing-imports ./py

FROM tengelisconsulting/pyzmq:alpine
RUN python3 -m pip install \
        ez_arch_worker==0.2.1
WORKDIR /app
COPY ./py ./py
ENTRYPOINT [ "tini", "/app/py/main.py" ]

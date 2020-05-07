FROM tengelisconsulting/mypy
WORKDIR /app
COPY ./py ./py
RUN mypy --ignore-missing-imports ./py

FROM tengelisconsulting/pyzmq:alpine
WORKDIR /app
COPY ./py ./py
ENTRYPOINT [ "tini", "/app/py/main.py" ]

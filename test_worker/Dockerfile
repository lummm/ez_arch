FROM alpine:3.10
WORKDIR /app
RUN apk update \
        && apk add --no-cache \
        python3 \
        py3-zmq \
        tini
COPY ./requirements.txt ./requirements/txt
RUN python3 -m pip install -r ./requirements/txt
COPY ./py ./py
ENTRYPOINT [ "tini", "/app/py/main.py" ]

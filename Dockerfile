FROM alpine:3.12.1

RUN apk add --update \
        elixir \
        tini \
        && mix local.hex --force \
        && mix local.rebar --force

WORKDIR /app
# COPY ./config ./config
COPY ./mix.exs ./mix.exs
COPY ./mix.lock ./mix.lock
RUN mix deps.get
RUN mix deps.compile

COPY ./entrypoint.sh ./entrypoint.sh
COPY ./test ./test
COPY ./lib ./lib

RUN MIX_ENV=prod mix release

ENTRYPOINT ["tini", "/app/entrypoint.sh"]

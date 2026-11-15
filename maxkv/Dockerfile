FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ make && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN make build/maxkv

FROM debian:bookworm-slim
RUN useradd -r -u 10001 maxkv && mkdir /data && chown maxkv /data
COPY --from=build /src/build/maxkv /usr/local/bin/maxkv
USER maxkv
WORKDIR /data
EXPOSE 6380
ENTRYPOINT ["maxkv", "--host", "0.0.0.0", "--aof", "/data/maxkv.aof"]

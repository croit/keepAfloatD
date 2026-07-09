FROM debian:trixie-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        iproute2 \
        iputils-arping \
        jq \
        procps \
        tcpdump \
    && rm -rf /var/lib/apt/lists/*

CMD ["sleep", "infinity"]

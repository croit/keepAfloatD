FROM keepafloatd:runtime-local

USER root

# the lab's health probes use pgrep; trixie-slim doesn't ship procps
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

COPY haproxy /usr/local/bin/haproxy
COPY entrypoint.sh /usr/local/bin/haproxy-e2e-entrypoint
COPY configs/ /opt/haproxy-e2e/configs/

RUN chmod 0755 /usr/local/bin/haproxy /usr/local/bin/haproxy-e2e-entrypoint

ENTRYPOINT ["/usr/local/bin/haproxy-e2e-entrypoint"]

FROM keepafloatd:runtime-local

USER root

# The fixture health checks and controls HAProxy with pgrep and pkill.
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

COPY keepafloatd /usr/local/bin/keepafloatd
COPY haproxy /usr/local/bin/haproxy
COPY entrypoint.sh /usr/local/bin/haproxy-e2e-entrypoint
COPY configs/ /opt/haproxy-e2e/configs/

RUN chmod 0755 /usr/local/bin/keepafloatd /usr/local/bin/haproxy /usr/local/bin/haproxy-e2e-entrypoint

ENTRYPOINT ["/usr/local/bin/haproxy-e2e-entrypoint"]

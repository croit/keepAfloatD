FROM keepafloatd:runtime-local

USER root

COPY haproxy /usr/local/bin/haproxy
COPY entrypoint.sh /usr/local/bin/haproxy-e2e-entrypoint
COPY configs/ /opt/haproxy-e2e/configs/

RUN chmod 0755 /usr/local/bin/haproxy /usr/local/bin/haproxy-e2e-entrypoint

ENTRYPOINT ["/usr/local/bin/haproxy-e2e-entrypoint"]

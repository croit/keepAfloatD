ARG DOCKER_HUB_MIRROR=docker.io
FROM ${DOCKER_HUB_MIRROR}/library/debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132

RUN mkdir -p /opt/keepafloatd-tests/configs /opt/keepafloatd-tests/configs22 /opt/keepafloatd-tests/configs26 /opt/keepafloatd-tests/configs26-equivalent

COPY configs/ /opt/keepafloatd-tests/configs/
RUN cp -a /opt/keepafloatd-tests/configs /opt/keepafloatd-tests/configs-secret-file \
    && sed -n 's/^cluster_secret: "\(.*\)"/\1/p' /opt/keepafloatd-tests/configs/a.yaml \
        >/opt/keepafloatd-tests/configs-secret-file/cluster.secret \
    && chmod 0600 /opt/keepafloatd-tests/configs-secret-file/cluster.secret \
    && sed -i 's/^cluster_secret:.*/cluster_secret_file: "cluster.secret"/' \
        /opt/keepafloatd-tests/configs-secret-file/b.yaml \
    && sed -i 's|^cluster_secret:.*|cluster_secret_file: "/opt/keepafloatd-tests/configs-secret-file/cluster.secret"|' \
        /opt/keepafloatd-tests/configs-secret-file/c.yaml
# Leave room for OpenRaft's leader lease and greater-log backoff inside the
# handoff scenario's ten-second bound without shortening its failure windows.
RUN mkdir /opt/keepafloatd-tests/configs-stop \
    && for node in a b c; do \
        sed -e 's/stale_secs: 6/stale_secs: 60/' \
            -e 's/election_timeout_min_ms: 1500/election_timeout_min_ms: 400/' \
            -e 's/election_timeout_max_ms: 2500/election_timeout_max_ms: 800/' \
            -e 's/heartbeat_interval_ms: 500/heartbeat_interval_ms: 250/' \
            "/opt/keepafloatd-tests/configs/${node}.yaml" \
            >"/opt/keepafloatd-tests/configs-stop/${node}.yaml"; \
        printf '\nfailover_delay_secs: 30\n' \
            >>"/opt/keepafloatd-tests/configs-stop/${node}.yaml"; \
    done
RUN mkdir /opt/keepafloatd-tests/configs6 \
    && for node in a b c; do \
        sed 's/10\.50\.0\.100/2001:db8:50::100/; s/10\.50\.0\.101/2001:db8:50::101/; s/10\.50\.0\.102/2001:db8:50::102/' \
            "/opt/keepafloatd-tests/configs/${node}.yaml" \
            >"/opt/keepafloatd-tests/configs6/${node}.yaml"; \
    done
COPY configs22/ /opt/keepafloatd-tests/configs22/
COPY configs/a.yaml /opt/keepafloatd-tests/configs26/a.yaml
COPY configs/b.yaml /opt/keepafloatd-tests/configs26/b.yaml
COPY configs26/c.yaml /opt/keepafloatd-tests/configs26/c.yaml
RUN cp /opt/keepafloatd-tests/configs/a.yaml /opt/keepafloatd-tests/configs26-equivalent/a.yaml \
    && cp /opt/keepafloatd-tests/configs/b.yaml /opt/keepafloatd-tests/configs26-equivalent/b.yaml \
    && cp /opt/keepafloatd-tests/configs/c.yaml /opt/keepafloatd-tests/configs26-equivalent/c.yaml \
    && sed -i 's/interval_ms: 1000/interval_ms: 3000/; s/stale_secs: 6/stale_secs: 10/' /opt/keepafloatd-tests/configs26-equivalent/a.yaml /opt/keepafloatd-tests/configs26-equivalent/b.yaml \
    && sed -i 's/interval_ms: 1000/interval_ms: 3000/; s/stale_secs: 6/stale_secs: 11/' /opt/keepafloatd-tests/configs26-equivalent/c.yaml \
    && printf '\nfailback: false\nfailback_delay_secs: 1\n' >> /opt/keepafloatd-tests/configs26-equivalent/a.yaml \
    && printf '\nfailback: false\nfailback_delay_secs: 1\n' >> /opt/keepafloatd-tests/configs26-equivalent/b.yaml \
    && printf '\nfailback: false\nfailback_delay_secs: 99\n' >> /opt/keepafloatd-tests/configs26-equivalent/c.yaml
COPY scripts/health.sh /opt/keepafloatd-tests/health.sh
COPY scripts/address-commands-finished.sh /opt/keepafloatd-tests/address-commands-finished.sh

FROM debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a

RUN mkdir -p /opt/keepafloatd-tests/configs /opt/keepafloatd-tests/configs22 /opt/keepafloatd-tests/configs26 /opt/keepafloatd-tests/configs26-equivalent

COPY configs/ /opt/keepafloatd-tests/configs/
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

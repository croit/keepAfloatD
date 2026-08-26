FROM debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132

RUN mkdir -p /opt/keepafloatd-tests/configs

COPY configs/ /opt/keepafloatd-tests/configs/
COPY scripts/health.sh /opt/keepafloatd-tests/health.sh
